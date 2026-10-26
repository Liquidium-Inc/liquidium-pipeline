//! The deposit funding transfer: measuring the MEXC balance, sizing the
//! transfer with its fee reserve, the saved send, and the MEXC approval bumps.
use super::*;
use crate::finalizers::bridge_planner::BridgePlanner;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// Drives the funding transfer; `intent` starts a new one. On landing it
    /// records the transfer and returns true.
    pub(super) async fn drive_funding(
        &self,
        state: &mut CexState,
        intent: Option<OutboundIntent>,
    ) -> Result<bool, String> {
        let landed = step_outbound(
            &mut state.deposit.deposit_funding,
            self.outbound.as_ref(),
            intent.as_ref(),
            now_ts(),
        )
        .await?;
        state.send_saved = intent.is_some();
        let Some(tx_id) = landed else {
            return Ok(false);
        };
        state.deposit.deposit_txid = Some(tx_id);
        state.deposit.deposit_sent_at_ts = Some(now_ts());
        Ok(true)
    }

    /// A deposit straight to the MEXC address: one funding transfer, then the
    /// balance check confirms that the funds arrived.
    pub(super) async fn deposit_direct(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
    ) -> Result<(), String> {
        if state.deposit.deposit_txid.is_none() {
            let intent = if state.deposit.deposit_funding.is_some() {
                None
            } else {
                Some(
                    self.prepare_direct_funding(state, planned_asset, planned_network)
                        .await?,
                )
            };
            if !self.drive_funding(state, intent).await? {
                return Ok(());
            }

            if matches!(state.deposit.deposit_asset, ChainToken::Icp { .. })
                && !Self::is_native_icp_token(&state.deposit.deposit_asset)
            {
                let approved = self
                    .maybe_bump_mexc_approval(&state.liq_id, &state.deposit.deposit_asset)
                    .await;

                if approved > 0 {
                    state.deposit.approval_bump_count = Some(approved);
                }
            }

            state.step = CexStep::DepositPending;
            return Ok(());
        }

        self.check_deposit(state).await
    }

    /// The exchange balance before the deposit. A failed read counts as zero.
    async fn record_balance_before_deposit(&self, state: &mut CexState, planned_asset: &str) {
        let baseline = self.backend.get_balance(planned_asset).await.unwrap_or_else(|error| {
            debug!(
                "[mexc] liq_id={} could not get baseline balance before deposit: {} (using 0.0)",
                state.liq_id, error
            );
            0.0
        });
        debug!(
            "[mexc] liq_id={} baseline balance before deposit: {}",
            state.liq_id, baseline
        );
        state.deposit.deposit_balance_before = Some(baseline);
    }

    /// Measures the exchange balance and sizes the direct funding transfer.
    /// It runs once per transfer; retries resend the saved transfer.
    pub(super) async fn prepare_direct_funding(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
    ) -> Result<OutboundIntent, String> {
        self.record_balance_before_deposit(state, planned_asset).await;
        let addr = self
            .backend
            .get_deposit_address(planned_asset, planned_network)
            .await?;

        if Self::is_native_icp_token(&state.deposit.deposit_asset)
            && addr.tag.as_deref().is_some_and(|tag| !tag.trim().is_empty())
        {
            let tag = addr.tag.clone().unwrap_or_default();
            return Err(format!(
                "native ICP MEXC deposit returned unsupported memo/tag for liq_id {} asset={} network={} address={} tag={}",
                state.liq_id, planned_asset, planned_network, addr.address, tag
            ));
        }

        state.deposit.bridge.deposit_bridge_destination_snapshot = Some(addr.address.clone());

        let (transfer_value, transfer_amount) =
            Self::compute_fee_adjusted_deposit_transfer(&state.deposit.deposit_asset, &state.size_in, false)?;
        let destination = Self::direct_deposit_destination_account(
            &state.deposit.deposit_asset,
            planned_network,
            &addr.address,
        )?;
        state.trade.trade_next_amount_in = Some(transfer_amount.to_f64());
        Ok(OutboundIntent {
            token: state.deposit.deposit_asset.clone(),
            to: destination,
            amount_native: transfer_value,
        })
    }

    /// Measures the exchange balance and sizes the bridged funding transfer,
    /// saving the bridge amounts it implies. It runs once per transfer.
    pub(super) async fn prepare_bridged_funding(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        planned_asset: &str,
    ) -> Result<OutboundIntent, String> {
        self.record_balance_before_deposit(state, planned_asset).await;
        let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
        let bridge_destination = <Self as BridgePlanner>::bridge_source_transfer_destination(
            route.source_chain,
            &bridge_source_address,
        )?;

        let BridgeDepositSizing {
            transfer_value,
            bridge_amount: transfer_amount,
            source_fee_reserve,
            fee_budget,
        } = Self::size_bridged_deposit(bridge, route, &state.deposit.deposit_asset, &state.size_in).await?;

        if let Some(source_fee_reserve) = source_fee_reserve {
            let source_transfer_amount =
                ChainTokenAmount::from_raw(state.deposit.deposit_asset.clone(), transfer_value.clone());
            info!(
                "[mexc] liq_id={} bridged deposit reserving reverse bridge source fee budget: reserve={} source_transfer={} bridge_amount={} route={}@{}->{}",
                state.liq_id,
                source_fee_reserve.formatted(),
                source_transfer_amount.formatted(),
                transfer_amount.formatted(),
                route.source_asset,
                route.source_chain,
                route.target_asset,
            );
        }

        Self::ensure_minimum_bridge_amount(bridge, route, &transfer_amount).await?;
        let bridge_submit_amount = Self::spend_f64(&transfer_amount)?;
        let bridge_expected_amount = Self::compute_expected_bridge_deposit_amount(
            route,
            bridge_submit_amount,
            fee_budget.destination_fee_budget,
        )?;
        state.deposit.bridge.deposit_bridge_submit_amount = Some(bridge_submit_amount);
        state.deposit.bridge.deposit_bridge_expected_amount = Some(bridge_expected_amount);
        state.deposit.bridge.deposit_bridge_provider_fee_budget_native_units =
            fee_budget.provider_fee_budget_native_units.clone();
        Ok(OutboundIntent {
            token: state.deposit.deposit_asset.clone(),
            to: bridge_destination,
            amount_native: transfer_value,
        })
    }

    /// Ledger fees held back from a deposit transfer: the transfer itself,
    /// plus the approval bumps that only direct non-native ICP deposits send.
    pub(super) fn deposit_fee_count(deposit_asset: &ChainToken, bridged: bool) -> u8 {
        let bumps_approvals =
            !bridged && matches!(deposit_asset, ChainToken::Icp { .. }) && !Self::is_native_icp_token(deposit_asset);
        if bumps_approvals { 1 + APPROVE_BUMP_MAX_COUNT } else { 1 }
    }

    pub(in crate::finalizers::mexc) fn compute_fee_adjusted_deposit_transfer(
        deposit_asset: &ChainToken,
        size_in: &ChainTokenAmount,
        bridged: bool,
    ) -> Result<(Nat, ChainTokenAmount), String> {
        let fee = deposit_asset.fee() * Self::deposit_fee_count(deposit_asset, bridged);
        let transfer_amount = Self::amount_less_reserve(deposit_asset, &size_in.value, fee, "ledger fee")?;
        Ok((transfer_amount.value.clone(), transfer_amount))
    }

    // MEXC approvals are flaky; we send a burst of tiny approvals to increase the chance
    // the exchange notices the approval without blocking the main liquidation flow.
    pub(super) async fn maybe_bump_mexc_approval(&self, liq_id: &str, asset: &ChainToken) -> u32 {
        let already_bumped = {
            let Ok(approve_bumps) = self.approve_bumps.lock() else {
                warn!("[mexc] liq_id={} approve_bumps mutex poisoned, skipping bump", liq_id);
                return 0;
            };
            *approve_bumps.get(liq_id).unwrap_or(&0)
        };

        if already_bumped >= APPROVE_BUMP_MAX_COUNT {
            debug!(
                "[mexc] liq_id={} deposit transfer: approve bump already done (count={})",
                liq_id, already_bumped
            );
            return 0;
        }

        let approve_amount = Nat::from(1u8);
        let spender = ChainAccount::Icp(Account {
            owner: self.liquidator_principal,
            subaccount: None,
        });
        info!(
            "[mexc] liq_id={} deposit transfer: approve bump {}+{} amount={} asset={}",
            liq_id, APPROVE_BUMP_BATCH_SIZE, APPROVE_BUMP_BATCH_SIZE, approve_amount, asset
        );

        let mut ok = true;
        let mut approved_count: u32 = 0;
        let short_pause = Duration::from_millis(APPROVE_BUMP_DELAY_MS);
        for i in 0..APPROVE_BUMP_BATCH_SIZE {
            if let Err(err) = self
                .transfer_service
                .approve(asset, &spender, approve_amount.clone())
                .await
            {
                ok = false;
                info!("[mexc] liq_id={} approve bump failed: {}", liq_id, err);
                break;
            }
            approved_count += 1;
            if i < APPROVE_BUMP_BATCH_SIZE - 1 {
                sleep(short_pause).await;
            }
        }
        if ok {
            sleep(Duration::from_secs(APPROVE_BUMP_BATCH_DELAY_SECS)).await;
            for i in 0..APPROVE_BUMP_BATCH_SIZE {
                if let Err(err) = self
                    .transfer_service
                    .approve(asset, &spender, approve_amount.clone())
                    .await
                {
                    ok = false;
                    info!("[mexc] liq_id={} approve bump failed: {}", liq_id, err);
                    break;
                }
                approved_count += 1;
                if i < APPROVE_BUMP_BATCH_SIZE - 1 {
                    sleep(short_pause).await;
                }
            }
        }

        if ok && let Ok(mut approve_bumps) = self.approve_bumps.lock() {
            approve_bumps.insert(liq_id.to_string(), APPROVE_BUMP_MAX_COUNT);
        }
        approved_count
    }
}
