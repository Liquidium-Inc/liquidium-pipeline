//! The deposit funding transfer: measuring the exchange balance, sizing the
//! transfer with its fee reserve, the saved send, and the approval bumps.
use super::*;
use liquidium_pipeline_connectors::backend::cex_backend::FundingRouteError;

use crate::finalizers::bridge_legs::BridgeLegError;
use crate::finalizers::bridge_legs::fee_adjusted_transfer;

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// Drives the funding transfer; `intent` starts a new one. On landing it
    /// records the transfer and returns true.
    pub(super) async fn drive_funding(
        &self,
        state: &mut CexState,
        intent: Option<OutboundIntent>,
    ) -> Result<bool, CexBackendError> {
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

    /// A deposit straight to the exchange address: one funding transfer, then the
    /// balance check confirms that the funds arrived.
    pub(super) async fn deposit_direct(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
    ) -> Result<(), CexBackendError> {
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

            if self.bumps_approvals(&state.deposit.deposit_asset) {
                let approved = self
                    .maybe_bump_approval(&state.liq_id, &state.deposit.deposit_asset)
                    .await;

                if approved > 0 {
                    state.deposit.approval_bump_count = Some(approved);
                }
            }

            state.step = CexStep::DepositPending;
            return Ok(());
        }

        Ok(self.check_deposit(state).await?)
    }

    /// The exchange balance before the deposit. A failed read counts as zero.
    pub(super) async fn record_balance_before_deposit(&self, state: &mut CexState, planned_asset: &str) {
        let baseline = self.backend.get_balance(planned_asset).await.unwrap_or_else(|error| {
            debug!(
                "[cex] liq_id={} could not get baseline balance before deposit: {} (using 0.0)",
                state.liq_id, error
            );
            0.0
        });
        debug!(
            "[cex] liq_id={} baseline balance before deposit: {}",
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
    ) -> Result<OutboundIntent, CexBackendError> {
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
                "native ICP exchange deposit returned unsupported memo/tag for liq_id {} asset={} network={} address={} tag={}",
                state.liq_id, planned_asset, planned_network, addr.address, tag
            )
            .into());
        }

        state.deposit.bridge.deposit_bridge_destination_snapshot = Some(addr.address.clone());

        let (transfer_value, transfer_amount) = fee_adjusted_transfer(
            &state.deposit.deposit_asset,
            &state.size_in,
            self.deposit_fee_count(&state.deposit.deposit_asset),
        )?;
        let destination = Self::direct_deposit_destination_account(
            &state.deposit.deposit_asset,
            planned_network,
            &addr.address,
        )?;
        self.ensure_deposit_credited(state, transfer_amount.to_f64()).await?;
        state.trade.trade_next_amount_in = Some(transfer_amount.to_f64());
        Ok(OutboundIntent {
            token: state.deposit.deposit_asset.clone(),
            to: destination,
            amount_native: transfer_value,
        })
    }

    /// Asks the exchange again, just before the first send, whether it credits
    /// the deposit at its fresh size. A deposit under its minimum is never credited.
    pub(in crate::finalizers::cex) async fn ensure_deposit_credited(
        &self,
        state: &CexState,
        arrival: f64,
    ) -> Result<(), CexBackendError> {
        let route = self.funding_route(state, arrival)?;
        self.backend
            .check_funding_route(&route)
            .await
            .map_err(|error| credit_refusal(error).into())
    }

    /// Whether a direct deposit of this asset sends approval bumps: only a
    /// non-native ICP deposit to a venue that needs them. A bridged deposit sends none.
    fn bumps_approvals(&self, deposit_asset: &ChainToken) -> bool {
        self.profile.approval_bumps()
            && matches!(deposit_asset, ChainToken::Icp { .. })
            && !Self::is_native_icp_token(deposit_asset)
    }

    /// Ledger fees held back from a direct deposit transfer: the transfer
    /// itself, plus the approval bumps when the deposit sends them.
    pub(in crate::finalizers::cex) fn deposit_fee_count(&self, deposit_asset: &ChainToken) -> u8 {
        if self.bumps_approvals(deposit_asset) {
            1 + APPROVE_BUMP_MAX_COUNT
        } else {
            1
        }
    }

    // Some venues notice approvals unreliably; a burst of tiny approvals raises the
    // chance the exchange notices one without blocking the liquidation flow.
    pub(super) async fn maybe_bump_approval(&self, liq_id: &str, asset: &ChainToken) -> u32 {
        let already_bumped = {
            let Ok(approve_bumps) = self.approve_bumps.lock() else {
                warn!("[cex] liq_id={} approve_bumps mutex poisoned, skipping bump", liq_id);
                return 0;
            };
            *approve_bumps.get(liq_id).unwrap_or(&0)
        };

        if already_bumped >= APPROVE_BUMP_MAX_COUNT {
            debug!(
                "[cex] liq_id={} deposit transfer: approve bump already done (count={})",
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
            "[cex] liq_id={} deposit transfer: approve bump {}+{} amount={} asset={}",
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
                info!("[cex] liq_id={} approve bump failed: {}", liq_id, err);
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
                    info!("[cex] liq_id={} approve bump failed: {}", liq_id, err);
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

/// Why the exchange will not credit a deposit. Under the minimum, the leg ends
/// with its funds unsent; any other refusal retries.
pub(in crate::finalizers::cex) fn credit_refusal(error: FundingRouteError) -> BridgeLegError {
    match error {
        FundingRouteError::BelowMinimum(reason) => BridgeLegError::BelowMinimum(reason),
        FundingRouteError::Unavailable(reason) | FundingRouteError::Unsupported(reason) => {
            BridgeLegError::Other(reason)
        }
    }
}
