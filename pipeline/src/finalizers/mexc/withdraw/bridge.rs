//! The return bridge after a MEXC withdrawal: the deposit send to the
//! provider, the one-deposit-per-address wait, and the notice.
use super::*;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// The bridge destination for the final liquidator, from the snapshot
    /// taken when the bridged withdrawal started.
    pub(super) fn withdraw_bridge_destination(
        &self,
        state: &CexState,
        route: &BridgeRouteSpec,
    ) -> Result<BridgeDestination, String> {
        let snapshot = state
            .withdraw
            .bridge
            .withdraw_bridge_destination_snapshot
            .as_deref()
            .ok_or_else(|| "missing final destination snapshot for bridged withdraw".to_string())?;
        self.bridge_destination_for_final_liquidator(route.destination_kind, snapshot)
    }

    /// The ckSOL deposit address is shared, and the minter takes one deposit
    /// at a time. A second send would merge with the open one.
    pub(super) async fn waits_for_other_deposit(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        liq_id: &str,
    ) -> Result<bool, String> {
        if route.route_kind != BridgeRouteKind::SolanaToIcp {
            return Ok(false);
        }
        let Some(other) = Self::other_open_deposit_send(bridge, route, liq_id).await? else {
            return Ok(false);
        };
        info!(
            "[mexc] liq_id={} bridge deposit waits for liq_id={} on the same deposit address",
            liq_id, other
        );
        Ok(true)
    }

    /// Another row's deposit send on this route that has not minted yet.
    async fn other_open_deposit_send(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        liq_id: &str,
    ) -> Result<Option<String>, String> {
        let claims = bridge.claims.claims_since(now_ts()).await?;
        Ok(claims
            .open_deposit_sends
            .into_iter()
            .find(|send| {
                send.liq_id != liq_id
                    && send.asset.eq_ignore_ascii_case(route.source_asset)
                    && send.network.eq_ignore_ascii_case(route.source_chain)
            })
            .map(|send| send.liq_id))
    }

    /// The return bridge route from the planned MEXC withdrawal to the final asset.
    pub(in crate::finalizers::mexc) fn withdraw_route(state: &CexState) -> Option<&'static BridgeRouteSpec> {
        resolve_route(
            &<Self as BridgePlanner>::planned_withdraw_asset(state),
            &<Self as BridgePlanner>::planned_withdraw_network(state),
            &state.withdraw.withdraw_asset.symbol(),
        )
    }

    /// Runs one step of a return bridge whose provider watches a deposit
    /// address: the send, then the notice. `request` starts a new send.
    pub(super) async fn advance_withdraw_deposit_send(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        destination: &BridgeDestination,
        request: Option<&BridgeRequest>,
    ) -> Result<(), CexBackendError> {
        state.step = CexStep::WithdrawPending;
        let now = now_ts();
        let liq_id = state.liq_id.clone();
        let funds = &mut state.withdraw.bridge;
        let Some(receipt) = funds.withdraw_bridge_funding_txid.clone() else {
            if let Some(request) = request {
                // The caller holds the submit lock, so this check is final.
                if Self::waits_for_other_deposit(bridge, route, &liq_id).await? {
                    return Ok(());
                }
                let bridge_send = match bridge.backend.prepare_bridge_send(request).await {
                    Ok(bridge_send) => bridge_send,
                    // A busy bridge wallet is a wait. Nothing was signed.
                    Err(error) if error.starts_with(BRIDGE_SUBMISSION_DEFERRED_PREFIX) => {
                        info!("[mexc] liq_id={} bridged withdraw deferred: {}", liq_id, error);
                        return Ok(());
                    }
                    Err(error) => return Err(bridge_refusal(error)),
                };
                funds.withdraw_bridge_funding = Some(OutboundRecord::prepared(bridge_send.send));
                funds.withdraw_bridge_send_handle = Some(bridge_send.handle);
                state.send_saved = true;
                return Ok(());
            }
            let landed = step_outbound(&mut funds.withdraw_bridge_funding, bridge.outbound.as_ref(), None, now).await?;
            if let Some(receipt) = landed {
                info!(
                    "[mexc] liq_id={} bridge deposit send landed: receipt={} route={}@{}->{}",
                    liq_id, receipt, route.source_asset, route.source_chain, route.target_asset
                );
                funds.withdraw_bridge_funding_txid = Some(receipt);
                funds.withdraw_bridge_funded_at_ts = Some(now);
            }
            // The provider reads the deposit at its own commitment, so the
            // tracking runs on the next cycle.
            return Ok(());
        };
        // The minter reads only finalized transfers, and each notice costs
        // cycles. A confirmed Solana transfer is final after about 13 seconds.
        let landed_for = now.saturating_sub(funds.withdraw_bridge_funded_at_ts.unwrap_or(0));
        if route.route_kind == BridgeRouteKind::SolanaToIcp && landed_for < SOL_FINALITY_SECS {
            return Ok(());
        }
        let handle = funds.withdraw_bridge_send_handle.clone().unwrap_or_default();
        match bridge
            .backend
            .track_deposit(route.source_asset, route.source_chain, destination, &handle, &receipt)
            .await?
        {
            DepositNotice::Accepted(submission) => {
                info!(
                    "[mexc] liq_id={} bridge deposit {} accepted: bridge_id={}",
                    liq_id, receipt, submission.bridge_id
                );
                funds.withdraw_bridge_id = Some(submission.bridge_id);
                funds.withdraw_bridge_submitted_at_ts = Some(now);
                Ok(())
            }
            DepositNotice::NotYet { reason } => {
                let waited = now.saturating_sub(funds.withdraw_bridge_funded_at_ts.unwrap_or(now));
                if waited > DEPOSIT_NOTICE_WINDOW_SECS {
                    return Err(side_effect_ambiguous(format!(
                        "bridge deposit {receipt} landed {waited}s ago and the provider has not accepted it: {reason}"
                    ))
                    .into());
                }
                info!(
                    "[mexc] liq_id={} bridge deposit {} not accepted yet: {}",
                    liq_id, receipt, reason
                );
                Ok(())
            }
        }
    }

    /// A withdrawal that a bridge carries on: the MEXC withdrawal to the bridge
    /// source, then the bridge call or deposit send, then its status.
    pub(super) async fn withdraw_bridged(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
    ) -> Result<(), CexBackendError> {
        let bridge = self.bridge_runtime()?;
        let route = Self::withdraw_route(state).ok_or_else(|| {
            format!(
                "bridge route not found for withdraw {}@{} -> {}",
                planned_asset,
                planned_network,
                state.withdraw.withdraw_asset.symbol()
            )
        })?;

        if state.withdraw.bridge.withdraw_bridge_destination_snapshot.is_none() {
            state.withdraw.bridge.withdraw_bridge_destination_snapshot = Some(state.withdraw.withdraw_address.clone());
        }

        let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
        if state.withdraw.withdraw_address != bridge_source_address {
            state.withdraw.withdraw_address = bridge_source_address.clone();
        }

        // Submit CEX withdraw once (to bridge source), then keep polling in WithdrawPending.
        if state.withdraw.withdraw_id.is_none() && state.withdraw.withdraw_txid.is_none() {
            let Some(accepted) = self
                .submit_withdraw(state, planned_asset, planned_network, amount)
                .await?
            else {
                return Ok(());
            };
            state.withdraw.withdraw_id = Some(accepted.id);
            state.withdraw.withdraw_txid = accepted.txid;
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }

        // Once CEX withdrawal is completed, submit bridge once.
        if state.withdraw.bridge.withdraw_bridge_id.is_none() {
            return self
                .start_withdraw_bridge(
                    state,
                    bridge,
                    route,
                    planned_asset,
                    planned_network,
                    amount,
                    bridge_source_address,
                )
                .await;
        }
        Ok(self.poll_withdraw_bridge(state, bridge, route).await?)
    }

    /// The MEXC withdrawal reached the bridge source: sizes the bridge amount
    /// from the source balance and starts the bridge call or deposit send.
    #[allow(clippy::too_many_arguments)]
    async fn start_withdraw_bridge(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
        bridge_source_address: String,
    ) -> Result<(), CexBackendError> {
        // A started deposit send has left the source account, so the
        // balance checks below would wait for it forever.
        if state.withdraw.bridge.deposit_send_started() {
            let destination = self.withdraw_bridge_destination(state, route)?;
            return self
                .advance_withdraw_deposit_send(state, bridge, route, &destination, None)
                .await;
        }
        // Skips the reads below while another row's deposit is open. The
        // check under the submit lock decides.
        if Self::waits_for_other_deposit(bridge, route, &state.liq_id).await? {
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }
        let withdraw_id = state
            .withdraw
            .withdraw_id
            .clone()
            .ok_or_else(|| "missing withdraw_id while waiting bridged withdraw completion".to_string())?;

        let withdraw_snapshot = self
            .backend
            .get_withdraw_status_snapshot_by_id(planned_asset, &withdraw_id)
            .await?;
        info!(
            "[mexc] liq_id={} withdraw snapshot: withdraw_id={} status={:?} txid_present={} fee={:?}",
            state.liq_id,
            withdraw_id,
            withdraw_snapshot.status,
            withdraw_snapshot.txid.as_ref().is_some_and(|tx| !tx.trim().is_empty()),
            withdraw_snapshot.transaction_fee,
        );

        match withdraw_snapshot.status {
            WithdrawStatus::Pending | WithdrawStatus::Unknown => {
                if withdraw_snapshot.txid.is_some() {
                    info!(
                        "[mexc] liq_id={} withdraw status={:?} but txid is present for withdraw_id={}; proceeding to source-balance bridge preflight",
                        state.liq_id, withdraw_snapshot.status, withdraw_id
                    );
                } else {
                    state.step = CexStep::WithdrawPending;
                    return Ok(());
                }
            }
            WithdrawStatus::Failed => {
                return Err(format!(
                    "cex withdraw failed for liq_id {} withdraw_id={}",
                    state.liq_id, withdraw_id
                )
                .into());
            }
            WithdrawStatus::Canceled => {
                return Err(format!(
                    "cex withdraw canceled for liq_id {} withdraw_id={}",
                    state.liq_id, withdraw_id
                )
                .into());
            }
            WithdrawStatus::Completed => {}
        }

        // Keep the txid in state when available so operators can correlate
        // pending bridge flow with the underlying CEX withdrawal broadcast.
        if state.withdraw.withdraw_txid.is_none() {
            state.withdraw.withdraw_txid = withdraw_snapshot.txid.clone();
        }

        let bridge_destination = self.withdraw_bridge_destination(state, route)?;

        let mut bridge_amount = amount;
        if let Some(withdraw_fee) = withdraw_snapshot.transaction_fee {
            let adjusted_amount = Self::net_of_withdraw_fee(bridge_amount, withdraw_fee).map_err(|error| {
                format!(
                    "cex withdraw fee exceeds bridged amount for liq_id {}: {error} withdraw_id={}",
                    state.liq_id, withdraw_id
                )
            })?;
            if adjusted_amount < bridge_amount {
                info!(
                    "[mexc] liq_id={} applying CEX withdraw fee to bridged amount: gross={} fee={} net={} asset={} network={} withdraw_id={}",
                    state.liq_id,
                    bridge_amount,
                    withdraw_fee,
                    adjusted_amount,
                    planned_asset,
                    planned_network,
                    withdraw_id
                );
                bridge_amount = adjusted_amount;
            }
        }
        let _submit_guard =
            acquire_bridge_submit_lock(route.source_asset, route.source_chain, &bridge_source_address).await;

        let source_available = bridge
            .backend
            .get_source_balance(route.source_asset, route.source_chain, &bridge_source_address)
            .await?;

        // CEX withdrawals can arrive net of withdrawal/network fees; use the reported fee
        // above to compute the expected net amount. Native ETH source balances are reported
        // as bridgeable balance after reserving gas, so submit that bridgeable amount instead
        // of waiting forever for the gross source balance to become bridgeable.
        if source_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < bridge_amount {
            if matches!(route.route_kind, BridgeRouteKind::EthToCkEth)
                && source_available > WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE
            {
                let adjusted_source_available = (source_available - NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN).max(0.0);
                if adjusted_source_available <= WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE {
                    debug!(
                        "[mexc] liq_id={} bridged native ETH withdraw waiting for source funding after safety margin: available={} adjusted={} expected={} tolerance={} source={} route={}@{}->{}",
                        state.liq_id,
                        source_available,
                        adjusted_source_available,
                        bridge_amount,
                        WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                        bridge_source_address,
                        route.source_asset,
                        route.source_chain,
                        route.target_asset,
                    );
                    state.step = CexStep::WithdrawPending;
                    return Ok(());
                }
                info!(
                    "[mexc] liq_id={} bridged native ETH withdraw adjusting to bridgeable source balance: amount {} -> {} (source includes gas reserve; safety_margin={} tolerance={} source={} route={}@{}->{})",
                    state.liq_id,
                    bridge_amount,
                    adjusted_source_available,
                    NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    bridge_source_address,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                bridge_amount = adjusted_source_available;
            } else {
                debug!(
                    "[mexc] liq_id={} bridged withdraw waiting for source funding: available={} expected={} tolerance={} source={} route={}@{}->{}",
                    state.liq_id,
                    source_available,
                    bridge_amount,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    bridge_source_address,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                state.step = CexStep::WithdrawPending;
                return Ok(());
            }
        }

        if source_available < bridge_amount {
            info!(
                "[mexc] liq_id={} adjusting bridged withdraw amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={}@{}->{})",
                state.liq_id,
                bridge_amount,
                source_available,
                WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                bridge_source_address,
                route.source_asset,
                route.source_chain,
                route.target_asset,
            );
            bridge_amount = source_available;
        }

        if route.route_kind == BridgeRouteKind::SolanaToIcp {
            let gross = ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), bridge_amount);
            let source_fee_budget = bridge
                .backend
                .get_source_fee_budget(route.source_asset, route.source_chain, route.target_asset)
                .await?;
            bridge_amount = Self::spend_f64(
                &Self::solana_repayment_bridge_amount(bridge, route, &gross, source_fee_budget).await?,
            )?;
        }

        let request = BridgeRequest {
            asset: route.source_asset.to_string(),
            source_chain: route.source_chain.to_string(),
            source_address: bridge_source_address,
            target_asset: route.target_asset.to_string(),
            destination: bridge_destination,
            amount: bridge_amount,
            provider_fee_budget_native_units: None,
        };
        // The bridge signs the send that funds the route, and the
        // outbound driver saves, sends and settles it.
        if route.route_kind.funded_by_signed_send() {
            return self
                .advance_withdraw_deposit_send(state, bridge, route, &request.destination, Some(&request))
                .await;
        }
        let submission = match bridge.backend.submit_bridge(request).await {
            Ok(submission) => submission,
            // Same wait semantics as the deposit side: no budget spent.
            Err(error) if error.starts_with(BRIDGE_SUBMISSION_DEFERRED_PREFIX) => {
                info!("[mexc] liq_id={} bridged withdraw deferred: {}", state.liq_id, error);
                state.step = CexStep::WithdrawPending;
                return Ok(());
            }
            Err(error) => return Err(bridge_refusal(error)),
        };
        state.withdraw.bridge.withdraw_bridge_id = Some(submission.bridge_id);
        state.withdraw.bridge.withdraw_bridge_submitted_at_ts = Some(now_ts());
        state.step = CexStep::WithdrawPending;
        Ok(())
    }

    /// Reads the bridge status: a mint completes the leg; a dropped ckSOL
    /// sweep gets one new notice; a failure is an error.
    async fn poll_withdraw_bridge(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
    ) -> Result<(), String> {
        let bridge_id = state
            .withdraw
            .bridge
            .withdraw_bridge_id
            .clone()
            .ok_or_else(|| "missing bridge id while polling bridged withdraw".to_string())?;
        // The older flow saved the SOL transfer without a minter id. The notice
        // step asks the minter for it, as after a new transfer.
        if let Some(transfer) = cksol_transfer_without_deposit_id(&bridge_id) {
            let funds = &mut state.withdraw.bridge;
            funds.withdraw_bridge_funding_txid = Some(transfer);
            funds.withdraw_bridge_funded_at_ts = funds
                .withdraw_bridge_funded_at_ts
                .or(funds.withdraw_bridge_submitted_at_ts)
                .or(Some(now_ts()));
            funds.withdraw_bridge_id = None;
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }
        state.withdraw.bridge.withdraw_bridge_polled_at_ts = Some(now_ts());
        match bridge.backend.get_bridge_status(&bridge_id).await? {
            BridgeStatus::Minted(receipt) => {
                // The EVM backend already checks its receipt against the
                // submitted expectation. The Solana backend has no such check,
                // so this comparison is the only one for ckSOL. Keep it.
                let destination = state
                    .withdraw
                    .bridge
                    .withdraw_bridge_destination_snapshot
                    .as_deref()
                    .ok_or("missing expected mint destination")?;
                let expected = crate::finalizers::bridge_planner::parse_icp_account_like(destination)?;
                if receipt.recipient != expected
                    || !receipt
                        .token_symbol
                        .eq_ignore_ascii_case(&state.withdraw.withdraw_asset.symbol())
                {
                    return Err("minter receipt does not match the committed return destination/token".to_string());
                }
                // Report destination credit, not the trade output before
                // exchange fees and bridge source adjustments.
                state.withdraw.size_out = Some(ChainTokenAmount::from_raw(
                    state.withdraw.withdraw_asset.clone(),
                    receipt.amount.clone(),
                ));
                state.withdraw.bridge.withdraw_bridge_mint_receipt = Some(receipt);
                state.step = CexStep::Completed;
                Ok(())
            }
            BridgeStatus::Completed => {
                state.step = CexStep::Completed;
                Ok(())
            }
            BridgeStatus::Pending | BridgeStatus::Unknown => {
                state.step = CexStep::WithdrawPending;
                Ok(())
            }
            // The ckSOL minter dropped its sweep and released the deposit
            // address. The funds stay there, and a new notice queues them again.
            BridgeStatus::Failed { reason }
                if route.route_kind == BridgeRouteKind::SolanaToIcp
                    && state.withdraw.bridge.withdraw_bridge_funding_txid.is_some() =>
            {
                let reason = reason.unwrap_or_default();
                state.withdraw.bridge.withdraw_bridge_id = None;
                state.step = CexStep::WithdrawPending;
                // The landing time stays, so a sweep that keeps failing parks
                // instead of a notice loop. A release then gives one new notice.
                let now = now_ts();
                let waited = now.saturating_sub(state.withdraw.bridge.withdraw_bridge_funded_at_ts.unwrap_or(now));
                if waited > DEPOSIT_NOTICE_WINDOW_SECS {
                    return Err(side_effect_ambiguous(format!(
                        "bridge deposit {bridge_id} dropped by the provider {waited}s after the funds landed: {reason}"
                    )));
                }
                warn!(
                    "[mexc] liq_id={} bridge deposit {} dropped by the provider, notifying again: {}",
                    state.liq_id, bridge_id, reason
                );
                Ok(())
            }
            BridgeStatus::Failed { reason } => Err(format!(
                "bridged withdraw failed for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "unknown failure".to_string())
            )),
            BridgeStatus::Canceled { reason } => Err(format!(
                "bridged withdraw canceled for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "no reason provided".to_string())
            )),
        }
    }
}

/// A bridge backend refusal as a step error. The bridge still reports its
/// amount floor as text, until `BridgeBackend` returns typed errors.
fn bridge_refusal(error: String) -> CexBackendError {
    if error.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX) {
        CexBackendError::BelowMinimum(error)
    } else {
        CexBackendError::Other(error)
    }
}
