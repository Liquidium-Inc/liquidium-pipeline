//! The return bridge after a MEXC withdrawal: the deposit send to the
//! provider, the one-deposit-per-address wait, and the notice.
use super::*;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// The bridge destination for the final liquidator, from the snapshot
    /// taken when the bridged withdrawal started.
    pub(super) fn withdraw_bridge_destination(&self, state: &CexState, route: &BridgeRouteSpec) -> Result<BridgeDestination, String> {
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
    pub(super) async fn waits_for_other_deposit(bridge: &BridgeRuntime, route: &BridgeRouteSpec, liq_id: &str) -> Result<bool, String> {
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
    ) -> Result<(), String> {
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
                    Err(error) => return Err(error),
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
                    )));
                }
                info!(
                    "[mexc] liq_id={} bridge deposit {} not accepted yet: {}",
                    liq_id, receipt, reason
                );
                Ok(())
            }
        }
    }
}
