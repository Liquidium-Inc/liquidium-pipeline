//! A bridged withdrawal on the exchange side: the exchange withdrawal to the
//! bridge wallet and its delivery check around the shared leg back, and its
//! outcome mapped onto the steps.
use super::*;
use crate::finalizers::bridge_legs::{
    BridgeInInputs, BridgeLegOutcome, WaitBound, advance_bridge_in, awaits_other_deposit, in_route,
    net_of_withdraw_fee, wait_within_bound,
};

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// The return bridge route from the planned exchange withdrawal to the final asset.
    pub(in crate::finalizers::cex) fn withdraw_route(state: &CexState) -> Option<&'static BridgeRouteSpec> {
        in_route(
            &<Self as BridgePlanner>::planned_withdraw_asset(state),
            &<Self as BridgePlanner>::planned_withdraw_network(state),
            &state.withdraw.withdraw_asset.symbol(),
        )
    }

    /// A withdrawal that a bridge carries on: the exchange withdrawal to the bridge
    /// wallet, then the shared leg back once the exchange has delivered.
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

        // The final liquidator address is kept before the exchange withdrawal
        // is pointed at the bridge wallet.
        if state.withdraw.bridge.withdraw_bridge_destination_snapshot.is_none() {
            state.withdraw.bridge.withdraw_bridge_destination_snapshot = Some(state.withdraw.withdraw_address.clone());
        }
        let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
        if state.withdraw.withdraw_address != bridge_source_address {
            state.withdraw.withdraw_address = bridge_source_address;
        }

        // The exchange withdrawal once, to the bridge wallet.
        if state.withdraw.withdraw_id.is_none() && state.withdraw.withdraw_txid.is_none() {
            let Some(accepted) = self
                .submit_withdraw(state, planned_asset, planned_network, amount)
                .await?
            else {
                return Ok(());
            };
            state.withdraw.withdraw_id = Some(accepted.id);
            state.withdraw.withdraw_txid = accepted.txid;
            state.withdraw.withdraw_accepted_at_ts = Some(now_ts());
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }

        // Before the leg sizes its first send, the exchange must have delivered.
        // While another row's deposit is open on the route, nothing is read.
        let mut bridge_amount = amount;
        if state.withdraw.bridge.awaits_funds() {
            if awaits_other_deposit(bridge, route, &state.liq_id).await? {
                state.step = CexStep::WithdrawPending;
                return Ok(());
            }
            let Some(delivered) = self
                .exchange_delivery_for_bridge(state, planned_asset, planned_network, amount)
                .await?
            else {
                return Ok(());
            };
            bridge_amount = delivered;
        }

        let final_destination = state
            .withdraw
            .bridge
            .withdraw_bridge_destination_snapshot
            .clone()
            .unwrap_or_default();
        let outcome = advance_bridge_in(
            bridge,
            &mut state.withdraw.bridge,
            BridgeInInputs {
                leg_id: &state.liq_id,
                route,
                amount: bridge_amount,
                final_asset: &state.withdraw.withdraw_asset,
                final_destination: &final_destination,
                wait: WaitBound {
                    since: &mut state.withdraw.withdraw_accepted_at_ts,
                    timeout_secs: WITHDRAW_DELIVERY_TIMEOUT_SECS,
                },
            },
        )
        .await?;
        match outcome {
            BridgeLegOutcome::Waiting { stalled } => {
                state.step = CexStep::WithdrawPending;
                if let Some(line) = stalled {
                    state.last_error = Some(line);
                }
            }
            BridgeLegOutcome::Saved { .. } => {
                state.send_saved = true;
                state.step = CexStep::WithdrawPending;
            }
            BridgeLegOutcome::Funded { .. } | BridgeLegOutcome::Submitted => state.step = CexStep::WithdrawPending,
            BridgeLegOutcome::Completed { receipt } => {
                if let Some(receipt) = receipt {
                    // Report destination credit, not the trade output before
                    // exchange fees and bridge source adjustments.
                    state.withdraw.size_out = Some(ChainTokenAmount::from_raw(
                        state.withdraw.withdraw_asset.clone(),
                        receipt.amount,
                    ));
                }
                state.step = CexStep::Completed;
            }
        }
        Ok(())
    }

    /// Whether the exchange has sent the withdrawal, and the amount the bridge
    /// wallet should then hold, net of the exchange fee. `None` means the leg
    /// waits, or the withdrawal was requested again.
    async fn exchange_delivery_for_bridge(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
    ) -> Result<Option<f64>, CexBackendError> {
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
            "[cex] liq_id={} withdraw snapshot: withdraw_id={} status={:?} txid_present={} fee={:?}",
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
                        "[cex] liq_id={} withdraw status={:?} but txid is present for withdraw_id={}; proceeding to source-balance bridge preflight",
                        state.liq_id, withdraw_snapshot.status, withdraw_id
                    );
                } else {
                    state.step = CexStep::WithdrawPending;
                    wait_within_bound(
                        &state.liq_id,
                        &mut WaitBound {
                            since: &mut state.withdraw.withdraw_accepted_at_ts,
                            timeout_secs: WITHDRAW_DELIVERY_TIMEOUT_SECS,
                        },
                        "bridged withdraw",
                        "the exchange has not sent the withdrawal",
                    )?;
                    return Ok(None);
                }
            }
            WithdrawStatus::Failed | WithdrawStatus::Canceled => {
                Self::resubmit_failed_withdrawal(state, &withdraw_id, &withdraw_snapshot)?;
                return Ok(None);
            }
            WithdrawStatus::Completed => {}
        }

        // Keep the txid in state when available so operators can correlate
        // pending bridge flow with the underlying CEX withdrawal broadcast.
        if state.withdraw.withdraw_txid.is_none() {
            state.withdraw.withdraw_txid = withdraw_snapshot.txid.clone();
        }

        let mut bridge_amount = amount;
        if let Some(withdraw_fee) = withdraw_snapshot.transaction_fee {
            let adjusted_amount = net_of_withdraw_fee(bridge_amount, withdraw_fee).map_err(|error| {
                format!(
                    "cex withdraw fee exceeds bridged amount for liq_id {}: {error} withdraw_id={}",
                    state.liq_id, withdraw_id
                )
            })?;
            if adjusted_amount < bridge_amount {
                info!(
                    "[cex] liq_id={} applying CEX withdraw fee to bridged amount: gross={} fee={} net={} asset={} network={} withdraw_id={}",
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
        Ok(Some(bridge_amount))
    }
}
