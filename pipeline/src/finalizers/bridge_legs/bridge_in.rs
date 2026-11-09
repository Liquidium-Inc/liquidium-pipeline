//! The leg back: a foreign asset at the bridge wallet to its ck form on ICP.
//! The source preflight and sizing, then the saved provider call or the signed
//! deposit send with its notice, then the mint poll.
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeError, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus, DepositNotice,
    cksol_transfer_without_deposit_id, resolve_route,
};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use log::{debug, info, warn};

use super::{
    BridgeInState, BridgeLegError, BridgeLegOutcome, DEPOSIT_NOTICE_WINDOW_SECS, MAX_REVERTED_BRIDGE_RESUBMITS,
    NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN, SOL_FINALITY_SECS, SOURCE_BALANCE_TOLERANCE, SavedCallStep, WaitBound,
    bridge_failure,
    bridge_sizing::{solana_repayment_bridge_amount, spend_f64},
    drive_saved_call, note_bridge_wait, read_bridge_status, route_label, save_provider_call, wait_within_bound,
};
use crate::{
    bridge::{call_sender::BridgeCall, runtime::BridgeRuntime},
    finalizers::bridge_planner::{bridge_destination, parse_icp_account_like},
    outbound::driver::{OutboundError, OutboundRecord, step_outbound},
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    utils::now_ts,
};

pub struct BridgeInInputs<'a> {
    pub leg_id: &'a str,
    pub route: &'static BridgeRouteSpec,
    /// What the bridge wallet should hold for this leg, net of what the sender took.
    pub amount: f64,
    /// The ck asset to mint.
    pub final_asset: &'a ChainToken,
    /// The ICP account the mint goes to. Snapshotted on first use.
    pub final_destination: &'a str,
    pub wait: WaitBound<'a>,
}

/// The return bridge route from the asset at the bridge wallet to the final ck asset.
pub fn in_route(source_asset: &str, source_chain: &str, final_symbol: &str) -> Option<&'static BridgeRouteSpec> {
    resolve_route(source_asset, source_chain, final_symbol)
}

/// One cycle of the leg back: start the bridge from the funds at the wallet,
/// then poll it until the mint.
pub async fn advance_bridge_in(
    bridge: &BridgeRuntime,
    state: &mut BridgeInState,
    mut inputs: BridgeInInputs<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    if state.withdraw_bridge_destination_snapshot.is_none() {
        state.withdraw_bridge_destination_snapshot = Some(inputs.final_destination.to_string());
    }
    if state.withdraw_bridge_id.is_none() {
        return start(bridge, state, &mut inputs).await;
    }
    poll(bridge, state, inputs.route, inputs.leg_id).await
}

/// The final destination, from the snapshot taken when the leg started.
fn final_destination(state: &BridgeInState, route: &BridgeRouteSpec) -> Result<BridgeDestination, String> {
    let snapshot = state
        .withdraw_bridge_destination_snapshot
        .as_deref()
        .ok_or_else(|| "missing final destination snapshot for bridged withdraw".to_string())?;
    bridge_destination(route.destination_kind, snapshot)
        .map_err(|error| format!("invalid final destination for bridge withdraw: {error}"))
}

/// The ckSOL deposit address is shared, and the minter takes one deposit at a
/// time. A second send would merge with the open one. A caller asks before it
/// reads its own sender's state, so an open deposit costs no exchange read; the
/// check under the submit lock decides.
pub async fn awaits_other_deposit(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    leg_id: &str,
) -> Result<bool, String> {
    if route.route_kind != BridgeRouteKind::SolanaToIcp {
        return Ok(false);
    }
    let claims = bridge.claims.claims_since(now_ts()).await?;
    let other = claims.open_deposit_sends.into_iter().find(|send| {
        send.liq_id != leg_id
            && send.asset.eq_ignore_ascii_case(route.source_asset)
            && send.network.eq_ignore_ascii_case(route.source_chain)
    });
    let Some(other) = other else {
        return Ok(false);
    };
    info!(
        "[bridge] liq_id={} bridge deposit waits for liq_id={} on the same deposit address",
        leg_id, other.liq_id
    );
    Ok(true)
}

/// The funds reached the bridge wallet: sizes the bridge amount from the source
/// balance and starts the provider call or the deposit send.
async fn start(
    bridge: &BridgeRuntime,
    state: &mut BridgeInState,
    inputs: &mut BridgeInInputs<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let route = inputs.route;
    let leg_id = inputs.leg_id;
    let source_address = bridge.sources.for_chain(route.source_chain)?;
    // A started deposit send has left the source account, so the balance
    // checks below would wait for it forever.
    if state.deposit_send_started() {
        let destination = final_destination(state, route)?;
        return advance_deposit_send(bridge, state, route, &destination, None, leg_id, &mut inputs.wait).await;
    }
    // A saved call may have run and spent the source balance, so it settles
    // before the balance reads below.
    if state.withdraw_bridge_call.is_some() {
        let _submit_guard = acquire_bridge_submit_lock(route.source_asset, route.source_chain, &source_address).await;
        return advance_call(bridge, state, leg_id, &mut inputs.wait).await;
    }
    let bridge_destination = final_destination(state, route)?;
    let mut bridge_amount = inputs.amount;
    let _submit_guard = acquire_bridge_submit_lock(route.source_asset, route.source_chain, &source_address).await;

    let source_available = bridge
        .backend
        .get_source_balance(route.source_asset, route.source_chain, &source_address)
        .await?;

    // The sender may deliver net of its fees; `amount` already accounts for that.
    // Native ETH source balances are reported as bridgeable balance after
    // reserving gas, so submit that bridgeable amount instead of waiting forever
    // for the gross source balance to become bridgeable.
    if source_available + SOURCE_BALANCE_TOLERANCE < bridge_amount {
        if matches!(route.route_kind, BridgeRouteKind::EthToCkEth) && source_available > SOURCE_BALANCE_TOLERANCE {
            let adjusted_source_available = (source_available - NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN).max(0.0);
            if adjusted_source_available <= SOURCE_BALANCE_TOLERANCE {
                debug!(
                    "[bridge] liq_id={} bridged native ETH return waiting for source funding after safety margin: available={} adjusted={} expected={} tolerance={} source={} route={}",
                    leg_id,
                    source_available,
                    adjusted_source_available,
                    bridge_amount,
                    SOURCE_BALANCE_TOLERANCE,
                    source_address,
                    route_label(route),
                );
                return wait_within_bound(
                    leg_id,
                    &mut inputs.wait,
                    "bridged withdraw",
                    "the bridge source has no bridgeable ETH yet",
                );
            }
            info!(
                "[bridge] liq_id={} bridged native ETH return adjusting to bridgeable source balance: amount {} -> {} (source includes gas reserve; safety_margin={} tolerance={} source={} route={})",
                leg_id,
                bridge_amount,
                adjusted_source_available,
                NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN,
                SOURCE_BALANCE_TOLERANCE,
                source_address,
                route_label(route),
            );
            bridge_amount = adjusted_source_available;
        } else {
            debug!(
                "[bridge] liq_id={} bridged return waiting for source funding: available={} expected={} tolerance={} source={} route={}",
                leg_id,
                source_available,
                bridge_amount,
                SOURCE_BALANCE_TOLERANCE,
                source_address,
                route_label(route),
            );
            return wait_within_bound(
                leg_id,
                &mut inputs.wait,
                "bridged withdraw",
                "the bridge source holds less than the withdrawal",
            );
        }
    }

    if source_available < bridge_amount {
        info!(
            "[bridge] liq_id={} adjusting bridged return amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={})",
            leg_id,
            bridge_amount,
            source_available,
            SOURCE_BALANCE_TOLERANCE,
            source_address,
            route_label(route),
        );
        bridge_amount = source_available;
    }

    if route.route_kind == BridgeRouteKind::SolanaToIcp {
        let gross = ChainTokenAmount::from_formatted(inputs.final_asset.clone(), bridge_amount);
        let source_fee_budget = bridge
            .backend
            .get_source_fee_budget(route.source_asset, route.source_chain, route.target_asset)
            .await?;
        bridge_amount = spend_f64(&solana_repayment_bridge_amount(bridge, route, &gross, source_fee_budget).await?)?;
    }

    let request = BridgeRequest {
        asset: route.source_asset.to_string(),
        source_chain: route.source_chain.to_string(),
        source_address,
        target_asset: route.target_asset.to_string(),
        destination: bridge_destination,
        amount: bridge_amount,
        provider_fee_budget_native_units: None,
    };
    // The bridge signs the send that funds the route, and the outbound
    // driver saves, sends and settles it.
    if route.route_kind.funded_by_signed_send() {
        return advance_deposit_send(
            bridge,
            state,
            route,
            &request.destination,
            Some(&request),
            leg_id,
            &mut inputs.wait,
        )
        .await;
    }
    save_call(state, bridge, &request, leg_id).await
}

/// Runs one step of a return bridge whose provider watches a deposit address:
/// the send, then the notice. `request` starts a new send.
#[allow(clippy::too_many_arguments)]
async fn advance_deposit_send(
    bridge: &BridgeRuntime,
    state: &mut BridgeInState,
    route: &BridgeRouteSpec,
    destination: &BridgeDestination,
    request: Option<&BridgeRequest>,
    leg_id: &str,
    wait: &mut WaitBound<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let now = now_ts();
    let Some(receipt) = state.withdraw_bridge_funding_txid.clone() else {
        if let Some(request) = request {
            // The caller holds the submit lock, so this check is final.
            if awaits_other_deposit(bridge, route, leg_id).await? {
                return Ok(BridgeLegOutcome::Waiting { stalled: None });
            }
            let bridge_send = match bridge.backend.prepare_bridge_send(request).await {
                Ok(bridge_send) => bridge_send,
                // A busy bridge wallet is a wait. Nothing was signed.
                Err(BridgeError::Deferred(error)) => {
                    info!("[bridge] liq_id={} bridged return deferred: {}", leg_id, error);
                    return wait_within_bound(leg_id, wait, "bridged withdraw", &error);
                }
                Err(error) => return Err(error.into()),
            };
            state.withdraw_bridge_funding = Some(OutboundRecord::prepared(bridge_send.send));
            state.withdraw_bridge_send_handle = Some(bridge_send.handle);
            return Ok(BridgeLegOutcome::Saved { side_costs: Vec::new() });
        }
        let landed = match step_outbound(&mut state.withdraw_bridge_funding, bridge.outbound.as_ref(), None, now).await
        {
            Ok(landed) => landed,
            // The revert moved no funds, and the driver cleared the send, so the
            // next cycle makes it again. Past the bound an operator decides.
            Err(OutboundError::Reverted(reason)) if state.withdraw_bridge_reverts < MAX_REVERTED_BRIDGE_RESUBMITS => {
                state.withdraw_bridge_reverts += 1;
                warn!(
                    "[bridge] liq_id={} bridge deposit send reverted; sending again ({}/{}): {}",
                    leg_id, state.withdraw_bridge_reverts, MAX_REVERTED_BRIDGE_RESUBMITS, reason
                );
                return Ok(BridgeLegOutcome::Waiting { stalled: None });
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(receipt) = landed {
            info!(
                "[bridge] liq_id={} bridge deposit send landed: receipt={} route={}",
                leg_id,
                receipt,
                route_label(route)
            );
            state.withdraw_bridge_funding_txid = Some(receipt);
            state.withdraw_bridge_funded_at_ts = Some(now);
        }
        // The provider reads the deposit at its own commitment, so the
        // tracking runs on the next cycle.
        return Ok(BridgeLegOutcome::Waiting { stalled: None });
    };
    // The minter reads only finalized transfers, and each notice costs cycles.
    let landed_for = now.saturating_sub(state.withdraw_bridge_funded_at_ts.unwrap_or(0));
    if route.route_kind == BridgeRouteKind::SolanaToIcp && landed_for < SOL_FINALITY_SECS {
        return Ok(BridgeLegOutcome::Waiting { stalled: None });
    }
    let handle = state.withdraw_bridge_send_handle.clone().unwrap_or_default();
    match bridge
        .backend
        .track_deposit(route.source_asset, route.source_chain, destination, &handle, &receipt)
        .await?
    {
        DepositNotice::Accepted(submission) => {
            info!(
                "[bridge] liq_id={} bridge deposit {} accepted: bridge_id={}",
                leg_id, receipt, submission.bridge_id
            );
            state.withdraw_bridge_id = Some(submission.bridge_id);
            state.withdraw_bridge_submitted_at_ts = Some(now);
            Ok(BridgeLegOutcome::Submitted)
        }
        DepositNotice::NotYet { reason } => {
            let waited = now.saturating_sub(state.withdraw_bridge_funded_at_ts.unwrap_or(now));
            if waited > DEPOSIT_NOTICE_WINDOW_SECS {
                return Err(BridgeLegError::Ambiguous(format!(
                    "bridge deposit {receipt} landed {waited}s ago and the provider has not accepted it: {reason}"
                )));
            }
            info!(
                "[bridge] liq_id={} bridge deposit {} not accepted yet: {}",
                leg_id, receipt, reason
            );
            Ok(BridgeLegOutcome::Waiting { stalled: None })
        }
    }
}

/// Saves the provider call with the history cursor taken before it. The call
/// runs on the next step, so a lost answer has a record to settle.
async fn save_call(
    state: &mut BridgeInState,
    bridge: &BridgeRuntime,
    request: &BridgeRequest,
    leg_id: &str,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let destination = state
        .withdraw_bridge_destination_snapshot
        .clone()
        .ok_or_else(|| "missing final destination snapshot for bridged withdraw".to_string())?;
    let call = BridgeCall {
        asset: request.asset.clone(),
        source_chain: request.source_chain.clone(),
        source_address: request.source_address.clone(),
        target_asset: request.target_asset.clone(),
        destination,
        amount: request.amount,
        provider_fee_budget_native_units: None,
        cursor: None,
        started_at_ts: now_ts(),
    };
    state.withdraw_bridge_call = Some(save_provider_call(bridge, call, "bridged return", leg_id).await?);
    Ok(BridgeLegOutcome::Saved { side_costs: Vec::new() })
}

/// Drives the saved call: one step sends it, and a lost answer settles from
/// the provider history before any new call.
async fn advance_call(
    bridge: &BridgeRuntime,
    state: &mut BridgeInState,
    leg_id: &str,
    wait: &mut WaitBound<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    match drive_saved_call(bridge, &mut state.withdraw_bridge_call).await? {
        SavedCallStep::Submitted(bridge_id) => {
            info!(
                "[bridge] liq_id={} bridged return submitted: bridge_id={}",
                leg_id, bridge_id
            );
            state.withdraw_bridge_id = Some(bridge_id);
            state.withdraw_bridge_submitted_at_ts = Some(now_ts());
            state.withdraw_bridge_call = None;
            Ok(BridgeLegOutcome::Submitted)
        }
        SavedCallStep::Pending => Ok(BridgeLegOutcome::Waiting { stalled: None }),
        // The next cycle sizes the amount again and saves a new call.
        SavedCallStep::Deferred(error) => {
            info!("[bridge] liq_id={} bridged return deferred: {}", leg_id, error);
            wait_within_bound(leg_id, wait, "bridged withdraw", &error)
        }
    }
}

/// Reads the bridge status: a mint completes the leg; a dropped ckSOL sweep
/// gets one new notice; a failure is an error.
async fn poll(
    bridge: &BridgeRuntime,
    state: &mut BridgeInState,
    route: &BridgeRouteSpec,
    leg_id: &str,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let bridge_id = state
        .withdraw_bridge_id
        .clone()
        .ok_or_else(|| "missing bridge id while polling bridged withdraw".to_string())?;
    // The older flow saved the SOL transfer without a minter id. The notice
    // step asks the minter for it, as after a new transfer.
    if let Some(transfer) = cksol_transfer_without_deposit_id(&bridge_id) {
        state.withdraw_bridge_funding_txid = Some(transfer);
        state.withdraw_bridge_funded_at_ts = state
            .withdraw_bridge_funded_at_ts
            .or(state.withdraw_bridge_submitted_at_ts)
            .or(Some(now_ts()));
        state.withdraw_bridge_id = None;
        return Ok(BridgeLegOutcome::Waiting { stalled: None });
    }
    state.withdraw_bridge_polled_at_ts = Some(now_ts());
    let (status, unreadable) = read_bridge_status(bridge, &bridge_id).await?;
    match status {
        BridgeStatus::Minted(receipt) => {
            // The EVM backend already checks its receipt against the submitted
            // expectation. The Solana backend has no such check, so this
            // comparison is the only one for ckSOL. Keep it.
            let destination = state
                .withdraw_bridge_destination_snapshot
                .as_deref()
                .ok_or("missing expected mint destination")?;
            let expected = parse_icp_account_like(destination)?;
            if receipt.recipient != expected || !receipt.token_symbol.eq_ignore_ascii_case(route.target_asset) {
                return Err("minter receipt does not match the committed return destination/token".into());
            }
            state.withdraw_bridge_mint_receipt = Some(receipt.clone());
            Ok(BridgeLegOutcome::Completed { receipt: Some(receipt) })
        }
        BridgeStatus::Completed => Ok(BridgeLegOutcome::Completed { receipt: None }),
        BridgeStatus::Pending | BridgeStatus::Unknown => {
            let stalled = note_bridge_wait(
                leg_id,
                state.withdraw_bridge_submitted_at_ts,
                &bridge_id,
                unreadable.as_deref(),
            )?;
            Ok(BridgeLegOutcome::Waiting { stalled })
        }
        // The ckSOL minter dropped its sweep and released the deposit address.
        // The funds stay there, and a new notice queues them again.
        BridgeStatus::Failed { reason }
            if route.route_kind == BridgeRouteKind::SolanaToIcp && state.withdraw_bridge_funding_txid.is_some() =>
        {
            let reason = reason.unwrap_or_default();
            state.withdraw_bridge_id = None;
            // The landing time stays, so a sweep that keeps failing parks instead
            // of a notice loop. A release then gives one new notice.
            let now = now_ts();
            let waited = now.saturating_sub(state.withdraw_bridge_funded_at_ts.unwrap_or(now));
            if waited > DEPOSIT_NOTICE_WINDOW_SECS {
                return Err(BridgeLegError::Ambiguous(format!(
                    "bridge deposit {bridge_id} dropped by the provider {waited}s after the funds landed: {reason}"
                )));
            }
            warn!(
                "[bridge] liq_id={} bridge deposit {} dropped by the provider, notifying again: {}",
                leg_id, bridge_id, reason
            );
            Ok(BridgeLegOutcome::Waiting { stalled: None })
        }
        ended => Err(bridge_failure("bridged withdraw", leg_id, &bridge_id, ended)),
    }
}
