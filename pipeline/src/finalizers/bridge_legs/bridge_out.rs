//! The leg out: an ICP asset into the bridge wallet, the provider call for a
//! bridge handle, then the status poll. The provider call is a saved send: the
//! outbound driver saves it before it runs, and settles a lost answer from the
//! provider history.
use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::amount_utils::nat_units_to_amount_via_core;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeFeeBudget, BridgeRouteKind, BridgeRouteSpec, BridgeStatus, resolve_route,
};
use liquidium_pipeline_core::{
    outbound::{OutboundIntent, OutboundSender},
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::side_cost::SideCost,
};
use log::{debug, info};

use super::{
    BRIDGE_FEE_TOKEN_CHAIN, BRIDGE_FEE_TOKEN_SYMBOL, BRIDGE_START_TIMEOUT_SECS, BridgeLegError, BridgeLegOutcome,
    BridgeOutState, CKETH_DECIMALS, SOURCE_BALANCE_TOLERANCE, SavedCallStep, WaitBound, bridge_failure,
    bridge_sizing::{
        BridgeDepositSizing, BridgedDepositArrival, bridge_source_transfer_destination, bridged_deposit_arrival,
    },
    drive_saved_call, note_bridge_wait, read_bridge_status, route_label, save_provider_call, wait_within_bound,
};
use crate::{
    bridge::{
        call_sender::{BridgeCall, prepare_bridge_call},
        legacy::LegacyBridgeSubmitPhase,
        runtime::BridgeRuntime,
    },
    outbound::driver::{OutboundPhase, OutboundRecord, step_outbound},
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    utils::now_ts,
};

/// Where the bridge delivers. An exchange looks its deposit address up when the
/// call is saved; Rift knows it from the order.
#[async_trait]
pub trait BridgeDestinationProvider: Send + Sync {
    async fn destination(&self) -> Result<String, String>;
}

/// The caller's record of the transfer that funds the bridge wallet. An
/// exchange shares these fields with its direct deposit, so they stay with it.
pub struct FundingSlot<'a> {
    pub record: &'a mut Option<OutboundRecord>,
    pub txid: &'a mut Option<String>,
    pub sent_at_ts: &'a mut Option<i64>,
}

pub struct BridgeOutInputs<'a> {
    pub leg_id: &'a str,
    pub route: &'static BridgeRouteSpec,
    /// The ICP asset that leaves.
    pub asset: &'a ChainToken,
    /// The leg's allocation.
    pub size_in: &'a ChainTokenAmount,
    /// Sends the funding transfer from the caller's account.
    pub outbound: &'a dyn OutboundSender,
    pub destination: &'a dyn BridgeDestinationProvider,
}

/// The bridge route from an ICP asset to the asset the far side receives.
pub fn out_route(asset: &ChainToken, target_asset: &str) -> Result<&'static BridgeRouteSpec, String> {
    let source_symbol = asset.symbol();
    let source_chain = asset.chain();
    resolve_route(&source_symbol, &source_chain, target_asset)
        .ok_or_else(|| format!("bridge route not found for deposit {source_symbol}@{source_chain} -> {target_asset}"))
}

/// One cycle of the leg out: fund the bridge wallet, save and send the provider
/// call for a bridge handle, then poll the bridge until it completes.
pub async fn advance_bridge_out(
    bridge: &BridgeRuntime,
    state: &mut BridgeOutState,
    funding: FundingSlot<'_>,
    inputs: BridgeOutInputs<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    // Phase A: move the asset into the bridge wallet.
    if funding.txid.is_none() {
        let (intent, side_costs) = if funding.record.is_some() {
            (None, Vec::new())
        } else {
            let (intent, side_costs) = prepare_funding(bridge, state, &inputs).await?;
            (Some(intent), side_costs)
        };
        let landed = step_outbound(funding.record, inputs.outbound, intent.as_ref(), now_ts()).await?;
        let Some(tx_id) = landed else {
            return Ok(if intent.is_some() {
                BridgeLegOutcome::Saved { side_costs }
            } else {
                BridgeLegOutcome::Waiting { stalled: None }
            });
        };
        *funding.txid = Some(tx_id);
        *funding.sent_at_ts = Some(now_ts());
        // This build funded the bridge, so no call ran without a record.
        state.deposit_bridge_call_unrecorded = false;
        return Ok(BridgeLegOutcome::Funded { side_costs });
    }

    if state.deposit_bridge_id.is_none() {
        return advance_submission(bridge, state, funding.sent_at_ts, &inputs).await;
    }

    poll(bridge, state, inputs.leg_id).await
}

/// Sizes the funding transfer and saves the bridge amounts it implies. It runs
/// once per transfer. Returns the transfer and the side costs the route pays.
async fn prepare_funding(
    bridge: &BridgeRuntime,
    state: &mut BridgeOutState,
    inputs: &BridgeOutInputs<'_>,
) -> Result<(OutboundIntent, Vec<SideCost>), BridgeLegError> {
    let route = inputs.route;
    let source_address = bridge.sources.for_chain(route.source_chain)?;
    let destination = bridge_source_transfer_destination(route.source_chain, &source_address)?;

    let BridgedDepositArrival {
        sizing:
            BridgeDepositSizing {
                transfer_value,
                bridge_amount: transfer_amount,
                source_fee_reserve,
                fee_budget,
            },
        submit_amount,
        expected_amount,
    } = bridged_deposit_arrival(bridge, route, inputs.asset, inputs.size_in).await?;

    if let Some(source_fee_reserve) = source_fee_reserve {
        let source_transfer_amount = ChainTokenAmount::from_raw(inputs.asset.clone(), transfer_value.clone());
        info!(
            "[bridge] liq_id={} bridged deposit reserving reverse bridge source fee budget: reserve={} source_transfer={} bridge_amount={} route={}",
            inputs.leg_id,
            source_fee_reserve.formatted(),
            source_transfer_amount.formatted(),
            transfer_amount.formatted(),
            route_label(route),
        );
    }

    state.deposit_bridge_submit_amount = Some(submit_amount);
    state.deposit_bridge_expected_amount = Some(expected_amount);
    state.deposit_bridge_provider_fee_budget_native_units = fee_budget.provider_fee_budget_native_units.clone();
    Ok((
        OutboundIntent {
            token: inputs.asset.clone(),
            to: destination,
            amount_native: transfer_value,
        },
        fee_budget.side_costs,
    ))
}

/// What one cycle of the submission works with besides the state. Resolved
/// once, so the phase functions read like the state machine they are.
struct Submission<'a> {
    bridge: &'a BridgeRuntime,
    route: &'static BridgeRouteSpec,
    source_address: String,
    /// The provider fee quote saved on first sight, so every call uses it.
    provider_fee: Option<Nat>,
}

impl Submission<'_> {
    /// The amount the bridge can take now, or `None` while the bridge wallet
    /// or its ckETH fee balance is short.
    async fn funding(
        &self,
        leg_id: &str,
        requested: f64,
        fee_budget: &BridgeFeeBudget,
        provider_fee: Option<&Nat>,
    ) -> Result<Option<f64>, String> {
        let route = self.route;
        let source_available = self
            .bridge
            .backend
            .get_source_balance(route.source_asset, route.source_chain, &self.source_address)
            .await?;
        let required_source_available = requested + fee_budget.source_fee_budget;
        if source_available + SOURCE_BALANCE_TOLERANCE < required_source_available {
            debug!(
                "[bridge] liq_id={} bridged deposit waiting for source funding: available={} expected={} tolerance={} source={} route={}",
                leg_id,
                source_available,
                required_source_available,
                SOURCE_BALANCE_TOLERANCE,
                self.source_address,
                route_label(route),
            );
            return Ok(None);
        }

        let amount = if source_available < requested {
            info!(
                "[bridge] liq_id={} adjusting bridged deposit amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={})",
                leg_id,
                requested,
                source_available,
                SOURCE_BALANCE_TOLERANCE,
                self.source_address,
                route_label(route),
            );
            source_available
        } else {
            requested
        };

        if route.route_kind == BridgeRouteKind::CkEthErc20Reverse
            && let Some(required_fee_budget) = provider_fee
        {
            let required_fee_budget = nat_units_to_amount_via_core(required_fee_budget, CKETH_DECIMALS)?;
            let fee_available = self
                .bridge
                .backend
                .get_source_balance(BRIDGE_FEE_TOKEN_SYMBOL, BRIDGE_FEE_TOKEN_CHAIN, &self.source_address)
                .await?;
            if fee_available + SOURCE_BALANCE_TOLERANCE < required_fee_budget {
                debug!(
                    "[bridge] liq_id={} bridged deposit waiting for fee funding: available={} expected={} tolerance={} source={} fee_asset={}@{} route={}",
                    leg_id,
                    fee_available,
                    required_fee_budget,
                    SOURCE_BALANCE_TOLERANCE,
                    self.source_address,
                    BRIDGE_FEE_TOKEN_SYMBOL,
                    BRIDGE_FEE_TOKEN_CHAIN,
                    route_label(route),
                );
                return Ok(None);
            }
        }
        Ok(Some(amount))
    }
}

/// Moves the leg from funds in the bridge wallet to a bridge handle: one cycle
/// saves the provider call, the next sends it.
async fn advance_submission(
    bridge: &BridgeRuntime,
    state: &mut BridgeOutState,
    funded_at_ts: &mut Option<i64>,
    inputs: &BridgeOutInputs<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let route = inputs.route;
    let submission = Submission {
        bridge,
        route,
        source_address: bridge.sources.for_chain(route.source_chain)?,
        provider_fee: state.deposit_bridge_provider_fee_budget_native_units.clone(),
    };
    upgrade_legacy_bridge_call(state, inputs.size_in, &submission)?;
    let _submit_guard =
        acquire_bridge_submit_lock(route.source_asset, route.source_chain, &submission.source_address).await;
    if state.deposit_bridge_call.is_none() {
        return save_call(state, funded_at_ts, &submission, inputs).await;
    }

    match drive_saved_call(bridge, &mut state.deposit_bridge_call).await? {
        SavedCallStep::Submitted(bridge_id) => {
            state.deposit_bridge_id = Some(bridge_id);
            state.deposit_bridge_submitted_at_ts = Some(now_ts());
            state.deposit_bridge_call = None;
            Ok(BridgeLegOutcome::Submitted)
        }
        SavedCallStep::Pending => Ok(BridgeLegOutcome::Waiting { stalled: None }),
        // The next cycle measures the funds again and saves a new call.
        SavedCallStep::Deferred(error) => {
            info!("[bridge] liq_id={} bridged deposit deferred: {}", inputs.leg_id, error);
            wait_before_start(inputs.leg_id, funded_at_ts, &error)
        }
    }
}

/// A wait before the bridge starts, after the funding transfer landed.
fn wait_before_start(
    leg_id: &str,
    funded_at_ts: &mut Option<i64>,
    reason: &str,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let mut wait = WaitBound {
        since: funded_at_ts,
        timeout_secs: BRIDGE_START_TIMEOUT_SECS,
    };
    wait_within_bound(leg_id, &mut wait, "bridged deposit", reason)
}

/// Checks funding and, when it is present, saves the provider call with the
/// history cursor taken before it. The call runs on the next step.
async fn save_call(
    state: &mut BridgeOutState,
    funded_at_ts: &mut Option<i64>,
    submission: &Submission<'_>,
    inputs: &BridgeOutInputs<'_>,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let destination = inputs.destination.destination().await?;
    state.deposit_bridge_destination_snapshot = Some(destination.clone());

    let requested = state
        .deposit_bridge_submit_amount
        .unwrap_or_else(|| inputs.size_in.to_f64());
    let route = submission.route;
    let fee_budget = submission
        .bridge
        .backend
        .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
        .await?;
    let saved_fee = &mut state.deposit_bridge_provider_fee_budget_native_units;
    if saved_fee.is_none() {
        *saved_fee = fee_budget.provider_fee_budget_native_units.clone();
    }
    let provider_fee = saved_fee.clone();
    let Some(amount) = submission
        .funding(inputs.leg_id, requested, &fee_budget, provider_fee.as_ref())
        .await?
    else {
        return wait_before_start(
            inputs.leg_id,
            funded_at_ts,
            "the bridge account or its ckETH fee balance is short",
        );
    };

    let call = BridgeCall {
        asset: route.source_asset.to_string(),
        source_chain: route.source_chain.to_string(),
        source_address: submission.source_address.clone(),
        target_asset: route.target_asset.to_string(),
        destination,
        amount,
        provider_fee_budget_native_units: provider_fee,
        cursor: None,
        started_at_ts: now_ts(),
    };
    state.deposit_bridge_call =
        Some(save_provider_call(submission.bridge, call, "bridged deposit", inputs.leg_id).await?);
    Ok(BridgeLegOutcome::Saved { side_costs: Vec::new() })
}

/// Moves a journal attempt from a row saved before the outbound driver into a
/// saved call. A recorded attempt may have run, so from now on it only settles
/// from the provider history.
fn upgrade_legacy_bridge_call(
    state: &mut BridgeOutState,
    size_in: &ChainTokenAmount,
    submission: &Submission<'_>,
) -> Result<(), String> {
    let unrecorded = std::mem::take(&mut state.deposit_bridge_call_unrecorded);
    let attempt = match state.legacy_bridge_submit_phase.take() {
        Some(LegacyBridgeSubmitPhase::Submitting(attempt)) => attempt,
        Some(LegacyBridgeSubmitPhase::WaitingFunds) => return Ok(()),
        None if unrecorded && state.deposit_bridge_call.is_none() => {
            return park_unrecorded_bridge_call(state, size_in, submission);
        }
        None => return Ok(()),
    };
    let call = BridgeCall {
        source_chain: submission.route.source_chain.to_string(),
        source_address: submission.source_address.clone(),
        target_asset: submission.route.target_asset.to_string(),
        provider_fee_budget_native_units: submission.provider_fee.clone(),
        ..attempt.bridge_call()
    };
    let mut record = OutboundRecord::prepared(prepare_bridge_call(attempt.id.clone(), &call)?);
    record.phase = if attempt.parked_for_operator {
        OutboundPhase::Parked {
            reason: format!(
                "bridge call {} {} to {} was parked by the bridge journal; answer it with reconcile",
                call.amount, call.asset, call.destination
            ),
        }
    } else {
        OutboundPhase::Submitted {
            at: attempt.called_at_ts.unwrap_or(attempt.started_at_ts),
        }
    };
    state.deposit_bridge_call = Some(record);
    Ok(())
}

/// An older build called the provider without a record, so its call may have
/// run. An operator answers the parked call before any new call.
fn park_unrecorded_bridge_call(
    state: &mut BridgeOutState,
    size_in: &ChainTokenAmount,
    submission: &Submission<'_>,
) -> Result<(), String> {
    let call = BridgeCall {
        asset: submission.route.source_asset.to_string(),
        source_chain: submission.route.source_chain.to_string(),
        source_address: submission.source_address.clone(),
        target_asset: submission.route.target_asset.to_string(),
        destination: state.deposit_bridge_destination_snapshot.clone().unwrap_or_default(),
        amount: state.deposit_bridge_submit_amount.unwrap_or_else(|| size_in.to_f64()),
        provider_fee_budget_native_units: submission.provider_fee.clone(),
        cursor: None,
        started_at_ts: now_ts(),
    };
    let id = crate::utils::new_venue_execution_id("bridge-call");
    let mut record = OutboundRecord::prepared(prepare_bridge_call(id, &call)?);
    record.phase = OutboundPhase::Parked {
        reason: format!(
            "an older build may have called the bridge for {} {} without a record; answer it with reconcile",
            call.amount, call.asset
        ),
    };
    state.deposit_bridge_call = Some(record);
    Ok(())
}

/// Reads the bridge status. A completed bridge hands over to the caller; a
/// failed or canceled bridge is an error.
async fn poll(
    bridge: &BridgeRuntime,
    state: &mut BridgeOutState,
    leg_id: &str,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    state.deposit_bridge_polled_at_ts = Some(now_ts());
    let bridge_id = state
        .deposit_bridge_id
        .clone()
        .ok_or_else(|| "missing bridge id while polling bridged deposit".to_string())?;
    let (status, unreadable) = read_bridge_status(bridge, &bridge_id).await?;
    match status {
        BridgeStatus::Completed => Ok(BridgeLegOutcome::Completed { receipt: None }),
        BridgeStatus::Minted(receipt) => Ok(BridgeLegOutcome::Completed { receipt: Some(receipt) }),
        BridgeStatus::Pending | BridgeStatus::Unknown => {
            let stalled = note_bridge_wait(
                leg_id,
                state.deposit_bridge_submitted_at_ts,
                &bridge_id,
                unreadable.as_deref(),
            )?;
            Ok(BridgeLegOutcome::Waiting { stalled })
        }
        ended => Err(bridge_failure("bridged deposit", leg_id, &bridge_id, ended)),
    }
}
