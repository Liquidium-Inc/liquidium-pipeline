//! A bridged deposit's path from funds in the bridge account to a bridge
//! handle. The provider call is a saved send: the outbound driver saves it
//! before it runs, and settles a lost answer from the provider history.
use candid::Nat;
use liquidium_pipeline_connectors::backend::amount_utils::nat_units_to_amount_via_core;
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeFeeBudget, BridgeRouteKind, BridgeRouteSpec};
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, CexBackendError};
use log::{debug, info};

use super::finalizer::{
    BRIDGE_FEE_TOKEN_CHAIN, BRIDGE_FEE_TOKEN_SYMBOL, CKETH_DECIMALS, CexVenueFinalizer,
    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
};
use crate::{
    bridge::{
        call_sender::{BridgeCall, BridgeCallSender, prepare_bridge_call},
        legacy::LegacyBridgeSubmitPhase,
        runtime::BridgeRuntime,
    },
    finalizers::{
        bridge_planner::BridgePlanner,
        cex::state::{CexState, CexStep},
    },
    outbound::driver::{OutboundError, OutboundPhase, OutboundRecord, step_outbound},
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    utils::now_ts,
};

/// What one cycle of a bridged deposit works with besides the state. Resolved
/// once, so the phase methods read like the state machine they are.
struct BridgedDeposit<'a> {
    bridge: &'a BridgeRuntime,
    route: &'static BridgeRouteSpec,
    source_address: String,
    /// The provider fee quote saved on first sight, so every call uses it.
    provider_fee: Option<Nat>,
}

impl BridgedDeposit<'_> {
    /// The amount the bridge can take now, or `None` while the shared bridge
    /// account or its ckETH fee balance is short.
    async fn funding(
        &self,
        liq_id: &str,
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
        if source_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < required_source_available {
            debug!(
                "[mexc] liq_id={} bridged deposit waiting for source funding: available={} expected={} tolerance={} source={} route={}@{}->{}",
                liq_id,
                source_available,
                required_source_available,
                WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                self.source_address,
                route.source_asset,
                route.source_chain,
                route.target_asset,
            );
            return Ok(None);
        }

        let amount = if source_available < requested {
            info!(
                "[mexc] liq_id={} adjusting bridged deposit amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={}@{}->{})",
                liq_id,
                requested,
                source_available,
                WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                self.source_address,
                route.source_asset,
                route.source_chain,
                route.target_asset,
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
            if fee_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < required_fee_budget {
                debug!(
                    "[mexc] liq_id={} bridged deposit waiting for fee funding: available={} expected={} tolerance={} source={} fee_asset={}@{} route={}@{}->{}",
                    liq_id,
                    fee_available,
                    required_fee_budget,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    self.source_address,
                    BRIDGE_FEE_TOKEN_SYMBOL,
                    BRIDGE_FEE_TOKEN_CHAIN,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                return Ok(None);
            }
        }
        Ok(Some(amount))
    }
}

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// Moves a bridged deposit from funds in the bridge account to a bridge
    /// handle: one cycle saves the provider call, the next sends it.
    pub(super) async fn advance_bridged_deposit_submission(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &'static BridgeRouteSpec,
    ) -> Result<(), CexBackendError> {
        state.step = CexStep::DepositPending;
        let deposit = BridgedDeposit {
            bridge,
            route,
            source_address: self.resolve_bridge_source_address(route.source_chain)?,
            provider_fee: state
                .deposit
                .bridge
                .deposit_bridge_provider_fee_budget_native_units
                .clone(),
        };
        Self::upgrade_legacy_bridge_call(state, &deposit)?;
        let _submit_guard =
            acquire_bridge_submit_lock(route.source_asset, route.source_chain, &deposit.source_address).await;
        if state.deposit.bridge.deposit_bridge_call.is_none() {
            return Ok(self.save_bridge_call(state, &deposit).await?);
        }

        let sender = BridgeCallSender::new(bridge.backend.clone(), bridge.claims.clone());
        match step_outbound(&mut state.deposit.bridge.deposit_bridge_call, &sender, None, now_ts()).await {
            Ok(Some(bridge_id)) => {
                Self::attach_bridge_handle(state, bridge_id);
                Ok(())
            }
            Ok(None) => Ok(()),
            // A busy bridge wallet is a wait. The next cycle measures the
            // funds again and saves a new call.
            Err(OutboundError::Deferred(error)) => {
                info!("[mexc] liq_id={} bridged deposit deferred: {}", state.liq_id, error);
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Checks funding and, when it is present, saves the provider call with
    /// the history cursor taken before it. The call runs on the next step.
    async fn save_bridge_call(&self, state: &mut CexState, deposit: &BridgedDeposit<'_>) -> Result<(), String> {
        let planned_asset = <Self as BridgePlanner>::planned_deposit_asset(state);
        let planned_network = <Self as BridgePlanner>::planned_deposit_network(state);
        let cex_destination = self
            .backend
            .get_deposit_address(&planned_asset, &planned_network)
            .await?;
        state.deposit.bridge.deposit_bridge_destination_snapshot = Some(cex_destination.address.clone());

        let requested = state
            .deposit
            .bridge
            .deposit_bridge_submit_amount
            .unwrap_or_else(|| state.size_in.to_f64());
        let route = deposit.route;
        let fee_budget = deposit
            .bridge
            .backend
            .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
            .await?;
        let saved_fee = &mut state.deposit.bridge.deposit_bridge_provider_fee_budget_native_units;
        if saved_fee.is_none() {
            *saved_fee = fee_budget.provider_fee_budget_native_units.clone();
        }
        let provider_fee = saved_fee.clone();
        let Some(amount) = deposit
            .funding(&state.liq_id, requested, &fee_budget, provider_fee.as_ref())
            .await?
        else {
            return Ok(());
        };

        let mut call = BridgeCall {
            asset: route.source_asset.to_string(),
            source_chain: route.source_chain.to_string(),
            source_address: deposit.source_address.clone(),
            target_asset: route.target_asset.to_string(),
            destination: cex_destination.address,
            amount,
            provider_fee_budget_native_units: provider_fee,
            cursor: None,
            started_at_ts: now_ts(),
        };
        call.cursor = deposit.bridge.backend.prepare_submission(&call.request()?).await?;
        info!(
            "[mexc] liq_id={} bridged deposit funded; call saved amount={} destination={} cursor={:?}",
            state.liq_id, call.amount, call.destination, call.cursor
        );
        let id = crate::utils::new_venue_execution_id("bridge-call");
        state.deposit.bridge.deposit_bridge_call = Some(OutboundRecord::prepared(prepare_bridge_call(id, &call)?));
        state.send_saved = true;
        Ok(())
    }

    /// Moves a journal attempt from a row saved before the outbound driver
    /// into a saved call. A recorded attempt may have run, so from now on it
    /// only settles from the provider history.
    fn upgrade_legacy_bridge_call(state: &mut CexState, deposit: &BridgedDeposit<'_>) -> Result<(), String> {
        let unrecorded = std::mem::take(&mut state.deposit.bridge.deposit_bridge_call_unrecorded);
        let attempt = match state.deposit.bridge.legacy_bridge_submit_phase.take() {
            Some(LegacyBridgeSubmitPhase::Submitting(attempt)) => attempt,
            Some(LegacyBridgeSubmitPhase::WaitingFunds) => return Ok(()),
            None if unrecorded && state.deposit.bridge.deposit_bridge_call.is_none() => {
                return Self::park_unrecorded_bridge_call(state, deposit);
            }
            None => return Ok(()),
        };
        let call = BridgeCall {
            source_chain: deposit.route.source_chain.to_string(),
            source_address: deposit.source_address.clone(),
            target_asset: deposit.route.target_asset.to_string(),
            provider_fee_budget_native_units: deposit.provider_fee.clone(),
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
        state.deposit.bridge.deposit_bridge_call = Some(record);
        Ok(())
    }

    /// An older build called the provider without a record, so its call may
    /// have run. An operator answers the parked call before any new call.
    fn park_unrecorded_bridge_call(state: &mut CexState, deposit: &BridgedDeposit<'_>) -> Result<(), String> {
        let bridge = &state.deposit.bridge;
        let call = BridgeCall {
            asset: deposit.route.source_asset.to_string(),
            source_chain: deposit.route.source_chain.to_string(),
            source_address: deposit.source_address.clone(),
            target_asset: deposit.route.target_asset.to_string(),
            destination: bridge.deposit_bridge_destination_snapshot.clone().unwrap_or_default(),
            amount: bridge
                .deposit_bridge_submit_amount
                .unwrap_or_else(|| state.size_in.to_f64()),
            provider_fee_budget_native_units: deposit.provider_fee.clone(),
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
        state.deposit.bridge.deposit_bridge_call = Some(record);
        Ok(())
    }

    pub(super) fn attach_bridge_handle(state: &mut CexState, bridge_id: String) {
        state.deposit.bridge.deposit_bridge_id = Some(bridge_id);
        state.deposit.bridge.deposit_bridge_submitted_at_ts = Some(now_ts());
        state.deposit.bridge.deposit_bridge_call = None;
        state.trade.trade_next_amount_in = state.deposit.bridge.deposit_bridge_expected_amount;
        state.step = CexStep::DepositPending;
    }
}
