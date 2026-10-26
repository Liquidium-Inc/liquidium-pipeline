//! A bridged deposit's path from funds in the bridge account to a bridge
//! handle. The phase is persisted before the bridge call, so a restart can
//! tell a long funding wait from a call whose handle was lost.
use candid::Nat;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BRIDGE_SUBMISSION_DEFERRED_PREFIX, BridgeFeeBudget, BridgeRequest, BridgeRouteKind, BridgeRouteSpec,
};
use liquidium_pipeline_connectors::backend::SIDE_EFFECT_AMBIGUOUS_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use log::{debug, info, warn};

use super::mexc_finalizer::{
    BRIDGE_FEE_TOKEN_CHAIN, BRIDGE_FEE_TOKEN_SYMBOL, CKETH_DECIMALS, MexcFinalizer,
    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE, nat_to_units,
};
use crate::{
    finalizers::{
        bridge_planner::BridgePlanner,
        bridge_runtime::BridgeRuntime,
        cex_finalizer::{
            BridgeSubmitAttempt, BridgeSubmitPhase, CexState, CexStep,
        },
    },
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    utils::now_ts,
};

/// How long an empty lookup waits before the row parks. The IC drops an
/// unexecuted ingress after about five minutes, but a call that started can
/// still finish later, so the wait is a grace period and not a proof.
const LOST_BRIDGE_CALL_GRACE_SECS: i64 = 10 * 60;
/// Rows written this long before the attempt may hold a handle after its cursor.
const CLAIM_SCAN_MARGIN_SECS: i64 = 60 * 60;

enum LostCall {
    Found(String),
    StillPossible,
    NeverLanded,
}

/// What one cycle of a bridged deposit works with besides the state. Resolved
/// once, so the phase methods read like the state machine they are.
struct BridgedDeposit<'a> {
    bridge: &'a BridgeRuntime,
    route: &'static BridgeRouteSpec,
    source_address: String,
    fee_budget: BridgeFeeBudget,
    provider_fee: Option<Nat>,
}

impl BridgedDeposit<'_> {
    /// The amount the bridge can take now, or `None` while the shared bridge
    /// account or its ckETH fee balance is short.
    async fn funding(&self, liq_id: &str, requested: f64) -> Result<Option<f64>, String> {
        let route = self.route;
        let source_available = self
            .bridge
            .backend
            .get_source_balance(route.source_asset, route.source_chain, &self.source_address)
            .await?;
        let required_source_available = requested + self.fee_budget.source_fee_budget;
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
            && let Some(required_fee_budget) = self.provider_fee.as_ref()
        {
            let required_fee_budget = nat_to_units(required_fee_budget, CKETH_DECIMALS)?;
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

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// Moves a bridged deposit through the funding wait, the persisted attempt
    /// and the bridge call. Every exit leaves the step pending.
    pub(super) async fn advance_bridged_deposit_submission(
        &self,
        state: &mut CexState,
        bridge: &BridgeRuntime,
        route: &'static BridgeRouteSpec,
        deposit_fee_budget: Option<BridgeFeeBudget>,
    ) -> Result<(), String> {
        state.step = CexStep::DepositPending;
        let deposit = self.bridged_deposit(state, bridge, route, deposit_fee_budget).await?;
        let _submit_guard =
            acquire_bridge_submit_lock(route.source_asset, route.source_chain, &deposit.source_address).await;

        let mut attempt = match state.deposit.bridge.deposit_bridge_submit_phase.clone() {
            Some(BridgeSubmitPhase::Submitting(attempt)) => attempt,
            _ => return self.record_bridge_attempt(state, &deposit).await,
        };

        if attempt.called_at_ts.is_some() {
            let lookup = self.find_lost_bridge_call(state, &deposit, &attempt).await;
            if lookup
                .as_ref()
                .is_err_and(|error| error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX))
            {
                state.deposit.bridge.mark_bridge_attempt_parked();
            }
            match lookup? {
                LostCall::Found(bridge_id) => {
                    warn!(
                        "[mexc] liq_id={} attached bridge handle {} found on the provider for a lost call",
                        state.liq_id, bridge_id
                    );
                    Self::attach_bridge_handle(state, bridge_id);
                }
                LostCall::StillPossible => {
                    info!(
                        "[mexc] liq_id={} lost bridge call not visible yet; waiting before a new attempt",
                        state.liq_id
                    );
                }
                LostCall::NeverLanded => {
                    info!(
                        "[mexc] liq_id={} operator cleared the lost bridge call; funds are rechecked next cycle",
                        state.liq_id
                    );
                    state.deposit.bridge.wait_for_funds();
                }
            }
            return Ok(());
        }

        if deposit.funding(&state.liq_id, attempt.amount).await? != Some(attempt.amount) {
            // Another row spent the shared balance since the attempt was recorded.
            state.deposit.bridge.wait_for_funds();
            return Ok(());
        }

        // Persisted with the error if the call fails, so the retry looks it up first.
        attempt.called_at_ts = Some(now_ts());
        state.deposit.bridge.record_attempt(attempt.clone());
        match bridge
            .backend
            .submit_bridge(Self::bridge_request(&deposit, &attempt)?)
            .await
        {
            Ok(submission) => {
                Self::attach_bridge_handle(state, submission.bridge_id);
                Ok(())
            }
            // A busy bridge wallet is a wait, not a failure. Nothing was sent.
            Err(error) if error.starts_with(BRIDGE_SUBMISSION_DEFERRED_PREFIX) => {
                info!("[mexc] liq_id={} bridged deposit deferred: {}", state.liq_id, error);
                state.deposit.bridge.wait_for_funds();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Resolves the route's source account and fee budgets. The provider fee
    /// quote is persisted on first sight so every later cycle submits with it.
    async fn bridged_deposit<'a>(
        &self,
        state: &mut CexState,
        bridge: &'a BridgeRuntime,
        route: &'static BridgeRouteSpec,
        deposit_fee_budget: Option<BridgeFeeBudget>,
    ) -> Result<BridgedDeposit<'a>, String> {
        let source_address = self.resolve_bridge_source_address(route.source_chain)?;
        let fee_budget = match deposit_fee_budget {
            Some(fee_budget) => fee_budget,
            None => {
                bridge
                    .backend
                    .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
                    .await?
            }
        };
        let provider_fee = &mut state.deposit.bridge.deposit_bridge_provider_fee_budget_native_units;
        if provider_fee.is_none() {
            *provider_fee = fee_budget.provider_fee_budget_native_units.clone();
        }
        let provider_fee = provider_fee.clone();
        Ok(BridgedDeposit {
            bridge,
            route,
            source_address,
            fee_budget,
            provider_fee,
        })
    }

    /// Checks funding and, when it is present, persists what the next cycle
    /// will submit. The bridge is not called here.
    async fn record_bridge_attempt(&self, state: &mut CexState, deposit: &BridgedDeposit<'_>) -> Result<(), String> {
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
        let Some(amount) = deposit.funding(&state.liq_id, requested).await? else {
            state.deposit.bridge.wait_for_funds();
            return Ok(());
        };

        let attempt = BridgeSubmitAttempt {
            id: crate::utils::new_venue_execution_id("bridge-attempt"),
            asset: deposit.route.source_asset.to_string(),
            destination: cex_destination.address,
            amount,
            cursor: None,
            started_at_ts: now_ts(),
            called_at_ts: None,
            parked_for_operator: false,
        };
        let cursor = deposit
            .bridge
            .backend
            .prepare_submission(&Self::bridge_request(deposit, &attempt)?)
            .await?;
        info!(
            "[mexc] liq_id={} bridged deposit funded; attempt recorded amount={} destination={} cursor={:?}",
            state.liq_id, attempt.amount, attempt.destination, cursor
        );
        state
            .deposit
            .bridge
            .record_attempt(BridgeSubmitAttempt { cursor, ..attempt });
        Ok(())
    }

    /// Looks the attempt up on the provider. Exactly one unclaimed record that
    /// no other open attempt can own is this row's handle. An empty lookup
    /// waits a grace period and then parks: only an operator can rule out a
    /// call that is still running. Anything else parks at once. A leg that
    /// runs again after a park was released by an operator.
    async fn find_lost_bridge_call(
        &self,
        state: &CexState,
        deposit: &BridgedDeposit<'_>,
        attempt: &BridgeSubmitAttempt,
    ) -> Result<LostCall, String> {
        let ambiguous = |reason: String| {
            format!(
                "{SIDE_EFFECT_AMBIGUOUS_PREFIX}: liq_id {} bridge attempt {} {} -> {} (cursor {:?}): {reason}; operator must attach a handle or clear the attempt",
                state.liq_id, attempt.amount, attempt.asset, attempt.destination, attempt.cursor
            )
        };
        let Some(cursor) = attempt.cursor else {
            return Err(ambiguous("the provider cannot look up lost submissions".into()));
        };
        let claims = deposit
            .bridge
            .claims
            .claims_since(attempt.started_at_ts - CLAIM_SCAN_MARGIN_SECS)
            .await
            .map_err(|error| ambiguous(format!("claim registry unavailable: {error}")))?;
        let candidates = deposit
            .bridge
            .backend
            .find_submissions(&Self::bridge_request(deposit, attempt)?, cursor, &claims.bridge_ids)
            .await?;

        if candidates.is_empty() {
            if attempt.parked_for_operator {
                return Ok(LostCall::NeverLanded);
            }
            let called_at = attempt.called_at_ts.unwrap_or(attempt.started_at_ts);
            let waited = now_ts() - called_at;
            if waited < LOST_BRIDGE_CALL_GRACE_SECS {
                return Ok(LostCall::StillPossible);
            }
            return Err(ambiguous(format!(
                "no provider record after {waited}s; confirm on the minter that no withdrawal of this amount exists, then re-enqueue to submit again"
            )));
        }
        let contested = candidates.iter().any(|candidate| {
            claims
                .open_attempts
                .iter()
                .filter(|other| other.id != attempt.id)
                .any(|other| other.may_own(attempt, candidate.sequence))
        });
        if candidates.len() == 1 && !contested {
            return Ok(LostCall::Found(candidates[0].bridge_id.clone()));
        }
        let ids: Vec<&str> = candidates
            .iter()
            .map(|candidate| candidate.bridge_id.as_str())
            .collect();
        Err(ambiguous(format!(
            "{} unclaimed provider records match {:?}{}",
            candidates.len(),
            ids,
            if contested {
                " and another open attempt could own one"
            } else {
                ""
            }
        )))
    }

    fn bridge_request(deposit: &BridgedDeposit<'_>, attempt: &BridgeSubmitAttempt) -> Result<BridgeRequest, String> {
        let route = deposit.route;
        Ok(BridgeRequest {
            asset: route.source_asset.to_string(),
            source_chain: route.source_chain.to_string(),
            source_address: deposit.source_address.clone(),
            target_asset: route.target_asset.to_string(),
            destination: <Self as BridgePlanner>::route_destination_for_deposit(
                route.destination_kind,
                &attempt.destination,
            )?,
            amount: attempt.amount,
            provider_fee_budget_native_units: deposit.provider_fee.clone(),
        })
    }

    fn attach_bridge_handle(state: &mut CexState, bridge_id: String) {
        state.deposit.bridge.deposit_bridge_id = Some(bridge_id);
        state.deposit.bridge.deposit_bridge_submitted_at_ts = Some(now_ts());
        state.deposit.bridge.deposit_bridge_submit_phase = None;
        state.trade.trade_next_amount_in = state.deposit.bridge.deposit_bridge_expected_amount;
        state.step = CexStep::DepositPending;
    }
}
