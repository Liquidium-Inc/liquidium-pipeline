//! The exchange leg as the shared leg driver sees it: its state, its step,
//! its settlement wait, and its planning checks.
use std::{collections::HashSet, sync::Mutex};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::resolve_route;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, CexBackendError};
use log::{info, warn};
use serde::{Deserialize, Serialize};

use super::finalizer::CexVenueFinalizer;
use crate::finalizers::bridge_legs::check_bridged_minimum;
use crate::{
    finalizers::{
        bridge_planner::BridgePlanner,
        cex::state::{CexState, CexStep},
        multi_venue::{
            GatedLeg, LegPreview, LegState, OperatorQuestion, StepFlow, VenueAmountError, VenueGate, VenueLeg,
            VenuePlanningContext, VenueStepError,
        },
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueLegFailure, VenueLegState},
    swappers::model::{SwapExecution, SwapRequest},
    utils::now_ts,
};

/// How long a leg keeps re-offering an order that the exchange rejects as not
/// yet tradable before an operator has to look at it. Deposit credit and the
/// matching engine are minutes apart on MEXC, longer than the retry budget.
const SETTLEMENT_WAIT_TIMEOUT_SECS: i64 = 15 * 60;

/// The exchange leg's state plus the persistence gate. The gate keeps its
/// keys at the top level, as every saved row has them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexVenueExecutionState {
    cex: CexState,
    #[serde(flatten)]
    gate: VenueGate,
}

impl GatedLeg for CexVenueExecutionState {
    type State = CexState;
    fn state(&self) -> &CexState {
        &self.cex
    }
    fn state_mut(&mut self) -> &mut CexState {
        &mut self.cex
    }
    fn gate(&self) -> &VenueGate {
        &self.gate
    }
    fn gate_mut(&mut self) -> &mut VenueGate {
        &mut self.gate
    }
}

impl LegState for CexState {
    fn liq_id(&self) -> &str {
        &self.liq_id
    }
    fn step_label(&self) -> String {
        format!("{:?}", self.step)
    }
    fn is_completed(&self) -> bool {
        self.step == CexStep::Completed
    }
    fn is_failed(&self) -> bool {
        self.step == CexStep::Failed
    }
    fn end_below_minimum(&mut self) {
        self.step = CexStep::Failed;
    }
    /// A minimum refusal is the only way an exchange leg fails for good.
    fn failure(&self) -> Option<VenueLegFailure> {
        (self.step == CexStep::Failed).then_some(VenueLegFailure::BelowMinimum)
    }
    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    fn set_last_error(&mut self, error: Option<String>) {
        self.last_error = error;
    }
    fn take_send_saved(&mut self) -> bool {
        std::mem::take(&mut self.send_saved)
    }
    fn funds_left_trader_account(&self) -> bool {
        CexState::funds_left_trader_account(self)
    }
}

/// A pending settlement is the exchange's own wait; the four shared kinds pass through.
impl From<CexBackendError> for VenueStepError {
    fn from(error: CexBackendError) -> Self {
        match error {
            CexBackendError::BelowMinimum(message) => Self::BelowMinimum(message),
            CexBackendError::Ambiguous(message) => Self::Ambiguous(message),
            CexBackendError::Unreachable(message) => Self::Unreachable(message),
            CexBackendError::PendingSettlement(_) | CexBackendError::Other(_) => Self::Other(error.to_string()),
        }
    }
}

impl<B> CexVenueFinalizer<B>
where
    B: CexBackend + 'static,
{
    /// Holds a leg whose funds the exchange reports as not tradable yet. The
    /// deposit is in the account, but the matching engine refuses it; only
    /// the exchange can say when that changes, so the wait has its own bound.
    async fn settlement_hold(&self, state: &mut CexState, error: String) -> Result<StepFlow, VenueStepError> {
        let now = now_ts();
        let waiting_since = *state.trade.trade_settlement_waiting_since_ts.get_or_insert(now);
        let waited = now.saturating_sub(waiting_since);
        if waited < SETTLEMENT_WAIT_TIMEOUT_SECS {
            info!(
                "[cex] liq_id={} waiting for venue settlement: waited={}s timeout={}s err={}",
                state.liq_id, waited, SETTLEMENT_WAIT_TIMEOUT_SECS, error
            );
            return Ok(StepFlow::Hold(error));
        }
        // The wait is over and the venue never released the funds. One balance
        // read tells the operator which case they have: funds present and
        // stuck, or an account that never held them.
        let asset = <Self as BridgePlanner>::planned_deposit_asset(state);
        let observed = match self.backend.get_balance(&asset).await {
            Ok(balance) => format!("{balance}"),
            Err(balance_error) => format!("unavailable ({balance_error})"),
        };
        warn!(
            "[cex] liq_id={} venue settlement wait expired after {}s free_{}={} err={}",
            state.liq_id, waited, asset, observed, error
        );
        Err(VenueStepError::Ambiguous(format!(
            "{} leg not settled after {waited}s: {error} (free {asset}={observed})",
            self.profile.name()
        )))
    }

    #[cfg(test)]
    pub(super) async fn advance_decoded_leg(
        &self,
        execution: CexVenueExecutionState,
        checkpoint: &dyn crate::finalizers::multi_venue::VenueLegCheckpoint,
    ) -> Result<crate::finalizers::multi_venue::VenueLegProgress, String> {
        crate::finalizers::multi_venue::advance_execution(self, execution, checkpoint).await
    }
}

#[async_trait]
impl<B> VenueLeg for CexVenueFinalizer<B>
where
    B: CexBackend + 'static,
{
    type Exec = CexVenueExecutionState;

    fn leg_venue_id(&self) -> &'static str {
        self.profile.venue_id()
    }

    fn venue_name(&self) -> String {
        self.profile.name().to_string()
    }

    fn armed_intents(&self) -> &Mutex<HashSet<String>> {
        &self.multi_venue_armed_intents
    }

    fn new_execution(&self, state: CexState) -> CexVenueExecutionState {
        CexVenueExecutionState {
            cex: state,
            gate: VenueGate::default(),
        }
    }

    fn check_allocation(&self, state: &CexState, leg: &VenueLegState) -> Result<(), String> {
        if state.size_in != leg.request.pay_amount {
            return Err(format!(
                "{} persisted allocation does not match the leg request",
                self.profile.name()
            ));
        }
        if state.withdraw.withdraw_asset.asset_id() != leg.request.receive_asset {
            return Err(format!(
                "{} persisted receive asset does not match the leg request",
                self.profile.name()
            ));
        }
        Ok(())
    }

    /// A restarted leg may go on only from a durable id it can poll. Legacy
    /// plan backfilling can turn a direct step into a bridged one, so the plan
    /// is resolved first.
    fn resume_after_restart(&self, state: &mut CexState, intent_id: &str) -> Result<(), String> {
        self.ensure_plan_on_state(state);
        if state.resume_after_restart(now_ts()) {
            return Ok(());
        }
        Err(format!(
            "persisted {} intent `{intent_id}` has ambiguous submission state after restart",
            self.profile.name()
        ))
    }

    async fn step(&self, state: &mut CexState) -> Result<StepFlow, VenueStepError> {
        match self.advance_current_step(state).await {
            // A step that completes proves the venue is caught up, so a later
            // wait starts from its own first refusal.
            Ok(()) => {
                state.trade.trade_settlement_waiting_since_ts = None;
                Ok(StepFlow::Done)
            }
            Err(error @ CexBackendError::PendingSettlement(_)) => self.settlement_hold(state, error.to_string()).await,
            Err(error) => Err(error.into()),
        }
    }

    fn finish(&self, state: &CexState) -> Result<SwapExecution, String> {
        self.finish_state(state)
    }

    /// The CEX machine uses its Pending phases as durable restart points, so
    /// recovery accepts them even without a last error.
    fn may_recover(&self, state: &CexState) -> Result<(), String> {
        if state.last_error.is_none()
            && !matches!(
                state.step,
                CexStep::DepositPending | CexStep::TradePending | CexStep::WithdrawPending | CexStep::Failed
            )
        {
            return Err(format!(
                "{} leg at {:?} is not in recovery",
                self.profile.name(),
                state.step
            ));
        }
        Ok(())
    }

    fn questions(&self, state: &CexState) -> Vec<OperatorQuestion> {
        Self::operator_questions_for(state)
    }

    fn answer(&self, state: &mut CexState, answer: Option<&OperatorDecision>) -> Result<(), String> {
        Self::answer_operator(state, answer)
    }

    fn check_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err("token registry is required for amount-scoped CEX previews".to_string());
        }
        Ok(())
    }

    async fn check_leg_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        let state = self
            .prepare_swap_request("amount-check", request)
            .map_err(VenueAmountError::Unavailable)?;
        let Some(route) = resolve_route(
            &state.deposit.deposit_asset.symbol(),
            &state.deposit.deposit_asset.chain(),
            &<Self as BridgePlanner>::planned_deposit_asset(&state),
        ) else {
            return Ok(());
        };
        let bridge = self.bridge_runtime().map_err(VenueAmountError::Unavailable)?;
        check_bridged_minimum(bridge, route, &state.deposit.deposit_asset, &state.size_in).await
    }

    /// The amount-scoped CEX preview; route preparation stays in the CEX machine.
    async fn preview_leg(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<LegPreview<CexState>, String> {
        let execution_id = crate::utils::new_venue_execution_id(self.profile.venue_id());
        let preview = self.preview_swap_request(&execution_id, request).await?;
        Ok(LegPreview {
            state: preview.state,
            quote: preview.quote,
            conservative_receive: preview.conservative_receive,
            market_receive: preview.market_receive,
            side_costs: preview.side_costs,
        })
    }
}

#[cfg(test)]
#[path = "multi_venue_adapter_tests.rs"]
mod tests;
