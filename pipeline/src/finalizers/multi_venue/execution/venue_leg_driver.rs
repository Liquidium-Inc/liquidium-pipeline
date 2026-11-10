//! One driver for every venue leg that runs behind the persistence gate:
//! decode the leg, open the gate, run one step, checkpoint a saved send,
//! judge the outcome, park, recover. A venue supplies the step and its rules.
use std::{collections::HashSet, sync::Mutex};

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::{tokens::chain_token_amount::ChainTokenAmount, types::side_cost::SideCost};
use serde::{Serialize, de::DeserializeOwned};

use super::venue_gate::{GateOpening, StepVerdict, VenueGate, VenueStepError};
use crate::{
    finalizers::multi_venue::{
        MultiVenueAdapter, OperatorQuestion, VenueAmountError, VenueLegCheckpoint, VenueLegProgress,
        VenuePlanningContext, VenueRoutePreview,
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueExecutionState, VenueLegFailure, VenueLegState, VenueLegStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapRequest},
};

/// The saved state of one venue leg, as the driver reads it.
pub trait LegState: Clone + Send + Sync {
    fn liq_id(&self) -> &str;
    /// The step, for logs.
    fn step_label(&self) -> String;
    fn is_completed(&self) -> bool;
    fn is_failed(&self) -> bool;
    /// Ends the leg under a route minimum, with its funds still in the trader account.
    fn end_below_minimum(&mut self);
    /// Why a failed leg failed.
    fn failure(&self) -> Option<VenueLegFailure>;
    fn last_error(&self) -> Option<&str>;
    fn set_last_error(&mut self, error: Option<String>);
    /// True once when a step saved a send or an order input in this cycle.
    /// The driver writes the leg before the step acts on it.
    fn take_send_saved(&mut self) -> bool;
    /// Once the funds left the trader account, a minimum refusal cannot end
    /// the leg: the funds are elsewhere and an operator must settle them.
    fn funds_left_trader_account(&self) -> bool;
}

/// A leg's state next to its gate, as the row saves it. Each venue keeps its
/// own key for the state, so saved rows stay readable.
pub trait GatedLeg: Clone + Serialize + DeserializeOwned + Send + Sync {
    type State: LegState;
    fn state(&self) -> &Self::State;
    fn state_mut(&mut self) -> &mut Self::State;
    fn gate(&self) -> &VenueGate;
    fn gate_mut(&mut self) -> &mut VenueGate;
}

/// What one step reported when it did not fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepFlow {
    /// The step did its work, or has nothing to do yet.
    Done,
    /// The venue holds the funds and says to wait. The leg stays running,
    /// spends no retry budget and shows the message in the TUI.
    Hold(String),
}

/// The amount-scoped preview of a venue, before the driver wraps it.
pub struct LegPreview<S> {
    pub state: S,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
    /// The output at the venue's own price for the whole allocation, before
    /// fixed transfer fees.
    pub market_receive: Nat,
    pub side_costs: Vec<SideCost>,
}

/// A venue's own part of a leg. Everything else is the driver's.
#[async_trait]
pub trait VenueLeg: Send + Sync {
    type Exec: GatedLeg + 'static;

    fn leg_venue_id(&self) -> &'static str;
    /// The venue as messages name it.
    fn venue_name(&self) -> String;
    /// Intents armed in this process. A leg whose intent is not here restarted.
    fn armed_intents(&self) -> &Mutex<HashSet<String>>;
    /// A fresh leg from a preview, with its gate closed.
    fn new_execution(&self, state: <Self::Exec as GatedLeg>::State) -> Self::Exec;
    /// Checks a decoded leg against the request the parent plan committed.
    fn check_allocation(&self, state: &<Self::Exec as GatedLeg>::State, leg: &VenueLegState) -> Result<(), String>;
    /// Prepares a leg whose intent was armed in another process. An error
    /// parks the leg with that reason.
    fn resume_after_restart(&self, state: &mut <Self::Exec as GatedLeg>::State, intent_id: &str)
    -> Result<(), String>;
    /// One step of the venue's own flow.
    async fn step(&self, state: &mut <Self::Exec as GatedLeg>::State) -> Result<StepFlow, VenueStepError>;
    /// The result of a completed leg.
    fn finish(&self, state: &<Self::Exec as GatedLeg>::State) -> Result<SwapExecution, String>;
    /// Whether a re-enqueued leg at this step may be recovered.
    #[allow(dead_code)]
    fn may_recover(&self, state: &<Self::Exec as GatedLeg>::State) -> Result<(), String> {
        let _ = state;
        Ok(())
    }
    /// Venue bookkeeping once a park is released.
    fn after_release(&self, state: &mut <Self::Exec as GatedLeg>::State) {
        let _ = state;
    }
    fn questions(&self, state: &<Self::Exec as GatedLeg>::State) -> Vec<OperatorQuestion>;
    fn answer(
        &self,
        state: &mut <Self::Exec as GatedLeg>::State,
        answer: Option<&OperatorDecision>,
    ) -> Result<(), String>;

    fn check_configuration(&self) -> Result<(), String> {
        Ok(())
    }
    async fn check_leg_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        let _ = request;
        Ok(())
    }
    async fn preview_leg(
        &self,
        context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<LegPreview<<Self::Exec as GatedLeg>::State>, String>;
}

/// The driver's own steps, written once for every venue leg.
trait Driver: VenueLeg {
    /// Decodes this leg only and checks it against the committed request.
    fn decode(&self, leg: &VenueLegState) -> Result<Self::Exec, String> {
        if leg.venue_id != self.leg_venue_id() {
            return Err(format!(
                "{} adapter cannot advance venue `{}`",
                self.venue_name(),
                leg.venue_id
            ));
        }
        let exec = leg
            .execution
            .decode::<Self::Exec>(self.leg_venue_id())?
            .ok_or_else(|| format!("{} leg execution state decoded to the wrong venue", self.venue_name()))?;
        self.check_allocation(exec.state(), leg)?;
        Ok(exec)
    }

    fn progress(&self, exec: Self::Exec, error: Option<String>) -> Result<VenueLegProgress, String> {
        let state = exec.state();
        let status = if state.is_completed() {
            VenueLegStatus::Completed
        } else if state.is_failed() {
            VenueLegStatus::FailedPermanent
        } else {
            VenueLegStatus::Running
        };
        let result = if state.is_completed() {
            Some(self.finish(state)?)
        } else {
            None
        };
        let last_error = error.clone().or_else(|| state.last_error().map(str::to_string));
        // A permanently failed leg keeps its error but never calls it retryable.
        let retryable_error = match status {
            VenueLegStatus::FailedPermanent => None,
            _ => error,
        };
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.leg_venue_id(), &exec)?,
            status,
            result,
            last_error,
            retryable_error,
            failure: state.failure(),
        })
    }

    /// A leg that keeps running with a venue's own wait, outside the retry budget.
    fn held(&self, exec: Self::Exec, message: String) -> Result<VenueLegProgress, String> {
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.leg_venue_id(), &exec)?,
            status: VenueLegStatus::Running,
            result: None,
            last_error: Some(message),
            retryable_error: None,
            failure: None,
        })
    }

    fn park(&self, mut exec: Self::Exec, error: String) -> Result<VenueLegProgress, String> {
        exec.gate_mut().operator_required = true;
        exec.state_mut().set_last_error(Some(error.clone()));
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.leg_venue_id(), &exec)?,
            status: VenueLegStatus::OperatorRequired,
            result: None,
            last_error: Some(error),
            retryable_error: None,
            failure: None,
        })
    }

    fn release(&self, exec: &mut Self::Exec) {
        exec.gate_mut().release_park();
        self.after_release(exec.state_mut());
    }
}

impl<V: VenueLeg> Driver for V {}

/// Advances exactly one step behind the gate. It never touches the parent row.
pub async fn advance_execution<V: VenueLeg>(
    venue: &V,
    mut exec: V::Exec,
    checkpoint: &dyn VenueLegCheckpoint,
) -> Result<VenueLegProgress, String> {
    if exec.state().is_completed() {
        return venue.progress(exec, None);
    }
    if exec.state().is_failed() {
        let last_error = exec.state().last_error().map(str::to_string);
        return venue.progress(exec, last_error);
    }
    if exec.gate().operator_required {
        let error = exec
            .state()
            .last_error()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{} leg requires operator reconciliation", venue.venue_name()));
        return venue.park(exec, error);
    }

    let intent_id = match exec.gate_mut().open(venue.leg_venue_id(), venue.armed_intents())? {
        GateOpening::Armed => return venue.progress(exec, None),
        GateOpening::Open { intent_id, armed_here } => {
            if !armed_here && let Err(reason) = venue.resume_after_restart(exec.state_mut(), &intent_id) {
                return venue.park(exec, reason);
            }
            intent_id
        }
    };
    exec.gate_mut().close();
    exec.state_mut().set_last_error(None);

    let mut outcome = venue.step(exec.state_mut()).await;
    // A step that saved a send writes it to the WAL with the gate still set,
    // then acts on it in the same cycle. A crash between restarts into the
    // saved record.
    if matches!(outcome, Ok(StepFlow::Done)) && exec.state_mut().take_send_saved() {
        let mut saved = exec.clone();
        *saved.gate_mut() = exec.gate().held(intent_id);
        checkpoint.checkpoint(venue.progress(saved, None)?).await?;
        outcome = venue.step(exec.state_mut()).await;
        let _ = exec.state_mut().take_send_saved();
    }

    let outcome = match outcome {
        Ok(StepFlow::Done) => Ok(()),
        Ok(StepFlow::Hold(message)) => {
            exec.gate_mut().unreachable_since_ts = None;
            exec.state_mut().set_last_error(Some(message.clone()));
            return venue.held(exec, message);
        }
        Err(error) => Err(error),
    };
    if let Err(error) = &outcome {
        exec.state_mut().set_last_error(Some(error.to_string()));
    }
    let funds_left = exec.state().funds_left_trader_account();
    let context = format!("liq_id={} step={}", exec.state().liq_id(), exec.state().step_label());
    match exec
        .gate_mut()
        .judge(outcome, funds_left, venue.leg_venue_id(), &context)
    {
        StepVerdict::Continue | StepVerdict::Wait => venue.progress(exec, None),
        StepVerdict::Retry(message) => venue.progress(exec, Some(message)),
        StepVerdict::Park(message) => venue.park(exec, message),
        StepVerdict::EndBelowMinimum(message) => {
            exec.state_mut().end_below_minimum();
            venue.progress(exec, Some(message))
        }
    }
}

#[async_trait]
impl<V: VenueLeg + 'static> MultiVenueAdapter for V {
    fn venue_id(&self) -> &'static str {
        self.leg_venue_id()
    }

    fn validate_configuration(&self) -> Result<(), String> {
        self.check_configuration()
    }

    async fn check_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        self.check_leg_amount(request).await
    }

    async fn preview(
        &self,
        context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        let preview = self.preview_leg(context, request).await?;
        let exec = self.new_execution(preview.state);
        Ok(VenueRoutePreview {
            market_receive: Some(preview.market_receive),
            side_costs: preview.side_costs,
            ..VenueRoutePreview::new(
                self.leg_venue_id(),
                request.clone(),
                preview.quote,
                preview.conservative_receive,
                VenueExecutionState::new(self.leg_venue_id(), &exec)?,
            )
        })
    }

    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        advance_execution(self, self.decode(leg)?, checkpoint).await
    }

    fn operator_questions(&self, leg: &VenueLegState) -> Result<Vec<OperatorQuestion>, String> {
        let exec = self.decode(leg)?;
        Ok(self.questions(exec.state()))
    }

    fn operator_release(
        &self,
        leg: &VenueLegState,
        answer: Option<&OperatorDecision>,
    ) -> Result<VenueExecutionState, String> {
        let mut exec = self.decode(leg)?;
        self.answer(exec.state_mut(), answer)?;
        self.release(&mut exec);
        VenueExecutionState::new(self.leg_venue_id(), &exec)
    }

    /// A re-enqueued parked leg is the operator's authorization to arm a new
    /// intent. This only arms it; the venue acts on a later cycle.
    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut exec = self.decode(leg)?;
        if exec.state().is_completed() {
            return Err(format!(
                "completed {} leg does not require recovery",
                self.venue_name()
            ));
        }
        if exec.gate().operator_required {
            self.release(&mut exec);
        }
        self.may_recover(exec.state())?;
        advance_execution(self, exec, checkpoint).await
    }
}
