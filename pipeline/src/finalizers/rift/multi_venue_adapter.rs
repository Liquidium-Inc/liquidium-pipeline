//! The Rift leg as the multi-venue planner and orchestrator see it: a preview,
//! then one step per cycle behind the persistence gate.
use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::rift_backend::RiftBackend;
use serde::{Deserialize, Serialize};

use super::{
    finalizer::RiftVenueFinalizer,
    state::{RiftState, RiftStep},
};
use crate::{
    finalizers::multi_venue::{
        GateOpening, MultiVenueAdapter, OperatorQuestion, RIFT_VENUE_ID, StepVerdict, VenueAmountError, VenueGate,
        VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext, VenueRoutePreview,
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueExecutionState, VenueLegFailure, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
    utils::now_ts,
};

/// The leg's state plus the persistence gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RiftVenueExecutionState {
    rift: RiftState,
    #[serde(flatten)]
    gate: VenueGate,
}

impl RiftVenueExecutionState {
    /// Clears a park. A release while waiting for Rift starts the wait bounds again.
    fn release_park(&mut self) {
        self.gate.release_park();
        if self.rift.step == RiftStep::Wait
            && let Some(order) = self.rift.order.as_mut()
        {
            order.wait_since_ts = Some(now_ts());
        }
    }
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    async fn preview_leg_request(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        let execution_id = crate::utils::new_venue_execution_id(RIFT_VENUE_ID);
        let preview = self.preview_swap_request(&execution_id, request).await?;
        let execution = RiftVenueExecutionState {
            rift: preview.state,
            gate: VenueGate::default(),
        };
        Ok(VenueRoutePreview {
            market_receive: Some(preview.market_receive),
            side_costs: preview.side_costs,
            ..VenueRoutePreview::new(
                RIFT_VENUE_ID,
                request.clone(),
                preview.quote,
                preview.conservative_receive,
                VenueExecutionState::new(RIFT_VENUE_ID, &execution)?,
            )
        })
    }

    /// Decodes this leg only and checks that its saved allocation still
    /// matches the request the parent plan committed.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<RiftVenueExecutionState, String> {
        if leg.venue_id != RIFT_VENUE_ID {
            return Err(format!("Rift adapter cannot advance venue `{}`", leg.venue_id));
        }
        let state = leg
            .execution
            .decode::<RiftVenueExecutionState>(RIFT_VENUE_ID)?
            .ok_or_else(|| "Rift leg execution state decoded to the wrong venue".to_string())?;
        if state.rift.size_in != leg.request.pay_amount {
            return Err("Rift persisted allocation does not match the leg request".to_string());
        }
        if state.rift.final_asset.asset_id() != leg.request.receive_asset {
            return Err("Rift persisted receive asset does not match the leg request".to_string());
        }
        Ok(state)
    }

    /// Advances exactly one step behind the gate. It never touches the parent row.
    async fn advance_decoded_leg(
        &self,
        mut execution: RiftVenueExecutionState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        if execution.rift.step == RiftStep::Completed {
            return self.progress_for(execution, None);
        }
        if execution.rift.step == RiftStep::Failed {
            let last_error = execution.rift.last_error.clone();
            return self.progress_for(execution, last_error);
        }
        if execution.gate.operator_required {
            let error = execution
                .rift
                .last_error
                .clone()
                .unwrap_or_else(|| "Rift leg requires operator reconciliation".to_string());
            return self.operator_required_progress(execution, error);
        }

        let intent_id = match execution.gate.open(RIFT_VENUE_ID, &self.multi_venue_armed_intents)? {
            GateOpening::Armed => return self.progress_for(execution, None),
            GateOpening::Open { intent_id, armed_here } => {
                // Not armed here means a restart. Every Rift action is saved
                // before it runs, so the leg resumes; a saved send is settled,
                // never sent again.
                if !armed_here {
                    execution.rift.settle_saved_sends_after_restart(now_ts());
                }
                intent_id
            }
        };
        execution.gate.close();
        execution.rift.last_error = None;

        let mut outcome = self.advance_current_step(&mut execution.rift).await;
        // A step that saved a send or a quote id writes it to the WAL with the
        // gate still set, then acts on it in the same cycle. A crash between
        // restarts into the saved record.
        if outcome.is_ok() && std::mem::take(&mut execution.rift.send_saved) {
            let mut saved = execution.clone();
            saved.gate = execution.gate.held(intent_id);
            checkpoint.checkpoint(self.progress_for(saved, None)?).await?;
            outcome = self.advance_current_step(&mut execution.rift).await;
            execution.rift.send_saved = false;
        }

        if let Err(error) = &outcome {
            execution.rift.last_error = Some(error.to_string());
        }
        let funds_left = execution.rift.funds_left_trader_account();
        let context = format!("liq_id={} step={:?}", execution.rift.liq_id, execution.rift.step);
        match execution.gate.judge(outcome, funds_left, RIFT_VENUE_ID, &context) {
            StepVerdict::Continue => self.progress_for(execution, None),
            StepVerdict::Wait => self.progress_for(execution, None),
            StepVerdict::Retry(message) => self.progress_for(execution, Some(message)),
            StepVerdict::Park(message) => self.operator_required_progress(execution, message),
            StepVerdict::EndBelowMinimum(message) => {
                execution.rift.step = RiftStep::Failed;
                execution.rift.failure = Some(VenueLegFailure::BelowMinimum);
                self.progress_for(execution, Some(message))
            }
        }
    }

    fn progress_for(
        &self,
        execution: RiftVenueExecutionState,
        error: Option<String>,
    ) -> Result<VenueLegProgress, String> {
        let status = match execution.rift.step {
            RiftStep::Completed => VenueLegStatus::Completed,
            RiftStep::Failed => VenueLegStatus::FailedPermanent,
            _ => VenueLegStatus::Running,
        };
        let result = if execution.rift.step == RiftStep::Completed {
            Some(self.finish_state(&execution.rift)?)
        } else {
            None
        };
        let last_error = error.clone().or_else(|| execution.rift.last_error.clone());
        // A permanently failed leg keeps its error but never calls it retryable.
        let retryable_error = match status {
            VenueLegStatus::FailedPermanent => None,
            _ => error,
        };
        let failure = execution.rift.failure;
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(RIFT_VENUE_ID, &execution)?,
            status,
            result,
            last_error,
            retryable_error,
            failure,
        })
    }

    fn operator_required_progress(
        &self,
        mut execution: RiftVenueExecutionState,
        error: String,
    ) -> Result<VenueLegProgress, String> {
        execution.gate.operator_required = true;
        execution.rift.last_error = Some(error.clone());
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(RIFT_VENUE_ID, &execution)?,
            status: VenueLegStatus::OperatorRequired,
            result: None,
            last_error: Some(error),
            retryable_error: None,
            failure: None,
        })
    }
}

#[async_trait]
impl<R: RiftBackend + 'static> MultiVenueAdapter for RiftVenueFinalizer<R> {
    fn venue_id(&self) -> &'static str {
        RIFT_VENUE_ID
    }

    fn validate_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err("token registry is required for amount-scoped Rift previews".to_string());
        }
        Ok(())
    }

    /// Rift carries only the listed ck assets. ckERC20 routes have no minimum.
    async fn check_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        let (pay_amount, receive_token, _) = self
            .swap_request_parts(request)
            .map_err(VenueAmountError::Unavailable)?;
        Self::leg_routes(&pay_amount.token, &receive_token).map_err(VenueAmountError::Unavailable)?;
        Ok(())
    }

    async fn preview(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        self.preview_leg_request(request).await
    }

    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_decoded_leg(self.decode_leg_state(leg)?, checkpoint).await
    }

    fn operator_questions(&self, leg: &VenueLegState) -> Result<Vec<OperatorQuestion>, String> {
        let execution = self.decode_leg_state(leg)?;
        Ok(Self::operator_questions_for(&execution.rift))
    }

    fn operator_release(
        &self,
        leg: &VenueLegState,
        answer: Option<&OperatorDecision>,
    ) -> Result<VenueExecutionState, String> {
        let mut execution = self.decode_leg_state(leg)?;
        Self::answer_operator(&mut execution.rift, answer)?;
        execution.release_park();
        VenueExecutionState::new(RIFT_VENUE_ID, &execution)
    }

    /// A re-enqueued parked leg is the operator's authorization to arm a new
    /// intent. Every step saves before it acts, so any step may be recovered.
    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut execution = self.decode_leg_state(leg)?;
        if execution.rift.step == RiftStep::Completed {
            return Err("completed Rift leg does not require recovery".to_string());
        }
        if execution.gate.operator_required {
            execution.release_park();
        }
        self.advance_decoded_leg(execution, checkpoint).await
    }
}
