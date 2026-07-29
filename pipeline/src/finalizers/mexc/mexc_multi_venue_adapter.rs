use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use serde::{Deserialize, Serialize};

use super::mexc_finalizer::MexcFinalizer;
use crate::{
    finalizers::{
        cex_finalizer::{CexFinalizerLogic, CexState, CexStep},
        multi_venue::{
            MultiVenueAdapter, VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext, VenueRoutePreview,
        },
    },
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
};

const VENUE_ID: &str = "mexc";

/// MEXC's complete venue-local state plus a persistence gate. The gate makes
/// every call that may submit a transfer, order, withdrawal, or bridge request
/// take two orchestrator cycles: first persist `ready_to_advance`, then execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MexcVenueExecutionState {
    cex: CexState,
    #[serde(default)]
    ready_to_advance: bool,
    #[serde(default)]
    intent_id: Option<String>,
    #[serde(default)]
    operator_required: bool,
}

// Step 6 registers this implementation in the live venue registry. Until then
// the private dispatch helpers are exercised only by focused adapter tests.
#[allow(dead_code)]
impl<B> MexcFinalizer<B>
where
    B: CexBackend,
{
    /// Translates the amount-scoped MEXC preview into the generic venue shape;
    /// all route preparation remains on the existing MEXC/CEX implementation.
    async fn preview_leg_request(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        let execution_id = crate::utils::new_venue_execution_id(VENUE_ID);
        let preview = self.preview_swap_request(&execution_id, request).await?;
        let execution = MexcVenueExecutionState {
            cex: preview.state,
            ready_to_advance: false,
            intent_id: None,
            operator_required: false,
        };

        Ok(VenueRoutePreview {
            venue_id: VENUE_ID.to_string(),
            request: request.clone(),
            quote: preview.quote,
            conservative_receive: preview.conservative_receive,
            initial_execution_state: VenueExecutionState::new(VENUE_ID, &execution)?,
        })
    }

    /// Decodes this leg only and verifies that its persisted MEXC allocation
    /// still matches the immutable request committed by the parent plan.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<MexcVenueExecutionState, String> {
        if leg.venue_id != VENUE_ID {
            return Err(format!("MEXC adapter cannot advance venue `{}`", leg.venue_id));
        }
        let state = leg
            .execution
            .decode::<MexcVenueExecutionState>(VENUE_ID)?
            .ok_or_else(|| "MEXC leg execution state decoded to the wrong venue".to_string())?;
        if state.cex.size_in != leg.request.pay_amount {
            return Err("MEXC persisted allocation does not match the leg request".to_string());
        }
        if state.cex.withdraw.withdraw_asset.asset_id() != leg.request.receive_asset {
            return Err("MEXC persisted receive asset does not match the leg request".to_string());
        }
        Ok(state)
    }

    /// Advances exactly one venue-local state-machine phase. This method never
    /// receives a WAL handle and therefore cannot change the parent row status.
    async fn advance_leg(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        self.advance_decoded_leg(self.decode_leg_state(leg)?).await
    }

    /// Advances an already validated state so recovery does not decode the
    /// same persisted JSON twice in one orchestrator cycle.
    async fn advance_decoded_leg(&self, mut execution: MexcVenueExecutionState) -> Result<VenueLegProgress, String> {
        if execution.cex.step == CexStep::Completed {
            return self.progress_for(execution, None);
        }
        if execution.cex.step == CexStep::Failed {
            let last_error = execution.cex.last_error.clone();
            return self.progress_for(execution, last_error);
        }
        if execution.operator_required {
            let error = execution
                .cex
                .last_error
                .clone()
                .unwrap_or_else(|| "MEXC leg requires operator reconciliation".to_string());
            return self.operator_required_progress(execution, error);
        }

        if !execution.ready_to_advance {
            execution.ready_to_advance = true;
            let intent_id = crate::utils::new_venue_execution_id("mexc-intent");
            execution.intent_id = Some(intent_id.clone());
            self.multi_venue_armed_intents
                .lock()
                .map_err(|_| "MEXC multi-venue intent lock poisoned".to_string())?
                .insert(intent_id);
            return self.progress_for(execution, None);
        }

        let intent_id = execution
            .intent_id
            .clone()
            .ok_or_else(|| "persisted MEXC execution gate has no intent ID".to_string())?;
        let armed_here = self
            .multi_venue_armed_intents
            .lock()
            .map_err(|_| "MEXC multi-venue intent lock poisoned".to_string())?
            .remove(&intent_id);
        if !armed_here {
            return self.operator_required_progress(
                execution,
                format!("persisted MEXC intent `{intent_id}` has ambiguous submission state after restart"),
            );
        }
        execution.ready_to_advance = false;
        execution.intent_id = None;
        execution.cex.last_error = None;

        let result = self.advance_current_step(&mut execution.cex).await;
        let error = result.err();
        if let Some(error) = &error {
            execution.cex.last_error = Some(error.clone());
            if error.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX) {
                execution.cex.step = CexStep::Failed;
            }
        }
        self.progress_for(execution, error)
    }

    /// Maps the detailed CEX state onto the generic leg status and creates a
    /// result only after this MEXC leg itself reaches `Completed`.
    fn progress_for(
        &self,
        execution: MexcVenueExecutionState,
        error: Option<String>,
    ) -> Result<VenueLegProgress, String> {
        let status = match execution.cex.step {
            CexStep::Completed => VenueLegStatus::Completed,
            CexStep::Failed => VenueLegStatus::FailedPermanent,
            _ => VenueLegStatus::Running,
        };
        let result = if execution.cex.step == CexStep::Completed {
            Some(self.finish_state(&execution.cex)?)
        } else {
            None
        };
        let last_error = error.clone().or_else(|| execution.cex.last_error.clone());
        // A permanently failed leg keeps its error for diagnostics but must never
        // advertise it as retryable. Both paths that reach here with an error --
        // the `CexStep::Failed` short-circuit and the amount-floor rejection --
        // describe a swap that cannot succeed on a later cycle, so the retry
        // signal has to be dropped at the source rather than relying on the
        // orchestrator to filter it back out.
        let retryable_error = match status {
            VenueLegStatus::FailedPermanent => None,
            _ => error,
        };
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(VENUE_ID, &execution)?,
            status,
            result,
            last_error,
            retryable_error,
        })
    }

    fn operator_required_progress(
        &self,
        mut execution: MexcVenueExecutionState,
        error: String,
    ) -> Result<VenueLegProgress, String> {
        execution.operator_required = true;
        execution.cex.last_error = Some(error.clone());
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(VENUE_ID, &execution)?,
            status: VenueLegStatus::OperatorRequired,
            result: None,
            last_error: Some(error),
            retryable_error: None,
        })
    }
}

#[async_trait]
impl<B> MultiVenueAdapter for MexcFinalizer<B>
where
    B: CexBackend + 'static,
{
    fn venue_id(&self) -> &'static str {
        VENUE_ID
    }

    fn validate_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err("token registry is required for amount-scoped MEXC previews".to_string());
        }
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
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_leg(leg).await
    }

    async fn recover(
        &self,
        leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut execution = self.decode_leg_state(leg)?;
        if execution.cex.step == CexStep::Completed {
            return Err("completed MEXC leg does not require recovery".to_string());
        }
        // OperatorRequired rows are excluded from automatic WAL polling. An
        // explicit re-enqueue is therefore the operator's authorization to
        // discard the old in-memory submission token and retry the persisted
        // CEX step through a newly persisted intent. This call only arms that
        // intent; the external action still happens in a later WAL cycle.
        if execution.operator_required {
            execution.operator_required = false;
            execution.ready_to_advance = false;
            execution.intent_id = None;
        }
        // The legacy CEX machine uses its Pending phases as durable restart
        // reconciliation points; unlike ICPSwap it has no separate Recover
        // variants. Recovery therefore accepts pending phases even without a
        // last error, but still passes through the persistence gate before the
        // state machine can poll or submit an idempotent follow-up action.
        if execution.cex.last_error.is_none()
            && !matches!(
                execution.cex.step,
                CexStep::DepositPending | CexStep::TradePending | CexStep::WithdrawPending | CexStep::Failed
            )
        {
            return Err(format!("MEXC leg at {:?} is not in recovery", execution.cex.step));
        }
        self.advance_decoded_leg(execution).await
    }
}

#[cfg(test)]
#[path = "mexc_multi_venue_adapter_tests.rs"]
mod tests;
