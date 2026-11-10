//! The Rift leg as the shared leg driver sees it: its state, its step, and
//! its planning checks.
use std::{collections::HashSet, sync::Mutex};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::rift_backend::RiftBackend;
use serde::{Deserialize, Serialize};

use super::{
    finalizer::{RiftVenueFinalizer, leg_routes},
    state::{RiftState, RiftStep},
};
use crate::{
    finalizers::bridge_legs::check_bridged_minimum,
    finalizers::multi_venue::{
        GatedLeg, LegPreview, LegState, OperatorQuestion, RIFT_VENUE_ID, StepFlow, VenueAmountError, VenueGate,
        VenueLeg, VenuePlanningContext, VenueStepError,
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueLegFailure, VenueLegState},
    swappers::model::{SwapExecution, SwapRequest},
    utils::now_ts,
};

/// The leg's state plus the persistence gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiftVenueExecutionState {
    rift: RiftState,
    #[serde(flatten)]
    gate: VenueGate,
}

impl GatedLeg for RiftVenueExecutionState {
    type State = RiftState;
    fn state(&self) -> &RiftState {
        &self.rift
    }
    fn state_mut(&mut self) -> &mut RiftState {
        &mut self.rift
    }
    fn gate(&self) -> &VenueGate {
        &self.gate
    }
    fn gate_mut(&mut self) -> &mut VenueGate {
        &mut self.gate
    }
}

impl LegState for RiftState {
    fn liq_id(&self) -> &str {
        &self.liq_id
    }
    fn step_label(&self) -> String {
        format!("{:?}", self.step)
    }
    fn is_completed(&self) -> bool {
        self.step == RiftStep::Completed
    }
    fn is_failed(&self) -> bool {
        self.step == RiftStep::Failed
    }
    fn end_below_minimum(&mut self) {
        self.step = RiftStep::Failed;
        self.failure = Some(VenueLegFailure::BelowMinimum);
    }
    fn failure(&self) -> Option<VenueLegFailure> {
        self.failure
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
        RiftState::funds_left_trader_account(self)
    }
}

#[async_trait]
impl<R: RiftBackend + 'static> VenueLeg for RiftVenueFinalizer<R> {
    type Exec = RiftVenueExecutionState;

    fn leg_venue_id(&self) -> &'static str {
        RIFT_VENUE_ID
    }

    fn venue_name(&self) -> String {
        "Rift".to_string()
    }

    fn armed_intents(&self) -> &Mutex<HashSet<String>> {
        &self.multi_venue_armed_intents
    }

    fn new_execution(&self, state: RiftState) -> RiftVenueExecutionState {
        RiftVenueExecutionState {
            rift: state,
            gate: VenueGate::default(),
        }
    }

    fn check_allocation(&self, state: &RiftState, leg: &VenueLegState) -> Result<(), String> {
        if state.size_in != leg.request.pay_amount {
            return Err("Rift persisted allocation does not match the leg request".to_string());
        }
        if state.final_asset.asset_id() != leg.request.receive_asset {
            return Err("Rift persisted receive asset does not match the leg request".to_string());
        }
        Ok(())
    }

    /// Every Rift action is saved before it runs, so a restarted leg resumes;
    /// a saved send is settled from the chain, never sent again.
    fn resume_after_restart(&self, state: &mut RiftState, _intent_id: &str) -> Result<(), String> {
        state.settle_saved_sends_after_restart(now_ts());
        Ok(())
    }

    async fn step(&self, state: &mut RiftState) -> Result<StepFlow, VenueStepError> {
        self.advance_current_step(state).await.map(|()| StepFlow::Done)
    }

    fn finish(&self, state: &RiftState) -> Result<SwapExecution, String> {
        self.finish_state(state)
    }

    /// A release while waiting for Rift starts the wait bounds again.
    fn after_release(&self, state: &mut RiftState) {
        if state.step == RiftStep::Wait
            && let Some(order) = state.order.as_mut()
        {
            order.wait_since_ts = Some(now_ts());
        }
    }

    fn questions(&self, state: &RiftState) -> Vec<OperatorQuestion> {
        Self::operator_questions_for(state)
    }

    fn answer(&self, state: &mut RiftState, answer: Option<&OperatorDecision>) -> Result<(), String> {
        Self::answer_operator(state, answer)
    }

    fn check_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err("token registry is required for amount-scoped Rift previews".to_string());
        }
        Ok(())
    }

    /// Rift carries only the listed ck assets, and a minter with a minimum
    /// refuses a small one before any quote.
    async fn check_leg_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        let (pay_amount, receive_token, _) = self
            .swap_request_parts(request)
            .map_err(VenueAmountError::Unavailable)?;
        let (out, _) = leg_routes(&pay_amount.token, &receive_token).map_err(VenueAmountError::Unavailable)?;
        check_bridged_minimum(&self.bridge, out, &pay_amount.token, &pay_amount).await
    }

    async fn preview_leg(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<LegPreview<RiftState>, String> {
        let execution_id = crate::utils::new_venue_execution_id(RIFT_VENUE_ID);
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
