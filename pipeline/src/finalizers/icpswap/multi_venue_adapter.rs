use std::sync::Mutex;

use super::{
    finalizer::{IcpswapFinalizer, RETRY_COOLDOWN_NANOS, is_terminal_step},
    hop_route::{IcpswapLegState, release_route, route_questions},
    operator,
};
use crate::{
    finalizers::multi_venue::{
        MultiVenueAdapter, OperatorQuestion, VenueLegCheckpoint, VenueLegProgress, VenueRoutePreview,
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            execution::IcpswapExecutionStateStore,
            types::{IcpswapExecutionState, IcpswapStep},
        },
        model::SwapExecution,
    },
};
use async_trait::async_trait;

/// ICPSwap-local state store whose persists are checkpointed into the complete
/// parent envelope, allowing `advance_loaded` to drive the whole workflow.
struct CheckpointedLegStateStore<'a> {
    state: Mutex<IcpswapExecutionState>,
    /// The leg this execution belongs to. A route is saved whole on every persist.
    leg: IcpswapLegState,
    checkpoint: &'a dyn VenueLegCheckpoint,
}

impl<'a> CheckpointedLegStateStore<'a> {
    fn new(leg: IcpswapLegState, checkpoint: &'a dyn VenueLegCheckpoint) -> Self {
        Self {
            state: Mutex::new(leg.current().clone()),
            leg,
            checkpoint,
        }
    }

    fn snapshot(&self) -> Result<IcpswapExecutionState, String> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| "ICPSwap checkpointed leg state lock poisoned".to_string())
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for CheckpointedLegStateStore<'_> {
    async fn load(&self, execution_id: &str) -> Result<Option<IcpswapExecutionState>, String> {
        let state = self.snapshot()?;
        if state.execution_id != execution_id {
            return Err(format!(
                "ICPSwap execution ID {} does not match leg key {execution_id}",
                state.execution_id
            ));
        }
        Ok(Some(state))
    }

    async fn persist(&self, execution_id: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        if state.execution_id != execution_id {
            return Err(format!(
                "ICPSwap execution ID {} does not match leg key {execution_id}",
                state.execution_id
            ));
        }
        {
            let mut held = self
                .state
                .lock()
                .map_err(|_| "ICPSwap checkpointed leg state lock poisoned".to_string())?;
            // Waiting steps re-persist identical bytes on every daemon poll, and
            // the checkpoint below is an fsync'd WAL write. The held state came
            // from the WAL, so equality means the row is already current.
            if *held == *state {
                return Ok(());
            }
            *held = state.clone();
        }

        // Completed/refunded results are constructed by the adapter after the
        // workflow returns. Persisting them here would briefly create a
        // terminal parent leg without its required aggregate result. If the
        // process crashes first, the preceding pending checkpoint is resumed
        // and reconciled safely.
        let leg = self.leg.with_current(state.clone());
        let status = leg.status();
        if matches!(
            status,
            VenueLegStatus::Completed
                | VenueLegStatus::Recovered
                | VenueLegStatus::OperatorRequired
                | VenueLegStatus::FailedPermanent
        ) {
            return Ok(());
        }

        self.checkpoint.checkpoint(running_progress(&leg)?).await
    }
}

/// The checkpoint of a leg that still runs: its state, with no result yet.
fn running_progress(leg: &IcpswapLegState) -> Result<VenueLegProgress, String> {
    Ok(VenueLegProgress {
        execution: leg.encode()?,
        status: leg.status(),
        result: None,
        last_error: leg.current().last_error.clone(),
        retryable_error: None,
        failure: None,
    })
}

impl IcpswapFinalizer {
    /// Decodes only this leg and rejects accidental cross-venue dispatch.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<IcpswapLegState, String> {
        if leg.venue_id != VENUE_ID {
            return Err(format!("ICPSwap adapter cannot advance venue `{}`", leg.venue_id));
        }
        IcpswapLegState::decode(&leg.execution)
    }

    /// Packages the updated leg for parent persistence and creates a result
    /// only after the leg itself has reached `Completed`.
    async fn progress_for(
        &self,
        leg: &VenueLegState,
        state: IcpswapLegState,
        advance_error: Option<String>,
    ) -> Result<VenueLegProgress, String> {
        let status = state.status();
        let result: Option<SwapExecution> = if status == VenueLegStatus::Completed {
            let now_nanos = (self.clock)();
            Some(match &state {
                IcpswapLegState::Single(single) => self.workflow.finish(&leg.request, single, now_nanos).await?,
                IcpswapLegState::Route(route) => self.route_result(&leg.request, route, now_nanos).await?,
            })
        } else {
            None
        };
        let last_error = advance_error.clone().or_else(|| state.last_error());
        Ok(VenueLegProgress {
            execution: state.encode()?,
            status,
            result,
            last_error,
            retryable_error: advance_error,
            failure: None,
        })
    }

    /// Drives the leg's current execution through all immediately runnable
    /// ICPSwap phases. The workflow persists prepared intent state through the
    /// narrow checkpoint before every external side effect and stops on delay,
    /// error, or terminal state so the next daemon tick can resume safely.
    async fn advance_current(
        &self,
        leg: IcpswapLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<(IcpswapLegState, Option<String>), String> {
        let mut state = leg.current().clone();
        let execution_id = state.execution_id.clone();

        let now_nanos = (self.clock)();
        if is_terminal_step(state.step) || state.next_attempt_at_nanos.is_some_and(|ready_at| now_nanos < ready_at) {
            self.notify_operator_required(&execution_id, &state).await;
            return Ok((leg, None));
        }

        state.next_attempt_at_nanos = None;
        let store = CheckpointedLegStateStore::new(leg.with_current(state.clone()), checkpoint);
        // Opening the session re-derives the signer from the configured
        // mnemonic and verifies it against the persisted descriptor before any
        // identity-bound ICPSwap call is made.
        let session = self.execution_session(&state)?;
        let result = self
            .workflow
            .advance_loaded(&session, &store, &execution_id, now_nanos, state)
            .await;
        let advance_error = result.err();
        let mut state = store.snapshot()?;
        if advance_error.is_some() && !is_terminal_step(state.step) {
            state.next_attempt_at_nanos = Some(now_nanos.saturating_add(RETRY_COOLDOWN_NANOS));
            store.persist(&execution_id, &state).await?;
        }
        self.notify_operator_required(&execution_id, &state).await;
        Ok((leg.with_current(state), advance_error))
    }

    /// Advances the leg. A route also plans each next step as soon as the
    /// step before it has paid, so one call can cross several pools.
    async fn advance_leg(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut state = self.decode_leg_state(leg)?;
        loop {
            if let IcpswapLegState::Route(route) = &state
                && route.parked.is_some()
            {
                self.notify_route_parked(route).await;
                return self.progress_for(leg, state, None).await;
            }
            let (advanced, advance_error) = self.advance_current(state, checkpoint).await?;
            state = advanced;
            let IcpswapLegState::Route(route) = &mut state else {
                return self.progress_for(leg, state, advance_error).await;
            };
            let step_paid = advance_error.is_none() && route.current.state.step == IcpswapStep::Completed;
            if !step_paid || route.is_last_step() {
                return self.progress_for(leg, state, advance_error).await;
            }
            if !self.plan_next_step(route, (self.clock)()).await {
                self.notify_route_parked(route).await;
                return self.progress_for(leg, state, None).await;
            }
            // Save the new step before its first side effect.
            checkpoint.checkpoint(running_progress(&state)?).await?;
        }
    }
}

#[async_trait]
impl MultiVenueAdapter for IcpswapFinalizer {
    fn venue_id(&self) -> &'static str {
        VENUE_ID
    }

    async fn preview(
        &self,
        context: &crate::finalizers::multi_venue::VenuePlanningContext,
        request: &crate::swappers::model::SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        // Re-checked at the execution adapter boundary so a future planner or
        // forced-venue caller cannot bypass the venue policy.
        if self.trader.subaccount.is_some() {
            return Err("ICPSwap requires the trader's default ledger account".to_string());
        }
        self.preview_paths(context, request).await
    }

    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_leg(leg, checkpoint).await
    }

    fn operator_questions(&self, leg: &VenueLegState) -> Result<Vec<OperatorQuestion>, String> {
        let state = self.decode_leg_state(leg)?;
        if let IcpswapLegState::Route(route) = &state
            && let Some(questions) = route_questions(route)
        {
            return Ok(questions);
        }
        operator::operator_questions(state.current())
    }

    fn operator_release(
        &self,
        leg: &VenueLegState,
        answer: Option<&OperatorDecision>,
    ) -> Result<VenueExecutionState, String> {
        let mut state = self.decode_leg_state(leg)?;
        match &mut state {
            IcpswapLegState::Route(route) if route.parked.is_some() => release_route(route, answer)?,
            IcpswapLegState::Route(route) => operator::release(&mut route.current.state, answer)?,
            IcpswapLegState::Single(single) => operator::release(single, answer)?,
        }
        state.encode()
    }

    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let state = self.decode_leg_state(leg)?;
        let step = state.current().step;
        if !matches!(
            step,
            IcpswapStep::Recover
                | IcpswapStep::RecoverPending
                | IcpswapStep::OperatorRequired
                | IcpswapStep::Refunded
                | IcpswapStep::Failed
        ) {
            return Err(format!("ICPSwap leg at {step:?} is not in recovery"));
        }
        self.advance_leg(leg, checkpoint).await
    }
}
