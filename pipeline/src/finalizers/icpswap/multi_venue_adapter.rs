use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use candid::Principal;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;

use super::finalizer::{IcpswapFinalizer, RETRY_COOLDOWN_NANOS, is_terminal_step};
use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenueExecutionLock, VenueLegProgress, VenueRoutePreview},
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            execution::IcpswapExecutionStateStore,
            plan::net_expected_output,
            types::{IcpswapExecutionState, IcpswapStep},
        },
        model::SwapExecution,
    },
    utils::ICP_LEDGER_PRINCIPAL,
};

const PERSIST_BEFORE_SIDE_EFFECT: &str = "ICPSwap leg pending state ready for parent persistence";

/// Leg-local store used to reuse the existing ICPSwap state machine without
/// letting it write the parent WAL row. For a non-pending input step, the first
/// persist is captured and deliberately stops execution before the side effect.
struct LegStateStore {
    state: Mutex<IcpswapExecutionState>,
    stop_after_first_persist: bool,
    stopped: AtomicBool,
}

impl LegStateStore {
    fn new(state: IcpswapExecutionState, stop_after_first_persist: bool) -> Self {
        Self {
            state: Mutex::new(state),
            stop_after_first_persist,
            stopped: AtomicBool::new(false),
        }
    }

    fn snapshot(&self) -> Result<IcpswapExecutionState, String> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| "ICPSwap leg state lock poisoned".to_string())
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for LegStateStore {
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
        *self
            .state
            .lock()
            .map_err(|_| "ICPSwap leg state lock poisoned".to_string())? = state.clone();

        if self.stop_after_first_persist && !self.stopped.swap(true, Ordering::SeqCst) {
            return Err(PERSIST_BEFORE_SIDE_EFFECT.to_string());
        }
        Ok(())
    }
}

/// Identifies transitions whose next workflow action can move funds. These
/// transitions are intentionally split across two orchestrator cycles.
fn needs_parent_persist_before_advance(step: IcpswapStep) -> bool {
    matches!(
        step,
        IcpswapStep::Transfer
            | IcpswapStep::Deposit
            | IcpswapStep::Trade
            | IcpswapStep::Withdraw
            | IcpswapStep::Recover
    )
}

/// Projects ICPSwap's detailed state machine onto the generic leg lifecycle.
fn leg_status(step: IcpswapStep) -> VenueLegStatus {
    match step {
        IcpswapStep::Completed => VenueLegStatus::Completed,
        IcpswapStep::Refunded => VenueLegStatus::Recovered,
        // ICPSwap uses a shared trader account, so parking this outer leg would
        // retain the venue-wide owner lock indefinitely. Preserve the detailed
        // ambiguous state inside `VenueExecutionState`, but abandon this leg so
        // the orchestrator can release the lock and later liquidations continue.
        IcpswapStep::OperatorRequired => VenueLegStatus::FailedPermanent,
        IcpswapStep::Failed => VenueLegStatus::FailedPermanent,
        _ => VenueLegStatus::Running,
    }
}

/// Rejects every IC token except the canonical native ICP ledger. Keeping this
/// check at the adapter boundary means a future planner or forced-venue caller
/// cannot bypass ICPSwap's initial ICP-only policy.
fn require_native_icp(request: &crate::swappers::model::SwapRequest) -> Result<(), String> {
    let native_ledger = Principal::from_text(ICP_LEDGER_PRINCIPAL)
        .map_err(|error| format!("invalid configured native ICP ledger principal: {error}"))?;
    match &request.pay_amount.token {
        ChainToken::Icp { ledger, .. } if *ledger == native_ledger => Ok(()),
        ChainToken::Icp { ledger, .. } => Err(format!(
            "ICPSwap only accepts native ICP input ledger {native_ledger}; received {ledger}"
        )),
        _ => Err("ICPSwap only accepts native ICP input".to_string()),
    }
}

impl IcpswapFinalizer {
    /// Decodes only this leg and rejects accidental cross-venue dispatch.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<IcpswapExecutionState, String> {
        if leg.venue_id != VENUE_ID {
            return Err(format!("ICPSwap adapter cannot advance venue `{}`", leg.venue_id));
        }
        leg.execution
            .decode::<IcpswapExecutionState>(VENUE_ID)?
            .ok_or_else(|| "ICPSwap leg execution state decoded to the wrong venue".to_string())
    }

    /// Packages the updated ICPSwap state for parent persistence and creates a
    /// result only after the leg itself has reached `Completed`.
    async fn progress_for(
        &self,
        leg: &VenueLegState,
        state: IcpswapExecutionState,
        advance_error: Option<String>,
    ) -> Result<VenueLegProgress, String> {
        let status = leg_status(state.step);
        let result: Option<SwapExecution> = if state.step == IcpswapStep::Completed {
            Some(self.workflow.finish(&leg.request, &state, (self.clock)()).await?)
        } else {
            None
        };
        let last_error = advance_error.clone().or_else(|| state.last_error.clone());
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(VENUE_ID, &state)?,
            status,
            result,
            last_error,
            retryable_error: advance_error,
        })
    }

    /// Runs one leg-local workflow transition without access to the parent WAL.
    /// A pre-side-effect persistence sentinel is an expected yielded transition,
    /// while operational failures remain attached to the returned leg state.
    async fn advance_leg(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        let mut state = self.decode_leg_state(leg)?;
        let execution_id = state.execution_id.clone();

        let now_nanos = (self.clock)();
        if state.next_attempt_at_nanos.is_some_and(|ready_at| now_nanos < ready_at) {
            self.notify_operator_required(&execution_id, &state).await;
            return self.progress_for(leg, state, None).await;
        }

        state.next_attempt_at_nanos = None;
        let store = LegStateStore::new(state.clone(), needs_parent_persist_before_advance(state.step));
        let result = self
            .workflow
            .advance_loaded(&store, &execution_id, self.trader, now_nanos, state)
            .await;
        let advance_error = match result {
            Ok(_) => None,
            Err(error) if error == PERSIST_BEFORE_SIDE_EFFECT => None,
            Err(error) => Some(error),
        };
        let mut state = store.snapshot()?;
        if advance_error.is_some() && !is_terminal_step(state.step) {
            state.next_attempt_at_nanos = Some(now_nanos.saturating_add(RETRY_COOLDOWN_NANOS));
        }
        self.notify_operator_required(&execution_id, &state).await;
        self.progress_for(leg, state, advance_error).await
    }
}

#[async_trait]
impl MultiVenueAdapter for IcpswapFinalizer {
    fn venue_id(&self) -> &'static str {
        VENUE_ID
    }

    async fn preview(&self, request: &crate::swappers::model::SwapRequest) -> Result<VenueRoutePreview, String> {
        require_native_icp(request)?;
        if self.trader.subaccount.is_some() {
            return Err("ICPSwap requires the trader's default ledger account".to_string());
        }
        let preview = self
            .workflow
            .preview_route(request)
            .await
            .map_err(|error| error.to_string())?;
        let conservative_value = net_expected_output(
            &preview.route.amount_out_minimum.value,
            &preview.route.output_ledger_fee.value,
        )
        .map_err(|error| error.to_string())?;
        let conservative_receive = liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
            preview.route.gross_quoted_out.token.clone(),
            conservative_value,
        );
        // The selected preview is persisted before execution, so its generated
        // ID becomes the durable idempotency key used for every later advance.
        let execution_id = crate::utils::new_venue_execution_id(VENUE_ID);
        let state = self.workflow.prepare(&execution_id, preview.route, self.trader);
        Ok(VenueRoutePreview {
            venue_id: VENUE_ID.to_string(),
            request: request.clone(),
            quote: preview.quote,
            conservative_receive,
            initial_execution_state: VenueExecutionState::new(VENUE_ID, &state)?,
        })
    }

    fn execution_lock(&self, leg: &VenueLegState) -> Result<Option<VenueExecutionLock>, String> {
        let state = self.decode_leg_state(leg)?;
        Ok(Some(VenueExecutionLock {
            owner_key: state.owner.owner.to_text(),
            execution_id: state.execution_id,
        }))
    }

    async fn advance(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        self.advance_leg(leg).await
    }

    async fn recover(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        let state = self.decode_leg_state(leg)?;
        if !matches!(
            state.step,
            IcpswapStep::Recover
                | IcpswapStep::RecoverPending
                | IcpswapStep::OperatorRequired
                | IcpswapStep::Refunded
                | IcpswapStep::Failed
        ) {
            return Err(format!("ICPSwap leg at {:?} is not in recovery", state.step));
        }
        self.advance_leg(leg).await
    }
}
