use std::sync::Arc;

use async_trait::async_trait;
use icrc_ledger_types::icrc1::account::Account;

use crate::{
    finalizers::dex_finalizer::{DexRouteFinalizer, DexRoutePreview},
    finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult},
    persistance::{FinalizerDecisionSnapshot, VenueExecutionState, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{
            VENUE_ID, ensure_supported_pair,
            execution::{IcpswapExecutionStateStore, WalIcpswapExecutionStateStore},
            plan::input_fee_budget,
            session::{IcpswapExecutionSession, IcpswapExecutionSessionFactory},
            transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
            types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep},
            venue::IcpswapFinalizerLogic,
        },
        model::SwapRequest,
    },
    utils::now_nanos,
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
    watchdog::{Watchdog, WatchdogEvent, noop_watchdog},
};

pub(crate) const ICPSWAP_FINALIZER_PERMANENT_PREFIX: &str = "permanent ICPSwap finalizer: ";

/// Cooldown applied after a non-terminal step fails.
///
/// Such a failure re-enqueues the row without consuming the finalize stage's
/// retry budget, so nothing else paces it -- the exponential backoff there only
/// gates `FailedRetryable`. Roughly one order of magnitude above the daemon
/// cycle, so a persistently failing step backs off instead of re-running every
/// two seconds, while a transient blip still recovers promptly.
pub(super) const RETRY_COOLDOWN_NANOS: u64 = 20 * 1_000_000_000;

type Clock = dyn Fn() -> u64 + Send + Sync;

/// Steps after which no ICPSwap side effect may be submitted again.
pub(super) fn is_terminal_step(step: IcpswapStep) -> bool {
    matches!(
        step,
        IcpswapStep::Completed | IcpswapStep::Refunded | IcpswapStep::Failed
    )
}

/// WAL-backed finalizer for the manual ICPSwap execution lifecycle.
pub struct IcpswapFinalizer {
    pub(super) workflow: Arc<dyn IcpswapFinalizerLogic>,
    pub(super) trader: Account,
    pub(super) sessions: Arc<dyn IcpswapExecutionSessionFactory>,
    pub(super) clock: Arc<Clock>,
    pub(super) watchdog: Arc<dyn Watchdog>,
}

impl IcpswapFinalizer {
    pub fn new(
        workflow: Arc<dyn IcpswapFinalizerLogic>,
        trader: Account,
        sessions: Arc<dyn IcpswapExecutionSessionFactory>,
    ) -> Self {
        Self::from_workflow_with_clock(workflow, trader, sessions, Arc::new(now_nanos))
    }

    pub fn from_workflow_with_clock(
        workflow: Arc<dyn IcpswapFinalizerLogic>,
        trader: Account,
        sessions: Arc<dyn IcpswapExecutionSessionFactory>,
        clock: Arc<Clock>,
    ) -> Self {
        Self {
            workflow,
            trader,
            sessions,
            clock,
            watchdog: noop_watchdog(),
        }
    }

    pub(super) fn prepare_execution_state(
        &self,
        execution_id: &str,
        liquidation_id: &str,
        request: &SwapRequest,
        plan: IcpswapExecutionPlan,
    ) -> Result<IcpswapExecutionState, String> {
        ensure_supported_pair(request)?;
        if plan.amount_in.token.asset_id() != request.pay_asset
            || plan.gross_quoted_out.token.asset_id() != request.receive_asset
        {
            return Err("ICPSwap execution plan token pair does not match its swap request".to_string());
        }
        let identity = self.sessions.descriptor(liquidation_id)?;
        let child = Account {
            owner: identity.principal,
            subaccount: None,
        };
        let allocation = request.pay_amount.value.clone();
        let fee = plan.input_ledger_fee.value.clone();
        let required = plan.amount_in.value.clone() + input_fee_budget(&fee);
        if allocation != required {
            return Err(format!(
                "ICPSwap allocation {allocation} does not equal pool input plus three ledger fees {required}"
            ));
        }
        let funding = IcpswapFundingState::new(self.trader, child, plan.input_ledger_fee.clone());
        let address = request
            .receive_address
            .as_deref()
            .ok_or_else(|| "ICPSwap request is missing receive_address".to_string())?;
        let destination = Account {
            owner: candid::Principal::from_text(address)
                .map_err(|error| format!("invalid ICPSwap receive principal `{address}`: {error}"))?,
            subaccount: None,
        };
        let settlement = IcpswapSettlementState {
            kind: None,
            destination,
            fee: plan.output_ledger_fee.clone(),
            transfer: IcpswapLedgerTransferState::default(),
            interrupted_transfer: None,
            interrupted_observed_debit: None,
            recovery_credit: None,
            residual_dust: None,
        };
        IcpswapExecutionState::prepare(execution_id, plan, identity, funding, settlement)
    }

    pub(super) fn execution_session(&self, state: &IcpswapExecutionState) -> Result<IcpswapExecutionSession, String> {
        self.sessions.open(&state.identity)
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
    }

    async fn load_state(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        liquidation_id: &str,
    ) -> Result<IcpswapExecutionState, String> {
        store
            .load(liquidation_id)
            .await?
            .ok_or_else(|| format!("missing persisted ICPSwap state for liquidation {liquidation_id}"))
    }

    async fn result_for_state(
        &self,
        receipt: &ExecutionReceipt,
        state: &IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<FinalizerResult, String> {
        match state.step {
            IcpswapStep::Completed => Ok(FinalizerResult {
                swap_result: Some(
                    self.workflow
                        .finish(
                            receipt
                                .request
                                .swap_args
                                .as_ref()
                                .ok_or_else(|| permanent_message("completed receipt has no swap request"))?,
                            state,
                            now_nanos,
                        )
                        .await
                        .map_err(|error| permanent_message(&error))?,
                ),
                finalized: true,
                operator_required: false,
                swapper: Some("icpswap".to_string()),
                reason: None,
            }),
            IcpswapStep::Refunded => Ok(FinalizerResult {
                swap_result: None,
                finalized: true,
                operator_required: false,
                swapper: Some("recovery".to_string()),
                reason: Some("ICPSwap failed; deposited ICP was withdrawn back to the trader account".to_string()),
            }),
            IcpswapStep::Failed => Err(permanent_message(
                state
                    .last_error
                    .as_deref()
                    .unwrap_or("manual ICPSwap failed without detail"),
            )),
            _ => Ok(FinalizerResult::noop()),
        }
    }

    /// Alerts for as long as the execution stays parked, not just on the way in.
    /// Alerting once means a single dropped webhook can hide ambiguous custody
    /// indefinitely. The watchdog's cooldown key throttles repeated polls into
    /// a periodic re-escalation.
    pub(super) async fn notify_operator_required(&self, execution_id: &str, state: &IcpswapExecutionState) {
        if state.step != IcpswapStep::OperatorRequired {
            return;
        }
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: execution_id.to_string(),
                venue: VENUE_ID.to_string(),
                pending_step: state
                    .operator_pending_step
                    .map(|step| format!("{step:?}"))
                    .unwrap_or_else(|| "unknown".to_string()),
                owner: state.owner.to_string(),
                details: state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "ICPSwap requires operator reconciliation".to_string()),
            })
            .await;
    }
}

#[async_trait]
impl Finalizer for IcpswapFinalizer {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        if !matches!(receipt.status, ExecutionStatus::Success) || receipt.request.swap_args.is_none() {
            return Ok(FinalizerResult::noop());
        }

        let liquidation_id = liq_id_from_receipt(&receipt)?;
        let store = WalIcpswapExecutionStateStore::new(wal);
        let mut loaded_state = self.load_state(&store, &liquidation_id).await?;
        let now_nanos = (self.clock)();
        if is_terminal_step(loaded_state.step) {
            return self.result_for_state(&receipt, &loaded_state, now_nanos).await;
        }

        // Back off after a failed step instead of re-running it every cycle.
        if loaded_state
            .next_attempt_at_nanos
            .is_some_and(|ready_at| now_nanos < ready_at)
        {
            // Escalation is driven by persisted state, not by making progress, so
            // it must not be throttled by this cooldown -- a parked execution
            // going quiet is exactly the failure this alert exists to surface.
            self.notify_operator_required(&liquidation_id, &loaded_state).await;
            return Ok(FinalizerResult::noop());
        }

        // Cleared before advancing, not after, so the workflow's own persist
        // carries the reset -- a successful step must not inherit the cooldown
        // left behind by the failure that preceded it.
        loaded_state.next_attempt_at_nanos = None;

        // One step per call. Each step is an IC update round trip (~seconds of
        // consensus), and the finalize stage runs inline on the daemon cycle
        // ahead of the next opportunity scan. Driving further here would delay
        // claiming -- which is competitive -- by a full round trip per extra
        // step, so the state machine advances once and resumes next cycle.
        let session = self.execution_session(&loaded_state)?;
        let (state, advance_error) = match self
            .workflow
            .advance_loaded(&session, &store, &liquidation_id, now_nanos, loaded_state)
            .await
        {
            Ok(state) => (state, None),
            Err(error) => {
                let mut state = self.load_state(&store, &liquidation_id).await?;
                state.last_error = Some(error.clone());
                if !is_terminal_step(state.step) {
                    state.next_attempt_at_nanos = Some(now_nanos.saturating_add(RETRY_COOLDOWN_NANOS));
                }
                store.persist(&liquidation_id, &state).await?;
                (state, Some(error))
            }
        };

        self.notify_operator_required(&liquidation_id, &state).await;

        if let Some(error) = advance_error {
            if state.step == IcpswapStep::Failed {
                return Err(permanent_message(&error));
            }
            return Ok(FinalizerResult::noop());
        }

        self.result_for_state(&receipt, &state, now_nanos).await
    }

    fn classify_error(&self, error: &str) -> FinalizerErrorKind {
        if error.starts_with(ICPSWAP_FINALIZER_PERMANENT_PREFIX) {
            FinalizerErrorKind::Permanent
        } else {
            FinalizerErrorKind::Retryable
        }
    }
}

#[async_trait]
impl DexRouteFinalizer for IcpswapFinalizer {
    fn venue_id(&self) -> &'static str {
        VENUE_ID
    }

    async fn preview_route(&self, request: &SwapRequest) -> Result<DexRoutePreview, String> {
        let preview = self
            .workflow
            .preview_route(request)
            .await
            .map_err(|error| error.to_string())?;
        DexRoutePreview::new(preview.quote, VENUE_ID, &preview.route)
    }

    async fn has_committed_route(&self, wal: &dyn WalStore, receipt: &ExecutionReceipt) -> Result<bool, String> {
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let Some(row) = wal_load(wal, &liquidation_id).await? else {
            return Ok(false);
        };
        let Some(wrapper) = decode_receipt_wrapper(&row)? else {
            return Ok(false);
        };
        Ok(wrapper
            .venue_execution
            .as_ref()
            .is_some_and(|record| record.is_venue(VENUE_ID)))
    }

    async fn commit_route(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
        decision: FinalizerDecisionSnapshot,
        preview: DexRoutePreview,
    ) -> Result<(), String> {
        if self.trader.subaccount.is_some() {
            return Err("ICPSwap route commit requires the trader's default ledger account".to_string());
        }
        let route: IcpswapExecutionPlan = preview.route(VENUE_ID)?;
        if decision.chosen != "dex" {
            return Err(format!(
                "ICPSwap route commit requires chosen=dex, got {}",
                decision.chosen
            ));
        }
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let mut row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt wrapper in WAL meta_json for {}", row.id))?;

        if let Some(existing) = &wrapper.finalizer_decision
            && matches!(existing.chosen.as_str(), "dex" | "cex")
            && existing.chosen != "dex"
        {
            return Err(format!(
                "refusing to replace persisted {} route with dex for liquidation {liquidation_id}",
                existing.chosen
            ));
        }

        match &wrapper.venue_execution {
            Some(record) if record.is_venue(VENUE_ID) => {
                let existing = record.decode::<IcpswapExecutionState>(VENUE_ID)?.ok_or_else(|| {
                    format!("ICPSwap execution state for liquidation {liquidation_id} decoded to the wrong venue")
                })?;
                if existing.plan != route {
                    return Err(format!(
                        "refusing to replace persisted ICPSwap pool plan for liquidation {liquidation_id}"
                    ));
                }
            }
            Some(record) => {
                return Err(format!(
                    "refusing to replace persisted {} execution state for liquidation {liquidation_id}",
                    record.venue
                ));
            }
            None => {
                // Committing only records this liquidation's plan in its WAL
                // row. No pool account is touched until `finalize` advances the
                // persisted state.
                let request = receipt
                    .request
                    .swap_args
                    .as_ref()
                    .ok_or_else(|| "ICPSwap route commit receipt has no swap request".to_string())?;
                let state = self.prepare_execution_state(&liquidation_id, &liquidation_id, request, route)?;
                wrapper.venue_execution = Some(VenueExecutionState::new(VENUE_ID, &state)?);
            }
        }

        wrapper.finalizer_decision = Some(decision);
        encode_meta(&mut row, &wrapper)?;
        wal.upsert_result(row)
            .await
            .map_err(|error| format!("WAL ICPSwap route commit failed for {liquidation_id}: {error}"))
    }
}

fn permanent_message(message: &str) -> String {
    format!("{ICPSWAP_FINALIZER_PERMANENT_PREFIX}{message}")
}
