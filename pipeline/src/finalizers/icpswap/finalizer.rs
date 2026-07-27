use std::sync::Arc;

use async_trait::async_trait;
use icrc_ledger_types::icrc1::account::Account;
use tracing::debug;

use crate::{
    finalizers::dex_finalizer::{DexRouteFinalizer, DexRoutePreview},
    finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult},
    persistance::{FinalizerDecisionSnapshot, VenueExecutionState, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            execution::{IcpswapExecutionStateStore, WalIcpswapExecutionStateStore},
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
const ICPSWAP_LOCK_CLEANUP_PREFIX: &str = "ICPSwap lock cleanup: ";

/// Cooldown applied after a non-terminal step fails.
///
/// Such a failure re-enqueues the row without consuming the finalize stage's
/// retry budget, so nothing else paces it -- the exponential backoff there only
/// gates `FailedRetryable`. Roughly one order of magnitude above the daemon
/// cycle, so a persistently failing step backs off instead of re-running every
/// two seconds, while a transient blip still recovers promptly.
pub(super) const RETRY_COOLDOWN_NANOS: u64 = 20 * 1_000_000_000;

type Clock = dyn Fn() -> u64 + Send + Sync;

/// Terminal steps own the per-owner lock release: the execution is over, so the
/// slot must be freed whether it ended in success, refund, or failure.
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
    pub(super) clock: Arc<Clock>,
    pub(super) watchdog: Arc<dyn Watchdog>,
}

impl IcpswapFinalizer {
    pub fn new(workflow: Arc<dyn IcpswapFinalizerLogic>, trader: Account) -> Self {
        Self::from_workflow_with_clock(workflow, trader, Arc::new(now_nanos))
    }

    pub fn from_workflow_with_clock(
        workflow: Arc<dyn IcpswapFinalizerLogic>,
        trader: Account,
        clock: Arc<Clock>,
    ) -> Self {
        Self {
            workflow,
            trader,
            clock,
            watchdog: noop_watchdog(),
        }
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

    /// Frees the owner slot after terminal state is durable. Deletion is
    /// idempotent, so propagating a failure keeps the WAL row retryable and the
    /// next cycle can perform cleanup without replaying any IC side effect.
    async fn release_execution_lock(
        &self,
        wal: &dyn WalStore,
        owner_key: &str,
        liquidation_id: &str,
    ) -> Result<(), String> {
        if let Err(error) = wal.release_icpswap_execution_lock(owner_key, liquidation_id).await {
            let detail = format!("failed releasing terminal ICPSwap owner lock: {error}");
            self.watchdog
                .notify(WatchdogEvent::OperatorRequired {
                    execution_id: liquidation_id.to_string(),
                    venue: VENUE_ID.to_string(),
                    pending_step: "lock_cleanup".to_string(),
                    owner: owner_key.to_string(),
                    details: detail.clone(),
                })
                .await;
            return Err(format!("{ICPSWAP_LOCK_CLEANUP_PREFIX}{detail}"));
        }
        Ok(())
    }

    /// Alerts for as long as the execution stays parked, not just on the way in.
    /// A parked swap holds the owner lock and keeps polling, so it looks healthy
    /// from the outside; alerting once means a single dropped webhook hides a
    /// stalled venue indefinitely. The watchdog's cooldown key throttles this
    /// into a periodic re-escalation.
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
        let owner_key = loaded_state.owner.owner.to_text();

        // A prior cycle may have persisted terminal state and then failed to
        // delete the durable lock. Retry only that idempotent cleanup; never
        // call the venue workflow again for a terminal execution.
        if is_terminal_step(loaded_state.step) {
            self.release_execution_lock(wal, &owner_key, &liquidation_id)
                .await?;
            return self.result_for_state(&receipt, &loaded_state, now_nanos).await;
        }

        // Back off after a failed step instead of re-running it every cycle. The
        // owner lock is deliberately not taken yet: a cooling-down execution
        // still holds it, and re-acquiring here would be a no-op anyway.
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

        if !wal
            .acquire_icpswap_execution_lock(&owner_key, &liquidation_id)
            .await
            .map_err(|error| format!("failed acquiring ICPSwap owner lock: {error}"))?
        {
            // Contention, not failure. Returning `Err` here would count toward
            // the finalize stage's bounded retry budget and permanently fail a
            // valid liquidation for merely waiting its turn.
            debug!(
                owner = %self.trader.owner,
                liquidation_id, "ICPSwap owner busy; deferring to a later cycle"
            );
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
        let (state, advance_error) = match self
            .workflow
            .advance_loaded(&store, &liquidation_id, self.trader, now_nanos, loaded_state)
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

        if is_terminal_step(state.step) {
            self.release_execution_lock(wal, &owner_key, &liquidation_id)
                .await?;
        }

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
        } else if error.starts_with(ICPSWAP_LOCK_CLEANUP_PREFIX) {
            FinalizerErrorKind::LockCleanup
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
                // Committing only records this liquidation's own plan in its own
                // WAL row -- no pool account is touched -- so the owner lock is
                // left to `finalize`, which is where the IC calls happen. Taking
                // it here would reserve the venue before any work exists to do,
                // and turn a routine wait into a finalizer error.
                let state = self.workflow.prepare(&liquidation_id, route, self.trader);
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
