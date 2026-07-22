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

type Clock = dyn Fn() -> u64 + Send + Sync;

/// WAL-backed finalizer for the manual ICPSwap execution lifecycle.
pub struct IcpswapFinalizer {
    workflow: Arc<dyn IcpswapFinalizerLogic>,
    trader: Account,
    clock: Arc<Clock>,
    watchdog: Arc<dyn Watchdog>,
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
                swapper: Some("icpswap".to_string()),
                reason: None,
            }),
            IcpswapStep::Refunded => Ok(FinalizerResult {
                swap_result: None,
                finalized: true,
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

    async fn notify_operator_required(
        &self,
        execution_id: &str,
        previous_step: IcpswapStep,
        state: &IcpswapExecutionState,
    ) {
        if previous_step == IcpswapStep::OperatorRequired || state.step != IcpswapStep::OperatorRequired {
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
        let loaded_state = self.load_state(&store, &liquidation_id).await?;
        let previous_step = loaded_state.step;
        let now_nanos = (self.clock)();

        let owner_key = self.trader.owner.to_text();
        if !wal
            .acquire_icpswap_owner_lock(&owner_key, &liquidation_id)
            .await
            .map_err(|error| format!("failed acquiring ICPSwap owner lock: {error}"))?
        {
            return Err(format!(
                "another manual ICPSwap execution is active for owner {}",
                self.trader.owner
            ));
        }
        let state = match self
            .workflow
            .advance_loaded(&store, &liquidation_id, self.trader, now_nanos, loaded_state)
            .await
        {
            Ok(state) => state,
            Err(error) => {
                let mut state = self.load_state(&store, &liquidation_id).await?;
                state.last_error = Some(error.clone());
                store.persist(&liquidation_id, &state).await?;
                self.notify_operator_required(&liquidation_id, previous_step, &state)
                    .await;
                if state.step == IcpswapStep::Failed {
                    return Err(permanent_message(&error));
                }
                return Ok(FinalizerResult::noop());
            }
        };

        self.notify_operator_required(&liquidation_id, previous_step, &state)
            .await;

        let result = self.result_for_state(&receipt, &state, now_nanos).await;
        if matches!(state.step, IcpswapStep::Completed | IcpswapStep::Refunded) {
            wal.release_icpswap_owner_lock(&owner_key, &liquidation_id)
                .await
                .map_err(|error| format!("failed releasing ICPSwap owner lock: {error}"))?;
        }
        result
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

        let mut acquired_owner_lock = false;
        match &wrapper.venue_execution {
            Some(record) if record.is_venue(VENUE_ID) => {
                let existing = record
                    .decode::<IcpswapExecutionState>(VENUE_ID)?
                    .expect("venue was checked above");
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
                let owner_key = self.trader.owner.to_text();
                if !wal
                    .acquire_icpswap_owner_lock(&owner_key, &liquidation_id)
                    .await
                    .map_err(|error| format!("failed acquiring ICPSwap owner lock: {error}"))?
                {
                    return Err(format!(
                        "another manual ICPSwap execution is active for owner {}",
                        self.trader.owner
                    ));
                }
                acquired_owner_lock = true;
                let state = self.workflow.prepare(&liquidation_id, route, self.trader);
                wrapper.venue_execution = Some(VenueExecutionState::new(VENUE_ID, &state)?);
            }
        }

        wrapper.finalizer_decision = Some(decision);
        if let Err(error) = encode_meta(&mut row, &wrapper) {
            if acquired_owner_lock {
                let _ = wal
                    .release_icpswap_owner_lock(&self.trader.owner.to_text(), &liquidation_id)
                    .await;
            }
            return Err(error);
        }
        let result = wal
            .upsert_result(row)
            .await
            .map_err(|error| format!("WAL ICPSwap route commit failed for {liquidation_id}: {error}"));
        if result.is_err() && acquired_owner_lock {
            let _ = wal
                .release_icpswap_owner_lock(&self.trader.owner.to_text(), &liquidation_id)
                .await;
        }
        result
    }
}

fn permanent_message(message: &str) -> String {
    format!("{ICPSWAP_FINALIZER_PERMANENT_PREFIX}{message}")
}
