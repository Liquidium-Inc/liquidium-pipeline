//! Operator actions on parked rows: list a row's open questions, write an
//! answer and release a parked leg. The finalize stage does not read parked
//! rows, so an action and a finalize cycle never write the same row together.
use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::finalizers::multi_venue::{MultiVenueFinalizer, derive_outcome, set_meta_v2};
use crate::{
    finalizers::multi_venue::MultiVenueAdapter,
    operator::{OperatorConsole, ReconcileAction, ReconcileRequest},
    persistance::{FinalizerMetaPayload, MultiVenueExecutionState, ResultStatus, VenueLegStatus, WalStore},
    wal::{decode_receipt_wrapper, wal_load},
};

pub struct OperatorDesk {
    wal: Arc<dyn WalStore>,
    finalizer: Arc<MultiVenueFinalizer>,
    lock: tokio::sync::Mutex<()>,
}

impl OperatorDesk {
    pub fn new(wal: Arc<dyn WalStore>, finalizer: Arc<MultiVenueFinalizer>) -> Self {
        Self {
            wal,
            finalizer,
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl OperatorConsole for OperatorDesk {
    async fn reconcile(&self, request: ReconcileRequest) -> Result<String, String> {
        // One action at a time, so two operators cannot release one leg twice.
        let _guard = self.lock.lock().await;
        self.finalizer.reconcile(self.wal.as_ref(), &request).await
    }
}

impl MultiVenueFinalizer {
    /// Shows a row, or answers and releases one parked leg of it. A release
    /// only clears the park; the leg acts in a later finalize cycle.
    pub async fn reconcile(&self, wal: &dyn WalStore, request: &ReconcileRequest) -> Result<String, String> {
        let mut row = wal_load(wal, &request.liq_id)
            .await?
            .ok_or_else(|| format!("no WAL row {}", request.liq_id))?;
        let mut wrapper =
            decode_receipt_wrapper(&row)?.ok_or_else(|| format!("row {} has no receipt wrapper", row.id))?;
        let mut state = match wrapper.meta_v2.as_ref().map(|meta| &meta.payload) {
            Some(FinalizerMetaPayload::MultiVenueSwap(state)) => state.clone(),
            _ => {
                return Err(format!(
                    "row {} has no multi-venue swap state; reconcile works on those rows only",
                    row.id
                ));
            }
        };
        let report = self.describe(&row.id, row.status, row.last_error.as_deref(), &state);
        let answer = match &request.action {
            ReconcileAction::Show => return Ok(report),
            ReconcileAction::Release => None,
            ReconcileAction::Answer(answer) => Some(answer),
        };
        if row.status != ResultStatus::OperatorRequired {
            return Err(format!("row {} is {:?}, not parked", row.id, row.status));
        }

        let index = parked_leg_index(&state, request.leg_id.as_deref())?;
        let leg_id = state.legs[index].leg_id.clone();
        let adapter = self.operator_adapter(&state.legs[index].venue_id)?;
        let execution = adapter.operator_release(&state.legs[index], answer)?;
        state.legs[index].execution = execution;
        state.legs[index].status = VenueLegStatus::Running;
        state.outcome = derive_outcome(&state.legs);
        set_meta_v2(&mut wrapper, &state)?;
        row.status = ResultStatus::Enqueued;
        self.persist_wrapper(wal, &mut row, &wrapper).await?;
        info!(
            liquidation_id = %row.id,
            leg_id = %leg_id,
            action = ?request.action,
            "Operator released a parked leg"
        );
        Ok(format!(
            "{report}released leg {leg_id}; row {} is queued again\n",
            row.id
        ))
    }

    fn operator_adapter(&self, venue_id: &str) -> Result<&dyn MultiVenueAdapter, String> {
        self.venue_adapter(venue_id)
            .ok_or_else(|| format!("no adapter for venue `{venue_id}`"))
    }

    fn describe(
        &self,
        id: &str,
        status: ResultStatus,
        last_error: Option<&str>,
        state: &MultiVenueExecutionState,
    ) -> String {
        let mut lines = vec![format!("row {id}: {status:?}")];
        if let Some(error) = last_error {
            lines.push(format!("  last error: {error}"));
        }
        for leg in &state.legs {
            lines.push(format!("  leg {} at {}: {:?}", leg.leg_id, leg.venue_id, leg.status));
            if let Some(error) = &leg.last_error {
                lines.push(format!("    last error: {error}"));
            }
            if leg.status != VenueLegStatus::OperatorRequired {
                continue;
            }
            let questions = self
                .operator_adapter(&leg.venue_id)
                .and_then(|adapter| adapter.operator_questions(leg));
            match questions {
                Ok(questions) if questions.is_empty() => {
                    lines.push("    no open question: --release lets the leg check again".to_string());
                }
                Ok(questions) => {
                    lines.extend(
                        questions
                            .into_iter()
                            .map(|question| format!("    question: {}", question.detail)),
                    );
                }
                Err(error) => lines.push(format!("    questions unavailable: {error}")),
            }
        }
        lines.push(String::new());
        lines.join("\n")
    }
}

fn parked_leg_index(state: &MultiVenueExecutionState, leg_id: Option<&str>) -> Result<usize, String> {
    let parked: Vec<usize> = state
        .legs
        .iter()
        .enumerate()
        .filter(|(_, leg)| leg.status == VenueLegStatus::OperatorRequired)
        .map(|(index, _)| index)
        .collect();
    match (leg_id, parked.as_slice()) {
        (Some(id), _) => parked
            .into_iter()
            .find(|&index| state.legs[index].leg_id == id)
            .ok_or_else(|| format!("leg {id} is not parked")),
        (None, [index]) => Ok(*index),
        (None, []) => Err("no leg of this row is parked".to_string()),
        (None, _) => Err(format!("{} legs are parked; choose one with --leg", parked.len())),
    }
}
