use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    finalizers::{
        finalizer::{Finalizer, FinalizerResult},
        wal::{
            decode_meta, liq_id_from_receipt, wal_load, wal_mark_inflight, wal_mark_permanent_failed,
            wal_mark_retryable_failed, wal_mark_succeeded,
        },
    },
    persistance::{LiqResultRecord, ResultStatus, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapExecution, SwapRequest},
};

const MAX_FINALIZER_ATTEMPTS: i32 = 5;

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[async_trait]
pub trait DexFinalizerLogic: Send + Sync {
    async fn swap(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

#[async_trait]
impl<T> Finalizer for T
where
    T: DexFinalizerLogic,
{
    async fn finalize(
        &self,
        wal: &dyn WalStore,
        receipts: Vec<ExecutionReceipt>,
    ) -> Result<Vec<FinalizerResult>, String> {
        let mut out = Vec::with_capacity(receipts.len());

        for receipt in receipts {
            // Only finalize successful executions
            if !matches!(receipt.status, ExecutionStatus::Success) {
                out.push(FinalizerResult::noop());
                continue;
            }

            // If no swap is needed, noop
            let Some(swap_req) = &receipt.request.swap_args else {
                out.push(FinalizerResult::noop());
                continue;
            };

            let liq_id = liq_id_from_receipt(&receipt)?;

            let row = if let Some(row) = wal_load(wal, &liq_id).await? {
                match row.status {
                    ResultStatus::Succeeded => {
                        let meta = decode_meta(&row)?;
                        out.push(FinalizerResult { swap_result: meta.swap });
                        continue;
                    }
                    ResultStatus::InFlight | ResultStatus::FailedRetryable => row,
                    ResultStatus::Enqueued | ResultStatus::FailedPermanent => {
                        return Err(format!("invalid WAL state {:?} for liq_id {}", row.status, row.liq_id));
                    }
                }
            } else {
                LiqResultRecord {
                    liq_id,
                    status: ResultStatus::InFlight,
                    attempt: 0,
                    created_at: now_ts(),
                    updated_at: now_ts(),
                    meta_json: "{}".into(),
                }
            };

            wal_mark_inflight(wal, &row.liq_id).await?;

            let swap_exec = match self.swap(swap_req).await {
                Ok(s) => s,
                Err(e) => {
                    let next_attempt = row.attempt + 1;
                    if next_attempt >= MAX_FINALIZER_ATTEMPTS {
                        let _ = wal_mark_permanent_failed(wal, &row.liq_id).await;
                    } else {
                        let _ = wal_mark_retryable_failed(wal, &row.liq_id).await;
                    }
                    return Err(e);
                }
            };

            wal_mark_succeeded(wal, row, Some(swap_exec.clone())).await?;

            out.push(FinalizerResult {
                swap_result: Some(swap_exec),
            });
        }

        Ok(out)
    }
}
