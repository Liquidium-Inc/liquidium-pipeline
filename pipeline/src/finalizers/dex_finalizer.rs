use async_trait::async_trait;
use log::{debug, warn};

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    persistance::{ResultStatus, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapExecution, SwapRequest},
    wal::{
        decode_meta, liq_id_from_receipt, wal_load, wal_mark_inflight, wal_mark_permanent_failed,
        wal_mark_retryable_failed, wal_mark_succeeded,
    },
};

const MAX_FINALIZER_ATTEMPTS: i32 = 5;

// Tunables
const BASE_SLIPPAGE_BPS: u32 = 7500; // 0.75%
const STEP_SLIPPAGE_BPS: u32 = 5000; // +0.5% per retry
const MAX_SLIPPAGE_BPS: u32 = 50000; // 5.0% cap
const MAX_SLIPPAGE_RETRIES: u32 = 3; // total attempts = MAX_SLIPPAGE_RETRIES + 1

fn slippage_for_retry(retry: u32, explicit: Option<u32>) -> u32 {
    let base = explicit.unwrap_or(BASE_SLIPPAGE_BPS);
    let bump = STEP_SLIPPAGE_BPS.saturating_mul(retry);
    base.saturating_add(bump).min(MAX_SLIPPAGE_BPS)
}

#[async_trait]
pub trait DexFinalizerLogic: Send + Sync {
    async fn swap(&self, req: &SwapRequest) -> Result<SwapExecution, String>;

    async fn swap_with_slippage_retry(&self, swap_req: SwapRequest) -> Result<SwapExecution, String> {
        let mut last_err: Option<String> = None;

        for retry in 0..=MAX_SLIPPAGE_RETRIES {
            let mut req = swap_req.clone();

            let eff_slippage = slippage_for_retry(retry, req.max_slippage_bps);
            req.max_slippage_bps = Some(eff_slippage);

            debug!("[Slippage] {}", eff_slippage);

            match self.swap(&req).await {
                Ok(exec) => return Ok(exec),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.expect("at least one attempt was made"))
    }
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

            let (row, mut outcome) = if let Some(row) = wal_load(wal, &liq_id).await? {
                let row = match row.status {
                    ResultStatus::Succeeded => {
                        continue;
                    }
                    ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable => row,
                    ResultStatus::FailedPermanent => {
                        return Err(format!("invalid WAL state {:?} for liq_id {}", row.status, row.liq_id));
                    }
                };

                let outcome = decode_meta(&row)?;

                if outcome.is_none() {
                    return Err("Could not decode liquidation outoome".to_string());
                }
                (row, outcome.unwrap())
            } else {
                warn!("Liquidation {} was not found in journal, skipping...", liq_id);
                continue;
            };

            wal_mark_inflight(wal, &row.liq_id).await?;

            let swap_exec = match self.swap_with_slippage_retry(swap_req.clone()).await {
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

            let finlizer_result = FinalizerResult {
                swap_result: Some(swap_exec),
            };

            outcome.finalizer_result = finlizer_result.clone();
            wal_mark_succeeded(wal, row, outcome).await?;

            out.push(finlizer_result)
        }

        Ok(out)
    }
}
