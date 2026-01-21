use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, info};

use crate::finalizers::finalizer::{Finalizer, FinalizerResult};
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;

use crate::persistance::WalStore;
use crate::stage::PipelineStage;
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::wal::{
    decode_receipt_wrapper, wal_mark_enqueued, wal_mark_inflight, wal_mark_permanent_failed, wal_mark_retryable_failed,
    wal_mark_succeeded,
};

const MAX_FINALIZER_ERRORS: i32 = 5;

//
// FinalizeStage: pipeline stage over a concrete Finalizer
//
pub struct FinalizeStage<F, D, P>
where
    F: Finalizer,
    D: WalStore,
    P: ProfitCalculator,
{
    pub wal: Arc<D>,
    pub finalizer: Arc<F>,
    pub profit_calc: Arc<P>,
}

impl<F, D, P> FinalizeStage<F, D, P>
where
    F: Finalizer,
    D: WalStore,
    P: ProfitCalculator,
{
    pub fn new(wal: Arc<D>, finalizer: Arc<F>, profit_calc: Arc<P>) -> Self {
        Self {
            wal,
            finalizer,
            profit_calc,
        }
    }
}

#[async_trait]
impl<'a, F, D, P> PipelineStage<'a, (), Vec<LiquidationOutcome>> for FinalizeStage<F, D, P>
where
    F: Finalizer + Sync + Send,
    D: WalStore + Sync + Send,
    P: ProfitCalculator + Sync + Send,
{
    async fn process(&self, _: &'a ()) -> Result<Vec<LiquidationOutcome>, String> {
        // Load pending entries from WAL
        let rows = self.wal.get_pending(100).await.map_err(|e| e.to_string())?;
        debug!("Finalizing rows {:?}", rows);
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Decode all receipts and build mappings:
        // - liq_id -> wal_row_id
        // - liq_id -> created_at
        // Then collect receipts for per-receipt processing.
        let mut wal_id_by_liq: HashMap<u128, String> = HashMap::new();
        let mut created_at_by_liq: HashMap<u128, i64> = HashMap::new();
        let mut error_count_by_liq: HashMap<u128, i32> = HashMap::new();
        let mut receipts: Vec<ExecutionReceipt> = vec![];

        for row in rows {
            let meta = decode_receipt_wrapper(&row)?
                .ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id))?;
            let receipt: ExecutionReceipt = meta.receipt;

            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

            let liq_id = liq.id;
            wal_id_by_liq.insert(liq_id, row.id.clone());
            created_at_by_liq.insert(liq_id, row.created_at);
            error_count_by_liq.insert(liq_id, row.error_count);

            receipts.push(receipt);
        }

        let mut fin_results: Vec<(FinalizerResult, ExecutionReceipt)> = vec![];

        // Run each receipt independently (no batching).
        for receipt in receipts {
            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;
            let liq_id = liq.id;

            info!(
                "[finalize] 🧾 executing receipt: liq_id={} pay={} recv={} debt_repaid={} collateral_received={} swap={}",
                liq_id,
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol(),
                liq.amounts.debt_repaid,
                liq.amounts.collateral_received,
                receipt.request.swap_args.is_some()
            );

            if receipt.request.swap_args.is_none() {
                let wal_id = wal_id_by_liq
                    .get(&liq_id)
                    .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;
                let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                fin_results.push((
                    FinalizerResult {
                        swap_result: None,
                        finalized: true,
                        swapper: Some("none".to_string()),
                    },
                    receipt,
                ));
                continue;
            }

            if let Some(wal_id) = wal_id_by_liq.get(&liq_id) {
                wal_mark_inflight(&*self.wal, wal_id).await?;
            }

            match self.finalizer.finalize(&*self.wal, receipt.clone()).await {
                Ok(res) => {
                    if res.finalized {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                        fin_results.push((res.clone(), receipt));
                    } else {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        let _ = wal_mark_enqueued(&*self.wal, wal_id).await;
                    }
                }
                Err(e) => {
                    let base_errors = error_count_by_liq.get(&liq_id).copied().unwrap_or(0);
                    let next_errors = base_errors + 1;
                    let err_msg = e.to_string();

                    let wal_id = wal_id_by_liq
                        .get(&liq_id)
                        .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                    debug!("Failed finalization {}", err_msg);
                    if next_errors >= MAX_FINALIZER_ERRORS {
                        let _ = wal_mark_permanent_failed(&*self.wal, wal_id, err_msg.clone()).await;

                        let mut failed_receipt = receipt.clone();
                        failed_receipt.status =
                            ExecutionStatus::SwapFailed(format!("finalizer permanent failure: {}", err_msg));

                        fin_results.push((
                            FinalizerResult {
                                swap_result: None,
                                finalized: true,
                                swapper: None,
                            },
                            failed_receipt,
                        ));
                    } else {
                        let _ = wal_mark_retryable_failed(&*self.wal, wal_id, err_msg.clone()).await;
                    }

                    debug!("Finalization failed for receipt; continuing: {}", err_msg);
                }
            }
        }

        let mut outcomes = vec![];

        for (fin_res, receipt) in fin_results {
            let req = &receipt.request;
            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for request: {:?}", req))?;

            let round_trip_secs = if fin_res.finalized {
                created_at_by_liq
                    .get(&liq.id)
                    .map(|created| now_ts().saturating_sub(*created))
            } else {
                None
            };

            let expected_profit = self.profit_calc.expected(req, Some(liq));
            let realized_profit = self.profit_calc.realized(liq, fin_res.swap_result.as_ref());

            outcomes.push(LiquidationOutcome {
                request: req.clone(),
                execution_receipt: receipt.clone(),
                finalizer_result: fin_res.clone(),
                status: receipt.status.clone(),
                expected_profit,
                realized_profit,
                round_trip_secs,
            });
        }

        Ok(outcomes)
    }
}
