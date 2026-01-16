use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use log::{debug, info};

use crate::finalizers::finalizer::{Finalizer, FinalizerResult};
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;

use crate::persistance::WalStore;
use crate::stage::PipelineStage;
use crate::stages::executor::ExecutionReceipt;
use crate::wal::{
    decode_meta, wal_mark_enqueued, wal_mark_inflight, wal_mark_permanent_failed, wal_mark_retryable_failed,
    wal_mark_succeeded,
};

const MAX_FINALIZER_ATTEMPTS: i32 = 5;

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
        debug!("Finalizing rowns {:?}", rows);
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Decode all receipts and build mappings:
        // - liq_id -> attempt
        // - liq_id -> wal_row_id
        // Then group receipts by (pay_asset, receive_asset).
        let mut attempt_by_liq: HashMap<u128, i32> = HashMap::new();
        let mut wal_id_by_liq: HashMap<u128, String> = HashMap::new();
        let mut grouped: HashMap<(AssetId, AssetId), Vec<ExecutionReceipt>> = HashMap::new();

        for row in rows {
            let receipt: ExecutionReceipt = decode_meta(&row)?.expect("receipt not found in WAL meta_json");

            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

            let liq_id = liq.id;
            attempt_by_liq.insert(liq_id, row.attempt);
            wal_id_by_liq.insert(liq_id, row.id.clone());

            let pay = receipt.request.collateral_asset.asset_id();
            let recv = receipt.request.debt_asset.asset_id();

            grouped.entry((pay, recv)).or_default().push(receipt);
        }

        let mut fin_results: Vec<(FinalizerResult, ExecutionReceipt)> = vec![];

        // Iterate per asset-pair group. For each group:
        // - collapse receipts into one synthetic ExecutionReceipt
        // - call finalizer once for the collapsed receipt
        // - fan out the result to each underlying receipt and update WAL per liq
        for ((_pay_asset, _recv_asset), receipts) in grouped {
            if receipts.is_empty() {
                continue;
            }

            let collapsed = collapse_receipts(&receipts);

            info!("Executing collapesed receipt {:?}", collapsed);

            // Use the first liq in this batch to derive an attempt baseline for error handling
            let first_liq_id = collapsed
                .liquidation_result
                .as_ref()
                .ok_or_else(|| "missing liquidation_result in collapsed receipt".to_string())?
                .id;

            // Mark all rows in this batch as in-flight before calling the finalizer
            for r in &receipts {
                if let Some(liq) = &r.liquidation_result
                    && let Some(wal_id) = wal_id_by_liq.get(&liq.id)
                {
                    wal_mark_inflight(&*self.wal, wal_id).await?;
                }
            }

            match self.finalizer.finalize(&*self.wal, collapsed.clone()).await {
                Ok(res) => {
                    if res.finalized {
                        for r in receipts {
                            let liq = r
                                .liquidation_result
                                .as_ref()
                                .ok_or_else(|| "missing liquidation_result in batched receipt".to_string())?;
                            let liq_id = liq.id;

                            let wal_id = wal_id_by_liq
                                .get(&liq_id)
                                .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                            let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                            fin_results.push((res.clone(), r));
                        }
                    } else {
                        for r in receipts {
                            let liq = r
                                .liquidation_result
                                .as_ref()
                                .ok_or_else(|| "missing liquidation_result in batched receipt".to_string())?;
                            let liq_id = liq.id;

                            let wal_id = wal_id_by_liq
                                .get(&liq_id)
                                .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                            let _ = wal_mark_enqueued(&*self.wal, wal_id).await;
                            fin_results.push((res.clone(), r));
                        }
                    }
                }
                Err(e) => {
                    let base_attempt = attempt_by_liq.get(&first_liq_id).copied().unwrap_or(0);
                    let next_attempt = base_attempt + 1;

                    for r in receipts {
                        let liq = r
                            .liquidation_result
                            .as_ref()
                            .ok_or_else(|| "missing liquidation_result in batched receipt".to_string())?;
                        let liq_id = liq.id;

                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        if next_attempt >= MAX_FINALIZER_ATTEMPTS {
                            let _ = wal_mark_permanent_failed(&*self.wal, wal_id).await;
                        } else {
                            let _ = wal_mark_retryable_failed(&*self.wal, wal_id).await;
                        }
                    }

                    return Err(e);
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

            let expected_profit = self.profit_calc.expected(req, Some(liq));
            let realized_profit = self.profit_calc.realized(liq, fin_res.swap_result.as_ref());

            outcomes.push(LiquidationOutcome {
                request: req.clone(),
                execution_receipt: receipt.clone(),
                finalizer_result: fin_res.clone(),
                status: receipt.status.clone(),
                expected_profit,
                realized_profit,
            });
        }

        Ok(outcomes)
    }
}

// Collapse multiple receipts in a batch into a single synthetic ExecutionReceipt
// by summing liquidation amounts. All other fields are taken from the first receipt.
fn collapse_receipts(legs: &[ExecutionReceipt]) -> ExecutionReceipt {
    assert!(!legs.is_empty());

    let mut base = legs[0].clone();

    let mut total_collateral = Nat::from(0u32);
    let mut total_debt = Nat::from(0u32);

    for r in legs {
        if let Some(liq) = &r.liquidation_result {
            total_collateral += liq.amounts.collateral_received.clone();
            total_debt += liq.amounts.debt_repaid.clone();
        }
    }

    if let Some(liq) = &mut base.liquidation_result {
        liq.amounts.collateral_received = total_collateral;
        liq.amounts.debt_repaid = total_debt;
    }

    base
}
