use std::sync::Arc;

use async_trait::async_trait;

use crate::finalizers::finalizer::Finalizer;
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;

use crate::persistance::WalStore;
use crate::stage::PipelineStage;
use crate::stages::executor::ExecutionReceipt;

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
impl<'a, F, D, P> PipelineStage<'a, Vec<ExecutionReceipt>, Vec<LiquidationOutcome>> for FinalizeStage<F, D, P>
where
    F: Finalizer + Sync + Send,
    D: WalStore + Sync + Send,
    P: ProfitCalculator + Sync + Send,
{
    async fn process(&self, input: &'a Vec<ExecutionReceipt>) -> Result<Vec<LiquidationOutcome>, String> {
        // Call batch finalizer
        let fin_results = self.finalizer.finalize(&*self.wal, input.clone()).await?;

        if fin_results.len() != input.len() {
            return Err("finalizer returned mismatched result count".to_string());
        }

        let mut outcomes = Vec::with_capacity(input.len());

        for (receipt, fin_res) in input.iter().zip(fin_results.iter()) {
            let req = &receipt.request;
            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for request: {:?}", req))?;

            let expected_profit = self.profit_calc.expected(req, Some(liq));

            let realized_profit =
                self.profit_calc
                    .realized(req, liq, fin_res.swap_result.as_ref(), receipt.change_received);

            outcomes.push(LiquidationOutcome {
                request: req.clone(),
                liquidation_result: liq.clone(),
                swap_result: fin_res.swap_result.clone(),
                status: receipt.status.clone(),
                expected_profit,
                realized_profit,
            });
        }

        Ok(outcomes)
    }
}
