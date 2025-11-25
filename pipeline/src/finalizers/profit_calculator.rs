//
// Profit calculation abstraction
//

use liquidium_pipeline_core::types::protocol_types::LiquidationResult;

use crate::{executors::executor::ExecutorRequest, swappers::model::SwapExecution};

pub trait ProfitCalculator: Send + Sync {
    fn expected(&self, req: &ExecutorRequest, liq: Option<&LiquidationResult>) -> i128;

    fn realized(
        &self,
        req: &ExecutorRequest,
        liq: &LiquidationResult,
        swap: Option<&SwapExecution>,
        change_received: bool,
    ) -> i128;
}

// Simple passthrough impl you can replace with real math
#[derive(Default)]
pub struct PassthroughProfitCalculator;

impl ProfitCalculator for PassthroughProfitCalculator {

    fn expected(&self, req: &ExecutorRequest, _liq: Option<&LiquidationResult>) -> i128 {
        req.expected_profit
    }

    fn realized(
        &self,
        req: &ExecutorRequest,
        _liq: &LiquidationResult,
        _swap: Option<&SwapExecution>,
        _change_received: bool,
    ) -> i128 {
        // For now just echo expected; replace with real realized PnL logic
        req.expected_profit
    }
}
