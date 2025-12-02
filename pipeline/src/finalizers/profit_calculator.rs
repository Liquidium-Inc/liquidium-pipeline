//
// Profit calculation abstraction
//

use liquidium_pipeline_core::types::protocol_types::LiquidationResult;

use log::warn;
use num_traits::ToPrimitive;

use crate::{executors::executor::ExecutorRequest, swappers::model::SwapExecution};

pub trait ProfitCalculator: Send + Sync {
    fn expected(&self, req: &ExecutorRequest, liq: Option<&LiquidationResult>) -> i128;

    fn realized(&self, liq: &LiquidationResult, swap: Option<&SwapExecution>) -> i128;
}

// Simple passthrough impl you can replace with real math
#[derive(Default)]
pub struct PassthroughProfitCalculator;

impl ProfitCalculator for PassthroughProfitCalculator {
    fn expected(&self, req: &ExecutorRequest, _liq: Option<&LiquidationResult>) -> i128 {
        req.expected_profit
    }

    fn realized(&self, liq: &LiquidationResult, swap: Option<&SwapExecution>) -> i128 {
        // For now just echo expected; replace with real realized PnL logic
        if swap.is_some() {
            let res = swap.unwrap().receive_amount.clone() - liq.amounts.debt_repaid.clone();
            return res.0.to_i128().expect("could not covert profit to integer");
        }

        warn!("Swap not found could not calculate profit!");

        0i128
    }
}
