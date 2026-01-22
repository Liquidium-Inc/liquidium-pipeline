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
pub struct SimpleProfitCalculator;

impl ProfitCalculator for SimpleProfitCalculator {
    fn expected(&self, req: &ExecutorRequest, _liq: Option<&LiquidationResult>) -> i128 {
        req.expected_profit
    }

    fn realized(&self, liq: &LiquidationResult, swap: Option<&SwapExecution>) -> i128 {
        // For now just echo expected; replace with real realized PnL logic
        if let Some(swap) = swap {
            let recv = swap.receive_amount.clone().0.to_i128();
            let debt = liq.amounts.debt_repaid.clone().0.to_i128();
            return match (recv, debt) {
                (Some(r), Some(d)) => r - d,
                _ => {
                    warn!("Profit calc overflow: receive or debt too large for i128");
                    0i128
                }
            };
        }

        warn!("Swap not found could not calculate profit!");

        0i128
    }
}
