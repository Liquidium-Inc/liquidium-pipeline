use liquidium_pipeline_core::types::protocol_types::LiquidationResult;

use num_traits::ToPrimitive;

use crate::{executors::executor::ExecutorRequest, stages::executor::ExecutionStatus, swappers::model::SwapExecution};

#[derive(Debug, Clone)]
pub struct LiquidationOutcome {
    pub request: ExecutorRequest,
    pub liquidation_result: LiquidationResult,
    pub swap_result: Option<SwapExecution>,
    pub status: ExecutionStatus,
    pub expected_profit: i128,
    pub realized_profit: i128,
}

impl LiquidationOutcome {
    pub fn formatted_debt_repaid(&self) -> String {
        let amount = self.liquidation_result.amounts.debt_repaid.0.to_f64().unwrap()
            / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_received_collateral(&self) -> String {
        let amount = self.liquidation_result.amounts.collateral_received.0.to_f64().unwrap()
            / 10f64.powi(self.request.collateral_asset.decimals() as i32);
        format!("{amount} {}", self.request.collateral_asset.symbol())
    }

    pub fn formatted_swap_output(&self) -> String {
        if let Some(result) = &self.swap_result {
            let amount =
                result.receive_amount.0.to_f64().unwrap() / 10f64.powi(self.request.debt_asset.decimals() as i32);
            return format!("{amount} {}", self.request.debt_asset.symbol());
        }
        format!("0 {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_realized_profit(&self) -> String {
        let amount = self.realized_profit as f64 / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_expected_profit(&self) -> String {
        let amount = self.expected_profit as f64 / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_profit_delta(&self) -> String {
        let delta = self.realized_profit - self.expected_profit;
        let decimals = self.request.debt_asset.decimals() as i32;
        let abs_amount = (delta.abs() as f64) / 10f64.powi(decimals);
        let prefix = if delta >= 0 { "+" } else { "-" };
        format!("{prefix}{:.3}", abs_amount)
    }
}
