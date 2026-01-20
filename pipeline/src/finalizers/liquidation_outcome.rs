use candid::Nat;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::{
    executors::executor::ExecutorRequest,
    finalizers::finalizer::FinalizerResult,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationOutcome {
    pub request: ExecutorRequest,
    pub execution_receipt: ExecutionReceipt,
    pub finalizer_result: FinalizerResult,
    pub status: ExecutionStatus,
    pub expected_profit: i128,
    pub realized_profit: i128,
    pub round_trip_secs: Option<i64>,
}
impl LiquidationOutcome {
    fn scale(&self, amount: &Nat, decimals: u8) -> f64 {
        let raw = amount.0.to_f64().unwrap_or(0.0);
        raw / 10f64.powi(decimals as i32)
    }

    pub fn formatted_debt_repaid(&self) -> String {
        let lr = match &self.execution_receipt.liquidation_result {
            Some(v) => v,
            None => return format!("0 {}", self.request.debt_asset.symbol()),
        };

        let scaled = self.scale(&lr.amounts.debt_repaid, self.request.debt_asset.decimals());
        format!("{scaled} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_received_collateral(&self) -> String {
        let lr = match &self.execution_receipt.liquidation_result {
            Some(v) => v,
            None => return format!("0 {}", self.request.collateral_asset.symbol()),
        };

        let scaled = self.scale(
            &lr.amounts.collateral_received,
            self.request.collateral_asset.decimals(),
        );
        format!("{scaled} {}", self.request.collateral_asset.symbol())
    }

    pub fn formatted_swap_output(&self) -> String {
        let Some(result) = &self.finalizer_result.swap_result else {
            return format!("0 {}", self.request.debt_asset.symbol());
        };

        let scaled = self.scale(&result.receive_amount, self.request.debt_asset.decimals());
        format!("{scaled} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_realized_profit(&self) -> String {
        let scaled = (self.realized_profit as f64) / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{scaled} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_expected_profit(&self) -> String {
        let scaled = (self.expected_profit as f64) / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{scaled} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_profit_delta(&self) -> String {
        let delta = self.realized_profit - self.expected_profit;
        let decimals = self.request.debt_asset.decimals() as i32;

        let abs_scaled = (delta.abs() as f64) / 10f64.powi(decimals);
        let prefix = if delta >= 0 { "+" } else { "-" };

        format!("{prefix}{:.3}", abs_scaled)
    }

    pub fn formatted_round_trip_secs(&self) -> String {
        self.round_trip_secs
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    }
}
