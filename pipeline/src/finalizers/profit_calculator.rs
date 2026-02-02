//
// Profit calculation abstraction
//

use candid::Nat;
use liquidium_pipeline_core::types::protocol_types::LiquidationResult;

use log::warn;
use num_traits::ToPrimitive;

use crate::{executors::executor::ExecutorRequest, swappers::model::SwapExecution};

pub trait ProfitCalculator: Send + Sync {
    fn expected(&self, req: &ExecutorRequest, liq: Option<&LiquidationResult>) -> i128;
    fn realized(&self, req: &ExecutorRequest, liq: &LiquidationResult, swap: Option<&SwapExecution>) -> i128;
}

// Simple passthrough impl you can replace with real math
#[derive(Default)]
pub struct SimpleProfitCalculator;

impl ProfitCalculator for SimpleProfitCalculator {
    fn expected(&self, req: &ExecutorRequest, _liq: Option<&LiquidationResult>) -> i128 {
        req.expected_profit
    }

    fn realized(&self, req: &ExecutorRequest, liq: &LiquidationResult, swap: Option<&SwapExecution>) -> i128 {
        let mut fee_total = req.debt_asset.fee();
        if req.debt_approval_needed {
            fee_total = fee_total + req.debt_asset.fee();
        }

        let receive_amount = match swap {
            Some(swap) => swap.receive_amount.clone(),
            None => {
                warn!("Swap not found could not calculate profit!");
                liq.amounts.collateral_received.clone()
            }
        };

        let approval_fee_in_debt = if let Some(swap) = swap {
            if let Some(count) = swap.approval_count {
                if count > 0 && swap.pay_amount > Nat::from(0u8) {
                    let fee_native = req.collateral_asset.fee() * Nat::from(count);
                    (fee_native * swap.receive_amount.clone()) / swap.pay_amount.clone()
                } else {
                    Nat::from(0u8)
                }
            } else {
                Nat::from(0u8)
            }
        } else {
            Nat::from(0u8)
        };

        let recv = receive_amount.0.to_i128();
        let debt = liq.amounts.debt_repaid.clone().0.to_i128();
        let fee = fee_total.0.to_i128();
        let approval_fee = approval_fee_in_debt.0.to_i128();

        match (recv, debt, fee, approval_fee) {
            (Some(r), Some(d), Some(f), Some(a)) => r - d - f - a,
            _ => {
                warn!("Profit calc overflow: receive or debt too large for i128");
                0i128
            }
        }
    }
}
