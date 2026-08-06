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
            fee_total += req.debt_asset.fee();
        }
        // The protocol is sent the whole debt allowance and returns the unspent
        // part as a second transfer, whose fee is deducted from what comes back.
        // Counting only the outbound fee reported every liquidation as more
        // profitable than the ledger shows, by exactly one debt-asset fee.
        if req.liquidation.debt_amount > liq.amounts.debt_repaid {
            fee_total += req.debt_asset.fee();
        }

        let receive_amount = match swap {
            Some(swap) => swap.receive_amount.clone(),
            None => {
                if req.collateral_asset.asset_id() == req.debt_asset.asset_id() {
                    liq.amounts.collateral_received.clone()
                } else {
                    warn!("Swap not found for cross-asset liquidation; realized receive amount is treated as zero");
                    Nat::from(0u8)
                }
            }
        };

        let approval_fee_in_debt = if let Some(swap) = swap {
            if let Some(count) = swap.approval_count {
                if count > 0 && swap.pay_amount > 0u8 {
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

#[cfg(test)]
mod tests {
    use super::*;

    use candid::Principal;
    use liquidium_pipeline_core::{
        tokens::asset_id::AssetId,
        tokens::chain_token::ChainToken,
        types::protocol_types::{
            AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
            TxStatus,
        },
    };

    fn icp_token(symbol: &str, decimals: u8, fee: u64, ledger: Principal) -> ChainToken {
        ChainToken::Icp {
            ledger,
            symbol: symbol.to_string(),
            decimals,
            fee: Nat::from(fee),
        }
    }

    fn request(debt_asset: ChainToken, collateral_asset: ChainToken) -> ExecutorRequest {
        ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(0u8),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: None,
            debt_asset,
            collateral_asset,
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        }
    }

    /// A swap that only carries the amounts the profit maths reads; the price
    /// and leg fields play no part in it.
    fn swap_execution() -> SwapExecution {
        let asset = |symbol: &str| AssetId {
            chain: "icp".to_string(),
            address: Principal::anonymous().to_text(),
            symbol: symbol.to_string(),
        };
        SwapExecution {
            swap_id: 0,
            request_id: 0,
            status: "completed".to_string(),
            pay_asset: asset("ICP"),
            pay_amount: Nat::from(0u8),
            receive_asset: asset("ckUSDC"),
            receive_amount: Nat::from(0u8),
            mid_price: 0.0,
            exec_price: 0.0,
            realized_slippage_bps: 0.0,
            legs: Vec::new(),
            approval_count: None,
            ts: 0,
        }
    }

    fn liquidation(collateral_received: u64, debt_repaid: u64) -> LiquidationResult {
        LiquidationResult {
            id: 1,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(collateral_received),
                debt_repaid: Nat::from(debt_repaid),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 0,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
        }
    }

    /// Numbers taken from liquidation 1614 on mainnet, where the ledger showed
    /// 2.820225 ckUSDC while the pipeline reported 2.830225: the protocol was
    /// sent 11.629791, repaid 5.814395, and returned the remainder in a second
    /// transfer whose fee the old calculation ignored.
    #[test]
    fn realized_charges_the_fee_on_the_returned_change() {
        let ledger = Principal::anonymous();
        let token = icp_token("ckUSDC", 6, 10_000, ledger);
        let mut req = request(token.clone(), icp_token("ICP", 8, 10_000, ledger));
        req.liquidation.debt_amount = Nat::from(11_629_791u64);
        let liq = liquidation(416_949_805, 5_814_395);
        let swap = SwapExecution {
            receive_amount: Nat::from(8_654_620u64),
            ..swap_execution()
        };

        assert_eq!(SimpleProfitCalculator.realized(&req, &liq, Some(&swap)), 2_820_225);
    }

    /// Sending exactly what gets repaid leaves nothing to return, so only the
    /// outbound fee applies and the extra charge must not appear.
    #[test]
    fn realized_charges_one_fee_when_no_change_is_returned() {
        let ledger = Principal::anonymous();
        let token = icp_token("ckUSDC", 6, 10_000, ledger);
        let mut req = request(token.clone(), icp_token("ICP", 8, 10_000, ledger));
        req.liquidation.debt_amount = Nat::from(5_814_395u64);
        let liq = liquidation(416_949_805, 5_814_395);
        let swap = SwapExecution {
            receive_amount: Nat::from(8_654_620u64),
            ..swap_execution()
        };

        assert_eq!(SimpleProfitCalculator.realized(&req, &liq, Some(&swap)), 2_830_225);
    }

    #[test]
    fn realized_without_swap_same_asset_uses_collateral_as_receive_amount() {
        let ledger = Principal::anonymous();
        let token = icp_token("ckUSDC", 6, 1_000, ledger);
        let req = request(token.clone(), token);
        let liq = liquidation(1_500_000, 1_000_000);

        assert_eq!(SimpleProfitCalculator.realized(&req, &liq, None), 499_000);
    }

    #[test]
    fn realized_without_swap_cross_asset_treats_receive_amount_as_zero() {
        let debt = icp_token("ckUSDC", 6, 1_000, Principal::anonymous());
        let collateral = icp_token("ckETH", 18, 1_000, Principal::management_canister());
        let req = request(debt, collateral);
        let liq = liquidation(1_883_970_359_889_548, 2_248_987);

        assert_eq!(SimpleProfitCalculator.realized(&req, &liq, None), -2_249_987);
    }
}
