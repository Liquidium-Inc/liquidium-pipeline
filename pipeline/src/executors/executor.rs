use candid::Nat;
use liquidium_pipeline_core::{tokens::chain_token::ChainToken, types::protocol_types::LiquidationRequest};
use serde::{Deserialize, Serialize};

use crate::swappers::model::SwapRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorRequest {
    // The liquidation request to figure out how much debt we repay
    pub liquidation: LiquidationRequest,
    // If the debt and collaterals assets are the same we don't need to swap
    pub swap_args: Option<SwapRequest>,
    // The debt asset
    pub debt_asset: ChainToken,
    // The debt asset
    pub collateral_asset: ChainToken,
    // The expected profit
    pub expected_profit: i128,
    // Ref price
    #[serde(default)]
    pub ref_price: Nat,
    // Oracle price of the debt/receive asset. Older WAL records default to
    // zero and continue without the venue-level oracle guard.
    #[serde(default)]
    pub debt_ref_price: Nat,
    // Unix seconds when the two prices above were read, on our clock rather than
    // the canister's, so a finalizer can tell how stale they are. Older WAL
    // records default to zero, which reads as "age unknown".
    #[serde(default)]
    pub ref_price_at: i64,
    #[serde(default)]
    pub debt_approval_needed: bool,
    #[serde(default)]
    pub min_collateral_amount: Nat,
}
