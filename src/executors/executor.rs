use serde::{Deserialize, Serialize};

use crate::{
    icrc_token::icrc_token::IcrcToken, swappers::kong_types::SwapArgs, types::protocol_types::LiquidationRequest,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorRequest {
    // The liquidation request to figure out how much debt we repay
    pub liquidation: LiquidationRequest,
    // If the debt and collaterals assets are the same we don't need to swap
    pub swap_args: Option<SwapArgs>,
    // The debt asset
    pub debt_asset: IcrcToken,
    // The debt asset
    pub collateral_asset: IcrcToken,
    // The expected profit
    pub expected_profit: i128,
}
