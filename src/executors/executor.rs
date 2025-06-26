use async_trait::async_trait;
use lending::interface::liquidation::LiquidationRequest;

use crate::icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount};

use super::kong_swap::types::{SwapAmountsReply, SwapArgs, SwapReply};

#[derive(Debug)]
pub struct ExecutorRequest {
    // The liquidation request to figure out how much debt we repay
    pub liquidation: LiquidationRequest,
    // If the debt and collaterals assets are the same we don't need to swap
    pub swap_args: Option<SwapArgs>,
    // The expected profit
    pub expected_profit: u128
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcrcSwapExecutor: Send + Sync {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String>;

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String>;
}
