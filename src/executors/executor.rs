use async_trait::async_trait;
use lending::interface::liquidation::LiquidationRequest;

use crate::icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount};

use super::kong_swap::types::{SwapAmountsReply, SwapArgs, SwapReply};

#[derive(Debug)]
pub struct ExecutorRequest {
    pub liquidation: LiquidationRequest,
    pub swap_args: SwapArgs,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcrcSwapExecutor {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String>;

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String>;
}
