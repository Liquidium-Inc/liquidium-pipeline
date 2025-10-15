use async_trait::async_trait;

use crate::{
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    swappers::kong_types::{SwapAmountsReply, SwapArgs, SwapReply},
};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcrcSwapInterface: Send + Sync {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String>;

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String>;
}
