use async_trait::async_trait;

use crate::{
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    swappers::{
        kong::kong_types::{SwapAmountsReply, SwapArgs, SwapReply},
        model::{AssetId, SwapExecution, SwapQuote, SwapRequest},
    },
};

use std::sync::Arc;

#[async_trait]
pub trait SwapInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}
