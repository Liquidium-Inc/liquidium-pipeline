use async_trait::async_trait;

use crate::error::AppResult;
use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SwapInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> AppResult<SwapQuote>;
    async fn execute(&self, req: &SwapRequest) -> AppResult<SwapExecution>;
}
