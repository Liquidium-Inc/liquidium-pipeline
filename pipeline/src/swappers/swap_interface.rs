use async_trait::async_trait;

use crate::error::AppError;
use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SwapInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, AppError>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, AppError>;
}
