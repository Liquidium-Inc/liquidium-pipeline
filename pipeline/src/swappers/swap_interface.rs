use async_trait::async_trait;

use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait QuoteInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SwapInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

#[async_trait]
impl<T: SwapInterface + ?Sized> QuoteInterface for T {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        SwapInterface::quote(self, req).await
    }
}
