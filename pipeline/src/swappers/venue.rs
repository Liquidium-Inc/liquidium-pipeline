use async_trait::async_trait;

use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapRequest},
    swap_interface::SwapInterface,
};

/// Common quote contract implemented by a concrete exchange or DEX client.
///
/// Multi-venue adapters reuse this lower-level contract, but venue selection
/// belongs to `VenueRegistry` and the multi-venue planner.
#[async_trait]
pub trait SwapVenue: Send + Sync {
    fn venue_name(&self) -> &'static str;
    async fn init(&self) -> Result<(), String>;
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
}

/// Optional immediate-execution contract retained for standalone venue
/// implementations such as Kong. Multi-venue execution uses persisted legs.
#[async_trait]
pub trait ExecutableSwapVenue: SwapVenue {
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

#[async_trait]
impl<T: ExecutableSwapVenue + ?Sized> SwapInterface for T {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        SwapVenue::quote(self, req).await
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        ExecutableSwapVenue::execute(self, req).await
    }
}
