use async_trait::async_trait;

use crate::swappers::model::{SwapQuote, SwapRequest};

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
