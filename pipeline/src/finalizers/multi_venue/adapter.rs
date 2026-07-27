use async_trait::async_trait;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::{
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapRequest},
};

#[derive(Debug, Clone, PartialEq)]
pub struct VenueRoutePreview {
    pub venue_id: String,
    pub request: SwapRequest,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
    pub initial_execution_state: VenueExecutionState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VenueLegProgress {
    pub execution: VenueExecutionState,
    pub status: VenueLegStatus,
    pub result: Option<SwapExecution>,
    pub last_error: Option<String>,
}

/// Amount-scoped interface for one independently persisted venue leg.
///
/// Implementations do not own the WAL or the parent liquidation status. An
/// external side effect may only be submitted when the incoming leg already
/// contains its persisted pending/idempotency state; otherwise `advance`
/// returns that state for the orchestrator to persist first.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MultiVenueAdapter: Send + Sync {
    fn venue_id(&self) -> &'static str;

    async fn preview(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String>;

    async fn advance(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String>;

    async fn recover(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String>;
}
