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
    /// Error from this specific advance attempt. The orchestrator persists the
    /// updated leg first, then returns this error so normal finalizer backoff
    /// and retry accounting apply without losing the venue's latest state.
    pub retryable_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueExecutionLock {
    pub owner_key: String,
    pub execution_id: String,
}

/// Parent-owned persistence boundary exposed to a venue while it drives its
/// own internal workflow. Each checkpoint atomically replaces only this leg
/// inside the complete committed `meta_v2` envelope.
#[async_trait]
pub trait VenueLegCheckpoint: Send + Sync {
    async fn checkpoint(&self, progress: VenueLegProgress) -> Result<(), String>;
}

/// Amount-scoped interface for one independently persisted venue leg.
///
/// Implementations do not own the WAL or the parent liquidation status. An
/// external side effect may only be submitted when the incoming leg already
/// contains its persisted pending/idempotency state; otherwise `advance`
/// returns that state for the orchestrator to persist first.
#[async_trait]
pub trait MultiVenueAdapter: Send + Sync {
    /// Stable identifier persisted in venue legs and used for adapter lookup.
    fn venue_id(&self) -> &'static str;

    /// Rejects an adapter with missing runtime dependencies when the venue
    /// registry is built, before any liquidation reaches quote planning.
    fn validate_configuration(&self) -> Result<(), String> {
        Ok(())
    }

    /// Produces an amount-scoped quote and initial execution state without
    /// submitting transfers, orders, or swaps.
    async fn preview(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String>;

    /// Optional durable exclusivity key acquired by the parent orchestrator.
    /// Adapters remain unable to write the parent WAL row.
    fn execution_lock(&self, _leg: &VenueLegState) -> Result<Option<VenueExecutionLock>, String> {
        Ok(None)
    }

    /// Advances the supplied leg as far as this venue considers immediately
    /// safe. Multi-step venues checkpoint every prepared intent before its
    /// side effect; single-step venues may ignore the checkpoint and return.
    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String>;

    /// Reconciles or recovers only the supplied leg after restart or failure.
    #[allow(dead_code)]
    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String>;
}
