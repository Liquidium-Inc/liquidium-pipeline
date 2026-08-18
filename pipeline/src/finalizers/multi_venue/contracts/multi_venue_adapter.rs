use async_trait::async_trait;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use crate::{
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapRequest},
};

/// Immutable liquidation metadata supplied to every amount-scoped venue
/// preview without adding routing-only fields to the persisted `SwapRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenuePlanningContext {
    pub liquidation_id: String,
}

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

    /// Smallest pay amount this venue can carry all the way through execution,
    /// or `None` when nothing bounds it.
    ///
    /// A venue that reaches its exchange over a bridge inherits that bridge's
    /// own minimum withdrawal. That floor is a native per-asset amount, not a
    /// USD notional, so no dollar minimum can stand in for it: the same $8 is
    /// 0.005 ETH at one price and half that at another. Reporting it here lets
    /// the planner refuse an allocation it would otherwise commit and only
    /// discover at deposit time, once sibling legs have already moved money.
    ///
    /// The amount returned is gross: it includes the fees execution deducts
    /// before the bridge sees the transfer, so a leg sized at or above it
    /// clears the same check the bridge applies later.
    async fn minimum_executable_amount(&self, token: &ChainToken) -> Result<Option<ChainTokenAmount>, String> {
        let _ = token;
        Ok(None)
    }

    /// Produces an amount-scoped quote and initial execution state without
    /// submitting transfers, orders, or swaps.
    async fn preview(&self, context: &VenuePlanningContext, request: &SwapRequest)
    -> Result<VenueRoutePreview, String>;

    /// Advances the supplied leg as far as this venue considers immediately
    /// safe. Multi-step venues checkpoint every prepared intent before its
    /// side effect; single-step venues may ignore the checkpoint and return.
    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String>;

    /// Reconciles or recovers only the supplied leg after restart or failure.
    /// Has no caller in the finalize loop on purpose: a parked leg's external
    /// state is unknown, so only an explicit per-leg operator command may rearm
    /// it. Covered by
    /// `reenqueuing_the_parent_row_does_not_rearm_an_operator_required_mexc_leg`.
    #[allow(dead_code)]
    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String>;
}
