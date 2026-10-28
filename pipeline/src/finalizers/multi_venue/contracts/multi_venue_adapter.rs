use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::types::side_cost::SideCost;

use crate::{
    outbound::driver::OperatorDecision,
    persistance::{PricedSideCost, VenueExecutionState, VenueLegFailure, VenueLegState, VenueLegStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapRequest},
};

/// Immutable liquidation metadata supplied to every amount-scoped venue
/// preview without adding routing-only fields to the persisted `SwapRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenuePlanningContext {
    pub liquidation_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum VenueAmountError {
    #[error("{0}")]
    BelowMinimum(String),
    #[error("{0}")]
    Unavailable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct VenueRoutePreview {
    pub venue_id: String,
    pub request: SwapRequest,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
    pub initial_execution_state: VenueExecutionState,
    /// The output at the venue's own price before fixed transfer fees. The
    /// oracle guard reads this, so a correctly counted fee does not look like
    /// a bad price. `None` means the quote has no fixed fees to separate.
    pub market_receive: Option<Nat>,
    /// What the route pays from other balances, in their own assets.
    pub side_costs: Vec<SideCost>,
    /// The planner's pricing of `side_costs`. `None` until the planner prices it.
    pub priced_side_costs: Option<Vec<PricedSideCost>>,
}

impl VenueRoutePreview {
    /// A preview with no fixed fees to separate and no side costs. A venue
    /// that has either sets those fields after.
    pub fn new(
        venue_id: impl Into<String>,
        request: SwapRequest,
        quote: SwapQuote,
        conservative_receive: ChainTokenAmount,
        initial_execution_state: VenueExecutionState,
    ) -> Self {
        Self {
            venue_id: venue_id.into(),
            request,
            quote,
            conservative_receive,
            initial_execution_state,
            market_receive: None,
            side_costs: Vec::new(),
            priced_side_costs: None,
        }
    }
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
    /// Why a failed leg failed, where the row's outcome depends on it.
    pub failure: Option<VenueLegFailure>,
}

/// A fact that only an operator can supply before a parked leg moves on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorQuestion {
    /// What is undecided, with the ids the operator needs to check it.
    pub detail: String,
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

    /// Checks provider amount limits without transferring or reserving funds.
    async fn check_amount(&self, _request: &SwapRequest) -> Result<(), VenueAmountError> {
        Ok(())
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

    /// Questions that only an operator can answer before this parked leg
    /// moves on. A park without a question needs a release only.
    fn operator_questions(&self, leg: &VenueLegState) -> Result<Vec<OperatorQuestion>, String> {
        let _ = leg;
        Err(format!("venue `{}` has no operator release", self.venue_id()))
    }

    /// Writes an operator's answer (`None` releases only) into a parked leg and
    /// clears the park. It makes no venue call; the leg acts on the next cycle.
    fn operator_release(
        &self,
        leg: &VenueLegState,
        answer: Option<&OperatorDecision>,
    ) -> Result<VenueExecutionState, String> {
        let _ = answer;
        Err(format!(
            "venue `{}` has no operator release for leg `{}`",
            self.venue_id(),
            leg.leg_id
        ))
    }

    /// Reconciles or recovers only the supplied leg after restart or failure.
    #[allow(dead_code)]
    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String>;
}
