use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenueRoutePreview},
    persistance::{
        MultiVenueAllocationReason, MultiVenueExecutionOutcome, MultiVenueExecutionPlan, MultiVenueExecutionState,
        VenueLegState,
    },
    price_oracle::price_oracle::PriceOracle,
    swappers::model::SwapRequest,
};

use super::{
    icpswap_first_planner_utils::{edge_bps, meets_minimum_edge, preview_to_leg, sum_leg_outputs},
    venue_registry::{VenuePreviewOutcome, VenueQuoteBook, VenueRegistry},
};

pub const ICPSWAP_FIRST_STRATEGY_ID: &str = "icpswap_first";
pub const ICPSWAP_VENUE_ID: &str = "icpswap";
pub const MEXC_VENUE_ID: &str = "mexc";
pub const KRAKEN_VENUE_ID: &str = "kraken";

pub(super) const BPS_DENOMINATOR: u32 = 10_000;
/// Smallest forced ICPSwap test allocation the oracle quote guard is applied to,
/// in USD. See `oracle_guard_waived_for_test_leg`.
const ORACLE_GUARD_MIN_TEST_ALLOCATION_USD: f64 = 10.0;

mod cex_waterfall;
mod config;
mod icpswap_allocation;
mod input;
mod quote_guard;

pub use config::IcpswapFirstPlannerConfig;
pub use input::IcpswapFirstPlanInput;
#[cfg(test)]
pub(in crate::finalizers::multi_venue) use input::reference_price_usd;
pub(in crate::finalizers::multi_venue) use quote_guard::oracle_guard_waiver_banner;
#[cfg(test)]
pub(in crate::finalizers::multi_venue) use quote_guard::oracle_price_symbol;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapFirstPlannerError {
    #[error("invalid ICPSwap-first planner input: {0}")]
    InvalidInput(String),
    #[error("no viable ICPSwap-first route: {0}")]
    NoViableRoute(String),
    /// Every venue that could take this collateral answered, and the only
    /// reason none can execute is that the amount is below their execution
    /// minimum. Unlike `NoViableRoute` this is a property of the amount rather
    /// than of venue availability, so retrying cannot resolve it and the
    /// caller may move the collateral out of the swap path.
    ///
    /// Never construct this while any venue failed to quote: an outage may end,
    /// and a retry would then produce a real route.
    #[error("no venue can execute this amount: {0}")]
    BelowVenueMinimum(String),
}

pub struct IcpswapFirstPlanner {
    venues: Arc<VenueRegistry>,
    overflow_venue_ids: Vec<String>,
    config: IcpswapFirstPlannerConfig,
    price_oracle: Option<Arc<dyn PriceOracle>>,
}

impl IcpswapFirstPlanner {
    /// Builds a planner from an ordered adapter list and verifies that every
    /// venue required by the strategy is registered exactly once.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(
        adapters: Vec<Arc<dyn MultiVenueAdapter>>,
        config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, IcpswapFirstPlannerError> {
        let venues = Arc::new(VenueRegistry::new(adapters).map_err(IcpswapFirstPlannerError::InvalidInput)?);
        Self::from_registry(venues, config)
    }

    pub(in crate::finalizers::multi_venue) fn from_registry(
        venues: Arc<VenueRegistry>,
        config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, IcpswapFirstPlannerError> {
        config.validate()?;
        if let Some(target_usd) = config.icpswap_test_allocation_usd
            && target_usd < ORACLE_GUARD_MIN_TEST_ALLOCATION_USD
        {
            warn!("{}", oracle_guard_waiver_banner(target_usd));
        }
        let overflow_venue_ids = venues
            .venue_ids()
            .into_iter()
            .filter(|venue_id| venue_id != ICPSWAP_VENUE_ID)
            .collect();
        Ok(Self {
            venues,
            overflow_venue_ids,
            config,
            price_oracle: None,
        })
    }

    /// Supplies the oracle the venue guard reads at planning time. Without one
    /// the guard falls back to the receipt prices while they are younger than
    /// `oracle_snapshot_max_age_secs`.
    pub(in crate::finalizers::multi_venue) fn with_price_oracle(mut self, price_oracle: Arc<dyn PriceOracle>) -> Self {
        self.price_oracle = Some(price_oracle);
        self
    }

    /// Quotes eligible venues and applies the ICPSwap-first allocation policy
    /// without writing the WAL or executing any swap.
    pub async fn plan(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        input.validate()?;

        // Resolve the guard's oracle prices once, before any venue is quoted, so
        // every leg of one plan is bounded against the same reference.
        let resolved = self.resolve_oracle_prices(input, quoted_at).await;
        let input = &resolved;
        let context = input.planning_context();

        if !input.is_icpswap_supported_pair() || !self.venues.contains(ICPSWAP_VENUE_ID) {
            return self.plan_overflow_only(input, quoted_at).await;
        }

        if let Some(target_usd) = self.config.icpswap_test_allocation_usd {
            return self.plan_test_fixed_icpswap_split(input, quoted_at, target_usd).await;
        }

        // The common safe-ICPSwap path needs no CEX data. Quote ICPSwap alone
        // first so adding overflow venues does not add unconditional latency or
        // API load to every liquidation.
        let quotes = self
            .venues
            .preview_venues(&context, &[ICPSWAP_VENUE_ID.to_string()], |venue_id| {
                input.request_for(venue_id, input.total_pay.value.clone())
            })
            .await
            .map_err(IcpswapFirstPlannerError::InvalidInput)?;
        let quotes = self.drop_invalid_quotes(input, quotes);
        let icpswap = quotes
            .get(ICPSWAP_VENUE_ID)
            .ok_or_else(|| IcpswapFirstPlannerError::InvalidInput("ICPSwap full preview is missing".to_string()))?;

        // Route according to the validated result of the full-amount ICPSwap preview.
        match &icpswap.outcome {
            // A quoted full allocation wins immediately when its impact is below
            // the ICPSwap ceiling; overflow venues do not need to be queried.
            VenuePreviewOutcome::Quoted(preview) if self.is_safe_icpswap(preview) => {
                // Persist a plan containing only the safe full ICPSwap preview.
                self.build_state(
                    // Retain the liquidation amounts, assets, and edge requirements.
                    input,
                    // Clone the borrowed preview into the immutable persisted plan.
                    vec![preview.clone()],
                    // Record why this plan contains exactly one venue leg.
                    MultiVenueAllocationReason::SingleVenue {
                        // Store the stable adapter ID used later for execution dispatch.
                        venue_id: ICPSWAP_VENUE_ID.to_string(),
                    },
                    // Preserve the timestamp shared by every quote in this plan.
                    quoted_at,
                )
            }
            // A valid quote at or above the ICPSwap impact limit cannot execute
            // when no CEX adapter is enabled to receive the unsafe remainder.
            VenuePreviewOutcome::Quoted(_) if self.overflow_venue_ids.is_empty() => {
                // Return a retryable planning failure instead of forcing an unsafe swap.
                Err(IcpswapFirstPlannerError::NoViableRoute(
                    // Explain both the rejected ICPSwap quote and missing fallback.
                    "ICPSwap full quote exceeds the price-impact limit and no overflow venue is enabled".to_string(),
                ))
            }
            // A valid but unsafe full quote starts the normal capacity search:
            // retain the largest safe ICPSwap amount, then apply the CEX waterfall.
            VenuePreviewOutcome::Quoted(_) => {
                // Re-quote exact allocations before constructing the persisted split.
                self.plan_split_or_fallback(input, quoted_at).await
            }
            // An unavailable response is an operational venue failure; an invalid
            // response is a malformed or safety-rejected quote. Neither may be used.
            VenuePreviewOutcome::Unavailable(icpswap_error) | VenuePreviewOutcome::Invalid(icpswap_error) => {
                // Since no ICPSwap amount is proven safe, offer the full collateral
                // allocation to the ordered MEXC-then-Kraken overflow policy.
                let overflow = self
                    // Preview and size every CEX leg before any external side effect.
                    .ordered_overflow_previews(input, input.total_pay.value.clone())
                    .await
                    // Retain the ICPSwap failure alongside any CEX waterfall failure.
                    .map_err(|error| {
                        // Classify the combined failure as a route that may become viable later.
                        IcpswapFirstPlannerError::NoViableRoute(format!(
                            // Include both venue contexts in the operator-facing error.
                            "ICPSwap quote rejected ({icpswap_error}); {error}"
                        ))
                    })?;
                // A one-leg fallback is an availability decision; a multi-leg
                // MEXC/Kraken result is an amount-scoped price-impact split.
                let allocation_reason = if overflow.len() == 1 {
                    // Record which single CEX replaced the unavailable ICPSwap leg.
                    MultiVenueAllocationReason::VenueUnavailable {
                        // Dispatch execution to the sole CEX selected by the waterfall.
                        selected_venue_id: overflow[0].venue_id.clone(),
                        // Preserve ICPSwap as the venue that could not serve this plan.
                        unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
                    }
                } else {
                    // Multiple CEX legs mean MEXC reached its safe capacity and
                    // Kraken received the exact remaining allocation.
                    MultiVenueAllocationReason::PriceImpactSplit
                };
                // Validate totals and edge, then construct the immutable execution plan.
                self.build_state(input, overflow, allocation_reason, quoted_at)
            }
        }
    }

    // Re-quotes one venue for an exact allocation and verifies that its state,
    // request, and assets match what the planner asked for.
    pub(super) async fn preview_exact(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let adapter = self.venues.adapter(venue_id).ok_or_else(|| {
            IcpswapFirstPlannerError::InvalidInput(format!("venue adapter `{venue_id}` is not registered"))
        })?;
        let context = input.planning_context();
        let preview = adapter.preview(&context, request).await.map_err(|error| {
            IcpswapFirstPlannerError::NoViableRoute(format!("{} preview failed: {error}", adapter.venue_id()))
        })?;
        self.validate_venue_preview(input, adapter.venue_id(), request, &preview)?;
        Ok(preview)
    }

    // Converts malformed responses into per-venue Invalid outcomes so one bad
    // quote cannot prevent valid venues from being considered.
    fn drop_invalid_quotes(&self, input: &IcpswapFirstPlanInput, mut quotes: VenueQuoteBook) -> VenueQuoteBook {
        for venue in quotes.iter_mut() {
            let validation_error = match &venue.outcome {
                VenuePreviewOutcome::Quoted(preview) => self
                    .validate_venue_preview(input, &venue.venue_id, &venue.request, preview)
                    .err(),
                VenuePreviewOutcome::Unavailable(_) | VenuePreviewOutcome::Invalid(_) => None,
            };
            if let Some(error) = validation_error {
                venue.outcome = VenuePreviewOutcome::Invalid(error.to_string());
            }
        }
        quotes
    }

    // Converts selected previews into persisted legs, enforces the combined
    // conservative edge, and validates the complete execution state.
    fn build_state(
        &self,
        input: &IcpswapFirstPlanInput,
        previews: Vec<VenueRoutePreview>,
        allocation_reason: MultiVenueAllocationReason,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let legs: Vec<VenueLegState> = previews.into_iter().map(preview_to_leg).collect();
        let estimated_receive = sum_leg_outputs(&legs, false)?;
        let conservative_receive = sum_leg_outputs(&legs, true)?;
        let combined_net_edge_bps = edge_bps(&conservative_receive, &input.debt_repaid)?;

        // Bad debt is bought at a known loss, so measuring the swap against the
        // debt it repaid can only ever reject it. Its own floor decides how much
        // of that shortfall may be recycled without a human; the quote is still
        // held to the oracle by `validate_venue_preview`.
        let required_edge_bps = if input.buy_bad_debt {
            self.config.bad_debt_min_net_edge_bps
        } else {
            i32::try_from(self.config.min_net_edge_bps).unwrap_or(i32::MAX)
        };

        if !meets_minimum_edge(&conservative_receive, &input.debt_repaid, required_edge_bps)? {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "combined conservative output has {:.2} bps edge, below required {} bps",
                combined_net_edge_bps, required_edge_bps
            )));
        }

        let state = MultiVenueExecutionState {
            plan: MultiVenueExecutionPlan {
                strategy_id: ICPSWAP_FIRST_STRATEGY_ID.to_string(),
                total_pay: input.total_pay.clone(),
                receive_asset: input.receive_asset.clone(),
                debt_repaid: input.debt_repaid.clone(),
                allocation_reason,
                min_net_edge_bps: self.config.min_net_edge_bps,
                estimated_receive,
                conservative_receive,
                combined_net_edge_bps,
                quoted_at,
            },
            legs,
            outcome: MultiVenueExecutionOutcome::Running,
        };
        state.validate().map_err(IcpswapFirstPlannerError::InvalidInput)?;
        Ok(state)
    }
}
