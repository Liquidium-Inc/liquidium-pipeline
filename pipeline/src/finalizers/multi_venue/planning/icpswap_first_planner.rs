use std::{collections::BTreeMap, sync::Arc};

use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
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

mod cex_waterfall;
mod config;
mod icpswap_allocation;
mod input;
mod quote_guard;
mod test_allocation;

pub use config::IcpswapFirstPlannerConfig;
pub use input::IcpswapFirstPlanInput;
#[cfg(test)]
pub(in crate::finalizers::multi_venue) use input::reference_price_usd;
#[cfg(test)]
pub(in crate::finalizers::multi_venue) use quote_guard::oracle_price_symbol;
use test_allocation::ORACLE_GUARD_MIN_TEST_ALLOCATION_USD;
pub(in crate::finalizers::multi_venue) use test_allocation::oracle_guard_waiver_banner;

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
    /// The one allocation left to try was quoted, and we refused the quote on
    /// price. Terminal for the same reason as `BelowVenueMinimum`: retrying
    /// re-asks a question the venue already answered.
    ///
    /// Only produced where no alternative allocation exists — a venue absorbing
    /// a fold, whose split was itself already rejected. Anywhere a different
    /// division could still be tried, a refused quote stays `NoViableRoute` so
    /// the next cycle may find one.
    ///
    /// Never construct this from a venue that failed to answer: silence is an
    /// outage, and an outage ends.
    #[error("no venue will price this amount acceptably: {0}")]
    NoAcceptableQuote(String),
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
        if config.mexc_test_allocation_usd.is_some()
            && [ICPSWAP_VENUE_ID, MEXC_VENUE_ID, KRAKEN_VENUE_ID]
                .iter()
                .any(|venue_id| !venues.contains(venue_id))
        {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "test MEXC allocation requires ICPSwap, MEXC, and Kraken to all be enabled".to_string(),
            ));
        }
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
        let mut resolved = self.resolve_oracle_prices(input, quoted_at).await;

        // Same reasoning for the venue floors: part of each is a live gas quote,
        // and a split must not be sized against two different versions of it.
        resolved.venue_minimums = self.resolve_venue_minimums(&resolved).await;
        let input = &resolved;

        // Two routes bypass the ICPSwap quote entirely; everything else is the
        // normal path. Each is a named method so this reads as the routing
        // decision it is.
        if !input.is_icpswap_supported_pair() || !self.venues.contains(ICPSWAP_VENUE_ID) {
            return self.plan_overflow_only(input, quoted_at).await;
        }

        if let Some(target_usd) = self.config.icpswap_test_allocation_usd {
            return self.plan_test_fixed_icpswap_split(input, quoted_at, target_usd).await;
        }

        self.plan_by_full_icpswap_quote(input, quoted_at).await
    }

    /// Asks every overflow venue how small an allocation it can still execute.
    ///
    /// A venue that cannot answer is left unbounded rather than excluded: its
    /// own deposit path still enforces the real minimum, so a failed lookup
    /// must never drop a venue that could have executed the leg.
    async fn resolve_venue_minimums(&self, input: &IcpswapFirstPlanInput) -> BTreeMap<String, ChainTokenAmount> {
        let mut minimums = BTreeMap::new();
        for venue_id in &self.overflow_venue_ids {
            let Some(adapter) = self.venues.adapter(venue_id) else {
                continue;
            };
            match adapter.minimum_executable_amount(&input.total_pay.token).await {
                Ok(Some(minimum)) => {
                    minimums.insert(venue_id.clone(), minimum);
                }
                Ok(None) => {}
                Err(error) => warn!(
                    "[multi-venue] liq_id={} {venue_id} could not report a minimum executable amount ({error}); sizing it without a floor",
                    input.liquidation_id
                ),
            }
        }
        minimums
    }

    // Re-quotes one venue for an exact allocation and verifies that its state,
    // request, and assets match what the planner asked for.
    pub(super) async fn preview_exact(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let preview = self.quote_venue(input, venue_id, request).await?;
        self.validate_venue_preview(input, venue_id, request, &preview)?;
        Ok(preview)
    }

    /// Asks one venue for an exact quote, without judging the answer.
    ///
    /// Split from `preview_exact` so a caller can tell a venue that did not
    /// answer from one whose answer we refused. Those are different facts: an
    /// outage ends, while a quote we priced out will come back the same.
    pub(super) async fn quote_venue(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let adapter = self.venues.adapter(venue_id).ok_or_else(|| {
            IcpswapFirstPlannerError::InvalidInput(format!("venue adapter `{venue_id}` is not registered"))
        })?;
        let context = input.planning_context();
        adapter.preview(&context, request).await.map_err(|error| {
            IcpswapFirstPlannerError::NoViableRoute(format!("{} preview failed: {error}", adapter.venue_id()))
        })
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

        // Every CEX leg is withdrawn on its own, so each has to clear the exit
        // floor by itself rather than as part of the combined output below.
        // ICPSwap legs settle on-chain and have no such floor, so only the
        // overflow venues are held to it.
        if let Some(stranded) = legs.iter().find(|leg| {
            self.overflow_venue_ids.contains(&leg.venue_id)
                && !input.meets_receive_minimum(&leg.quote.conservative_receive, self.cex_receive_floor_usd())
        }) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "{} leg would produce {}, under the ${:.2} withdrawable minimum (${:.2} leg floor plus a ${:.2} dust allowance)",
                stranded.venue_id,
                stranded.quote.conservative_receive.formatted(),
                self.cex_receive_floor_usd(),
                self.config.min_leg_receive_usd,
                self.config.cex_min_exec_usd
            )));
        }

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
                enforced_min_net_edge_bps: Some(required_edge_bps),
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
