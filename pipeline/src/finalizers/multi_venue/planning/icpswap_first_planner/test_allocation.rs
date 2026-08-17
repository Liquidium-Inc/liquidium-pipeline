//! Explicit allocation overrides used to exercise multi-venue execution.
//!
//! Nothing in this module participates in normal price-impact allocation. It
//! is reached only when the corresponding test-only USD settings are present.

use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use tracing::warn;

use crate::{
    finalizers::multi_venue::VenueRoutePreview,
    persistance::{MultiVenueAllocationReason, MultiVenueExecutionState},
};

use super::{
    ICPSWAP_VENUE_ID, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError, KRAKEN_VENUE_ID,
    MEXC_VENUE_ID,
};

/// Smallest forced ICPSwap allocation whose quote can meaningfully be compared
/// with the oracle after fixed ledger fees, in USD.
pub(super) const ORACLE_GUARD_MIN_TEST_ALLOCATION_USD: f64 = 10.0;

/// Startup notice that a configured test allocation disables the oracle quote
/// guard for its deliberately tiny ICPSwap leg.
pub(in crate::finalizers::multi_venue) fn oracle_guard_waiver_banner(target_usd: f64) -> String {
    let rule = "=".repeat(66);
    format!(
        "\n{rule}\n\
         ⚠️  ORACLE QUOTE GUARD DISABLED FOR THE ICPSWAP TEST LEG  ⚠️\n\
         {rule}\n\
         ICPSWAP_TEST_ALLOCATION_USD=${target_usd:.2} is below the \
         ${ORACLE_GUARD_MIN_TEST_ALLOCATION_USD:.2} minimum, so that leg's\n\
         quote is NOT priced against the oracle on any liquidation. At this size\n\
         its fixed ledger fees are a larger share of the leg than the whole\n\
         discount budget, so the check cannot say anything about the price.\n\
         The overflow remainder is still checked. Raise the allocation to \
         ${ORACLE_GUARD_MIN_TEST_ALLOCATION_USD:.2} or\n\
         clear ICPSWAP_TEST_ALLOCATION_USD to price every leg again.\n\
         {rule}"
    )
}

impl IcpswapFirstPlanner {
    /// Builds the forced ICPSwap test leg, then either uses the normal CEX
    /// waterfall or the explicit MEXC-and-Kraken test allocation.
    pub(super) async fn plan_test_fixed_icpswap_split(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
        target_usd: f64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        if self.overflow_venue_ids.is_empty() {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "test ICPSwap split requires an enabled overflow venue".to_string(),
            ));
        }
        let reference_price = input.pay_reference_price_usd.ok_or_else(|| {
            IcpswapFirstPlannerError::NoViableRoute(
                "test ICPSwap USD split requires a positive collateral reference price".to_string(),
            )
        })?;
        let icpswap_value =
            ChainTokenAmount::from_formatted(input.total_pay.token.clone(), target_usd / reference_price).value;
        if icpswap_value == Nat::from(0u8) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "test ICPSwap allocation ${target_usd:.2} rounds to zero native units"
            )));
        }
        if icpswap_value >= input.total_pay.value {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "received collateral is too small to split ${target_usd:.2} to ICPSwap and leave a CEX remainder"
            )));
        }

        let remainder_value = input.total_pay.value.clone() - icpswap_value.clone();
        let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());
        if !input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd) {
            return self.plan_below_minimum_remainder(input, quoted_at).await;
        }

        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, icpswap_value);
        let unguarded_input = self.oracle_guard_waived_for_test_leg(target_usd).then(|| {
            warn!(
                "[multi-venue] liq_id={} oracle quote guard waived for the ${target_usd:.2} ICPSwap test leg",
                input.liquidation_id
            );
            let mut unguarded = input.clone();
            unguarded.pay_reference_price_ray = None;
            unguarded.receive_reference_price_ray = None;
            unguarded
        });
        let icpswap_input = unguarded_input.as_ref().unwrap_or(input);
        let icpswap = self
            .preview_exact(icpswap_input, ICPSWAP_VENUE_ID, &icpswap_request)
            .await?;
        if !self.is_safe_icpswap(&icpswap) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "test ICPSwap allocation impact {:.2} bps is not below {:.2} bps",
                icpswap.quote.estimated_price_impact_bps, self.config.max_price_impact_bps
            )));
        }

        let overflow = match self.config.mexc_test_allocation_usd {
            Some(mexc_target_usd) => {
                self.preview_forced_cex_split(input, remainder_value, reference_price, mexc_target_usd)
                    .await?
            }
            None => self.ordered_overflow_previews(input, remainder_value).await?,
        };
        let mut previews = vec![icpswap];
        previews.extend(overflow);

        self.build_state(input, previews, MultiVenueAllocationReason::PriceImpactSplit, quoted_at)
    }

    /// Quotes the fixed MEXC test leg and sends its exact remainder to Kraken.
    ///
    /// `cex_value` is whatever is left for the CEXes: the post-ICPSwap remainder
    /// on the three-venue path, or the whole allocation when ICPSwap cannot take
    /// the pair at all.
    async fn preview_forced_cex_split(
        &self,
        input: &IcpswapFirstPlanInput,
        cex_value: Nat,
        reference_price: f64,
        mexc_target_usd: f64,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        let mexc_value =
            ChainTokenAmount::from_formatted(input.total_pay.token.clone(), mexc_target_usd / reference_price).value;
        if mexc_value == Nat::from(0u8) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "test MEXC allocation ${mexc_target_usd:.2} rounds to zero native units"
            )));
        }
        if mexc_value >= cex_value {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "received collateral is too small to allocate ${mexc_target_usd:.2} to MEXC and leave a Kraken remainder"
            )));
        }

        let kraken_value = cex_value.clone() - mexc_value.clone();

        // An exact split cannot be honoured when one of its venues will not
        // quote, but refusing to plan strands collateral that is already seized.
        let abandoned = match self.preview_safe_cex(input, MEXC_VENUE_ID, mexc_value).await {
            Ok(mexc) => match self.preview_safe_cex(input, KRAKEN_VENUE_ID, kraken_value).await {
                Ok(kraken) => return Ok(vec![mexc, kraken]),
                Err(error) => format!("{KRAKEN_VENUE_ID} could not quote its remainder ({error})"),
            },
            Err(error) => format!("{MEXC_VENUE_ID} could not quote its forced ${mexc_target_usd:.2} leg ({error})"),
        };

        // Loudly, because the run no longer tests the split it was configured for.
        warn!(
            "[multi-venue] liq_id={} forced CEX split abandoned: {abandoned}; falling back to the ordered CEX waterfall",
            input.liquidation_id
        );
        self.ordered_overflow_previews(input, cex_value).await
    }

    /// Forces the MEXC/Kraken split on a pair ICPSwap cannot take at all.
    ///
    /// Reaching the CEX waterfall by price impact needs a book thin enough to
    /// exceed the impact ceiling before it stops filling, which the liquid pairs
    /// never do -- ICP/ckUSDT tops out around 165 bps against a 200 bps ceiling
    /// and then errors outright. Without this the two-CEX split has no reachable
    /// test path on those routes.
    ///
    /// Returns `None` whenever the split cannot be shaped, so a test setting can
    /// never fail a liquidation that the normal waterfall would have routed.
    pub(super) async fn plan_forced_cex_split_previews(
        &self,
        input: &IcpswapFirstPlanInput,
    ) -> Result<Option<Vec<VenueRoutePreview>>, IcpswapFirstPlannerError> {
        let Some(mexc_target_usd) = self.config.mexc_test_allocation_usd else {
            return Ok(None);
        };
        if !self.venues.contains(MEXC_VENUE_ID) || !self.venues.contains(KRAKEN_VENUE_ID) {
            return Ok(None);
        }
        let Some(reference_price) = input.pay_reference_price_usd else {
            warn!(
                "[multi-venue] liq_id={} forced CEX split skipped: no collateral reference price",
                input.liquidation_id
            );
            return Ok(None);
        };
        let mexc_value =
            ChainTokenAmount::from_formatted(input.total_pay.token.clone(), mexc_target_usd / reference_price).value;
        if mexc_value == Nat::from(0u8) || mexc_value >= input.total_pay.value {
            warn!(
                "[multi-venue] liq_id={} forced CEX split skipped: ${mexc_target_usd:.2} to MEXC leaves no Kraken remainder",
                input.liquidation_id
            );
            return Ok(None);
        }

        warn!(
            "[multi-venue] liq_id={} TEST-ONLY forced CEX split: ${mexc_target_usd:.2} to MEXC, remainder to Kraken",
            input.liquidation_id
        );
        self.preview_forced_cex_split(input, input.total_pay.value.clone(), reference_price, mexc_target_usd)
            .await
            .map(Some)
    }

    /// Whether fixed ledger fees make the forced ICPSwap leg too small for the
    /// oracle guard to produce a meaningful price comparison.
    fn oracle_guard_waived_for_test_leg(&self, target_usd: f64) -> bool {
        target_usd < ORACLE_GUARD_MIN_TEST_ALLOCATION_USD
    }
}
