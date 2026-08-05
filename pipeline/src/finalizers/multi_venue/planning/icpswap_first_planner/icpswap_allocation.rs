use std::cmp::max;

use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::{
    finalizers::multi_venue::VenueRoutePreview,
    persistance::{MultiVenueAllocationReason, MultiVenueExecutionState},
};

use super::{BPS_DENOMINATOR, ICPSWAP_VENUE_ID, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError};

impl IcpswapFirstPlanner {
    // Avoids a dust overflow leg by re-quoting the full amount on ICPSwap with
    // the narrowly relaxed fallback cap. If that quote is still too expensive,
    // the full amount is sent to the best executable overflow venue.
    pub(super) async fn plan_below_minimum_remainder(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, input.total_pay.value.clone());
        let icpswap = match self.preview_exact(input, ICPSWAP_VENUE_ID, &icpswap_request).await {
            Ok(icpswap) => icpswap,
            Err(error) => {
                return self
                    .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                    .await;
            }
        };
        if self.is_safe_dust_fallback_icpswap(&icpswap) {
            return self.build_state(
                input,
                vec![icpswap],
                MultiVenueAllocationReason::RemainderBelowMinimum {
                    skipped_venue_ids: self.overflow_venue_ids.clone(),
                    selected_venue_id: ICPSWAP_VENUE_ID.to_string(),
                },
                quoted_at,
            );
        }

        let overflow = self
            .full_amount_overflow(
                input,
                Some(
                    "full ICPSwap quote exceeds the dust-fallback price-impact limit and the below-minimum remainder cannot be split"
                        .to_string(),
                ),
            )
            .await?;
        let mut skipped_venue_ids = vec![ICPSWAP_VENUE_ID.to_string()];
        let selected_venue_ids = overflow
            .iter()
            .map(|preview| preview.venue_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        skipped_venue_ids.extend(
            self.overflow_venue_ids
                .iter()
                .filter(|venue_id| !selected_venue_ids.contains(venue_id.as_str()))
                .cloned(),
        );
        let allocation_reason = if overflow.len() == 1 {
            MultiVenueAllocationReason::RemainderBelowMinimum {
                skipped_venue_ids,
                selected_venue_id: overflow[0].venue_id.clone(),
            }
        } else {
            MultiVenueAllocationReason::PriceImpactSplit
        };
        self.build_state(input, overflow, allocation_reason, quoted_at)
    }

    // Applies the configured CEX waterfall to the complete allocation.
    //
    // `fallthrough_reason` prefixes the rejection summary when the caller
    // reached this path because another venue fell through. Its presence is
    // load-bearing, not cosmetic: every caller that supplies one arrived here
    // through a venue state a later retry could resolve, so those failures are
    // never reported as a permanent below-minimum dead end.
    async fn full_amount_overflow(
        &self,
        input: &IcpswapFirstPlanInput,
        fallthrough_reason: Option<String>,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        self.ordered_overflow_previews(input, input.total_pay.value.clone())
            .await
            .map_err(|error| match fallthrough_reason {
                Some(reason) => IcpswapFirstPlannerError::NoViableRoute(format!("{reason}; {error}")),
                None => error,
            })
    }

    // Non-native ICP collateral cannot use ICPSwap, so choose the executable
    // overflow venue with the highest conservative output.
    pub(super) async fn plan_overflow_only(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        if self.overflow_venue_ids.is_empty() {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "no enabled venue can accept this collateral asset".to_string(),
            ));
        }
        let previews = self.full_amount_overflow(input, None).await?;
        let allocation_reason = if previews.len() == 1 {
            MultiVenueAllocationReason::SingleVenue {
                venue_id: previews[0].venue_id.clone(),
            }
        } else {
            MultiVenueAllocationReason::PriceImpactSplit
        };
        self.build_state(input, previews, allocation_reason, quoted_at)
    }

    // Re-plans the complete amount on overflow venues when ICPSwap's final
    // exact quote cannot be used. The earlier remainder quotes are discarded
    // because they do not cover the amount that must now be allocated.
    async fn plan_full_overflow_after_icpswap_failure(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
        icpswap_error: &IcpswapFirstPlannerError,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let overflow = self
            .full_amount_overflow(input, Some(format!("ICPSwap exact quote rejected ({icpswap_error})")))
            .await?;
        let allocation_reason = if overflow.len() == 1 {
            MultiVenueAllocationReason::VenueUnavailable {
                selected_venue_id: overflow[0].venue_id.clone(),
                unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
            }
        } else {
            MultiVenueAllocationReason::PriceImpactSplit
        };
        self.build_state(input, overflow, allocation_reason, quoted_at)
    }

    // Finds the largest safe ICPSwap leg, then either sends the remainder to
    // the best overflow venue or selects one safe full-amount venue when the
    // remainder cannot meet the overflow minimum.
    pub(super) async fn plan_split_or_fallback(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let safe_icpswap = self.search_safe_icpswap(input).await?;
        let safe_value = safe_icpswap
            .as_ref()
            .map(|preview| preview.request.pay_amount.value.clone())
            .unwrap_or_else(|| Nat::from(0u8));
        let remainder_value = input.total_pay.value.clone() - safe_value.clone();
        let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());

        if !input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd) {
            return self.plan_below_minimum_remainder(input, quoted_at).await;
        }

        if safe_value == Nat::from(0u8) {
            let overflow = self.full_amount_overflow(input, None).await?;
            let allocation_reason = if overflow.len() == 1 {
                MultiVenueAllocationReason::SingleVenue {
                    venue_id: overflow[0].venue_id.clone(),
                }
            } else {
                MultiVenueAllocationReason::PriceImpactSplit
            };
            return self.build_state(input, overflow, allocation_reason, quoted_at);
        }

        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, safe_value);
        let icpswap = match self.preview_exact(input, ICPSWAP_VENUE_ID, &icpswap_request).await {
            Ok(icpswap) => icpswap,
            Err(error) => {
                return self
                    .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                    .await;
            }
        };
        if !self.is_safe_icpswap(&icpswap) {
            // An exact quote that comes back unsafe is no more usable than one
            // that failed outright, so it takes the same route: re-quote the full
            // amount on overflow venues instead of abandoning a liquidation that
            // a single venue could still execute.
            let error = IcpswapFirstPlannerError::NoViableRoute(format!(
                "exact ICPSwap allocation impact {:.2} bps is not below {:.2} bps",
                icpswap.quote.estimated_price_impact_bps, self.config.max_price_impact_bps
            ));
            return self
                .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                .await;
        }
        let overflow = self.ordered_overflow_previews(input, remainder_value).await?;
        let mut previews = vec![icpswap];
        previews.extend(overflow);

        self.build_state(input, previews, MultiVenueAllocationReason::PriceImpactSplit, quoted_at)
    }

    // Binary-searches for the largest amount whose confirmed ICPSwap impact
    // remains strictly below the configured limit. Quote failures are unsafe.
    async fn search_safe_icpswap(
        &self,
        input: &IcpswapFirstPlanInput,
    ) -> Result<Option<VenueRoutePreview>, IcpswapFirstPlannerError> {
        // `lower` is always zero or an amount backed by a confirmed safe quote.
        // `upper` is the smallest amount currently known to be unsafe or
        // unquotable. We never return `upper` as an allocation.
        let mut lower = Nat::from(0u8);
        let mut upper = input.total_pay.value.clone();

        // Searching more precisely than one ledger fee is not executable, and
        // total_pay / 10_000 caps relative allocation error at one basis point.
        let proportional_tolerance = input.total_pay.value.clone() / Nat::from(BPS_DENOMINATOR);
        let tolerance = max(Nat::from(1u8), max(input.total_pay.token.fee(), proportional_tolerance));

        // Keep the complete quote, not only its amount, so the returned lower
        // bound is always the last amount that was positively confirmed safe.
        let mut last_safe = None;

        for _ in 0..self.config.max_search_iterations {
            let range = upper.clone() - lower.clone();
            if range <= tolerance {
                break;
            }

            // Halve the remaining interval. The equality guard prevents a
            // non-progressing loop when integer division rounds to a bound.
            let midpoint = (lower.clone() + upper.clone()) / Nat::from(2u8);
            if midpoint == lower || midpoint == upper {
                break;
            }
            let request = input.request_for(ICPSWAP_VENUE_ID, midpoint.clone());
            match self.preview_exact(input, ICPSWAP_VENUE_ID, &request).await {
                Ok(preview) if self.is_safe_icpswap(&preview) => {
                    // Safe midpoint: retain its quote and search higher.
                    lower = midpoint;
                    last_safe = Some(preview);
                }
                // Unsafe impact, an unavailable quote, or a malformed quote all
                // mean this midpoint is not confirmed safe; continue below it.
                Ok(_) | Err(_) => upper = midpoint,
            }
        }

        Ok(last_safe)
    }

    // The threshold is strict: exactly 100 bps is not below a 100 bps limit.
    pub(super) fn is_safe_icpswap(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps < self.config.max_price_impact_bps
    }

    // This relaxed cap applies only when the overflow remainder cannot meet
    // its venue minimum. "Up to" is inclusive at exactly the configured cap.
    fn is_safe_dust_fallback_icpswap(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps <= self.config.dust_fallback_max_price_impact_bps
    }
}
