use std::cmp::max;

use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::finalizers::multi_venue::VenueRoutePreview;

use super::{
    BPS_DENOMINATOR, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError, KRAKEN_VENUE_ID,
    MEXC_VENUE_ID,
};

impl IcpswapFirstPlanner {
    /// Allocates one CEX amount in policy order: MEXC receives as much as it
    /// can quote within the CEX impact ceiling, then Kraken receives the exact
    /// unallocated remainder. All previews complete before any funds move.
    pub(super) async fn ordered_overflow_previews(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_value: Nat,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        let pay_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), pay_value.clone());
        if !input.meets_cex_minimum(&pay_amount, self.config.cex_min_exec_usd) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "CEX amount is below its minimum (${:.2} execution minimum)",
                self.config.cex_min_exec_usd
            )));
        }

        let mexc_enabled = self.venues.contains(MEXC_VENUE_ID);
        let kraken_enabled = self.venues.contains(KRAKEN_VENUE_ID);
        if !mexc_enabled && !kraken_enabled {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "neither MEXC nor Kraken is enabled for overflow".to_string(),
            ));
        }

        // A floor above the whole allocation cannot be met by any slice of it,
        // because every slice is smaller than the total that already failed.
        // Checked before any venue is quoted, so this can never be reported
        // while an outage is the real cause and a retry would still find a route.
        if input.below_every_venue_minimum(&self.overflow_venue_ids, &pay_amount) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "CEX amount {} is below every venue's minimum executable amount ({})",
                pay_amount.formatted(),
                input.venue_minimum_summary(&self.overflow_venue_ids)
            )));
        }

        // MEXC is quoted only when the whole allocation clears its own floor: a
        // partial can only ever be smaller, so a total below the floor rules out
        // every MEXC leg and the quote would be wasted.
        let mexc_quotable = mexc_enabled && input.meets_venue_minimum(MEXC_VENUE_ID, &pay_amount);

        let (partial_mexc, mexc_failure) = if mexc_quotable {
            let request = input.request_for(MEXC_VENUE_ID, pay_value.clone());
            match self.preview_exact(input, MEXC_VENUE_ID, &request).await {
                Ok(preview) if self.is_safe_cex(&preview) => return Ok(vec![preview]),
                Ok(preview) => {
                    let failure = format!(
                        "MEXC full quote impact {:.2} bps exceeds {:.2} bps",
                        preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
                    );
                    let partial = if kraken_enabled {
                        self.search_safe_cex_allocation(input, MEXC_VENUE_ID, pay_value.clone())
                            .await?
                    } else {
                        None
                    };
                    (partial, failure)
                }
                Err(error) => (None, error.to_string()),
            }
        } else if mexc_enabled {
            (
                None,
                format!(
                    "MEXC cannot execute {} ({})",
                    pay_amount.formatted(),
                    input.venue_minimum_summary(&[MEXC_VENUE_ID.to_string()])
                ),
            )
        } else {
            (None, "MEXC is disabled".to_string())
        };

        if !kraken_enabled {
            return Err(IcpswapFirstPlannerError::NoViableRoute(mexc_failure));
        }

        let (mexc, kraken_value) = match partial_mexc {
            Some(mexc) => {
                let remainder_value = pay_value.clone() - mexc.request.pay_amount.value.clone();
                let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());

                if input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd)
                    && input.meets_venue_minimum(KRAKEN_VENUE_ID, &remainder)
                {
                    (Some(mexc), remainder_value)
                } else {
                    // Never persist a dust Kraken leg, whether it is dust by
                    // notional or below the floor Kraken's own bridge enforces.
                    // Ask Kraken to take the complete amount instead of
                    // retaining the partial MEXC leg.
                    (None, pay_value)
                }
            }
            None => (None, pay_value),
        };

        // Kraken is called once, for either the remainder or the full amount.
        let kraken = self
            .preview_safe_cex(input, KRAKEN_VENUE_ID, kraken_value)
            .await
            .map_err(|kraken_error| {
                IcpswapFirstPlannerError::NoViableRoute(format!(
                    "MEXC could not accept the full allocation ({mexc_failure}); Kraken could not accept its allocation ({kraken_error})"
                ))
            })?;

        Ok(match mexc {
            Some(mexc) => vec![mexc, kraken],
            None => vec![kraken],
        })
    }

    /// Returns an exact CEX preview only when it satisfies the common impact
    /// ceiling and the planner's minimum-notional rule.
    pub(super) async fn preview_safe_cex(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let request = input.request_for(venue_id, pay_value);
        if !input.meets_cex_minimum(&request.pay_amount, self.config.cex_min_exec_usd) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "{venue_id} amount is below its minimum (${:.2} execution minimum)",
                self.config.cex_min_exec_usd
            )));
        }
        // The floor the venue reported for itself, which a notional minimum
        // cannot express. Checked here so every caller is covered, including
        // the forced test split that never reaches the waterfall.
        if !input.meets_venue_minimum(venue_id, &request.pay_amount) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "{venue_id} amount {} is below its minimum executable amount ({})",
                request.pay_amount.formatted(),
                input.venue_minimum_summary(&[venue_id.to_string()])
            )));
        }
        let preview = self.preview_exact(input, venue_id, &request).await?;
        if !self.is_safe_cex(&preview) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "{venue_id} quote impact {:.2} bps exceeds {:.2} bps",
                preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
            )));
        }
        Ok(preview)
    }

    /// Binary-searches the largest safely quotable allocation for one CEX.
    async fn search_safe_cex_allocation(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<Option<VenueRoutePreview>, IcpswapFirstPlannerError> {
        let mut lower = Nat::from(0u8);
        let mut upper = pay_value.clone();
        let proportional_tolerance = pay_value / Nat::from(BPS_DENOMINATOR);
        let tolerance = max(Nat::from(1u8), max(input.total_pay.token.fee(), proportional_tolerance));
        let mut last_safe = None;

        for _ in 0..self.config.max_search_iterations {
            let range = upper.clone() - lower.clone();
            if range <= tolerance {
                break;
            }
            let midpoint = (lower.clone() + upper.clone()) / Nat::from(2u8);
            if midpoint == lower || midpoint == upper {
                break;
            }
            let request = input.request_for(venue_id, midpoint.clone());
            match self.preview_exact(input, venue_id, &request).await {
                Ok(preview) if self.is_safe_cex(&preview) => {
                    lower = midpoint;
                    last_safe = Some(preview);
                }
                Ok(_) | Err(_) => upper = midpoint,
            }
        }

        // The largest safe amount is still only usable when it clears both
        // floors. Below either one this venue has no viable size at all, since
        // anything bigger is exactly what the impact ceiling already rejected.
        Ok(last_safe.filter(|preview| {
            input.meets_cex_minimum(&preview.request.pay_amount, self.config.cex_min_exec_usd)
                && input.meets_venue_minimum(venue_id, &preview.request.pay_amount)
        }))
    }

    fn is_safe_cex(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps <= self.config.max_cex_price_impact_bps
    }
}
