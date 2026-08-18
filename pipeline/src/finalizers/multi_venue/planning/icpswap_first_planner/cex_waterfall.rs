use std::cmp::max;

use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use tracing::warn;

use crate::finalizers::multi_venue::VenueRoutePreview;

use super::{
    BPS_DENOMINATOR, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError, KRAKEN_VENUE_ID,
    MEXC_VENUE_ID,
};

/// Whether a CEX quote has to satisfy the price-impact ceiling.
///
/// The ceiling is a sizing tool: it decides how much of an allocation a venue
/// may take before the rest has to go elsewhere. Once one venue is absorbing
/// the whole amount there is nothing left to size, so the ceiling has no
/// decision to make and enforcing it would reject a plan whose only alternative
/// is no plan at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ImpactCeiling {
    Enforced,
    /// Waived for the venue absorbing a fold. The estimate it is measured
    /// against is a single sweep of the book, which execution never performs:
    /// the leg is walked in impact-bounded slices instead. The oracle guard in
    /// `preview_exact` and the combined edge floor in `build_state` both still
    /// apply, and both measure the output rather than a proxy for it.
    WaivedForFold,
}

/// What MEXC can contribute to one CEX allocation.
///
/// The variants exist so the collapse decision is a match on named outcomes
/// rather than a pair of booleans: whether the whole allocation ends up on one
/// venue because a minimum forced it, or because MEXC could not be used at all,
/// changes whether the impact ceiling applies to whoever absorbs it.
enum MexcShare {
    /// Quoted the whole allocation within the impact ceiling. No Kraken leg.
    WholeAllocation(Box<VenueRoutePreview>),
    /// The largest slice MEXC can safely take, with why it could not take more.
    /// Kraken is offered the exact remainder.
    Partial {
        preview: Box<VenueRoutePreview>,
        /// MEXC's own quote for the whole allocation. Above the impact ceiling
        /// -- that is why a split was attempted at all -- but kept because MEXC
        /// absorbs the fold when the remainder turns out to be dust.
        ///
        /// Safe to accept there by construction: a dust remainder means MEXC's
        /// safe capacity was nearly the whole amount, so its impact over the
        /// whole is barely above the impact it already cleared. That is not
        /// true when MEXC's capacity is itself dust, which is why that case
        /// goes to Kraken instead.
        whole: Box<VenueRoutePreview>,
        rejection: String,
    },
    /// Every amount MEXC could quote safely is below a minimum, so it has no
    /// viable size at any allocation. A venue absorbing the whole amount here is
    /// absorbing a fold, and is quoted without the impact ceiling.
    BelowMinimum(String),
    /// MEXC is disabled, could not be reached, or quoted nothing safely at any
    /// size. A retry may still find a route, so this is not a fold and the
    /// ceiling still applies to whoever absorbs the allocation.
    Unusable(String),
}

impl MexcShare {
    /// Why MEXC could not cover the whole allocation, for the combined error.
    fn into_rejection(self) -> String {
        match self {
            Self::WholeAllocation(_) => "MEXC accepted the whole allocation".to_string(),
            Self::Partial { rejection, .. } | Self::BelowMinimum(rejection) | Self::Unusable(rejection) => rejection,
        }
    }
}

/// The largest allocation one venue can quote within the impact ceiling.
enum CexCapacity {
    /// A safe amount that also clears every minimum.
    Usable(Box<VenueRoutePreview>),
    /// A safe amount exists but is below a minimum, so no slice of this
    /// allocation is worth giving to this venue.
    BelowMinimum,
    /// Nothing quoted safely at any size.
    None,
}

impl IcpswapFirstPlanner {
    /// Allocates one CEX amount in policy order.
    ///
    /// MEXC has first refusal on the whole amount. If it can only take part,
    /// Kraken receives the exact remainder.
    ///
    /// A split is abandoned whenever it would leave either side below a minimum
    /// -- by notional or by the native floor a venue's own bridge enforces --
    /// because a leg too small to execute strands its collateral. Which venue
    /// then absorbs the whole allocation follows from which side was too small:
    /// a dust remainder folds back into MEXC, which had already quoted nearly
    /// all of it, while MEXC having no viable size of its own leaves Kraken to
    /// take everything. Either way the absorbing venue is quoted without the
    /// impact ceiling, which has nothing left to size.
    ///
    /// All previews complete before any funds move.
    pub(super) async fn ordered_overflow_previews(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_value: Nat,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        let pay_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), pay_value.clone());

        // Two floors, checked separately because they are not the same kind of
        // rule. The notional minimum applies to the allocation itself, so one
        // failure ends it. The native floors belong to individual venues, so the
        // amount is only unexecutable when every venue rejects it.
        if !input.meets_cex_minimum(&pay_amount, self.config.cex_min_exec_usd) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "CEX amount is below its minimum (${:.2} execution minimum)",
                self.config.cex_min_exec_usd
            )));
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

        let kraken_enabled = self.venues.contains(KRAKEN_VENUE_ID);
        if !self.venues.contains(MEXC_VENUE_ID) && !kraken_enabled {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "neither MEXC nor Kraken is enabled for overflow".to_string(),
            ));
        }

        let share = self
            .mexc_share(input, &pay_amount, pay_value.clone(), kraken_enabled)
            .await?;

        // Covering the whole allocation is the one outcome that needs no Kraken
        // leg, so it is settled before Kraken is required to exist.
        if !kraken_enabled && !matches!(share, MexcShare::WholeAllocation(_)) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(share.into_rejection()));
        }

        let (mexc, kraken_value, ceiling, rejection) = match share {
            MexcShare::WholeAllocation(mexc) => return Ok(vec![*mexc]),
            MexcShare::Partial {
                preview,
                whole,
                rejection,
            } => {
                let remainder_value = pay_value.clone() - preview.request.pay_amount.value.clone();
                let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());

                if input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd)
                    && input.meets_venue_minimum(KRAKEN_VENUE_ID, &remainder)
                {
                    (Some(*preview), remainder_value, ImpactCeiling::Enforced, rejection)
                } else {
                    // Never persist a dust Kraken leg, whether it is dust by
                    // notional or below the floor Kraken's own bridge enforces.
                    //
                    // The dust folds back into MEXC rather than handing the whole
                    // amount to Kraken: MEXC has first refusal, already quoted
                    // this exact amount, and could safely take nearly all of it.
                    // Passing it instead to the venue that was never asked for
                    // any of it would invert the venue order on a rounding
                    // detail. The ceiling is waived for the same reason it is
                    // waived elsewhere -- see `ImpactCeiling` -- and the overage
                    // is small by construction, because the remainder being dust
                    // is what makes MEXC's safe capacity nearly the whole amount.
                    warn!(
                        "[multi-venue] liq_id={} MEXC absorbs the whole allocation at {:.2} bps, above the {:.2} bps ceiling: its {} remainder is too small for Kraken to execute",
                        input.liquidation_id,
                        whole.quote.estimated_price_impact_bps,
                        self.config.max_cex_price_impact_bps,
                        remainder.formatted()
                    );
                    return Ok(vec![*whole]);
                }
            }
            // A minimum forced the collapse, so Kraken is handed an amount MEXC
            // already refused at the ceiling. Waived: see `ImpactCeiling`.
            MexcShare::BelowMinimum(rejection) => (None, pay_value, ImpactCeiling::WaivedForFold, rejection),
            // Availability, not size. A retry may still produce a split, so the
            // ceiling keeps its say over Kraken's quote.
            MexcShare::Unusable(rejection) => (None, pay_value, ImpactCeiling::Enforced, rejection),
        };

        // Kraken is called once, for either the remainder or the full amount.
        let kraken = self
            .preview_cex(input, KRAKEN_VENUE_ID, kraken_value, ceiling)
            .await
            .map_err(|kraken_error| {
                IcpswapFirstPlannerError::NoViableRoute(format!(
                    "MEXC could not accept the full allocation ({rejection}); Kraken could not accept its allocation ({kraken_error})"
                ))
            })?;

        Ok(match mexc {
            Some(mexc) => vec![mexc, kraken],
            None => vec![kraken],
        })
    }

    /// Asks MEXC what it can take of this allocation.
    ///
    /// `may_split` is false when no other venue could accept a remainder. MEXC
    /// is then only asked whether it can cover everything: searching for a
    /// partial it has nowhere to pair with would spend up to
    /// `max_search_iterations` quotes producing an answer no caller reads.
    async fn mexc_share(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_amount: &ChainTokenAmount,
        pay_value: Nat,
        may_split: bool,
    ) -> Result<MexcShare, IcpswapFirstPlannerError> {
        if !self.venues.contains(MEXC_VENUE_ID) {
            return Ok(MexcShare::Unusable("MEXC is disabled".to_string()));
        }

        // A floor above the whole allocation rules out every slice of it, so
        // MEXC is skipped before it is quoted rather than after a wasted call.
        if !input.meets_venue_minimum(MEXC_VENUE_ID, pay_amount) {
            return Ok(MexcShare::BelowMinimum(format!(
                "MEXC cannot execute {} ({})",
                pay_amount.formatted(),
                input.venue_minimum_summary(&[MEXC_VENUE_ID.to_string()])
            )));
        }

        let request = input.request_for(MEXC_VENUE_ID, pay_value.clone());
        let full = match self.preview_exact(input, MEXC_VENUE_ID, &request).await {
            Ok(full) => full,
            Err(error) => return Ok(MexcShare::Unusable(error.to_string())),
        };
        
        if self.is_safe_cex(&full) {
            return Ok(MexcShare::WholeAllocation(Box::new(full)));
        }

        let rejection = format!(
            "MEXC full quote impact {:.2} bps exceeds {:.2} bps",
            full.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
        );
        if !may_split {
            return Ok(MexcShare::Unusable(rejection));
        }

        Ok(
            match self.search_safe_cex_allocation(input, MEXC_VENUE_ID, pay_value).await? {
                CexCapacity::Usable(preview) => MexcShare::Partial {
                    preview,
                    whole: Box::new(full),
                    rejection,
                },
                // MEXC's safe capacity is itself dust, so it is far below the
                // whole allocation rather than just short of it. Absorbing
                // everything here would mean accepting an impact well past the
                // ceiling, not marginally over, so Kraken takes it instead.
                CexCapacity::BelowMinimum => MexcShare::BelowMinimum(format!(
                    "{rejection}, and its largest safe allocation is below a minimum"
                )),
                CexCapacity::None => MexcShare::Unusable(rejection),
            },
        )
    }

    /// Returns an exact CEX preview only when it satisfies the common impact
    /// ceiling and the planner's minimum-notional rule.
    pub(super) async fn preview_safe_cex(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        self.preview_cex(input, venue_id, pay_value, ImpactCeiling::Enforced)
            .await
    }

    /// Quotes one venue for an exact allocation, applying both minimums and,
    /// unless it is absorbing a fold, the impact ceiling.
    pub(super) async fn preview_cex(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
        ceiling: ImpactCeiling,
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
            if ceiling == ImpactCeiling::Enforced {
                return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                    "{venue_id} quote impact {:.2} bps exceeds {:.2} bps",
                    preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
                )));
            }
            // Loudly, because the leg is deliberately planned above the ceiling
            // and an operator comparing plan to fill should know why.
            warn!(
                "[multi-venue] liq_id={} {venue_id} absorbs the whole allocation at {:.2} bps, above the {:.2} bps ceiling; splitting it would leave a piece below a minimum, and execution slices the leg",
                input.liquidation_id, preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
            );
        }
        Ok(preview)
    }

    /// Binary-searches the largest safely quotable allocation for one CEX.
    ///
    /// Distinguishes "found a safe amount, but it is dust" from "found nothing
    /// safe", because only the former makes a later collapse a fold.
    async fn search_safe_cex_allocation(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<CexCapacity, IcpswapFirstPlannerError> {
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
        Ok(match last_safe {
            None => CexCapacity::None,
            Some(preview)
                if input.meets_cex_minimum(&preview.request.pay_amount, self.config.cex_min_exec_usd)
                    && input.meets_venue_minimum(venue_id, &preview.request.pay_amount) =>
            {
                CexCapacity::Usable(Box::new(preview))
            }
            Some(_) => CexCapacity::BelowMinimum,
        })
    }

    fn is_safe_cex(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps <= self.config.max_cex_price_impact_bps
    }
}
