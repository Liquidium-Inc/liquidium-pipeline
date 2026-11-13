//! The CEX side of a plan is an ordered waterfall: each exchange takes what it
//! can quote within the CEX impact ceiling, and the next one takes the rest.
//! A forced test split replaces the waterfall only when its setting is present.
use std::cmp::max;

use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use tracing::{info, warn};

use super::{
    BPS_DENOMINATOR, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError, KRAKEN_VENUE_ID,
    MEXC_VENUE_ID, RIFT_VENUE_ID,
};
use crate::finalizers::multi_venue::{VenueRoutePreview, planning::venue_registry::VenuePreviewOutcome};

/// The order the overflow venues are asked in: MEXC, then Rift when MEXC's
/// book is thin, then Kraken. A venue outside it follows in registry order.
const CEX_POLICY_ORDER: [&str; 3] = [MEXC_VENUE_ID, RIFT_VENUE_ID, KRAKEN_VENUE_ID];

/// Sorts the overflow venues into policy order. The sort is stable, so venues
/// the policy does not name keep their registry order.
pub(super) fn cex_policy_order(mut venue_ids: Vec<String>) -> Vec<String> {
    venue_ids.sort_by_key(|venue_id| {
        CEX_POLICY_ORDER
            .iter()
            .position(|policy_id| policy_id == venue_id)
            .unwrap_or(CEX_POLICY_ORDER.len())
    });
    venue_ids
}

/// Why one exchange did not take its share. A permanent refusal is one the
/// amount alone decides, so a retry cannot change it.
struct Refusal {
    reason: String,
    permanent: bool,
}

impl IcpswapFirstPlanner {
    /// Allocates one CEX amount in policy order. With one exchange enabled the
    /// ceiling does not apply, so the plan accepts what that exchange quotes.
    pub(super) async fn ordered_overflow_previews(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_value: Nat,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        if self.overflow_venue_ids.is_empty() {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "no enabled venue can accept this collateral asset".to_string(),
            ));
        }
        // Dust never reaches an exchange: fixed fees would make a fair quote
        // read as an oracle failure, and no exchange executes it anyway.
        let pay_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), pay_value.clone());
        if !input.meets_cex_minimum(&pay_amount, self.config.cex_min_exec_usd) {
            let summary = self
                .overflow_venue_ids
                .iter()
                .map(|venue_id| format!("{} amount is below its minimum", venue_id.to_uppercase()))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(summary));
        }
        // A venue that can never carry the pair is no second exchange: it must
        // not switch the ceiling on, and it has no quote to give.
        let (venue_ids, absent): (Vec<&String>, Vec<&String>) = self.overflow_venue_ids.iter().partition(|venue_id| {
            self.venues
                .adapter(venue_id)
                .is_none_or(|adapter| adapter.carries(&input.request_for(venue_id, pay_value.clone())))
        });
        let ceiling_applies = venue_ids.len() > 1;
        let mut legs = Vec::new();
        let mut remainder = pay_value;
        let mut refusals = Vec::new();
        // The pay amount of a thin-book partial or a capped slice that is still the
        // last leg. The $ floor that kept it ignores each later venue's own minimum.
        let mut open_partial: Option<Nat> = None;
        for (index, venue_id) in venue_ids.iter().copied().enumerate() {
            let is_last = index + 1 == venue_ids.len();
            let slice = match self.venue_slice(input, venue_id, is_last, &remainder) {
                Ok(slice) => slice,
                Err(refusal) => {
                    refusals.push(refusal);
                    continue;
                }
            };
            let capped = slice != remainder;
            let refusal = match self.preview_cex(input, venue_id, slice.clone()).await? {
                VenuePreviewOutcome::Quoted(preview) if !ceiling_applies || self.is_safe_cex(&preview) => {
                    legs.push(preview);
                    if !capped {
                        return Ok(legs);
                    }
                    // A capped slice is open too: the rest can be under the next venue's minimum.
                    open_partial = Some(slice.clone());
                    remainder -= slice;
                    continue;
                }
                VenuePreviewOutcome::Quoted(preview) => {
                    let reason = format!(
                        "{venue_id} quote impact {:.2} bps exceeds {:.2} bps",
                        preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
                    );
                    if !is_last
                        && let Some(partial) = self
                            .search_safe_cex_allocation(input, venue_id, slice.clone())
                            .await?
                    {
                        let left = remainder.clone() - partial.request.pay_amount.value.clone();
                        let left_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), left.clone());
                        // A dust remainder is never a leg, so the next exchange takes it all.
                        if input.meets_cex_minimum(&left_amount, self.config.cex_min_exec_usd) {
                            remainder = left;
                            open_partial = Some(partial.request.pay_amount.value.clone());
                            legs.push(partial);
                        }
                    }
                    Refusal {
                        reason,
                        permanent: false,
                    }
                }
                VenuePreviewOutcome::BelowMinimum(reason) => {
                    // The partial gives way when this venue can take the whole
                    // amount; otherwise it stays for a later venue.
                    if let Some(partial_value) = open_partial.clone() {
                        let whole = remainder.clone() + partial_value.clone();
                        if let Ok(whole_slice) = self.venue_slice(input, venue_id, is_last, &whole)
                            && let VenuePreviewOutcome::Quoted(preview) =
                                self.preview_cex(input, venue_id, whole_slice.clone()).await?
                            && (!ceiling_applies || self.is_safe_cex(&preview))
                        {
                            let given_back = legs.pop();
                            debug_assert!(
                                given_back
                                    .as_ref()
                                    .is_some_and(|leg| leg.request.pay_amount.value == partial_value),
                                "the open partial is the last leg"
                            );
                            info!(
                                "[multi-venue] liq_id={} {venue_id} takes the whole amount and the {} partial is dropped: the remainder was below the {venue_id} minimum ({reason})",
                                input.liquidation_id,
                                given_back.map_or_else(|| "-".to_string(), |leg| leg.venue_id),
                            );
                            legs.push(preview);
                            open_partial = None;
                            if whole_slice == whole {
                                return Ok(legs);
                            }
                            remainder = whole - whole_slice;
                            continue;
                        }
                    }
                    Refusal {
                        reason: format!("{} amount below minimum ({reason})", venue_id.to_uppercase()),
                        permanent: true,
                    }
                }
                VenuePreviewOutcome::Unavailable(error) => Refusal {
                    reason: format!("{} unavailable ({error})", venue_id.to_uppercase()),
                    permanent: false,
                },
                VenuePreviewOutcome::Invalid(error) => Refusal {
                    reason: format!("{} quote rejected ({error})", venue_id.to_uppercase()),
                    permanent: false,
                },
            };
            refusals.push(refusal);
        }
        let summary = refusals
            .iter()
            .map(|refusal| refusal.reason.clone())
            .chain(
                absent
                    .iter()
                    .map(|venue_id| format!("{} does not carry this pair", venue_id.to_uppercase())),
            )
            .collect::<Vec<_>>()
            .join("; ");
        // Only an amount every exchange refused as too small is a dead end; an
        // outage or a thin book can clear by the next cycle (audit L17).
        if legs.is_empty() && !refusals.is_empty() && refusals.iter().all(|refusal| refusal.permanent) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(summary));
        }
        Err(IcpswapFirstPlannerError::NoViableRoute(summary))
    }

    /// The most one order at this venue may carry, in pay units at the oracle
    /// price. Only Rift has a cap. Without a price the cap cannot be sized, so
    /// Rift refuses: an order with no bound is worse than no Rift leg.
    fn venue_order_cap(&self, input: &IcpswapFirstPlanInput, venue_id: &str) -> Result<Option<Nat>, Refusal> {
        if venue_id != RIFT_VENUE_ID {
            return Ok(None);
        }
        let cap_usd = self.config.rift_max_order_usd;
        match input.pay_reference_price_usd {
            Some(price) if price.is_finite() && price > 0.0 && cap_usd.is_finite() && cap_usd > 0.0 => Ok(Some(
                ChainTokenAmount::from_formatted(input.total_pay.token.clone(), cap_usd / price).value,
            )),
            _ => Err(Refusal {
                reason: "RIFT order cap cannot be sized without a reference price".to_string(),
                permanent: false,
            }),
        }
    }

    /// The amount one venue is asked for: all of `remainder`, or its order cap.
    /// The last venue cannot pass an excess on, so it refuses instead.
    fn venue_slice(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        is_last: bool,
        remainder: &Nat,
    ) -> Result<Nat, Refusal> {
        match self.venue_order_cap(input, venue_id)? {
            Some(cap) if cap < *remainder => {
                if is_last {
                    return Err(Refusal {
                        reason: format!(
                            "{} amount exceeds its order cap of ${:.0}",
                            venue_id.to_uppercase(),
                            self.config.rift_max_order_usd
                        ),
                        permanent: false,
                    });
                }
                let left = remainder.clone() - cap.clone();
                let left_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), left);
                // A dust leftover is never a leg, so the cap gives way by that much.
                if input.meets_cex_minimum(&left_amount, self.config.cex_min_exec_usd) {
                    Ok(cap)
                } else {
                    Ok(remainder.clone())
                }
            }
            _ => Ok(remainder.clone()),
        }
    }

    /// One exchange's answer for an exact amount, after its amount check and
    /// the quote guard.
    async fn preview_cex(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<VenuePreviewOutcome, IcpswapFirstPlannerError> {
        let context = input.planning_context();
        let quotes = self
            .venues
            .preview_venues(&context, &[venue_id.to_string()], |venue_id| {
                input.request_for(venue_id, pay_value.clone())
            })
            .await
            .map_err(IcpswapFirstPlannerError::InvalidInput)?;
        let quotes = self.drop_invalid_quotes(input, quotes);
        self.price_quote_book(input, quotes)
            .await
            .into_previews()
            .into_iter()
            .find(|preview| preview.venue_id == venue_id)
            .map(|preview| preview.outcome)
            .ok_or_else(|| IcpswapFirstPlannerError::InvalidInput(format!("{venue_id} preview is missing")))
    }

    /// An exact exchange quote that meets the CEX minimum and the impact
    /// ceiling, or the reason it does not.
    pub(super) async fn preview_safe_cex(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        pay_value: Nat,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let pay_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), pay_value.clone());
        if !input.meets_cex_minimum(&pay_amount, self.config.cex_min_exec_usd) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(format!(
                "{venue_id} amount is below its minimum (${:.2} execution minimum)",
                self.config.cex_min_exec_usd
            )));
        }
        let preview = match self.preview_cex(input, venue_id, pay_value).await? {
            VenuePreviewOutcome::Quoted(preview) => preview,
            VenuePreviewOutcome::BelowMinimum(reason) => {
                return Err(IcpswapFirstPlannerError::BelowVenueMinimum(reason));
            }
            VenuePreviewOutcome::Unavailable(error) | VenuePreviewOutcome::Invalid(error) => {
                return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                    "{venue_id} preview failed: {error}"
                )));
            }
        };
        if !self.is_safe_cex(&preview) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "{venue_id} quote impact {:.2} bps exceeds {:.2} bps",
                preview.quote.estimated_price_impact_bps, self.config.max_cex_price_impact_bps
            )));
        }
        Ok(preview)
    }

    /// Binary-searches the largest allocation one exchange quotes within the
    /// ceiling. A quote failure counts as unsafe, like the ICPSwap search.
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

        Ok(last_safe
            .filter(|preview| input.meets_cex_minimum(&preview.request.pay_amount, self.config.cex_min_exec_usd)))
    }

    /// The ceiling is inclusive: a quote exactly at it is safe.
    fn is_safe_cex(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps <= self.config.max_cex_price_impact_bps
    }

    /// The CEX legs of a forced test split, or the waterfall when the split
    /// cannot be shaped. A test setting must never fail a plan.
    pub(super) async fn forced_or_ordered_overflow_previews(
        &self,
        input: &IcpswapFirstPlanInput,
        cex_value: Nat,
    ) -> Result<Vec<VenueRoutePreview>, IcpswapFirstPlannerError> {
        let Some(mexc_target_usd) = self.config.mexc_test_allocation_usd else {
            return self.ordered_overflow_previews(input, cex_value).await;
        };
        // Without Kraken the MEXC test leg splits against ICPSwap, which cannot take this amount.
        if !self.venues.contains(KRAKEN_VENUE_ID) {
            warn!(
                "[multi-venue] liq_id={} forced CEX split skipped: Kraken is not enabled",
                input.liquidation_id
            );
            return self.ordered_overflow_previews(input, cex_value).await;
        }
        let Some(reference_price) = input.pay_reference_price_usd else {
            warn!(
                "[multi-venue] liq_id={} forced CEX split skipped: no collateral reference price",
                input.liquidation_id
            );
            return self.ordered_overflow_previews(input, cex_value).await;
        };
        let mexc_value =
            ChainTokenAmount::from_formatted(input.total_pay.token.clone(), mexc_target_usd / reference_price).value;
        if mexc_value == 0u8 || mexc_value >= cex_value {
            warn!(
                "[multi-venue] liq_id={} forced CEX split skipped: ${mexc_target_usd:.2} to MEXC leaves no Kraken remainder",
                input.liquidation_id
            );
            return self.ordered_overflow_previews(input, cex_value).await;
        }
        warn!(
            "[multi-venue] liq_id={} TEST-ONLY forced CEX split: ${mexc_target_usd:.2} to MEXC, remainder to Kraken",
            input.liquidation_id
        );
        let kraken_value = cex_value.clone() - mexc_value.clone();
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
}
