use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use super::{
    finalizer::CexVenueFinalizer,
    utils::{LIQUIDITY_EPS, TradeLeg},
};
use crate::swappers::model::{BPS_PER_RATIO_UNIT, adverse_price_impact_bps};

/// When a route is priced. Before any deposit, a leg the oracle bound would
/// hold is refused, so the deposit never waits on it. Once funded, the bound
/// holds the slices instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::finalizers::cex) enum RouteStage {
    Admission,
    Funded,
}

/// One normalized route preview shared by legacy CEX routing and the generic
/// multi-venue adapter. Prices are always receive units per pay unit.
pub(super) struct CexRoutePreview {
    pub receive_amount: f64,
    pub reference_price: f64,
    pub execution_price: f64,
    pub price_impact_bps: f64,
}

impl<B> CexVenueFinalizer<B>
where
    B: CexBackend,
{
    /// Compounds the normalized reference and execution prices for every CEX
    /// hop so all CEX routing paths make decisions from the same calculation.
    pub(super) async fn preview_resolved_trade_route(
        &self,
        legs: &[TradeLeg],
        initial_amount: f64,
        stage: RouteStage,
    ) -> Result<CexRoutePreview, String> {
        if !initial_amount.is_finite() || initial_amount <= LIQUIDITY_EPS {
            return Err(format!(
                "{} cannot quote a non-positive pay amount",
                self.profile.name()
            ));
        }

        let mut amount_in = initial_amount;
        let mut route_reference_price = 1.0;
        // The share of oracle value the legs so far keep, carried as execution carries it.
        let mut oracle_keep = 1.0;
        for leg in legs {
            // The fee comes from the same source the fills are accounted with.
            let preview = self.preview_leg(&leg.market, &leg.side, amount_in).await?;
            // Every slice is held to the oracle, so a market it cannot price,
            // or a leg it would hold, would take the deposit and then only hold.
            let oracle = match stage {
                RouteStage::Admission => self
                    .oracle_price_bound(leg, oracle_keep)
                    .await
                    .map_err(|error| error.to_string())?,
                RouteStage::Funded => None,
            };
            // Orders go out at the room the cap leaves after the fee.
            Self::price_room_or_err(
                &leg.side,
                self.max_sell_slippage_bps,
                preview.taker_fee_bps,
                &leg.market,
            )?;
            let (gross_amount_out, side_impact_bps) = (preview.gross_out, preview.impact_bps);
            let amount_out = gross_amount_out * (1.0 - preview.taker_fee_bps / BPS_PER_RATIO_UNIT);
            if let Some((oracle_price, bound)) = oracle {
                let sell = leg.side.eq_ignore_ascii_case("sell");
                let net_price = if sell {
                    amount_out / amount_in
                } else {
                    amount_in / amount_out
                };
                if !net_price.is_finite() || Self::is_worse_price(&leg.side, net_price, bound) {
                    return Err(format!(
                        "{} {} pays {net_price} net, past its oracle bound {bound}",
                        leg.market, leg.side
                    ));
                }
                let at_oracle = if sell {
                    amount_in * oracle_price
                } else {
                    amount_in / oracle_price
                };
                oracle_keep *= (amount_out / at_oracle).min(1.0);
            }
            let impact_ratio = side_impact_bps / BPS_PER_RATIO_UNIT;
            let gross_execution_price = gross_amount_out / amount_in;
            let reference_price = if leg.side.eq_ignore_ascii_case("buy") {
                gross_execution_price * (1.0 + impact_ratio)
            } else if impact_ratio < 1.0 {
                gross_execution_price / (1.0 - impact_ratio)
            } else {
                return Err(format!(
                    "{} returned invalid price impact for {}",
                    self.profile.name(),
                    leg.market
                ));
            };
            if !reference_price.is_finite() || reference_price <= 0.0 {
                return Err(format!(
                    "{} returned an invalid preview for {}",
                    self.profile.name(),
                    leg.market
                ));
            }
            route_reference_price *= reference_price;
            amount_in = amount_out;
        }

        let execution_price = amount_in / initial_amount;
        let reference_price = if legs.is_empty() {
            execution_price
        } else {
            route_reference_price
        };
        Ok(CexRoutePreview {
            receive_amount: amount_in,
            reference_price,
            execution_price,
            price_impact_bps: adverse_price_impact_bps(reference_price, execution_price),
        })
    }
}
