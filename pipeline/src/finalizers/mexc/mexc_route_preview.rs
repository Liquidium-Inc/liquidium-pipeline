use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use super::{
    mexc_finalizer::MexcFinalizer,
    mexc_utils::{LIQUIDITY_EPS, TradeLeg},
};
use crate::swappers::model::adverse_price_impact_bps;

const BPS_PER_RATIO_UNIT: f64 = 10_000.0;
/// Conservative quote-time fee per MEXC hop. Live fills resolve the exact
/// symbol fee; planning must not assume the gross order-book output is usable.
const MEXC_PREVIEW_TAKER_FEE_BPS: f64 = 10.0;

/// One normalized route preview shared by legacy CEX routing and the generic
/// multi-venue adapter. Prices are always receive units per pay unit.
pub(super) struct MexcRoutePreview {
    pub receive_amount: f64,
    pub reference_price: f64,
    pub execution_price: f64,
    pub price_impact_bps: f64,
}

impl<B> MexcFinalizer<B>
where
    B: CexBackend,
{
    /// Compounds the normalized reference and execution prices for every CEX
    /// hop so all MEXC routing paths make decisions from the same calculation.
    pub(super) async fn preview_resolved_trade_route(
        &self,
        legs: &[TradeLeg],
        initial_amount: f64,
    ) -> Result<MexcRoutePreview, String> {
        if !initial_amount.is_finite() || initial_amount <= LIQUIDITY_EPS {
            return Err("MEXC cannot quote a non-positive pay amount".to_string());
        }

        let mut amount_in = initial_amount;
        let mut route_reference_price = 1.0;
        for leg in legs {
            let (gross_amount_out, _side_vwap, side_impact_bps) =
                self.preview_leg(&leg.market, &leg.side, amount_in).await?;
            let amount_out = gross_amount_out * (1.0 - MEXC_PREVIEW_TAKER_FEE_BPS / BPS_PER_RATIO_UNIT);
            let impact_ratio = side_impact_bps / BPS_PER_RATIO_UNIT;
            let gross_execution_price = gross_amount_out / amount_in;
            let reference_price = if leg.side.eq_ignore_ascii_case("buy") {
                gross_execution_price * (1.0 + impact_ratio)
            } else if impact_ratio < 1.0 {
                gross_execution_price / (1.0 - impact_ratio)
            } else {
                return Err(format!("MEXC returned invalid price impact for {}", leg.market));
            };
            if !reference_price.is_finite() || reference_price <= 0.0 {
                return Err(format!("MEXC returned an invalid preview for {}", leg.market));
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
        Ok(MexcRoutePreview {
            receive_amount: amount_in,
            reference_price,
            execution_price,
            price_impact_bps: adverse_price_impact_bps(reference_price, execution_price),
        })
    }
}
