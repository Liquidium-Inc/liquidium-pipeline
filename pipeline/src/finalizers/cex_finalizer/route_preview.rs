use std::collections::{HashMap, hash_map::Entry};

use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, OrderBook};

use super::{CexFinalizer, DEFAULT_ORDERBOOK_LIMIT};
use crate::finalizers::cex_finalizer::utils::{LIQUIDITY_EPS, TradeLeg};
use crate::swappers::model::{BPS_PER_RATIO_UNIT, adverse_price_impact_bps};

/// Order books read during one route-resolution pass, keyed by market.
///
/// A book belongs to the market, not to the amount being routed, so candidate
/// routes that cross the same market share one read instead of one each. That
/// also makes the comparison between them fair: without it the route previewed
/// last is judged against a later book than the route previewed first, and the
/// winner partly reflects which one was quoted more recently.
///
/// Deliberately per-pass rather than a long-lived cache: a stale book would
/// then decide real routing.
#[derive(Default)]
pub(super) struct RouteOrderbooks {
    books: HashMap<String, OrderBook>,
}

impl RouteOrderbooks {
    /// Returns this market's book, reading it from the venue only the first
    /// time this pass asks for it.
    pub(super) async fn book<B>(&mut self, backend: &B, market: &str) -> Result<&OrderBook, String>
    where
        B: CexBackend,
    {
        match self.books.entry(market.to_string()) {
            Entry::Occupied(book) => Ok(book.into_mut()),
            Entry::Vacant(slot) => Ok(slot.insert(backend.get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT)).await?)),
        }
    }
}

/// One normalized route preview shared by legacy CEX routing and the generic
/// multi-venue adapter. Prices are always receive units per pay unit.
pub(super) struct CexResolvedRoutePreview {
    pub receive_amount: f64,
    pub reference_price: f64,
    pub execution_price: f64,
    pub price_impact_bps: f64,
}

impl<B> CexFinalizer<B>
where
    B: CexBackend,
{
    /// Compounds the normalized reference and execution prices for every CEX
    /// hop so all CEX routing paths make decisions from the same calculation.
    pub(super) async fn preview_resolved_trade_route(
        &self,
        legs: &[TradeLeg],
        initial_amount: f64,
    ) -> Result<CexResolvedRoutePreview, String> {
        self.preview_resolved_trade_route_with_books(&mut RouteOrderbooks::default(), legs, initial_amount)
            .await
    }

    /// `preview_resolved_trade_route` reusing the books an earlier candidate in
    /// the same pass already read.
    pub(super) async fn preview_resolved_trade_route_with_books(
        &self,
        books: &mut RouteOrderbooks,
        legs: &[TradeLeg],
        initial_amount: f64,
    ) -> Result<CexResolvedRoutePreview, String> {
        if !initial_amount.is_finite() || initial_amount <= LIQUIDITY_EPS {
            return Err(format!(
                "{} cannot quote a non-positive pay amount",
                self.profile.venue_id()
            ));
        }

        let mut amount_in = initial_amount;
        let mut route_reference_price = 1.0;
        for leg in legs {
            let (gross_amount_out, _side_vwap, side_impact_bps) = self
                .preview_leg_with_books(books, &leg.market, &leg.side, amount_in)
                .await?;
            if self.profile.funding_preflight_required() {
                self.backend
                    .validate_trade_amounts(&leg.market, &leg.side, amount_in, gross_amount_out)
                    .await?;
            }
            let amount_out = gross_amount_out * (1.0 - self.profile.preview_taker_fee_bps() / BPS_PER_RATIO_UNIT);
            let impact_ratio = side_impact_bps / BPS_PER_RATIO_UNIT;
            let gross_execution_price = gross_amount_out / amount_in;
            let reference_price = if leg.side.eq_ignore_ascii_case("buy") {
                gross_execution_price * (1.0 + impact_ratio)
            } else if impact_ratio < 1.0 {
                gross_execution_price / (1.0 - impact_ratio)
            } else {
                return Err(format!(
                    "{} returned invalid price impact for {}",
                    self.profile.venue_id(),
                    leg.market
                ));
            };
            if !reference_price.is_finite() || reference_price <= 0.0 {
                return Err(format!(
                    "{} returned an invalid preview for {}",
                    self.profile.venue_id(),
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
        Ok(CexResolvedRoutePreview {
            receive_amount: amount_in,
            reference_price,
            execution_price,
            price_impact_bps: adverse_price_impact_bps(reference_price, execution_price),
        })
    }
}
