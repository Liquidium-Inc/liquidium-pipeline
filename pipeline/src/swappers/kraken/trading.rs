//! Trading on Kraken: the sell quote, the order book, and one order per
//! slice, IOC at the caller's price limit, read to its final fill.
//!
//! Kraken takes order volume in base units, so a buy turns its quote budget
//! into a base volume here. The book walks that size an order live together.

use std::time::Duration;

use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackendError, OrderBook, OrderBookLevel, PriceLimit, SwapExecutionOptions, SwapFillReport,
};
use log::info;
use rust_decimal::{
    Decimal, RoundingStrategy,
    prelude::{FromPrimitive, ToPrimitive},
};

use super::api::{
    KrakenApiError, KrakenBook, KrakenBookLevel, KrakenOrder, KrakenOrderRequest, KrakenOrderStatus, KrakenPair,
    KrakenSide,
};
use super::client::{KrakenClient, decimal_f64, to_decimal};
use crate::swappers::model::BPS_PER_RATIO_UNIT;

pub(super) const ORDER_POLL_ATTEMPTS: usize = 20;
const ORDER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const ORDERBOOK_DEPTH: u32 = 100;

/// Measured on the live account: every crypto pair charged 80 bps, stablecoin
/// pairs 20. A preview that reads this errs high, which refuses no safe trade.
pub(super) const TAKER_FEE_BPS: f64 = 80.0;

/// One hundredth of a basis point: a reported fill stays this far under what
/// Kraken reports, so a later withdrawal never asks for more than the account
/// holds (liquidation 1634).
const FILL_ROUNDING_MARGIN_RATIO: Decimal = Decimal::from_parts(1, 0, 0, false, 6);

impl KrakenClient {
    pub(super) async fn quote_sell(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let pair = self.pair(market).await?;
        let volume = to_decimal(amount_in, "sell amount")?;
        let output = estimate_sell_output(&self.book(&pair, ORDERBOOK_DEPTH).await?, volume)?;
        decimal_f64(output, "sell quote")
    }

    pub(super) async fn read_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let pair = self.pair(market).await?;
        let book = self.book(&pair, limit.unwrap_or(ORDERBOOK_DEPTH)).await?;
        Ok(OrderBook {
            bids: convert_levels(book.bids)?,
            asks: convert_levels(book.asks)?,
            taker_fee_bps: TAKER_FEE_BPS,
        })
    }

    /// Places one order and reads it to its final fill.
    ///
    /// `amount_in` is what the route spends: base volume on a sell, the quote
    /// budget on a buy. With a price limit the order is IOC at that limit;
    /// Kraken supports IOC on every pair, so there is no market fallback.
    pub(super) async fn execute_order(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexBackendError> {
        let side = parse_side(side)?;
        let pair = self.pair(market).await?;

        // A resumed leg holds the client id of an order it may have placed.
        // A fill under that id is adopted, so the same funds never trade twice.
        if let Some(client_order_id) = options.client_order_id.as_deref() {
            match self.api.closed_order_by_client_id(client_order_id).await {
                Ok(Some(order)) if order.volume_executed > Decimal::ZERO => {
                    info!("[kraken] adopting the fill already placed under client id {client_order_id}");
                    return Ok(fill_report(side, &order)?);
                }
                Ok(_) => {}
                Err(KrakenApiError::Unreachable(error)) => return Err(CexBackendError::Unreachable(error)),
                Err(error) => {
                    return Err(format!("Kraken lookup of client order {client_order_id} failed: {error}").into());
                }
            }
        }

        let amount = to_decimal(amount_in, "order amount")?;
        let book = self.book(&pair, ORDERBOOK_DEPTH).await?;
        let limit_price = options
            .price_limit
            .as_ref()
            .map(|limit| ioc_limit_price(side, &net_of_taker_fee(limit)?, pair.price_decimals))
            .transpose()?;
        let volume = match side {
            KrakenSide::Sell => sell_volume(&pair, &book, amount)?,
            KrakenSide::Buy => buy_volume(&pair, &book, amount, limit_price, &options)?,
        };

        let request = KrakenOrderRequest {
            pair: pair.api_name.clone(),
            side,
            volume,
            limit_price,
            client_order_id: options.client_order_id.clone(),
        };
        let order_id = self.api.place_order(&request).await.map_err(|error| match error {
            KrakenApiError::Rejected(message) => CexBackendError::Other(message),
            KrakenApiError::Unreachable(message) => CexBackendError::Unreachable(message),
            KrakenApiError::Transport(message) => {
                CexBackendError::Ambiguous(format!("Kraken order on {market} may have been placed: {message}"))
            }
        })?;
        self.wait_for_order(&order_id, side).await
    }

    /// Reads an accepted order until it is final. Kraken may not show a new
    /// order at once, so a failed read is read again, not parked.
    pub(super) async fn wait_for_order(
        &self,
        order_id: &str,
        side: KrakenSide,
    ) -> Result<SwapFillReport, CexBackendError> {
        let mut last_failure = None;
        for attempt in 0..ORDER_POLL_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(ORDER_POLL_INTERVAL).await;
            }
            match self.api.order(order_id).await {
                Err(error) => last_failure = Some(error),
                Ok(order) => {
                    last_failure = None;
                    // An IOC order cancels its unfilled rest, so a cancelled order
                    // reports a partial or empty fill, not a failure.
                    if matches!(
                        order.status,
                        KrakenOrderStatus::Closed | KrakenOrderStatus::Canceled | KrakenOrderStatus::Expired
                    ) {
                        return Ok(fill_report(side, &order)?);
                    }
                }
            }
        }
        // The next cycle asks again under the same client id and adopts the fill.
        Err(match last_failure {
            Some(KrakenApiError::Unreachable(error)) => {
                CexBackendError::Unreachable(format!("Kraken order {order_id} is placed but not readable: {error}"))
            }
            Some(error) => {
                CexBackendError::Ambiguous(format!("Kraken order {order_id} is placed but not readable: {error}"))
            }
            None => CexBackendError::Ambiguous(format!("Kraken order {order_id} did not reach a final state")),
        })
    }
}

fn parse_side(side: &str) -> Result<KrakenSide, String> {
    if side.eq_ignore_ascii_case("sell") {
        Ok(KrakenSide::Sell)
    } else if side.eq_ignore_ascii_case("buy") {
        Ok(KrakenSide::Buy)
    } else {
        Err(format!("unsupported Kraken order side {side}"))
    }
}

fn truncate(value: Decimal, decimals: u32) -> Decimal {
    value.round_dp_with_strategy(decimals, RoundingStrategy::ToZero)
}

/// The caller measures a fill net of the fee, so the order's price room is the
/// limit less the taker fee. A limit that the fee uses up places no order.
pub(super) fn net_of_taker_fee(limit: &PriceLimit) -> Result<PriceLimit, String> {
    let price_room_bps = limit.max_slippage_bps - TAKER_FEE_BPS;
    if price_room_bps <= 0.0 {
        return Err(format!(
            "a {} bps limit leaves no room for the {TAKER_FEE_BPS} bps Kraken taker fee",
            limit.max_slippage_bps
        ));
    }
    Ok(PriceLimit {
        max_slippage_bps: price_room_bps,
        ..*limit
    })
}

/// The worst price the order accepts, rounded to the pair's precision in the
/// direction that keeps it inside the limit.
pub(super) fn ioc_limit_price(side: KrakenSide, limit: &PriceLimit, price_decimals: u32) -> Result<Decimal, String> {
    let reference = Decimal::from_f64(limit.reference_price)
        .filter(|price| *price > Decimal::ZERO)
        .ok_or_else(|| format!("invalid reference price {}", limit.reference_price))?;
    let cap = Decimal::from_f64(limit.max_slippage_bps / BPS_PER_RATIO_UNIT)
        .filter(|cap| *cap >= Decimal::ZERO && *cap < Decimal::ONE)
        .ok_or_else(|| format!("invalid slippage cap {} bps", limit.max_slippage_bps))?;
    let price = match side {
        KrakenSide::Sell => {
            (reference * (Decimal::ONE - cap)).round_dp_with_strategy(price_decimals, RoundingStrategy::AwayFromZero)
        }
        KrakenSide::Buy => {
            (reference * (Decimal::ONE + cap)).round_dp_with_strategy(price_decimals, RoundingStrategy::ToZero)
        }
    };
    if price <= Decimal::ZERO {
        return Err(format!("IOC price {price} is not valid"));
    }
    Ok(price)
}

fn below_minimum(message: String) -> CexBackendError {
    CexBackendError::BelowMinimum(message)
}

/// A sell spends the base volume itself, cut to the pair's lot precision.
fn sell_volume(pair: &KrakenPair, book: &KrakenBook, amount: Decimal) -> Result<Decimal, CexBackendError> {
    let volume = truncate(amount, pair.lot_decimals);
    if volume < pair.order_min {
        return Err(below_minimum(format!(
            "Kraken base volume {volume} is below order minimum {}",
            pair.order_min
        )));
    }
    let estimated_output = estimate_sell_output(book, volume)?;
    if estimated_output < pair.cost_min {
        return Err(below_minimum(format!(
            "Kraken sell estimate {estimated_output} is below cost minimum {}",
            pair.cost_min
        )));
    }
    Ok(volume)
}

/// A buy turns its quote budget into base volume. With a limit, every unit
/// fills at or under the limit, so `budget / limit` never spends more than the
/// budget. Without one, the volume comes from a walk over the asks.
fn buy_volume(
    pair: &KrakenPair,
    book: &KrakenBook,
    budget: Decimal,
    limit_price: Option<Decimal>,
    options: &SwapExecutionOptions,
) -> Result<Decimal, CexBackendError> {
    if options.buy_mode == BuyOrderInputMode::QuoteOrderQty {
        return Err("Kraken buys need a base quantity, not a quote order quantity"
            .to_string()
            .into());
    }
    if budget < pair.cost_min {
        return Err(below_minimum(format!(
            "Kraken quote budget {budget} is below cost minimum {}",
            pair.cost_min
        )));
    }
    let volume = match limit_price {
        Some(limit) => truncate(budget / limit, pair.lot_decimals),
        None => market_buy_volume(pair, book, budget, options)?,
    };
    if volume < pair.order_min {
        return Err(below_minimum(format!(
            "Kraken base volume {volume} is below order minimum {}",
            pair.order_min
        )));
    }
    Ok(volume)
}

fn market_buy_volume(
    pair: &KrakenPair,
    book: &KrakenBook,
    budget: Decimal,
    options: &SwapExecutionOptions,
) -> Result<Decimal, String> {
    let cap_bps = options.max_quote_overspend_bps.unwrap_or(0.0);
    if !cap_bps.is_finite() || !(0.0..=BPS_PER_RATIO_UNIT).contains(&cap_bps) {
        return Err(format!("invalid Kraken quote overspend cap {cap_bps} bps"));
    }
    let max_spend = budget * (Decimal::ONE + Decimal::from_f64(cap_bps / BPS_PER_RATIO_UNIT).unwrap_or_default());
    let mut remaining = budget;
    let mut volume = Decimal::ZERO;
    for level in book.asks.iter().filter(|level| is_tradable_level(level)) {
        if remaining <= Decimal::ZERO {
            break;
        }
        let take = level.quantity.min(remaining / level.price);
        volume += take;
        remaining -= take * level.price;
    }
    if remaining > Decimal::new(1, 10) {
        return Err("not enough Kraken ask liquidity".to_string());
    }
    let volume = truncate(volume, pair.lot_decimals);
    let estimated_cost = estimate_buy_cost(book, volume)?;
    if estimated_cost > max_spend {
        return Err(format!(
            "Kraken buy estimate {estimated_cost} exceeds quote-input cap {max_spend}"
        ));
    }
    Ok(volume)
}

/// A price at or below zero is not a price: dividing by it panics `Decimal`.
/// Every walk skips such levels the same way, so sizing and repricing agree.
fn is_tradable_level(level: &KrakenBookLevel) -> bool {
    level.price > Decimal::ZERO
}

pub(super) fn estimate_buy_cost(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    walk(&book.asks, base_volume).ok_or_else(|| "not enough Kraken ask liquidity".to_string())
}

pub(super) fn estimate_sell_output(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    walk(&book.bids, base_volume).ok_or_else(|| "not enough Kraken bid liquidity".to_string())
}

/// The quote value of `base_volume` taken from `levels`, or `None` when the
/// priced depth runs out first.
fn walk(levels: &[KrakenBookLevel], base_volume: Decimal) -> Option<Decimal> {
    let mut remaining = base_volume;
    let mut value = Decimal::ZERO;
    for level in levels.iter().filter(|level| is_tradable_level(level)) {
        let take = remaining.min(level.quantity);
        value += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            return Some(value);
        }
    }
    (remaining <= Decimal::ZERO).then_some(value)
}

fn convert_levels(levels: Vec<KrakenBookLevel>) -> Result<Vec<OrderBookLevel>, String> {
    levels
        .into_iter()
        .map(|level| {
            Ok(OrderBookLevel {
                price: decimal_f64(level.price, "orderbook price")?,
                quantity: decimal_f64(level.quantity, "orderbook quantity")?,
            })
        })
        .collect()
}

/// What a final order consumed and what it paid out, net of the fee.
///
/// The fee comes from what the order pays out: quote on a sell, base on a
/// buy. Kraken reports it in quote either way, so a buy converts it at the
/// fill's own price, `fee / cost` of the executed volume.
pub(super) fn fill_report(side: KrakenSide, order: &KrakenOrder) -> Result<SwapFillReport, String> {
    let (executed, cost, fee) = (order.volume_executed, order.cost, order.fee);
    let (consumed, received, fee_in_received) = match side {
        KrakenSide::Sell => (executed, cost, fee),
        KrakenSide::Buy if cost > Decimal::ZERO => {
            let base_fee = executed
                .checked_mul(fee)
                .and_then(|weighted| weighted.checked_div(cost))
                .ok_or_else(|| "Kraken order fee cannot be expressed in base units".to_string())?;
            (cost, executed, base_fee)
        }
        KrakenSide::Buy => (cost, executed, Decimal::ZERO),
    };
    let rounding_margin = received
        .checked_mul(FILL_ROUNDING_MARGIN_RATIO)
        .unwrap_or(Decimal::ZERO);
    Ok(SwapFillReport {
        input_consumed: decimal_f64(consumed, "order input")?,
        output_received: (received - fee_in_received - rounding_margin)
            .max(Decimal::ZERO)
            .to_f64()
            .ok_or_else(|| "Kraken order output cannot be represented as f64".to_string())?,
    })
}
