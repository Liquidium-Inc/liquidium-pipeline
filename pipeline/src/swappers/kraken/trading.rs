//! Trading on Kraken: the order book, and one IOC order per slice at the
//! caller's price limit, read to its final fill. Kraken takes order volume in
//! base units, so a buy turns its quote budget into a base volume here.

use std::time::Duration;

use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackendError, OrderBook, OrderBookLevel, SwapExecutionOptions, SwapFillReport,
};
use log::info;
use rust_decimal::{Decimal, RoundingStrategy};

use super::api::{
    KrakenApiError, KrakenBookLevel, KrakenOrder, KrakenOrderRequest, KrakenOrderStatus, KrakenPair, KrakenSide,
};
use super::client::{KrakenClient, decimal_f64, to_decimal};
use crate::swappers::model::ioc_limit_price;

pub(super) const ORDER_POLL_ATTEMPTS: usize = 20;
const ORDER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const ORDERBOOK_DEPTH: u32 = 100;

/// Measured on the live account: crypto pairs charged 80 bps, stablecoin pairs
/// 20. Erring high keeps previews safe, but it narrows a stablecoin pair's IOC price room by 60 bps.
pub(super) const TAKER_FEE_BPS: f64 = 80.0;

/// One hundredth of a basis point: a reported fill stays this far under what
/// Kraken reports, so a later withdrawal never asks for more than the account
/// holds (liquidation 1634).
const FILL_ROUNDING_MARGIN_RATIO: Decimal = Decimal::from_parts(1, 0, 0, false, 6);

impl KrakenClient {
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
    /// budget on a buy. Every order is IOC at its price limit; Kraken supports
    /// IOC on every pair, so an order without a limit is refused.
    pub(super) async fn execute_order(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexBackendError> {
        let side = parse_side(side)?;
        let limit = options
            .price_limit
            .as_ref()
            .ok_or_else(|| "a Kraken order needs a price limit".to_string())?;
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
        let limit_price = ioc_limit_price(side == KrakenSide::Sell, limit, None, pair.price_decimals)?;
        let volume = order_volume(&pair, side, amount, limit_price)?;

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

/// The base volume of an order, cut to the pair's lot precision. Kraken
/// checks its cost minimum on volume times the limit price.
fn order_volume(
    pair: &KrakenPair,
    side: KrakenSide,
    amount: Decimal,
    limit_price: Decimal,
) -> Result<Decimal, CexBackendError> {
    // A sell spends base. A buy spends quote, and every unit fills at or under
    // the limit, so `budget / limit` never spends more than the budget.
    let volume = match side {
        KrakenSide::Sell => truncate(amount, pair.lot_decimals),
        KrakenSide::Buy => truncate(amount / limit_price, pair.lot_decimals),
    };
    if volume < pair.order_min {
        return Err(CexBackendError::BelowMinimum(format!(
            "Kraken base volume {volume} is below order minimum {}",
            pair.order_min
        )));
    }
    let cost = volume * limit_price;
    if cost < pair.cost_min {
        return Err(CexBackendError::BelowMinimum(format!(
            "Kraken order cost {cost} is below cost minimum {}",
            pair.cost_min
        )));
    }
    Ok(volume)
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
        output_received: decimal_f64(
            (received - fee_in_received - rounding_margin).max(Decimal::ZERO),
            "order output",
        )?,
    })
}
