//! Trading on Kraken: the sell quote, the local preflight of an order
//! against pair minimums, and the market order itself -- sized from the
//! visible book, submitted once under the leg's client id, and reconciled
//! to a terminal fill.
//!
//! Kraken quotes market orders in base volume, so a buy has to derive that
//! volume from a quote budget; the walks over the book that size and reprice
//! an order live here together so they cannot drift apart.

use super::*;

const ORDER_POLL_ATTEMPTS: usize = 20;
const ORDER_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// One hundredth of a basis point, matching MEXC's `ROUND_HANDLER`: the margin
/// by which a reported fill is kept under the amount the venue credited.
const FILL_ROUNDING_MARGIN_RATIO: Decimal = Decimal::from_parts(1, 0, 0, false, 6);

impl KrakenClient {
    async fn wait_for_order(&self, order_id: &str, side: &str) -> Result<SwapFillReport, String> {
        let mut last_lookup_failure: Option<String> = None;
        for attempt in 0..ORDER_POLL_ATTEMPTS {
            let order = match self.api.order(order_id).await {
                Ok(order) => order,
                // Kraken already accepted this order and handed back its id, so
                // a read that cannot see it yet is the venue's own propagation
                // delay rather than a lost order. Re-reading submits nothing, so
                // polling on is safe -- and necessary: failing the first lookup
                // parked orders that had already filled, because the loop below
                // only ever retried on *status*, never on a failed read.
                Err(error) => {
                    last_lookup_failure = Some(api_reconciliation_error(error));
                    if attempt + 1 < ORDER_POLL_ATTEMPTS {
                        tokio::time::sleep(ORDER_POLL_INTERVAL).await;
                    }
                    continue;
                }
            };
            last_lookup_failure = None;
            match order.status {
                KrakenOrderStatus::Closed => return fill_report(side, order.volume_executed, order.cost, order.fee),
                KrakenOrderStatus::Canceled | KrakenOrderStatus::Expired => {
                    if order.volume_executed > Decimal::ZERO {
                        return fill_report(side, order.volume_executed, order.cost, order.fee);
                    }
                    return Err(format!("Kraken order {order_id} was {:?}", order.status));
                }
                KrakenOrderStatus::Pending | KrakenOrderStatus::Open if attempt + 1 < ORDER_POLL_ATTEMPTS => {
                    tokio::time::sleep(ORDER_POLL_INTERVAL).await;
                }
                KrakenOrderStatus::Pending | KrakenOrderStatus::Open => {}
            }
        }
        // Both exhaustion paths stay ambiguous: an order we still cannot read,
        // or one that never settled, may both have moved funds.
        Err(last_lookup_failure
            .unwrap_or_else(|| format!("{KRAKEN_AMBIGUOUS_PREFIX}order {order_id} did not reach a terminal state")))
    }

    pub(super) async fn preflight_trade_amounts(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> Result<(), String> {
        ensure_positive(amount_in)?;
        ensure_positive(amount_out)?;
        let pair = self.pair(market).await?;
        let input = Decimal::from_f64(amount_in).ok_or_else(|| "invalid Kraken trade input".to_string())?;
        let output = Decimal::from_f64(amount_out).ok_or_else(|| "invalid Kraken trade output".to_string())?;

        let (base_volume, quote_cost) = if side.eq_ignore_ascii_case("sell") {
            (truncate(input, pair.lot_decimals), output)
        } else if side.eq_ignore_ascii_case("buy") {
            (truncate(output, pair.lot_decimals), input)
        } else {
            return Err(format!("unsupported Kraken order side {side}"));
        };

        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            ));
        }

        if quote_cost < pair.cost_min {
            return Err(format!(
                "Kraken quote cost {quote_cost} is below cost minimum {}",
                pair.cost_min
            ));
        }
        Ok(())
    }

    pub(super) async fn quote_sell(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let pair = self.pair(market).await?;
        ensure_positive(amount_in)?;

        let mut remaining = amount_in;
        let mut output = 0.0;
        for level in self.book(&pair, 100).await?.bids {
            let take = remaining.min(decimal_f64(level.quantity, "bid quantity")?);
            output += take * decimal_f64(level.price, "bid price")?;
            remaining -= take;
            if remaining <= 1e-12 {
                break;
            }
        }

        if remaining > 1e-12 {
            return Err("not enough Kraken bid liquidity".to_string());
        }

        Ok(output)
    }

    /// Submits one Kraken spot market order and reconciles its terminal fill.
    ///
    /// `amount_in` is expressed in the asset being spent by the route:
    /// - for a sell, it is base-asset volume;
    /// - for a buy, it is the quote-asset spending budget.
    ///
    /// Kraken accepts market-order volume in base units. Buy orders therefore
    /// derive a precision-safe base volume from the quote budget, visible asks,
    /// the pair's taker fee, and the configured quote overspend cap. Sell orders
    /// truncate the supplied base amount directly to Kraken's lot precision.
    ///
    /// Before submission this method enforces local liquidity, lot precision,
    /// order-minimum, cost-minimum, fee, and overspend constraints. It attaches
    /// the persisted client order ID when present, submits exactly one order,
    /// then polls Kraken until the order reaches a terminal state. Transport
    /// uncertainty during submission or reconciliation is classified by the
    /// Kraken adapter so the shared CEX state machine does not blindly replay it.
    pub(super) async fn execute_market_order(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexSubmissionError> {
        // Reject zero, negative, NaN, and infinite inputs before reading Kraken state.
        ensure_positive(amount_in)?;

        // Resolve the canonical market to one unique, online Kraken pair and load
        // its precision, minimum-order, minimum-cost, and fee metadata.
        let pair = self.pair(market).await?;

        // Convert the route's floating-point input into decimal arithmetic before
        // performing any sizing or limit calculations.
        let amount = Decimal::from_f64(amount_in).ok_or_else(|| "invalid Kraken order amount".to_string())?;

        // Kraken's order endpoint always needs a base-asset volume, regardless
        // of whether this route spends base by selling or quote by buying.
        let base_volume = if side.eq_ignore_ascii_case("sell") {
            // A sell's route input is already base volume; truncate it so the
            // submitted quantity never exceeds the available amount.
            let base = truncate(amount, pair.lot_decimals);

            // Simulate the precision-adjusted sell against current bids to prove
            // liquidity and determine whether its quote proceeds satisfy costmin.
            let estimated_output = estimate_sell_output(&self.book(&pair, 100).await?, base)?;

            // Kraken rejects sells whose expected quote notional is below costmin.
            if estimated_output < pair.cost_min {
                return Err(format!(
                    "Kraken sell estimate {estimated_output} is below cost minimum {}",
                    pair.cost_min
                )
                .into());
            }

            // Submit the precision-safe base volume produced above.
            base
        } else if side.eq_ignore_ascii_case("buy") {
            // The shared quote-quantity mode cannot be sent directly because this
            // Kraken endpoint expects market-buy volume in base units.
            if options.buy_mode == BuyOrderInputMode::QuoteOrderQty {
                return Err("Kraken market buys require precision-safe base quantity mode"
                    .to_string()
                    .into());
            }

            // Reject malformed pair metadata before using its fee in budget math.
            if !pair.taker_fee_bps.is_finite() || !(0.0..=10_000.0).contains(&pair.taker_fee_bps) {
                return Err(format!("invalid Kraken taker fee {} bps", pair.taker_fee_bps).into());
            }

            // Default to no extra spending when the caller did not authorize an
            // overspend allowance for base-quantity buy sizing.
            let cap_bps = options.max_quote_overspend_bps.unwrap_or(0.0);

            // Keep the overspend cap within a finite zero-to-100% bps range.
            if !cap_bps.is_finite() || !(0.0..=10_000.0).contains(&cap_bps) {
                return Err(format!("invalid Kraken quote overspend cap {cap_bps} bps").into());
            }

            // Convert fee and overspend bps into exact decimal ratios.
            let fee_ratio = Decimal::from_f64(pair.taker_fee_bps / 10_000.0).unwrap_or_default();
            let cap_ratio = Decimal::from_f64(cap_bps / 10_000.0).unwrap_or_default();

            // The hard quote cap includes only the explicitly authorized overspend.
            let max_spend = amount * (Decimal::ONE + cap_ratio);

            // Reserve quote currency for the estimated taker fee before using the
            // remainder to purchase base volume.
            let trade_budget = max_spend / (Decimal::ONE + fee_ratio);

            // Read enough asks to derive the base quantity purchasable by that budget.
            let book = self.book(&pair, 100).await?;

            // Track the unallocated quote budget while walking asks best-price first.
            let mut budget = trade_budget;

            // Accumulate the corresponding base volume across visible ask levels.
            let mut base = Decimal::ZERO;

            // Consume each ask level until either the budget or visible depth ends.
            for level in &book.asks {
                // Stop once the entire quote trade budget has been allocated.
                if budget <= Decimal::ZERO {
                    break;
                }

                // Skipped rather than rejected outright, so one malformed level
                // deep in the book does not refuse an order the rest of the
                // depth can fill; the liquidity check below still refuses it if
                // it cannot. See `is_tradable_level` for why it cannot be used.
                if !is_tradable_level(level) {
                    continue;
                }

                // Buy no more than the level offers or the remaining budget affords.
                let take = level.quantity.min(budget / level.price);

                // Add this level's purchased quantity to the proposed base order.
                base += take;

                // Remove this level's quote cost from the remaining trade budget.
                budget -= take * level.price;
            }

            // Any material budget left after exhausting asks means visible depth
            // cannot support the requested quote-input order.
            if budget > Decimal::from_f64(1e-10).unwrap_or(Decimal::ZERO) {
                return Err("not enough Kraken ask liquidity".to_string().into());
            }

            // Truncate derived base volume to Kraken lot precision; rounding up
            // here could spend more quote currency than authorized.
            let base = truncate(base, pair.lot_decimals);

            // Reprice the exact truncated base quantity against the same asks.
            let estimated_cost = estimate_buy_cost(&book, base)?;

            // Include the estimated taker fee in the final quote spend check.
            let estimated_total = estimated_cost * (Decimal::ONE + fee_ratio);

            // Refuse submission if precision or fee math would cross the hard cap.
            if estimated_total > max_spend {
                return Err(format!(
                    "Kraken buy estimate with fee {estimated_total} exceeds quote-input cap {max_spend}"
                )
                .into());
            }

            // Submit the precision-safe base volume derived from the quote budget.
            base
        } else {
            // Only the two spot sides understood by Kraken are accepted locally.
            return Err(format!("unsupported Kraken order side {side}").into());
        };

        // Apply Kraken's minimum base-order volume after all precision truncation.
        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            )
            .into());
        }

        // For buys, the route input is the quote budget and must independently
        // satisfy Kraken's minimum quote cost.
        if side.eq_ignore_ascii_case("buy") && amount < pair.cost_min {
            return Err(format!("Kraken quote budget {amount} is below cost minimum {}", pair.cost_min).into());
        }

        // A leg that already submitted under this client id must adopt that
        // order rather than place a second one. The Kraken order id is returned
        // only once, so a leg interrupted between submission and reconciliation
        // knows the order solely by the client id persisted before it -- and
        // without this lookup the resumed attempt would re-sell collateral the
        // first order had already sold.
        if let Some(client_order_id) = options.client_order_id.as_deref() {
            match self.api.closed_order_by_client_id(client_order_id).await {
                Ok(Some(order)) if order.volume_executed > Decimal::ZERO => {
                    info!(
                        "[kraken] adopting order already settled under client id {client_order_id}: executed {} cost {}",
                        order.volume_executed, order.cost
                    );
                    return fill_report(side, order.volume_executed, order.cost, order.fee).map_err(Into::into);
                }
                Ok(_) => {}
                // Not knowing whether a prior order exists is not the same as
                // knowing there is none, so this must not fall through to a
                // submission that could duplicate it.
                Err(error) => {
                    return Err(self.classify_submission_error(&api_reconciliation_error(error)));
                }
            }
        }

        // Submit exactly one market order, carrying the persisted client order ID
        // that the shared CEX recovery logic prepared before this side effect.
        // Past this point a failure may have reached the exchange, so both the
        // submission and the reconciliation are classified here rather than
        // being flattened into a rejection the caller would be free to replay.
        let order_id = self
            .api
            .place_market_order(&pair.api_name, side, base_volume, options.client_order_id)
            .await
            .map_err(|error| self.classify_submission_error(&api_submit_error(error)))?;

        // Poll the returned Kraken order ID to a terminal state and translate its
        // consumed input, received output, and fees into the shared fill report.
        self.wait_for_order(&order_id, side)
            .await
            .map_err(|error| self.classify_submission_error(&error))
    }

    pub(super) async fn read_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let pair = self.pair(market).await?;
        let book = self.book(&pair, limit.unwrap_or(100)).await?;
        Ok(OrderBook {
            bids: convert_levels(book.bids)?,
            asks: convert_levels(book.asks)?,
        })
    }
}

fn truncate(value: Decimal, decimals: u32) -> Decimal {
    value.round_dp_with_strategy(decimals, RoundingStrategy::ToZero)
}

/// Whether a book level is one this pipeline can trade against at all.
///
/// Levels arrive exactly as Kraken sent them, and a price at or below zero is
/// not a price: dividing a budget by it panics `Decimal`, and a negative one
/// would hand back budget that was never spent. Every walk over a book answers
/// this the same way, because sizing an order against depth that the repricing
/// then treats differently is how an order slips past its own spend cap.
fn is_tradable_level(level: &KrakenBookLevel) -> bool {
    level.price > Decimal::ZERO
}

fn estimate_buy_cost(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    let mut remaining = base_volume;
    let mut cost = Decimal::ZERO;
    for level in &book.asks {
        if !is_tradable_level(level) {
            continue;
        }
        let take = remaining.min(level.quantity);
        cost += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if remaining > Decimal::ZERO {
        Err("not enough Kraken ask liquidity".to_string())
    } else {
        Ok(cost)
    }
}

fn estimate_sell_output(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    let mut remaining = base_volume;
    let mut output = Decimal::ZERO;
    for level in &book.bids {
        if !is_tradable_level(level) {
            continue;
        }
        let take = remaining.min(level.quantity);
        output += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if remaining > Decimal::ZERO {
        Err("not enough Kraken bid liquidity".to_string())
    } else {
        Ok(output)
    }
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

/// Splits a settled order into what it consumed and what it paid out.
///
/// Kraken charges the fee in the currency the order pays out, and no order
/// flags are set to change that, so both sides here take the fee off the
/// received amount. What differs is the units: Kraken reports `fee` in the
/// quote currency whichever side it actually took it from. A sell pays out
/// quote, so the number is already usable. A buy pays out base, so the reported
/// fee has to be converted at this fill's own price before it can be taken off
/// the volume the order says it executed.
///
/// Reading a buy as "quote spent plus a quote fee" instead left `executed`
/// standing as the amount received, which is a fee more than Kraken credits.
/// Every withdrawal of that amount was then rejected for insufficient funds,
/// and no retry could close a gap where neither side moves (liquidation 1634).
fn fill_report(side: &str, executed: Decimal, cost: Decimal, fee: Decimal) -> Result<SwapFillReport, String> {
    // A sell pays out quote and a buy pays out base; the fee comes out of
    // whichever that is. Kraken reports it in the quote currency either way, so
    // only a buy has to convert, and `fee / cost` -- the fee as a fraction of
    // the order -- is the one form that survives the change of units. A
    // zero-cost fill has no price to convert through and nothing to charge a
    // fee against.
    let (consumed, received, fee_in_received) = if side.eq_ignore_ascii_case("sell") {
        (executed, cost, fee)
    } else if cost > Decimal::ZERO {
        let base_fee = executed
            .checked_mul(fee)
            .and_then(|weighted| weighted.checked_div(cost))
            .ok_or_else(|| "Kraken order fee cannot be expressed in base units".to_string())?;
        (cost, executed, base_fee)
    } else {
        (cost, executed, Decimal::ZERO)
    };

    // Kraken rounds every figure it reports, so a fee derived from them can land
    // a rounding step under what was really withheld -- and a received amount
    // even slightly above what the venue credited is the whole failure this
    // fixes: the leg then asks to withdraw or spend money that is not there, and
    // retrying cannot close a gap where neither side moves. The same hundredth
    // of a basis point MEXC's `ROUND_HANDLER` shaves off a fill covers it here,
    // three orders of magnitude below the dust the venue leaves behind anyway.
    let rounding_margin = received
        .checked_mul(FILL_ROUNDING_MARGIN_RATIO)
        .unwrap_or(Decimal::ZERO);

    Ok(SwapFillReport {
        input_consumed: decimal_f64(consumed, "order input")?,
        output_received: decimal_f64(
            (received - fee_in_received - rounding_margin).max(Decimal::ZERO),
            "order output net of fee",
        )?,
    })
}

#[cfg(test)]
#[path = "kraken_trading_tests.rs"]
mod tests;
