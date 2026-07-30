use liquidium_pipeline_connectors::backend::cex_backend::OrderBookLevel;

use crate::swappers::model::{BPS_PER_RATIO_UNIT, LIQUIDITY_EPS};

/// Simulates selling base into bids from best to worst.
/// Returns `(quote_out, vwap, price_impact_bps, unfilled_base)`.
pub(crate) fn simulate_sell_from_bids(
    bids: &[OrderBookLevel],
    amount_in_base: f64,
) -> Result<(f64, f64, f64, f64), String> {
    if amount_in_base <= 0.0 {
        return Err("amount_in_base must be positive".to_string());
    }

    let best_bid = bids.first().map(|level| level.price).unwrap_or(0.0);
    if best_bid <= 0.0 {
        return Err("no bid liquidity".to_string());
    }

    let mut remaining = amount_in_base;
    let mut quote_out = 0.0;
    for level in bids {
        if remaining <= LIQUIDITY_EPS {
            break;
        }
        if level.price <= 0.0 || level.quantity <= 0.0 {
            continue;
        }
        let take = remaining.min(level.quantity);
        quote_out += take * level.price;
        remaining -= take;
    }

    let filled = (amount_in_base - remaining).max(0.0);
    if filled <= LIQUIDITY_EPS {
        return Err("could not fill any sell amount".to_string());
    }
    let vwap = quote_out / filled;
    let price_impact_bps = ((best_bid - vwap) / best_bid * BPS_PER_RATIO_UNIT).max(0.0);
    Ok((quote_out, vwap, price_impact_bps, remaining.max(0.0)))
}

/// Simulates spending quote currency into asks from best to worst.
/// Returns `(base_out, vwap, price_impact_bps, unspent_quote)`.
pub(crate) fn simulate_buy_from_asks(asks: &[OrderBookLevel], quote_in: f64) -> Result<(f64, f64, f64, f64), String> {
    if quote_in <= 0.0 {
        return Err("quote_in must be positive".to_string());
    }

    let best_ask = asks.first().map(|level| level.price).unwrap_or(0.0);
    if best_ask <= 0.0 {
        return Err("no ask liquidity".to_string());
    }

    let mut remaining_quote = quote_in;
    let mut base_out = 0.0;
    for level in asks {
        if remaining_quote <= LIQUIDITY_EPS {
            break;
        }
        if level.price <= 0.0 || level.quantity <= 0.0 {
            continue;
        }
        let max_base = remaining_quote / level.price;
        let take = level.quantity.min(max_base);
        base_out += take;
        remaining_quote -= take * level.price;
    }

    let spent = (quote_in - remaining_quote).max(0.0);
    if spent <= LIQUIDITY_EPS || base_out <= LIQUIDITY_EPS {
        return Err("could not fill any buy amount".to_string());
    }
    let vwap = spent / base_out;
    // This side-native value remains quote/base because the legacy MEXC slice
    // limiter is configured against it. The common `SwapQuote` separately
    // normalizes inverted buy prices to receive/pay units.
    let price_impact_bps = ((vwap - best_ask) / best_ask * BPS_PER_RATIO_UNIT).max(0.0);
    Ok((base_out, vwap, price_impact_bps, remaining_quote.max(0.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: f64, quantity: f64) -> OrderBookLevel {
        OrderBookLevel { price, quantity }
    }

    #[test]
    fn sell_quote_uses_best_bid_as_reference_and_filled_volume_for_vwap() {
        let (output, vwap, impact_bps, unfilled) =
            simulate_sell_from_bids(&[level(10.0, 1.0), level(9.9, 2.0)], 3.0).expect("sell quote");

        assert!((output - 29.8).abs() < 1e-9);
        assert!((vwap - (29.8 / 3.0)).abs() < 1e-9);
        assert!((impact_bps - 66.666_666_666_67).abs() < 1e-9);
        assert_eq!(unfilled, 0.0);
    }

    #[test]
    fn buy_quote_uses_best_ask_as_reference_and_purchased_volume_for_vwap() {
        let (output, vwap, impact_bps, unspent) =
            simulate_buy_from_asks(&[level(9.9, 1.0), level(9.95, 2.0)], 29.8).expect("buy quote");

        assert!((output - 3.0).abs() < 1e-9);
        assert!((vwap - (29.8 / 3.0)).abs() < 1e-9);
        let expected_impact = (vwap - 9.9) / 9.9 * BPS_PER_RATIO_UNIT;
        assert!((impact_bps - expected_impact).abs() < 1e-9);
        assert_eq!(unspent, 0.0);
    }

    #[test]
    fn quote_reports_unfilled_input_when_visible_depth_is_insufficient() {
        let (_, _, _, unfilled) = simulate_sell_from_bids(&[level(10.0, 1.0)], 3.0).expect("partial quote");

        assert_eq!(unfilled, 2.0);
    }
}
