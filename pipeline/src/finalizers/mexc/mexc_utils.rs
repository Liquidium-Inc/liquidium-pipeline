use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::OrderBookLevel;

use crate::error::AppError;

/// Floating-point epsilon used for liquidity comparisons.
pub(super) const LIQUIDITY_EPS: f64 = 1e-9;
/// Basis points per 1.00 ratio value.
const BPS_PER_RATIO_UNIT: f64 = 10_000.0;

/// One market leg in a routed CEX trade.
#[derive(Debug, Clone)]
pub(super) struct TradeLeg {
    pub market: String,
    pub side: String,
}

/// Planned one-slice execution preview against current book depth.
#[derive(Debug, Clone)]
pub(super) struct SlicePreview {
    pub chunk_in: f64,
    pub preview_mid_price: f64,
    pub preview_impact_bps: f64,
}

/// Parse `BASE_QUOTE` market symbols into uppercase `(base, quote)`.
pub(super) fn parse_market_symbols(market: &str) -> Option<(String, String)> {
    let mut parts = market.split('_');
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    if parts.next().is_some() {
        return None;
    }
    Some((base, quote))
}

/// True when the symbol is expected to be ~1 USD (native or wrapped stables).
pub(super) fn is_usd_stable_symbol(symbol: &str) -> bool {
    matches!(
        symbol.to_ascii_uppercase().as_str(),
        "USD" | "USDT" | "USDC" | "CKUSDT" | "CKUSDC"
    )
}

/// Convert a non-negative float into `Nat` using truncation.
pub(super) fn f64_to_nat(v: f64) -> Nat {
    Nat::from(v.max(0.0) as u128)
}

/// Simulate selling `amount_in_base` into `bids`.
/// Returns `(quote_out, avg_price, impact_bps, unfilled_base)`.
pub(super) fn simulate_sell_from_bids(
    bids: &[OrderBookLevel],
    amount_in_base: f64,
) -> Result<(f64, f64, f64, f64), AppError> {
    if amount_in_base <= 0.0 {
        return Err("amount_in_base must be positive".into());
    }

    let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);
    if best_bid <= 0.0 {
        return Err("no bid liquidity".into());
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
        return Err("could not fill any sell amount".into());
    }
    let avg_price = quote_out / filled;
    let impact_bps = ((best_bid - avg_price) / best_bid * BPS_PER_RATIO_UNIT).max(0.0);
    Ok((quote_out, avg_price, impact_bps, remaining.max(0.0)))
}

/// Simulate buying base with `quote_in` from `asks`.
/// Returns `(base_out, avg_price, impact_bps, unspent_quote)`.
pub(super) fn simulate_buy_from_asks(asks: &[OrderBookLevel], quote_in: f64) -> Result<(f64, f64, f64, f64), AppError> {
    if quote_in <= 0.0 {
        return Err("quote_in must be positive".into());
    }

    let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
    if best_ask <= 0.0 {
        return Err("no ask liquidity".into());
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
        return Err("could not fill any buy amount".into());
    }
    let avg_price = spent / base_out;
    let impact_bps = ((avg_price - best_ask) / best_ask * BPS_PER_RATIO_UNIT).max(0.0);
    Ok((base_out, avg_price, impact_bps, remaining_quote.max(0.0)))
}

/// Hardcoded multi-leg routes where direct market lookup is insufficient.
pub(super) fn mexc_special_trade_legs(deposit_symbol: &str, withdraw_symbol: &str) -> Option<Vec<TradeLeg>> {
    let deposit = deposit_symbol.to_ascii_uppercase();
    let withdraw = withdraw_symbol.to_ascii_uppercase();

    if deposit == "CKBTC" && withdraw == "CKUSDT" {
        return Some(vec![
            TradeLeg {
                market: "CKBTC_BTC".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "BTC_USDC".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "USDC_USDT".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "CKUSDT_USDT".to_string(),
                side: "buy".to_string(),
            },
        ]);
    }

    if deposit == "CKUSDT" && withdraw == "CKBTC" {
        return Some(vec![
            TradeLeg {
                market: "CKUSDT_USDT".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "USDC_USDT".to_string(),
                side: "buy".to_string(),
            },
            TradeLeg {
                market: "BTC_USDC".to_string(),
                side: "buy".to_string(),
            },
            TradeLeg {
                market: "CKBTC_BTC".to_string(),
                side: "buy".to_string(),
            },
        ]);
    }

    None
}
