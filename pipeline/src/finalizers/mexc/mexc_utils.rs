use candid::Nat;
/// Floating-point epsilon used for liquidity comparisons.
pub(super) const LIQUIDITY_EPS: f64 = 1e-9;

/// One market leg in a routed CEX trade.
#[derive(Debug, Clone)]
pub(crate) struct TradeLeg {
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

    if deposit == "ETH" && withdraw == "USDC" {
        return Some(vec![
            TradeLeg {
                market: "ETH_USDT".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "USDC_USDT".to_string(),
                side: "buy".to_string(),
            },
        ]);
    }

    if deposit == "USDC" && withdraw == "ETH" {
        return Some(vec![
            TradeLeg {
                market: "USDC_USDT".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "ETH_USDT".to_string(),
                side: "buy".to_string(),
            },
        ]);
    }

    None
}
