pub(super) use crate::swappers::model::{LIQUIDITY_EPS, f64_to_nat};

/// Stable 64-bit FNV-1a. Exchange ids derived from it must not change
/// between releases, so the std hasher is not an option.
pub(super) fn fnv1a_64(value: &str) -> u64 {
    value.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ byte as u64).wrapping_mul(0x100000001b3)
    })
}

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
    /// The market's taker fee, which a fill is measured net of.
    pub taker_fee_bps: f64,
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

/// Atomic decimals of an exchange asset. A ck token has its native asset's
/// decimals, and fiat USD uses six, as the USD stablecoins do.
pub(super) fn asset_decimals(symbol: &str) -> Option<u8> {
    use liquidium_pipeline_core::types::protocol_types::{Asset, Assets};
    let symbol = symbol.to_ascii_uppercase();
    if symbol == "USD" {
        return Some(6);
    }
    let native = symbol.strip_prefix("CK").unwrap_or(&symbol);
    [
        Assets::BTC,
        Assets::ICP,
        Assets::SOL,
        Assets::USDC,
        Assets::USDT,
        Assets::ETH,
    ]
    .into_iter()
    .find(|asset| asset.symbol() == native)
    .and_then(|asset| u8::try_from(asset.decimals()).ok())
}

/// A slice amount in atomic units of its asset, for the execution record.
/// An unknown asset keeps whole units, with a warning, so the record still completes.
pub(super) fn slice_amount_to_nat(amount: f64, symbol: &str) -> candid::Nat {
    let converted = asset_decimals(symbol)
        .ok_or_else(|| format!("unknown asset {symbol}"))
        .and_then(|decimals| {
            liquidium_pipeline_connectors::backend::amount_utils::amount_to_nat_units_strict(amount, decimals)
        });
    converted.unwrap_or_else(|error| {
        log::warn!("[cex] slice amount {amount} {symbol} kept in whole units: {error}");
        f64_to_nat(amount)
    })
}

/// True when the symbol is expected to be ~1 USD (native or wrapped stables).
pub(super) fn is_usd_stable_symbol(symbol: &str) -> bool {
    matches!(
        symbol.to_ascii_uppercase().as_str(),
        "USD" | "USDT" | "USDC" | "CKUSDT" | "CKUSDC"
    )
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
