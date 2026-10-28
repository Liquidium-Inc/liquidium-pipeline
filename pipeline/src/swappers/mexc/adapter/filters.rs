//! A market's trading rules from `exchangeInfo`: lot size, minimum order
//! value, precision, taker fee and price band, cached per symbol.
use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct SymbolFilters {
    pub(super) step_size: Option<Decimal>,
    pub(super) min_qty: Option<Decimal>,
    pub(super) min_notional: Option<Decimal>,
    pub(super) quote_precision: Option<u32>,
    pub(super) base_precision: Option<u32>,
    pub(super) taker_fee_bps: Option<f64>,
    pub(super) resolved_symbol: Option<String>,
    /// `PERCENT_PRICE_BY_SIDE`: how far above the market a buy may be priced.
    pub(super) buy_price_band: Option<Decimal>,
    /// `PERCENT_PRICE_BY_SIDE`: how far below the market a sell may be priced.
    pub(super) sell_price_band: Option<Decimal>,
}

pub(super) fn truncate_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step.is_zero() {
        return value;
    }
    let steps = (value / step).floor();
    steps * step
}

fn parse_decimal(v: &Value, key: &str) -> Option<Decimal> {
    v.get(key)
        .and_then(|raw| raw.as_str())
        .and_then(|s| Decimal::from_str_exact(s).ok())
}

fn parse_decimal_flexible(v: &Value, key: &str) -> Option<Decimal> {
    let raw = v.get(key)?;
    match raw {
        Value::String(s) => Decimal::from_str_exact(s).ok(),
        Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    }
}

fn parse_u32(v: &Value, key: &str) -> Option<u32> {
    v.get(key).and_then(|raw| raw.as_u64()).map(|v| v as u32)
}

/// Reads the advertised order types of one exchangeInfo symbol entry.
///
/// The live API always returns the list, so a missing list is treated as
/// supported only to stay compatible with older or partial metadata.
fn supports_market_orders(info: &Value) -> bool {
    match info.get("orderTypes").and_then(Value::as_array) {
        Some(types) => types.iter().any(|kind| kind.as_str() == Some("MARKET")),
        None => true,
    }
}

impl MexcClient {
    pub(super) fn parse_taker_fee_bps(info: &Value) -> Option<f64> {
        let commission_ratio = parse_decimal_flexible(info, "takerCommission")?;
        if commission_ratio < Decimal::ZERO {
            return None;
        }

        let bps = commission_ratio * Decimal::from_i32(10_000)?;
        let bps_f64 = bps.to_f64()?;
        if bps_f64.is_finite() && bps_f64 >= 0.0 && bps_f64 <= MAX_TAKER_FEE_BPS {
            Some(bps_f64)
        } else {
            None
        }
    }

    pub(super) fn resolve_taker_fee_bps(filters: Option<&SymbolFilters>) -> (f64, bool) {
        let bps = filters.and_then(|f| f.taker_fee_bps);
        match bps {
            Some(v) if v.is_finite() && v >= 0.0 && v + ROUND_HANDLER <= MAX_TAKER_FEE_BPS => {
                (v + ROUND_HANDLER, false)
            }
            _ => (MEXC_SPOT_FEE_BPS + ROUND_HANDLER, true),
        }
    }

    async fn fetch_symbol_info(&self, symbol: &str) -> Result<Option<(Value, String)>, String> {
        let mut direct_error = None;
        let url = format!("https://api.mexc.com/api/v3/exchangeInfo?symbol={}", symbol);
        let resp = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            let payload: Value = resp.json().await.map_err(|e| e.to_string())?;
            if let Some(info) = payload
                .get("symbols")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .cloned()
            {
                let resolved = info
                    .get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or(symbol)
                    .to_string();
                return Ok(Some((info, resolved)));
            }
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            direct_error = Some(format!("mexc exchangeInfo status={} body={}", status, body));
        }

        if let Some(err) = direct_error.as_ref() {
            debug!("[mexc] exchangeInfo lookup failed for {}: {}", symbol, err);
        }

        let resp = self
            .http
            .get("https://api.mexc.com/api/v3/exchangeInfo")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("mexc exchangeInfo status={} body={}", status, body));
        }

        let payload: Value = resp.json().await.map_err(|e| e.to_string())?;
        let target = normalize_market_symbol(symbol);
        let info = payload
            .get("symbols")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                arr.iter().find(|item| {
                    item.get("symbol")
                        .and_then(|v| v.as_str())
                        .map(|sym| normalize_market_symbol(sym) == target)
                        .unwrap_or(false)
                })
            })
            .cloned();

        Ok(info.map(|value| {
            let resolved = value
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or(symbol)
                .to_string();
            (value, resolved)
        }))
    }

    pub(super) async fn get_symbol_filters(&self, symbol: &str) -> Result<Option<SymbolFilters>, String> {
        let cache = self.symbol_filters.lock().await;
        if let Some(filters) = cache.get(symbol).cloned() {
            return Ok(Some(filters));
        }
        drop(cache);

        let Some((info, resolved_symbol)) = self.fetch_symbol_info(symbol).await? else {
            return Ok(None);
        };
        if resolved_symbol != symbol {
            debug!("[mexc] resolved symbol filters {} -> {}", symbol, resolved_symbol);
        }

        Self::validate_market_order_support(&info, &resolved_symbol)?;

        let mut filters = Self::symbol_filters_from_info(&info);
        filters.resolved_symbol = Some(resolved_symbol.clone());

        let mut cache = self.symbol_filters.lock().await;
        cache.insert(symbol.to_string(), filters.clone());
        if resolved_symbol != symbol {
            cache.insert(resolved_symbol, filters.clone());
        }
        Ok(Some(filters))
    }

    /// One symbol's trading rules from its `exchangeInfo` entry. MEXC sends no
    /// `LOT_SIZE` or `MIN_NOTIONAL` filter, so its top-level fields are the fallback.
    pub(super) fn symbol_filters_from_info(info: &Value) -> SymbolFilters {
        let mut filters = SymbolFilters::default();
        if let Some(entries) = info.get("filters").and_then(|v| v.as_array()) {
            for f in entries {
                let filter_type = f.get("filterType").and_then(|v| v.as_str()).unwrap_or("");
                match filter_type {
                    "LOT_SIZE" => {
                        filters.step_size = parse_decimal(f, "stepSize").or(filters.step_size);
                        filters.min_qty = parse_decimal(f, "minQty").or(filters.min_qty);
                    }
                    "MIN_NOTIONAL" | "NOTIONAL" => {
                        filters.min_notional = parse_decimal(f, "minNotional").or(filters.min_notional);
                    }
                    "PERCENT_PRICE_BY_SIDE" => {
                        filters.buy_price_band = parse_decimal(f, "bidMultiplierUp").or(filters.buy_price_band);
                        filters.sell_price_band = parse_decimal(f, "askMultiplierDown").or(filters.sell_price_band);
                    }
                    _ => {}
                }
            }
        }
        // A "0" limit means unconstrained, so it stays absent.
        filters.min_notional = filters
            .min_notional
            .or_else(|| parse_decimal(info, "quoteAmountPrecision"))
            .filter(|value| !value.is_zero());
        // `baseSizePrecision` is the smallest order, not a step: rounding to it can
        // exceed the venue's own precision (ICP_USDT, liquidation 1620).
        filters.min_qty = filters
            .min_qty
            .or_else(|| parse_decimal(info, "baseSizePrecision"))
            .filter(|value| !value.is_zero());

        filters.quote_precision = parse_u32(info, "quotePrecision")
            .or_else(|| parse_u32(info, "quoteAssetPrecision"))
            .or(filters.quote_precision);
        filters.base_precision = parse_u32(info, "baseAssetPrecision").or(filters.base_precision);
        filters.taker_fee_bps = Self::parse_taker_fee_bps(info);
        filters
    }

    /// An order without a price limit is a MARKET order, so a listed market is
    /// only usable when its metadata advertises them. IOC is accepted unlisted.
    pub(super) fn validate_market_order_support(info: &Value, symbol: &str) -> Result<(), String> {
        if !supports_market_orders(info) {
            return Err(format!("MEXC market {symbol} does not support MARKET orders"));
        }
        Ok(())
    }
}
