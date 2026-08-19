//! What MEXC will accept for a symbol: its order-size limits, precisions and
//! taker fee, read from `exchangeInfo` and cached per client.
//!
//! MEXC does not publish the standard `LOT_SIZE` / `MIN_NOTIONAL` filters;
//! the limits live in top-level fields with their own quirks, and the fee has
//! a fallback when the venue reports none. Every such rule is here.

use super::*;

/// MEXC spot fee assumption used to convert gross fills into net-usable output.
pub(super) const MEXC_SPOT_FEE_BPS: f64 = 5.01;
/// Tiny safety increment to avoid borderline balance/rounding rejections between legs.
const ROUND_HANDLER: f64 = 0.01;
/// Hard cap for fee inputs; values above this are treated as invalid.
const MAX_TAKER_FEE_BPS: f64 = 10_000.0;

#[derive(Debug, Clone, Default)]
pub(super) struct SymbolFilters {
    pub(super) step_size: Option<Decimal>,
    pub(super) min_qty: Option<Decimal>,
    pub(super) min_notional: Option<Decimal>,
    pub(super) quote_precision: Option<u32>,
    pub(super) base_precision: Option<u32>,
    pub(super) taker_fee_bps: Option<f64>,
    pub(super) resolved_symbol: Option<String>,
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

impl MexcClient {
    fn parse_taker_fee_bps(info: &Value) -> Option<f64> {
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
        let resp = self.http.get(url).send().await.map_err(|e| http_error_message(&e))?;
        if resp.status().is_success() {
            let payload: Value = resp.json().await.map_err(|e| http_error_message(&e))?;
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
            .map_err(|e| http_error_message(&e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("mexc exchangeInfo status={} body={}", status, body));
        }

        let payload: Value = resp.json().await.map_err(|e| http_error_message(&e))?;
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

        let mut filters = Self::symbol_filters_from_info(&info);
        filters.resolved_symbol = Some(resolved_symbol.clone());

        let mut cache = self.symbol_filters.lock().await;
        cache.insert(symbol.to_string(), filters.clone());
        if resolved_symbol != symbol {
            cache.insert(resolved_symbol, filters.clone());
        }
        Ok(Some(filters))
    }

    /// Reads one symbol's order-size limits out of an `exchangeInfo` entry.
    ///
    /// MEXC publishes only `PERCENT_PRICE_BY_SIDE` filters: it emits neither
    /// `LOT_SIZE` nor `MIN_NOTIONAL`, and carries the equivalent limits as the
    /// top-level `baseSizePrecision` and `quoteAmountPrecision` instead. Read
    /// from the filter array alone every limit here parses as `None`, which
    /// leaves the order-size guards permanently inert and defers every
    /// too-small order to a venue rejection that classifies as retryable.
    ///
    /// The filter arm is kept ahead of the fallbacks so a venue that does send
    /// the standard filters keeps deciding for itself.
    fn symbol_filters_from_info(info: &Value) -> SymbolFilters {
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
                    _ => {}
                }
            }
        }

        // Several live pairs report exactly "0" here. That means unconstrained,
        // so it has to stay absent: a `Some(0)` bound reads like an enforced
        // limit while comparing as a no-op.
        filters.min_notional = filters
            .min_notional
            .or_else(|| parse_decimal(info, "quoteAmountPrecision"))
            .filter(|value| !value.is_zero());
        // `baseSizePrecision` is the smallest order MEXC accepts, not a step
        // size. Rounding to it can exceed the venue's own quantity precision
        // (ICP_USDT: 0.0001 minimum, `baseAssetPrecision` 2), which MEXC rejects
        // as "quantity scale is invalid". Decimal places come from
        // `baseAssetPrecision`; a real `LOT_SIZE` filter still wins.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shape copied from a live exchangeInfo response for BTCUSDT: the only
    // filter MEXC emits is PERCENT_PRICE_BY_SIDE, so the order-size limits have
    // to come from the top-level fields or they are never read at all.
    #[test]
    fn symbol_limits_are_read_from_the_fields_mexc_actually_publishes() {
        let info = serde_json::json!({
            "symbol": "BTCUSDT",
            "baseAssetPrecision": 8,
            "quotePrecision": 2,
            "quoteAmountPrecision": "1",
            "baseSizePrecision": "0.000001",
            "filters": [{
                "filterType": "PERCENT_PRICE_BY_SIDE",
                "bidMultiplierUp": "0.005",
                "askMultiplierDown": "0.005"
            }]
        });

        let filters = MexcClient::symbol_filters_from_info(&info);

        assert_eq!(filters.min_notional, Decimal::from_str_exact("1").ok());
        // A minimum order size, not a step: MEXC sent no LOT_SIZE filter.
        assert_eq!(filters.min_qty, Decimal::from_str_exact("0.000001").ok());
        assert_eq!(filters.step_size, None);
        assert_eq!(filters.base_precision, Some(8));
    }

    #[test]
    fn a_minimum_order_size_finer_than_the_venue_precision_does_not_set_the_scale() {
        let info = serde_json::json!({
            "symbol": "ICPUSDT",
            "baseAssetPrecision": 2,
            "quotePrecision": 3,
            "quoteAmountPrecision": "1",
            "baseSizePrecision": "0.0001",
            "filters": [{
                "filterType": "PERCENT_PRICE_BY_SIDE",
                "bidMultiplierUp": "0.2",
                "askMultiplierDown": "0.2"
            }]
        });
        let filters = MexcClient::symbol_filters_from_info(&info);
        assert_eq!(filters.step_size, None);
        assert_eq!(filters.min_qty, Decimal::from_str_exact("0.0001").ok());
        assert_eq!(filters.base_precision, Some(2));

        // The exact quantity liquidation 1620 could not submit.
        let (_, qty, _) = MexcClient::prepare_sell_order(
            Decimal::from_str_exact("7.8073757").expect("test amount"),
            Some(&filters),
            "ICP_USDT",
        )
        .expect("a sell within the venue's precision");
        assert_eq!(qty, Decimal::from_str_exact("7.8").ok());

        // A real LOT_SIZE filter still decides where one is sent.
        let mut with_lot_size = filters.clone();
        with_lot_size.step_size = Decimal::from_str_exact("0.001").ok();
        let (_, qty, _) = MexcClient::prepare_sell_order(
            Decimal::from_str_exact("7.8073757").expect("test amount"),
            Some(&with_lot_size),
            "ICP_USDT",
        )
        .expect("a sell on the venue's own step");
        assert_eq!(qty, Decimal::from_str_exact("7.807").ok());
    }

    // CKBTC_BTC and other configured pairs report baseSizePrecision "0", which
    // means unconstrained. Recording it as a limit would look enforced while
    // comparing as a no-op.
    #[test]
    fn a_zero_limit_is_absent_rather_than_an_enforced_floor() {
        let info = serde_json::json!({
            "symbol": "CKBTCBTC",
            "quoteAmountPrecision": "0.000005",
            "baseSizePrecision": "0",
            "filters": []
        });

        let filters = MexcClient::symbol_filters_from_info(&info);

        assert_eq!(filters.min_notional, Decimal::from_str_exact("0.000005").ok());
        assert_eq!(filters.step_size, None);
        assert_eq!(filters.min_qty, None);
    }

    // The fallbacks must not override a venue that does send standard filters.
    #[test]
    fn an_explicit_filter_outranks_the_top_level_fallback() {
        let info = serde_json::json!({
            "symbol": "BTCUSDT",
            "quoteAmountPrecision": "1",
            "baseSizePrecision": "0.000001",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.05"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "10"}
            ]
        });

        let filters = MexcClient::symbol_filters_from_info(&info);

        assert_eq!(filters.min_notional, Decimal::from_str_exact("10").ok());
        assert_eq!(filters.step_size, Decimal::from_str_exact("0.01").ok());
        assert_eq!(filters.min_qty, Decimal::from_str_exact("0.05").ok());
    }

    // exchangeInfo returns takerCommission as ratio; we convert ratio -> bps.
    #[test]
    fn parse_taker_fee_bps_converts_ratio_to_bps() {
        let info = serde_json::json!({
            "takerCommission": "0.003"
        });
        let bps = MexcClient::parse_taker_fee_bps(&info).expect("bps");
        assert!((bps - 30.0).abs() < 1e-12);
    }

    // Guard against mis-scaled fee payloads that would otherwise imply impossible fee rates.
    #[test]
    fn parse_taker_fee_bps_rejects_values_above_10000() {
        // `takerCommission` is interpreted as a ratio; "2.0" means 200%.
        // Converted to bps this is 20_000 bps, above MAX_TAKER_FEE_BPS (10_000),
        // so parsing must fail and the caller should use fallback fee logic instead.
        let info = serde_json::json!({
            "takerCommission": "2.0"
        });
        let bps = MexcClient::parse_taker_fee_bps(&info);
        assert!(bps.is_none());
    }

    // Missing per-symbol fee must fall back to default fee plus round handler.
    #[test]
    fn resolve_taker_fee_bps_falls_back_to_default_when_missing() {
        let filters = SymbolFilters::default();
        let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
        assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
        assert!(fallback);
    }

    // Valid symbol fee should be used and padded by round handler.
    #[test]
    fn resolve_taker_fee_bps_adds_round_handler_to_symbol_fee() {
        let filters = SymbolFilters {
            taker_fee_bps: Some(5.0),
            ..SymbolFilters::default()
        };
        let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
        assert!((bps - 5.01).abs() < 1e-12);
        assert!(!fallback);
    }

    // Out-of-range symbol fees must be discarded and replaced with default fallback.
    #[test]
    fn resolve_taker_fee_bps_falls_back_for_symbol_fee_above_10000() {
        let filters = SymbolFilters {
            taker_fee_bps: Some(10_000.01),
            ..SymbolFilters::default()
        };
        let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
        assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
        assert!(fallback);
    }

    // Even boundary values should fallback when adding round handler would exceed hard cap.
    #[test]
    fn resolve_taker_fee_bps_falls_back_when_round_handler_pushes_over_10000() {
        let filters = SymbolFilters {
            taker_fee_bps: Some(10_000.0),
            ..SymbolFilters::default()
        };
        let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
        assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
        assert!(fallback);
    }
}
