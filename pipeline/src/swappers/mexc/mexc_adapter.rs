use std::collections::HashMap;
use std::env;

use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, DepositAddress, OrderBook, OrderBookLevel, WithdrawStatus, WithdrawalReceipt,
};
use log::{debug, info, warn};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::Value;

fn from_mexc_raw(s: &str) -> WithdrawStatus {
    match s {
        // adjust to whatever MEXC actually returns
        "WAIT" | "PENDING" | "PROCESSING" => WithdrawStatus::Pending,
        "SUCCESS" | "FINISHED" | "DONE" => WithdrawStatus::Completed,
        "FAILED" | "FAIL" => WithdrawStatus::Failed,
        "CANCEL" | "CANCELED" => WithdrawStatus::Canceled,
        _ => WithdrawStatus::Unknown,
    }
}

fn normalize_market_symbol(market: &str) -> String {
    market.replace(['/', '_', '-'], "").to_ascii_uppercase()
}

fn format_mexc_api_error(err: &v3::ApiError) -> String {
    match err {
        v3::ApiError::ErrorResponse(resp) => match &resp._extend {
            Some(extra) => format!("code={:?} msg={} extend={}", resp.code, resp.msg, extra),
            None => format!("code={:?} msg={}", resp.code, resp.msg),
        },
        v3::ApiError::ReqwestError(err) => match err.status() {
            Some(status) => format!("status={} err={}", status, err),
            None => format!("err={}", err),
        },
        other => other.to_string(),
    }
}

fn is_bad_symbol(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::BadSymbol
    )
}

fn is_coin_missing(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::CurrencyDoesNotExist
    )
}

#[derive(Debug, Clone, Default)]
struct SymbolFilters {
    step_size: Option<Decimal>,
    min_qty: Option<Decimal>,
    min_notional: Option<Decimal>,
    quote_precision: Option<u32>,
    base_precision: Option<u32>,
    resolved_symbol: Option<String>,
}

fn truncate_to_step(value: Decimal, step: Decimal) -> Decimal {
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

fn parse_u32(v: &Value, key: &str) -> Option<u32> {
    v.get(key).and_then(|raw| raw.as_u64()).map(|v| v as u32)
}

fn mexc_network_candidates(asset: &str, network: &str) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    let mut push_unique = |value: String| {
        if !candidates.iter().any(|item| item.eq_ignore_ascii_case(&value)) {
            candidates.push(value);
        }
    };

    let network_norm = network.trim().to_ascii_uppercase();
    let asset_norm = asset.trim().to_ascii_uppercase();
    if asset_norm.is_empty() {
        if !network_norm.is_empty() {
            push_unique(network_norm);
        }
        return candidates;
    }

    // Special-case: native ICP asset on ICP network should just be "ICP".
    if asset_norm == "ICP" && network.eq_ignore_ascii_case("icp") {
        push_unique("ICP".to_string());
        return candidates;
    }

    let asset_no_ck = asset_norm.strip_prefix("CK").unwrap_or(&asset_norm);
    let ck_asset = format!("CK{}", asset_no_ck);

    // For ck-assets, prefer CK network names only (avoid leaking base symbol like BTC/USDT as a network).
    if asset_norm.starts_with("CK") {
        push_unique(ck_asset.clone());
    }

    // Always include the explicit network if provided.
    if !network_norm.is_empty() {
        push_unique(network_norm.clone());
    }

    // If requested network is ICP, include ICP and CK-asset network names.
    if network.eq_ignore_ascii_case("icp") {
        push_unique("ICP".to_string());
        push_unique(ck_asset);
    }

    candidates
}

fn mexc_withdraw_network(asset: &str, network: &str) -> String {
    let asset_norm = asset.trim().to_ascii_uppercase();
    if asset_norm.starts_with("CK") && !asset_norm.is_empty() {
        return asset_norm;
    }

    if network.eq_ignore_ascii_case("icp") {
        let asset_norm = asset.trim().to_ascii_uppercase();
        if !asset_norm.is_empty() {
            return asset_norm;
        }
    }

    network.to_string()
}

fn mexc_deposit_asset_candidates(asset: &str) -> Vec<String> {
    let asset_trimmed = asset.trim();
    if asset_trimmed.is_empty() {
        return vec![];
    }

    let asset_upper = asset_trimmed.to_ascii_uppercase();
    let has_ck_prefix = asset_upper.starts_with("CK");
    let asset_no_ck = asset_upper.strip_prefix("CK").unwrap_or(&asset_upper);

    let mut candidates = Vec::new();

    if has_ck_prefix {
        // only ck variants
        candidates.push(format!("CK{}", asset_no_ck));
        candidates.push(format!("ck{}", asset_no_ck));
    } else {
        // normal asset + ck variants
        candidates.push(asset_trimmed.to_string());
        candidates.push(asset_upper.clone());
        candidates.push(format!("CK{}", asset_upper));
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

use mexc_rs::spot::{
    MexcSpotApiClientWithAuthentication,
    v3::{
        self,
        account_information::{AccountBalance, AccountInformationEndpoint},
        deposit_address::DepositAddressEndpoint,
        depth::{DepthEndpoint, DepthParams},
        enums::{OrderSide, OrderStatus},
        get_order::{GetOrderEndpoint, GetOrderParams},
        order::{OrderEndpoint, OrderParams},
        withdraw::{WithdrawEndpoint, WithdrawHistoryRequest, WithdrawRequest},
    },
};

use num_traits::{FromPrimitive, ToPrimitive};

pub struct MexcClient {
    inner: tokio::sync::Mutex<MexcSpotApiClientWithAuthentication>,
    symbol_filters: tokio::sync::Mutex<HashMap<String, SymbolFilters>>,
    http: reqwest::Client,
}

impl MexcClient {
    pub fn new(api_key: &str, secret: &str) -> Self {
        let api = MexcSpotApiClientWithAuthentication::new(
            mexc_rs::spot::MexcSpotApiEndpoint::Base,
            api_key.to_string(),
            secret.to_string(),
        );
        Self {
            inner: tokio::sync::Mutex::new(api),
            symbol_filters: tokio::sync::Mutex::new(HashMap::new()),
            http: reqwest::Client::new(),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let api_key = env::var("CEX_MEXC_API_KEY").map_err(|_| "CEX_MEXC_API_KEY not set".to_string())?;
        let api_secret = env::var("CEX_MEXC_API_SECRET").map_err(|_| "CEX_MEXC_API_SECRET not set".to_string())?;

        Ok(Self::new(&api_key, &api_secret))
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

    async fn get_symbol_filters(&self, symbol: &str) -> Result<Option<SymbolFilters>, String> {
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
        filters.quote_precision = parse_u32(&info, "quotePrecision")
            .or_else(|| parse_u32(&info, "quoteAssetPrecision"))
            .or(filters.quote_precision);
        filters.base_precision = parse_u32(&info, "baseAssetPrecision").or(filters.base_precision);
        filters.resolved_symbol = Some(resolved_symbol.clone());

        let mut cache = self.symbol_filters.lock().await;
        cache.insert(symbol.to_string(), filters.clone());
        if resolved_symbol != symbol {
            cache.insert(resolved_symbol, filters.clone());
        }
        Ok(Some(filters))
    }
}

impl MexcClient {
    // Round quote amount to exchange precision and signal whether quote-based buys are viable.
    fn adjust_quote_amount(amount_dec: Decimal, filters: Option<&SymbolFilters>) -> (Decimal, bool) {
        let mut quote_amt = amount_dec;
        let mut use_quote_order = true;

        if let Some(f) = filters {
            if let Some(precision) = f.quote_precision {
                quote_amt = quote_amt
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }
            if quote_amt <= Decimal::ZERO {
                use_quote_order = false;
            }
        } else if quote_amt <= Decimal::ZERO {
            use_quote_order = false;
        }

        (quote_amt, use_quote_order)
    }

    // Enforce notional minimums against the intended spend amount.
    fn ensure_min_notional(filters: Option<&SymbolFilters>, amount: Decimal, symbol: &str) -> Result<(), String> {
        if let Some(f) = filters
            && let Some(min_notional) = f.min_notional
            && amount < min_notional
        {
            return Err(format!(
                "quote amount {} below min_notional {} for {}",
                amount, min_notional, symbol
            ));
        }
        Ok(())
    }

    // Estimate base output from orderbook for a quote-denominated buy.
    async fn estimate_buy_quantity(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        api_symbol: &str,
        quote_amount: Decimal,
    ) -> Result<Decimal, String> {
        let ob = ex
            .depth(DepthParams {
                limit: Some(50),
                symbol: api_symbol,
            })
            .await
            .map_err(|e| e.to_string())?;

        if ob.asks.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining_quote = quote_amount;
        let mut base_out = Decimal::ZERO;
        for level in &ob.asks {
            if remaining_quote <= Decimal::ZERO {
                break;
            }
            if level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO {
                continue;
            }
            let max_base = remaining_quote / level.price;
            let take = level.quantity.min(max_base);
            base_out += take;
            remaining_quote -= take * level.price;
        }

        if remaining_quote > Decimal::ZERO {
            return Err("not enough ask liquidity".into());
        }

        Ok(base_out)
    }

    // Apply step size/base precision and min_qty checks to a computed base amount.
    fn adjust_buy_quantity(qty: Decimal, filters: Option<&SymbolFilters>, symbol: &str) -> Result<Decimal, String> {
        let mut adjusted = qty;
        if let Some(f) = filters {
            if let Some(step) = f.step_size {
                let step_scale = step.scale();
                adjusted = truncate_to_step(adjusted, step)
                    .round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
                    .normalize();
            } else if let Some(precision) = f.base_precision {
                adjusted = adjusted
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }
            if let Some(min_qty) = f.min_qty
                && adjusted < min_qty
            {
                return Err(format!(
                    "quantity {} below min_qty {} for {}",
                    adjusted, min_qty, symbol
                ));
            }
        }

        if adjusted <= Decimal::ZERO {
            return Err(format!("quantity {} not valid for {}", adjusted, symbol));
        }

        Ok(adjusted)
    }

    fn candidate_symbols(api_symbol: &str, market_symbol: &str, symbol: &str) -> Vec<String> {
        let mut candidates = vec![api_symbol.to_string(), market_symbol.to_string(), symbol.to_string()];
        candidates.sort();
        candidates.dedup();
        candidates
    }

    fn prepare_sell_order(
        amount_dec: Decimal,
        filters: Option<&SymbolFilters>,
        symbol: &str,
    ) -> Result<(OrderSide, Option<Decimal>, Option<Decimal>), String> {
        let mut qty = amount_dec;
        if let Some(f) = filters {
            if let Some(step) = f.step_size {
                let step_scale = step.scale();
                let adjusted = truncate_to_step(qty, step)
                    .round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
                    .normalize();

                if adjusted != qty {
                    debug!(
                        "[mexc] adjust sell qty {} -> {} using step_size={}",
                        qty, adjusted, step
                    );
                }
                qty = adjusted;
            } else if let Some(precision) = f.base_precision {
                qty = qty
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }

            if let Some(min_qty) = f.min_qty
                && qty < min_qty
            {
                return Err(format!("quantity {} below min_qty {} for {}", qty, min_qty, symbol));
            }
        }

        if qty <= Decimal::ZERO {
            return Err(format!("quantity {} not valid for {}", qty, symbol));
        }

        Ok((OrderSide::Sell, Some(qty), None))
    }

    async fn submit_market_order(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        candidates: &[String],
        order_side: OrderSide,
        quantity: Option<Decimal>,
        quote_order_quantity: Option<Decimal>,
        market: &str,
        side: &str,
        amount_in: f64,
    ) -> Result<(String, String), String> {
        let mut last_err: Option<String> = None;
        for candidate in candidates {
            info!("Swapping {} {} {} (symbol={})", market, side, amount_in, candidate);
            match ex
                .order(OrderParams {
                    symbol: candidate,
                    side: order_side,
                    order_type: v3::enums::OrderType::Market,
                    quantity,
                    new_client_order_id: None,
                    price: None,
                    quote_order_quantity,
                })
                .await
            {
                Ok(ok) => return Ok((candidate.clone(), ok.order_id.clone())),
                Err(e) => {
                    let details = format_mexc_api_error(&e);
                    warn!("[mexc] order error response: {}", details);
                    if is_bad_symbol(&e) {
                        last_err = Some(format!("Swap err: {}", details));
                        continue;
                    }
                    return Err(format!("Swap err: {}", details));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "Swap err: bad symbol".to_string()))
    }

    async fn fetch_filled_amount(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        symbol: &str,
        order_id: &str,
        side_norm: &str,
    ) -> Result<f64, String> {
        let order_res = ex
            .get_order(GetOrderParams {
                symbol,
                order_id: Some(order_id),
                new_client_order_id: None,
                original_client_order_id: None,
            })
            .await
            .map_err(|e| format!("Get_order err: {}", e))?;

        match order_res.status {
            OrderStatus::Filled => {}
            other => {
                return Err(format!("order not executed, status: {:?}", other));
            }
        }

        if order_res.executed_quantity <= Decimal::ZERO {
            return Err("order has zero executed quantity".into());
        }

        if side_norm == "buy" {
            return order_res
                .executed_quantity
                .to_f64()
                .ok_or("cannot convert executed_quantity to f64".to_string());
        }

        order_res
            .cummulative_quote_quantity
            .to_f64()
            .ok_or("cannot convert cummulative_quote_quantity to f64".to_string())
    }

    async fn prepare_buy_order(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        amount_dec: Decimal,
        filters: Option<&SymbolFilters>,
        api_symbol: &str,
        symbol: &str,
    ) -> Result<(OrderSide, Option<Decimal>, Option<Decimal>), String> {
        if amount_dec <= Decimal::ZERO {
            return Err(format!("quote amount {} not valid for {}", amount_dec, symbol));
        }

        // If quote rounds to zero at exchange precision, fall back to a base-quantity buy.
        let (quote_amt, use_quote_order) = Self::adjust_quote_amount(amount_dec, filters);
        let check_amt = if use_quote_order { quote_amt } else { amount_dec };
        Self::ensure_min_notional(filters, check_amt, symbol)?;

        if !use_quote_order {
            let base_out = self.estimate_buy_quantity(ex, api_symbol, amount_dec).await?;
            let qty = Self::adjust_buy_quantity(base_out, filters, symbol)?;
            return Ok((OrderSide::Buy, Some(qty), None));
        }

        if quote_amt <= Decimal::ZERO {
            return Err(format!("quote amount {} not valid for {}", quote_amt, symbol));
        }

        Ok((OrderSide::Buy, None, Some(quote_amt)))
    }
}

#[async_trait]
impl CexBackend for MexcClient {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams {
                limit: None,
                symbol: &symbol,
            })
            .await
            .map_err(|e| e.to_string())?;

        // asks: Vec<Vec<f64>> = [ [price, qty], ... ]
        let asks = &ob.asks;

        if asks.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining = Decimal::from_f64_retain(amount_in).expect("could not convert f64");
        let mut cost = Decimal::ZERO;

        for level in asks {
            let price = level.price;
            let qty = level.quantity;

            if remaining <= Decimal::ZERO {
                break;
            }

            let take = qty.min(remaining);
            cost += take * price;
            remaining -= take;
        }

        if remaining > Decimal::ZERO {
            return Err("not enough liquidity".into());
        }

        cost.to_f64().ok_or("f64 conversion failed".to_string())
    }

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let market_symbol = market.trim().to_ascii_uppercase();
        let symbol = normalize_market_symbol(&market_symbol);
        // Determine order params (side, quantity vs quote quantity) using filters.
        let side_norm = side.to_ascii_lowercase();
        let amount_dec = Decimal::from_f64(amount_in).ok_or("could not convert amount_in to Decimal")?;
        let filters = self.get_symbol_filters(&market_symbol).await?;
        let api_symbol = filters
            .as_ref()
            .and_then(|f| f.resolved_symbol.as_deref())
            .unwrap_or(&symbol);

        info!("Swapping {} {} {} (symbol={})", market, side, amount_in, api_symbol);

        let (order_side, quantity, quote_order_quantity) = match side_norm.as_str() {
            "sell" => Self::prepare_sell_order(amount_dec, filters.as_ref(), symbol.as_str())?,
            "buy" => {
                self.prepare_buy_order(&ex, amount_dec, filters.as_ref(), api_symbol, symbol.as_str())
                    .await?
            }
            _ => return Err(format!("unsupported side: {}", side)),
        };

        // Try multiple candidate symbols for MEXC quirks, then fetch the filled amount.
        let candidates = Self::candidate_symbols(api_symbol, &market_symbol, &symbol);
        let (chosen_symbol, order_id) = self
            .submit_market_order(
                &ex,
                &candidates,
                order_side,
                quantity,
                quote_order_quantity,
                market,
                side,
                amount_in,
            )
            .await?;

        self.fetch_filled_amount(&ex, &chosen_symbol, &order_id, &side_norm)
            .await
    }

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let ex = self.inner.lock().await;
        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams { limit, symbol: &symbol })
            .await
            .map_err(|e| e.to_string())?;

        let bids = ob
            .bids
            .iter()
            .map(|level| {
                let price = level.price.to_f64().ok_or("orderbook bid price to f64 failed")?;
                let quantity = level.quantity.to_f64().ok_or("orderbook bid qty to f64 failed")?;
                Ok(OrderBookLevel { price, quantity })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let asks = ob
            .asks
            .iter()
            .map(|level| {
                let price = level.price.to_f64().ok_or("orderbook ask price to f64 failed")?;
                let quantity = level.quantity.to_f64().ok_or("orderbook ask qty to f64 failed")?;
                Ok(OrderBookLevel { price, quantity })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(OrderBook { bids, asks })
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let ex = self.inner.lock().await;

        let candidates = mexc_network_candidates(asset, network);
        let asset_candidates = mexc_deposit_asset_candidates(asset);
        let mut last_err: Option<String> = None;
        let mut last_available: Option<Vec<String>> = None;

        let mut network_attempts: Vec<Option<String>> = candidates.iter().cloned().map(Some).collect();
        network_attempts.push(None);

        dbg!(&network_attempts, &asset_candidates);
        for coin in &asset_candidates {
            for net in &network_attempts {
                let res = match ex.get_deposit_address(coin.to_string(), net.as_deref()).await {
                    Ok(res) => res,
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                };

                if res.is_empty() {
                    last_err = Some(format!(
                        "no deposit addresses returned for coin={} network={:?}",
                        coin, net
                    ));
                    continue;
                }

                let addr = res.iter().find(|item| {
                    let item_network = item.network.to_ascii_uppercase();
                    candidates.iter().any(|cand| item_network.contains(cand))
                });

                if let Some(v) = addr {
                    return Ok(DepositAddress {
                        asset: asset.to_string(),
                        network: v.network.clone(),
                        address: v.address.clone(),
                        tag: v.memo.clone(),
                    });
                }

                last_available = Some(res.iter().map(|item| item.network.clone()).collect());
                last_err = Some(format!(
                    "address not found for coin={} network={:?} candidates={:?} available={:?}",
                    coin, net, candidates, last_available
                ));
            }
        }

        Err(format!(
            "address not found for asset={} network={} candidates={:?} asset_candidates={:?} available={:?} err={}",
            asset,
            network,
            candidates,
            asset_candidates,
            last_available.unwrap_or_default(),
            last_err.unwrap_or_else(|| "no deposit address candidates matched".to_string())
        ))
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let res = ex.account_information().await.map_err(|e| e.to_string())?;

        let asset_norm = asset.to_ascii_uppercase();
        let balance = match res
            .balances
            .iter()
            .find(|item| item.asset.to_ascii_uppercase() == asset_norm)
            .cloned()
        {
            Some(b) => b,
            None => {
                debug!(
                    "[mexc] balance not found for asset={}, available={:?}",
                    asset,
                    res.balances.iter().map(|item| item.asset.clone()).collect::<Vec<_>>()
                );
                AccountBalance {
                    asset: asset.to_string(),
                    free: Decimal::ZERO,
                    locked: Decimal::ZERO,
                }
            }
        };

        let var_name = "could not convert balance to f64";
        Ok(balance.free.to_f64().expect(var_name))
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String> {
        let ex = self.inner.lock().await;
        let push_unique = |list: &mut Vec<String>, value: String| {
            if !list.iter().any(|item| item.eq_ignore_ascii_case(&value)) {
                list.push(value);
            }
        };

        let asset_upper = asset.to_ascii_uppercase();
        let asset_no_ck = asset_upper.strip_prefix("CK").unwrap_or(&asset_upper);
        let mut candidates = Vec::new();
        // Prefer native symbol and its CK-prefixed form first.
        push_unique(&mut candidates, asset_upper.clone());
        push_unique(&mut candidates, format!("CK{}", asset_no_ck));
        push_unique(&mut candidates, asset_no_ck.to_string());
        push_unique(&mut candidates, asset.to_string());

        let mut network_candidates = mexc_network_candidates(asset, network);
        let network_mapped = mexc_withdraw_network(asset, network);
        let mut ordered_networks = Vec::new();
        push_unique(&mut ordered_networks, network_mapped.clone());
        for cand in network_candidates.drain(..) {
            push_unique(&mut ordered_networks, cand);
        }
        if ordered_networks.is_empty() {
            push_unique(&mut ordered_networks, network_mapped.clone());
        }
        let network_candidates = ordered_networks;

        info!(
            "Withdraw request coin={} network_candidates={:?} amount={} address={}",
            asset, network_candidates, amount, address
        );

        let mut last_err: Option<String> = None;
        let mut hard_err: Option<String> = None;
        let mut res = None;
        for coin in &candidates {
            for net in &network_candidates {
                match ex
                    .withdraw(WithdrawRequest {
                        address: address.to_string(),
                        amount: amount.to_string(),
                        coin: coin.to_string(),
                        memo: None,
                        network: Some(net.clone()),
                        remark: None,
                        withdraw_order_id: None,
                    })
                    .await
                {
                    Ok(ok) => {
                        res = Some(ok);
                        break;
                    }
                    Err(e) => {
                        if is_coin_missing(&e) {
                            last_err = Some(e.to_string());
                            continue;
                        }
                        last_err = Some(e.to_string());
                        if hard_err.is_none() {
                            hard_err = last_err.clone();
                        }
                    }
                }
            }
            if res.is_some() {
                break;
            }
        }

        let res = match res {
            Some(res) => res,
            None => return Err(hard_err.or(last_err).unwrap_or_else(|| "withdraw failed".to_string())),
        };

        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network_mapped,
            amount,
            txid: None,
            internal_id: Some(res.id),
        })
    }

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        let ex = self.inner.lock().await;
        let records = ex
            .withdraw_history(WithdrawHistoryRequest {
                coin: Some(coin.to_string()),
                status: None,
                limit: Some(50),
                start_time: None,
                end_time: None,
            })
            .await
            .map_err(|e| e.to_string())?;

        let rec = records
            .into_iter()
            .find(|r| r.id == withdraw_id || r.withdraw_order_id.as_deref() == Some(withdraw_id));

        let rec = match rec {
            Some(r) => r,
            None => return Ok(WithdrawStatus::Unknown),
        };

        let status = from_mexc_raw(rec.status.as_str());
        Ok(status)
    }
}
