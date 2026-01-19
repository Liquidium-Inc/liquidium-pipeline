use std::collections::HashMap;
use std::env;

use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, DepositAddress, WithdrawStatus, WithdrawalReceipt,
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
    let mut candidates = Vec::new();
    let network_norm = network.trim().to_ascii_uppercase();
    if !network_norm.is_empty() {
        candidates.push(network_norm);
    }

    if network.eq_ignore_ascii_case("icp") {
        let asset_norm = asset.trim().to_ascii_uppercase();
        if !asset_norm.is_empty() {
            candidates.push(asset_norm.clone());
            let asset_no_ck = asset_norm.strip_prefix("CK").unwrap_or(&asset_norm);
            let ck_asset = format!("CK{}", asset_no_ck);
            candidates.push(ck_asset);
            candidates.push(asset_no_ck.to_string());
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

fn mexc_withdraw_network(asset: &str, network: &str) -> String {
    if network.eq_ignore_ascii_case("icp") {
        let asset_norm = asset.trim().to_ascii_uppercase();
        if !asset_norm.is_empty() {
            return asset_norm;
        }
    }

    network.to_string()
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
        // place market order using quote_order_quantity
        let side_norm = side.to_ascii_lowercase();
        let amount_dec = Decimal::from_f64(amount_in).ok_or("could not convert amount_in to Decimal")?;
        let filters = self.get_symbol_filters(&market_symbol).await?;
        let api_symbol = filters
            .as_ref()
            .and_then(|f| f.resolved_symbol.as_deref())
            .unwrap_or(&symbol);
        info!("Swapping {} {} {} (symbol={})", market, side, amount_in, api_symbol);
        let (order_side, quantity, quote_order_quantity) = match side_norm.as_str() {
            "sell" => {
                let mut qty = amount_dec;
                if let Some(f) = &filters {
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
                    if let Some(min_qty) = f.min_qty {
                        if qty < min_qty {
                            return Err(format!("quantity {} below min_qty {} for {}", qty, min_qty, symbol));
                        }
                    }
                }
                if qty <= Decimal::ZERO {
                    return Err(format!("quantity {} not valid for {}", qty, symbol));
                }
                (OrderSide::Sell, Some(qty), None)
            }
            "buy" => {
                let mut quote_amt = amount_dec;
                if let Some(f) = &filters {
                    if let Some(precision) = f.quote_precision {
                        quote_amt = quote_amt
                            .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                            .normalize();
                    }
                    if let Some(min_notional) = f.min_notional {
                        if quote_amt < min_notional {
                            return Err(format!(
                                "quote amount {} below min_notional {} for {}",
                                quote_amt, min_notional, symbol
                            ));
                        }
                    }
                }
                (OrderSide::Buy, None, Some(quote_amt))
            }
            _ => return Err(format!("unsupported side: {}", side)),
        };
        let mut candidates = vec![api_symbol.to_string(), market_symbol.clone(), symbol.clone()];
        candidates.sort();
        candidates.dedup();
        let mut last_err: Option<String> = None;
        let mut chosen_symbol = api_symbol;
        let mut res = None;
        for candidate in &candidates {
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
                Ok(ok) => {
                    chosen_symbol = candidate;
                    res = Some(ok);
                    break;
                }
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

        let res = match res {
            Some(res) => res,
            None => return Err(last_err.unwrap_or_else(|| "Swap err: bad symbol".to_string())),
        };

        // fetch final state of the order
        let order_res = ex
            .get_order(GetOrderParams {
                symbol: chosen_symbol,
                order_id: Some(&res.order_id),
                new_client_order_id: None,
                original_client_order_id: None,
            })
            .await
            .map_err(|e| format!("Get_order err: {}", e))?;

        // check it actually executed
        match order_res.status {
            OrderStatus::Filled => {}
            other => {
                return Err(format!("order not executed, status: {:?}", other));
            }
        }

        if order_res.executed_quantity <= Decimal::ZERO {
            return Err("order has zero executed quantity".into());
        }

        let filled = if side_norm == "buy" {
            order_res
                .executed_quantity
                .to_f64()
                .ok_or("cannot convert executed_quantity to f64")?
        } else {
            order_res
                .cummulative_quote_quantity
                .to_f64()
                .ok_or("cannot convert cummulative_quote_quantity to f64")?
        };

        Ok(filled)
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let ex = self.inner.lock().await;

        println!("{:?} {:?}", asset, network);

        let res = ex
            .get_deposit_address(asset.to_string(), None)
            .await
            .map_err(|e| e.to_string())?;

        println!("{:?}", res);

        let candidates = mexc_network_candidates(asset, network);
        let addr = res.iter().find(|item| {
            let item_network = item.network.to_ascii_uppercase();
            candidates.iter().any(|cand| item_network.contains(cand))
        });

        let addr = match addr {
            Some(v) => v,
            None => {
                let available: Vec<String> = res.iter().map(|item| item.network.clone()).collect();
                return Err(format!(
                    "address not found for asset={} network={} candidates={:?} available={:?}",
                    asset, network, candidates, available
                ));
            }
        };

        Ok(DepositAddress {
            asset: asset.to_string(),
            network: addr.network.clone(),
            address: addr.address.clone(),
            tag: addr.memo.clone(),
        })
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

        let asset_upper = asset.to_ascii_uppercase();
        let asset_no_ck = asset_upper.strip_prefix("CK").unwrap_or(&asset_upper);
        let mut candidates = vec![
            asset.to_string(),
            asset_upper.clone(),
            asset_no_ck.to_string(),
            format!("CK{}", asset_no_ck),
        ];
        candidates.sort();
        candidates.dedup();

        let network_mapped = mexc_withdraw_network(asset, network);
        info!(
            "Withdraw request coin={} network={} amount={} address={}",
            asset.to_string(),
            network_mapped,
            amount,
            address
        );

        let mut last_err: Option<String> = None;
        let mut res = None;
        for coin in &candidates {
            match ex
                .withdraw(WithdrawRequest {
                    address: address.to_string(),
                    amount: amount.to_string(),
                    coin: coin.to_string(),
                    memo: None,
                    network: Some(network_mapped.clone()),
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
                    return Err(e.to_string());
                }
            }
        }

        let res = match res {
            Some(res) => res,
            None => return Err(last_err.unwrap_or_else(|| "withdraw failed".to_string())),
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
