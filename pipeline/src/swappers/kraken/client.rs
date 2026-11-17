//! [`KrakenClient`]: Kraken behind the venue-neutral [`CexBackend`]. Orders
//! live in `trading.rs` and deposits and withdrawals in `funding.rs`; this file
//! holds the market allowlist and the cached metadata reads they share.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, CexBackendError, DepositAddress, FundingRoute, FundingRouteError, OrderBook, SwapExecutionOptions,
    SwapFillReport, WithdrawError, WithdrawStatusSnapshot, WithdrawTerms, WithdrawalReceipt,
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use super::api::{
    KrakenApi, KrakenApiError, KrakenBook, KrakenDepositAddress, KrakenFundingMethod, KrakenPair,
    KrakenWithdrawalAddress,
};
use super::cache::{MetadataCache, PairsByMarket};
use super::rest::KrakenRestApi;

pub struct KrakenClient {
    pub(super) api: Arc<dyn KrakenApi>,
    cache: MetadataCache,
    allowed_pairs: Vec<String>,
}

impl KrakenClient {
    /// A client that trades only the markets in `allowed_pairs`. An empty list
    /// is refused, so a missing setting never means "every market".
    pub fn new(api_key: String, api_secret: String, allowed_pairs: Vec<String>) -> Result<Self, String> {
        Self::with_api(Arc::new(KrakenRestApi::new(api_key, api_secret)), allowed_pairs)
    }

    pub(super) fn with_api(api: Arc<dyn KrakenApi>, allowed_pairs: Vec<String>) -> Result<Self, String> {
        let allowed_pairs = allowed_pairs
            .iter()
            .map(|pair| normalize_market(pair))
            .filter(|pair| !pair.is_empty())
            .collect::<Vec<_>>();
        if allowed_pairs.is_empty() {
            return Err(
                "Kraken needs an explicit market allowlist; an empty one must not mean every market".to_string(),
            );
        }
        Ok(Self {
            api,
            cache: MetadataCache::default(),
            allowed_pairs,
        })
    }

    /// The allowed markets as `BASE_QUOTE`, with Kraken's own spellings resolved.
    pub fn allowed_pairs(&self) -> &[String] {
        &self.allowed_pairs
    }

    /// Kraken's pairs for the allowed markets only, so a lookup copies no
    /// part of the full pair list.
    async fn pairs_by_market(&self) -> Result<PairsByMarket, String> {
        self.cache
            .pairs
            .get_or_load((), async || {
                let mut by_market: HashMap<String, Vec<KrakenPair>> = HashMap::new();
                for pair in self.api.pairs().await? {
                    let market = format!("{}_{}", pair.base, pair.quote);
                    if self.allowed_pairs.contains(&market) {
                        by_market.entry(market).or_default().push(pair);
                    }
                }
                Ok(Arc::new(by_market))
            })
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn deposit_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        self.cache
            .deposit_methods
            .get_or_load(asset.to_string(), async || self.api.deposit_methods(asset).await)
            .await
    }

    pub(super) async fn deposit_addresses(
        &self,
        asset: &str,
        method: &str,
    ) -> Result<Vec<KrakenDepositAddress>, KrakenApiError> {
        self.cache
            .deposit_addresses
            .get_or_load((asset.to_string(), method.to_string()), async || {
                self.api.deposit_addresses(asset, method).await
            })
            .await
    }

    pub(super) async fn withdrawal_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        self.cache
            .withdrawal_methods
            .get_or_load(asset.to_string(), async || self.api.withdrawal_methods(asset).await)
            .await
    }

    pub(super) async fn withdrawal_addresses(
        &self,
        asset: &str,
    ) -> Result<Vec<KrakenWithdrawalAddress>, KrakenApiError> {
        self.cache
            .withdrawal_addresses
            .get_or_load(asset.to_string(), async || self.api.withdrawal_addresses(asset).await)
            .await
    }

    /// The one online Kraken pair for an allowed market. A cached pair that is
    /// not online is read again, so a short incident does not outlast the cache.
    pub(super) async fn pair(&self, market: &str) -> Result<KrakenPair, String> {
        let market = normalize_market(market);
        if !self.allowed_pairs.contains(&market) {
            return Err(format!("Kraken pair {market} is not allowed"));
        }
        let mut pair = self.listed_pair(&market).await?;
        if !pair.online {
            self.cache.pairs.forget(&());
            pair = self.listed_pair(&market).await?;
        }
        if !pair.online {
            return Err(format!("Kraken pair {market} is not online"));
        }
        Ok(pair)
    }

    async fn listed_pair(&self, market: &str) -> Result<KrakenPair, String> {
        let candidates = self.pairs_by_market().await?.get(market).cloned().unwrap_or_default();
        exactly_one(
            candidates,
            || format!("Kraken pair {market} is unavailable"),
            || format!("Kraken pair {market} is ambiguous"),
        )
    }

    pub(super) async fn book(&self, pair: &KrakenPair, depth: u32) -> Result<KrakenBook, String> {
        self.api
            .orderbook(&pair.api_name, depth)
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl CexBackend for KrakenClient {
    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexBackendError> {
        self.execute_order(market, side, amount_in, options).await
    }

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        self.read_orderbook(market, limit).await
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        self.deposit_address(asset, network).await
    }

    /// Kraken keeps no caller id for a withdrawal, so `_withdraw_order_id`
    /// finds nothing later. A lost reply parks the send for an operator.
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
        _withdraw_order_id: &str,
    ) -> Result<WithdrawalReceipt, WithdrawError> {
        self.submit_withdrawal(asset, network, address, amount).await
    }

    async fn get_withdraw_terms(&self, asset: &str, network: &str) -> Result<WithdrawTerms, String> {
        self.withdraw_terms(asset, network).await
    }

    async fn check_funding_route(&self, route: &FundingRoute) -> Result<(), FundingRouteError> {
        self.check_route(route).await
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        let balance = self
            .api
            .balance(&api_asset(asset))
            .await
            .map_err(|error| error.to_string())?;
        decimal_f64(balance, "balance")
    }

    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        self.withdrawal_status(coin, withdraw_id).await
    }
}

/// `BASE_QUOTE` in upper case, each symbol by its common name (XBT is BTC).
pub(super) fn normalize_market(market: &str) -> String {
    market
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '-'], "_")
        .split('_')
        .map(canonical_symbol)
        .collect::<Vec<_>>()
        .join("_")
}

/// The common name of a Kraken symbol: Kraken prefixes some legacy codes.
pub(super) fn canonical_symbol(symbol: &str) -> String {
    match symbol.trim().to_ascii_uppercase().as_str() {
        "XBT" | "XXBT" => "BTC".to_string(),
        "XDG" | "XXDG" => "DOGE".to_string(),
        "XETH" => "ETH".to_string(),
        "ZUSD" => "USD".to_string(),
        "ZEUR" => "EUR".to_string(),
        "ZGBP" => "GBP".to_string(),
        "ZJPY" => "JPY".to_string(),
        "ZCAD" => "CAD".to_string(),
        "ZCHF" => "CHF".to_string(),
        "ZAUD" => "AUD".to_string(),
        value => value.to_string(),
    }
}

/// The one item in `items`, or the error for none or for several.
pub(super) fn exactly_one<T, E>(mut items: Vec<T>, none: impl FnOnce() -> E, many: impl FnOnce() -> E) -> Result<T, E> {
    match items.len() {
        0 => Err(none()),
        1 => Ok(items.remove(0)),
        _ => Err(many()),
    }
}

/// Kraken's asset code for a symbol.
pub(super) fn api_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "BTC" => "XBT".to_string(),
        value => value.to_string(),
    }
}

pub(super) fn decimal_f64(value: Decimal, field: &str) -> Result<f64, String> {
    value
        .to_f64()
        .ok_or_else(|| format!("Kraken {field} cannot be represented as f64"))
}

pub(super) fn to_decimal(value: f64, field: &str) -> Result<Decimal, String> {
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("Kraken {field} must be finite and positive, got {value}"));
    }
    Decimal::from_f64(value).ok_or_else(|| format!("invalid Kraken {field} {value}"))
}
