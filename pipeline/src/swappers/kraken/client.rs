//! [`KrakenClient`]: Kraken behind the venue-neutral [`CexBackend`]. Orders
//! live in `trading.rs` and deposits and withdrawals in `funding.rs`; this file
//! holds the market allowlist and pair lookup they share.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, CexBackendError, DepositAddress, FundingRoute, FundingRouteError, OrderBook, SwapExecutionOptions,
    SwapFillReport, WithdrawError, WithdrawStatus, WithdrawStatusSnapshot, WithdrawTerms, WithdrawalReceipt,
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use tokio::sync::RwLock;

use super::api::{KrakenApi, KrakenBook, KrakenPair};
use super::rest::KrakenRestApi;

const PAIR_METADATA_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub struct KrakenClient {
    pub(super) api: Arc<dyn KrakenApi>,
    pairs: Arc<RwLock<Option<(Instant, Vec<KrakenPair>)>>>,
    allowed_pairs: Arc<Vec<String>>,
}

impl KrakenClient {
    /// A client that trades only the markets in `allowed_pairs`. An empty list
    /// is refused, so a missing setting never means "every market".
    pub fn new(api_key: String, api_secret: String, allowed_pairs: Vec<String>) -> Result<Self, String> {
        Self::with_api(Arc::new(KrakenRestApi::new(api_key, api_secret)), allowed_pairs)
    }

    pub fn with_api(api: Arc<dyn KrakenApi>, allowed_pairs: Vec<String>) -> Result<Self, String> {
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
            pairs: Arc::new(RwLock::new(None)),
            allowed_pairs: Arc::new(allowed_pairs),
        })
    }

    async fn all_pairs(&self) -> Result<Vec<KrakenPair>, String> {
        if let Some((loaded_at, pairs)) = self.pairs.read().await.as_ref()
            && loaded_at.elapsed() < PAIR_METADATA_TTL
        {
            return Ok(pairs.clone());
        }
        let pairs = self.api.pairs().await.map_err(|error| error.to_string())?;
        *self.pairs.write().await = Some((Instant::now(), pairs.clone()));
        Ok(pairs)
    }

    /// The one online Kraken pair for an allowed market.
    pub(super) async fn pair(&self, market: &str) -> Result<KrakenPair, String> {
        let market = normalize_market(market);
        if !self.allowed_pairs.contains(&market) {
            return Err(format!("Kraken pair {market} is not allowed"));
        }
        let matches = self
            .all_pairs()
            .await?
            .into_iter()
            .filter(|pair| format!("{}_{}", pair.base, pair.quote) == market)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Err(format!("Kraken pair {market} is unavailable")),
            [pair] if !pair.online => Err(format!("Kraken pair {market} is not online")),
            [pair] => Ok(pair.clone()),
            _ => Err(format!("Kraken pair {market} is ambiguous")),
        }
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
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        self.quote_sell(market, amount_in).await
    }

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        Ok(self
            .execute_swap_detailed(market, side, amount_in)
            .await?
            .output_received)
    }

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64) -> Result<SwapFillReport, String> {
        self.execute_order(market, side, amount_in, SwapExecutionOptions::default())
            .await
            .map_err(|error| error.to_string())
    }

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

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        Ok(self.withdrawal_status(coin, withdraw_id).await?.status)
    }

    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        self.withdrawal_status(coin, withdraw_id).await
    }
}

/// `BASE_QUOTE` in upper case, with Kraken's XBT spelled BTC.
pub(crate) fn normalize_market(market: &str) -> String {
    market
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '-'], "_")
        .split('_')
        .map(|symbol| match symbol {
            "XBT" | "XXBT" => "BTC",
            value => value,
        })
        .collect::<Vec<_>>()
        .join("_")
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
