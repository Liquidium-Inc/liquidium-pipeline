use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;
use kraken_async_rs::{
    clients::{
        core_kraken_client::CoreKrakenClient, errors::ClientError, http_response_types::ResultErrorResponse,
        kraken_client::KrakenClient, rate_limited_kraken_client::RateLimitedKrakenClient,
    },
    crypto::nonce_provider::{IncreasingNonceProvider, NonceProvider},
    request_types::{
        AddOrderRequest, ClosedOrdersRequest, DepositAddressesRequest, DepositMethodsRequest, OrderRequest,
        OrderbookRequest, StatusOfDepositWithdrawRequest, StringCSV, TradableAssetPairsRequest, WithdrawFundsRequest,
        WithdrawalAddressesRequest, WithdrawalMethodsRequest,
    },
    response_types::{BuySell, OrderStatus, OrderType, TradableAssetStatus, TransferStatus},
    secrets::secrets_provider::{Secrets, SecretsProvider},
};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenPair {
    pub api_name: String,
    pub base: String,
    pub quote: String,
    pub price_decimals: u32,
    pub lot_decimals: u32,
    pub order_min: Decimal,
    pub cost_min: Decimal,
    pub taker_fee_bps: f64,
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenBookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenBook {
    pub bids: Vec<KrakenBookLevel>,
    pub asks: Vec<KrakenBookLevel>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenFundingMethod {
    pub method: String,
    pub network: Option<String>,
    pub minimum: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenDepositAddress {
    pub address: String,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenWithdrawalAddress {
    pub address: String,
    pub method: String,
    pub key: String,
    pub verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KrakenOrderStatus {
    Pending,
    Open,
    Closed,
    Canceled,
    Expired,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenOrder {
    pub status: KrakenOrderStatus,
    pub volume_executed: Decimal,
    pub cost: Decimal,
    pub fee: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KrakenTransferStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenWithdrawal {
    pub ref_id: String,
    pub tx_id: Option<String>,
    pub fee: Decimal,
    pub status: KrakenTransferStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KrakenApiError {
    #[error("Kraken rejected request: {0}")]
    Rejected(String),
    #[error("Kraken transport error: {0}")]
    Transport(String),
    #[error("Kraken submission outcome is ambiguous: {0}")]
    Ambiguous(String),
    /// The request never reached Kraken, so nothing was submitted or read.
    #[error("Kraken was not reachable: {0}")]
    Unreachable(String),
}

/// Maps a client error, separating a request that never left the host from one
/// that may have reached Kraken.
///
/// Only a failure to connect qualifies. Anything else -- a timeout, a broken
/// response -- may have been acted on, so it stays [`KrakenApiError::Transport`].
fn transport_error(error: ClientError) -> KrakenApiError {
    match &error {
        ClientError::HyperClient(inner) if inner.is_connect() => KrakenApiError::Unreachable(error.to_string()),
        _ => KrakenApiError::Transport(error.to_string()),
    }
}

#[mockall::automock]
#[async_trait]
pub trait KrakenApi: Send + Sync {
    async fn pairs(&self) -> Result<Vec<KrakenPair>, KrakenApiError>;
    async fn orderbook(&self, pair: &str, depth: u32) -> Result<KrakenBook, KrakenApiError>;
    async fn balance(&self, asset: &str) -> Result<Decimal, KrakenApiError>;
    async fn deposit_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError>;
    async fn deposit_addresses(&self, asset: &str, method: &str) -> Result<Vec<KrakenDepositAddress>, KrakenApiError>;
    async fn withdrawal_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError>;
    async fn withdrawal_addresses(&self, asset: &str) -> Result<Vec<KrakenWithdrawalAddress>, KrakenApiError>;
    async fn place_market_order(
        &self,
        pair: &str,
        side: &str,
        base_volume: Decimal,
        client_order_id: Option<String>,
    ) -> Result<String, KrakenApiError>;
    async fn order(&self, order_id: &str) -> Result<KrakenOrder, KrakenApiError>;
    /// Finds an already-settled order by the client id we submitted it under.
    ///
    /// This is how a resumed leg discovers that its order exists: the Kraken
    /// order id lives only in the response we may never have read, whereas the
    /// client id is chosen before submission and persisted with the leg.
    /// `Ok(None)` means Kraken has no closed order under that id.
    async fn closed_order_by_client_id(&self, client_order_id: &str) -> Result<Option<KrakenOrder>, KrakenApiError>;
    async fn withdraw(&self, asset: &str, key: &str, address: &str, amount: Decimal) -> Result<String, KrakenApiError>;
    /// The newest `limit` withdrawals of `asset`. Kraken caps this at 500 over
    /// a 90-day window and the typed response carries no cursor, so a
    /// withdrawal that falls off that page cannot be paged back to.
    async fn withdrawals(&self, asset: &str, limit: i64) -> Result<Vec<KrakenWithdrawal>, KrakenApiError>;
}

struct OwnedSecretsProvider {
    key: String,
    secret: String,
}

impl Debug for OwnedSecretsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedSecretsProvider")
            .field("key", &"[redacted]")
            .field("secret", &"[redacted]")
            .finish()
    }
}

impl SecretsProvider for OwnedSecretsProvider {
    fn get_secrets(&mut self) -> Secrets {
        Secrets {
            key: self.key.clone().into(),
            secret: self.secret.clone().into(),
        }
    }
}

type Client = RateLimitedKrakenClient<CoreKrakenClient>;

pub struct KrakenRestApi {
    client: Mutex<Client>,
}

impl Debug for KrakenRestApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KrakenRestApi").finish_non_exhaustive()
    }
}

impl KrakenRestApi {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let secrets: Box<Arc<Mutex<dyn SecretsProvider>>> = Box::new(Arc::new(Mutex::new(OwnedSecretsProvider {
            key: api_key,
            secret: api_secret,
        })));
        let nonce: Box<Arc<Mutex<dyn NonceProvider>>> = Box::new(Arc::new(Mutex::new(IncreasingNonceProvider::new())));
        Self {
            client: Mutex::new(Client::new(secrets, nonce)),
        }
    }
}

/// Picks the entry a one-item request asked for out of Kraken's keyed response.
///
/// Kraken keys results by its own name for the thing, which is not always the
/// name that was sent, so a lone entry is still the answer to our question. More
/// than one entry without our key is somebody else's answer -- another market's
/// book, another order's fill -- and adopting it is worse than failing the call,
/// because it reconciles real money against the wrong row. `HashMap` iteration
/// order is arbitrary, so the entry a blind fallback picks is arbitrary too.
fn take_requested<T>(mut entries: HashMap<String, T>, key: &str) -> Option<T> {
    match entries.remove(key) {
        Some(entry) => Some(entry),
        None if entries.len() == 1 => entries.into_values().next(),
        None => None,
    }
}

/// Narrows a Kraken order to the fields execution actually reconciles against.
fn to_kraken_order(order: kraken_async_rs::response_types::Order) -> KrakenOrder {
    KrakenOrder {
        status: match order.status {
            OrderStatus::Pending => KrakenOrderStatus::Pending,
            OrderStatus::Open => KrakenOrderStatus::Open,
            OrderStatus::Closed => KrakenOrderStatus::Closed,
            OrderStatus::Canceled => KrakenOrderStatus::Canceled,
            OrderStatus::Expired => KrakenOrderStatus::Expired,
        },
        volume_executed: order.volume_executed,
        cost: order.cost,
        fee: order.fee,
    }
}

fn take_result<T>(response: ResultErrorResponse<T>) -> Result<T, KrakenApiError> {
    if !response.error.is_empty() {
        return Err(KrakenApiError::Rejected(response.error.join(" | ")));
    }
    response
        .result
        .ok_or_else(|| KrakenApiError::Transport("response contained neither result nor error".to_string()))
}

fn canonical_symbol(symbol: &str) -> String {
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

#[async_trait]
impl KrakenApi for KrakenRestApi {
    async fn pairs(&self) -> Result<Vec<KrakenPair>, KrakenApiError> {
        let request = TradableAssetPairsRequest::builder().build();
        let response = self
            .client
            .lock()
            .await
            .get_tradable_asset_pairs(&request)
            .await
            .map_err(transport_error)?;
        let pairs = take_result(response)?;
        Ok(pairs
            .into_iter()
            .filter_map(|(api_name, pair)| {
                let (base, quote) = pair.ws_name.split_once('/')?;
                Some(KrakenPair {
                    api_name,
                    base: canonical_symbol(base),
                    quote: canonical_symbol(quote),
                    price_decimals: u32::try_from(pair.pair_decimals).ok()?,
                    lot_decimals: u32::try_from(pair.lot_decimals).ok()?,
                    order_min: pair.order_min,
                    cost_min: pair.cost_min,
                    taker_fee_bps: pair.fees.first().map(|fee| fee.fee * 100.0).unwrap_or(40.0),
                    online: pair.status == TradableAssetStatus::Online,
                })
            })
            .collect())
    }

    async fn orderbook(&self, pair: &str, depth: u32) -> Result<KrakenBook, KrakenApiError> {
        let request = OrderbookRequest::builder(pair.to_string())
            .count(i64::from(depth))
            .build();
        let response = self
            .client
            .lock()
            .await
            .get_orderbook(&request)
            .await
            .map_err(transport_error)?;
        let book = take_requested(take_result(response)?, pair)
            .ok_or_else(|| KrakenApiError::Transport(format!("no order book returned for {pair}")))?;
        Ok(KrakenBook {
            bids: book
                .bids
                .into_iter()
                .map(|level| KrakenBookLevel {
                    price: level.price,
                    quantity: level.volume,
                })
                .collect(),
            asks: book
                .asks
                .into_iter()
                .map(|level| KrakenBookLevel {
                    price: level.price,
                    quantity: level.volume,
                })
                .collect(),
        })
    }

    async fn balance(&self, asset: &str) -> Result<Decimal, KrakenApiError> {
        let response = self
            .client
            .lock()
            .await
            .get_account_balance()
            .await
            .map_err(transport_error)?;
        let balances = take_result(response)?;
        Ok(balances
            .iter()
            .find(|(name, _)| canonical_symbol(name) == canonical_symbol(asset))
            .map(|(_, value)| *value)
            .unwrap_or(Decimal::ZERO))
    }

    async fn deposit_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        let request = DepositMethodsRequest::builder(asset.to_string()).build();
        let response = self
            .client
            .lock()
            .await
            .get_deposit_methods(&request)
            .await
            .map_err(transport_error)?;
        Ok(take_result(response)?
            .into_iter()
            .map(|method| KrakenFundingMethod {
                method: method.method,
                network: None,
                minimum: method.minimum,
            })
            .collect())
    }

    async fn deposit_addresses(&self, asset: &str, method: &str) -> Result<Vec<KrakenDepositAddress>, KrakenApiError> {
        let request = DepositAddressesRequest::builder(asset.to_string(), method.to_string())
            .is_new(false)
            .build();
        let response = self
            .client
            .lock()
            .await
            .get_deposit_addresses(&request)
            .await
            .map_err(transport_error)?;
        Ok(take_result(response)?
            .into_iter()
            .map(|address| KrakenDepositAddress {
                address: address.address,
                tag: address.tag.or(address.memo),
            })
            .collect())
    }

    async fn withdrawal_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        let request = WithdrawalMethodsRequest::builder().asset(asset.to_string()).build();
        let response = self
            .client
            .lock()
            .await
            .get_withdrawal_methods(&request)
            .await
            .map_err(transport_error)?;
        Ok(take_result(response)?
            .into_iter()
            .map(|method| KrakenFundingMethod {
                method: method.method,
                network: method.network,
                minimum: method.minimum,
            })
            .collect())
    }

    async fn withdrawal_addresses(&self, asset: &str) -> Result<Vec<KrakenWithdrawalAddress>, KrakenApiError> {
        let request = WithdrawalAddressesRequest::builder()
            .asset(asset.to_string())
            .verified(true)
            .build();
        let response = self
            .client
            .lock()
            .await
            .get_withdrawal_addresses(&request)
            .await
            .map_err(transport_error)?;
        Ok(take_result(response)?
            .into_iter()
            .map(|address| KrakenWithdrawalAddress {
                address: address.address,
                method: address.method,
                key: address.key,
                verified: address.verified,
            })
            .collect())
    }

    async fn place_market_order(
        &self,
        pair: &str,
        side: &str,
        base_volume: Decimal,
        client_order_id: Option<String>,
    ) -> Result<String, KrakenApiError> {
        let side = if side.eq_ignore_ascii_case("sell") {
            BuySell::Sell
        } else {
            BuySell::Buy
        };
        let mut builder = AddOrderRequest::builder(OrderType::Market, side, base_volume, pair.to_string());
        let request = match client_order_id {
            Some(client_order_id) => builder.client_order_id(client_order_id).build(),
            None => builder.build(),
        };
        let response = self
            .client
            .lock()
            .await
            .add_order(&request)
            .await
            .map_err(|error| KrakenApiError::Ambiguous(error.to_string()))?;
        let result = take_result(response)?;
        result
            .tx_id
            .into_iter()
            .next()
            .ok_or_else(|| KrakenApiError::Ambiguous("order response had no txid".to_string()))
    }

    async fn order(&self, order_id: &str) -> Result<KrakenOrder, KrakenApiError> {
        let request = OrderRequest::builder(StringCSV::new(vec![order_id.to_string()])).build();
        let response = self
            .client
            .lock()
            .await
            .query_orders_info(&request)
            .await
            .map_err(transport_error)?;
        let order = take_requested(take_result(response)?, order_id)
            .ok_or_else(|| KrakenApiError::Transport(format!("order {order_id} was not returned")))?;
        Ok(to_kraken_order(order))
    }

    async fn closed_order_by_client_id(&self, client_order_id: &str) -> Result<Option<KrakenOrder>, KrakenApiError> {
        let request = ClosedOrdersRequest::builder()
            .client_order_id(client_order_id.to_string())
            .build();
        let response = self
            .client
            .lock()
            .await
            .get_closed_orders(&request)
            .await
            .map_err(transport_error)?;
        let closed = take_result(response)?;
        // Kraken filters server-side, but the client id is re-checked here so a
        // filter the venue ignored cannot make us adopt an unrelated fill.
        Ok(closed
            .closed
            .into_values()
            .find(|order| order.client_order_id.as_deref() == Some(client_order_id))
            .map(to_kraken_order))
    }

    async fn withdraw(&self, asset: &str, key: &str, address: &str, amount: Decimal) -> Result<String, KrakenApiError> {
        let request = WithdrawFundsRequest::builder(asset.to_string(), key.to_string(), amount)
            .address(address.to_string())
            .build();
        let response = self
            .client
            .lock()
            .await
            .withdraw_funds(&request)
            .await
            .map_err(|error| KrakenApiError::Ambiguous(error.to_string()))?;
        Ok(take_result(response)?.ref_id)
    }

    async fn withdrawals(&self, asset: &str, limit: i64) -> Result<Vec<KrakenWithdrawal>, KrakenApiError> {
        let request = StatusOfDepositWithdrawRequest::builder()
            .asset(asset.to_string())
            .limit(limit)
            .build();
        let response = self
            .client
            .lock()
            .await
            .get_status_of_recent_withdrawals(&request)
            .await
            .map_err(transport_error)?;
        Ok(take_result(response)?
            .into_iter()
            .map(|withdrawal| KrakenWithdrawal {
                ref_id: withdrawal.ref_id,
                // Absent while pending, and blank on some venues once present;
                // both mean "no chain transaction yet" to every caller.
                tx_id: withdrawal.tx_id.filter(|tx_id| !tx_id.trim().is_empty()),
                fee: withdrawal.fee,
                status: match withdrawal.status {
                    TransferStatus::Success | TransferStatus::Settled => KrakenTransferStatus::Completed,
                    TransferStatus::Failure => KrakenTransferStatus::Failed,
                    TransferStatus::Initial | TransferStatus::Pending => KrakenTransferStatus::Pending,
                },
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kraken answers a one-item request with a map keyed by its own name for
    /// the thing, so a lone entry is still our answer. Two entries without our
    /// key are not: `HashMap` iteration order is arbitrary, so taking one anyway
    /// reconciles an order's fill, or prices a market, against whichever row the
    /// hasher happened to yield.
    #[test]
    fn a_keyed_response_only_answers_for_the_thing_that_was_asked_about() {
        let map = |pairs: &[(&str, u8)]| {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), *value))
                .collect::<HashMap<String, u8>>()
        };

        // The exact key wins, whatever else came back alongside it.
        assert_eq!(take_requested(map(&[("XXBTZUSD", 1), ("XETHZUSD", 2)]), "XXBTZUSD"), Some(1));

        // Kraken's alias for the same request: one entry, so it is ours.
        assert_eq!(take_requested(map(&[("XBTUSD", 1)]), "XXBTZUSD"), Some(1));

        // Somebody else's rows. The caller's transport error is the right answer.
        assert_eq!(take_requested(map(&[("XETHZUSD", 2), ("XXRPZUSD", 3)]), "XXBTZUSD"), None);
        assert_eq!(take_requested(map(&[]), "XXBTZUSD"), None);
    }

    #[test]
    fn canonical_symbol_only_strips_known_kraken_legacy_codes() {
        assert_eq!(canonical_symbol("XXBT"), "BTC");
        assert_eq!(canonical_symbol("ZUSD"), "USD");
        assert_eq!(canonical_symbol("XETH"), "ETH");
        assert_eq!(canonical_symbol("XRP"), "XRP");
        assert_eq!(canonical_symbol("XMR"), "XMR");
        assert_eq!(canonical_symbol("ZEC"), "ZEC");
    }

    #[test]
    fn debug_output_redacts_owned_credentials() {
        let provider = OwnedSecretsProvider {
            key: "public-key".to_string(),
            secret: "private-secret".to_string(),
        };
        let output = format!("{provider:?}");
        assert!(!output.contains("public-key"));
        assert!(!output.contains("private-secret"));
        assert!(output.contains("redacted"));
    }
}
