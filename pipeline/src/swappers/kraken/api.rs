//! What Kraken's REST API says, as this pipeline reads it. The trait is
//! mockable, so the client is tested without the live transport in `rest.rs`.

use async_trait::async_trait;
use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct KrakenPair {
    pub api_name: String,
    pub base: String,
    pub quote: String,
    pub price_decimals: u32,
    pub lot_decimals: u32,
    pub order_min: Decimal,
    pub cost_min: Decimal,
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
    /// Withdrawal methods report it; deposit methods do not.
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
pub enum KrakenSide {
    Buy,
    Sell,
}

/// One order. With a limit price it is an IOC limit order, else a market
/// order. The fee is always taken from the asset the order pays out.
#[derive(Debug, Clone, PartialEq)]
pub struct KrakenOrderRequest {
    pub pair: String,
    pub side: KrakenSide,
    /// In base units.
    pub volume: Decimal,
    pub limit_price: Option<Decimal>,
    pub client_order_id: Option<String>,
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
    /// In quote units.
    pub cost: Decimal,
    /// In quote units, whichever side paid it.
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
    /// Kraken answered with an error, so it did not act on the request.
    #[error("Kraken refused the request: {0}")]
    Rejected(String),
    /// The connection failed, so the request never reached Kraken.
    #[error("Kraken was not reachable: {0}")]
    Unreachable(String),
    /// The request may have reached Kraken, and no answer says what happened.
    #[error("Kraken transport error: {0}")]
    Transport(String),
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
    /// The fee Kraken takes from a withdrawal of `amount` to the address `key`.
    async fn withdrawal_fee(&self, asset: &str, key: &str, amount: Decimal) -> Result<Decimal, KrakenApiError>;
    /// Places one order and returns its Kraken id.
    async fn place_order(&self, request: &KrakenOrderRequest) -> Result<String, KrakenApiError>;
    async fn order(&self, order_id: &str) -> Result<KrakenOrder, KrakenApiError>;
    /// A closed order placed under this client id, if Kraken has one. A
    /// resumed leg knows its order only by this id.
    async fn closed_order_by_client_id(&self, client_order_id: &str) -> Result<Option<KrakenOrder>, KrakenApiError>;
    async fn withdraw(&self, asset: &str, key: &str, address: &str, amount: Decimal) -> Result<String, KrakenApiError>;
    /// The newest `limit` withdrawals of `asset`. Kraken caps this at 500 over
    /// 90 days and returns no cursor.
    async fn withdrawals(&self, asset: &str, limit: i64) -> Result<Vec<KrakenWithdrawal>, KrakenApiError>;
}
