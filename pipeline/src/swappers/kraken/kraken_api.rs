//! What Kraken's REST API says, as this pipeline reads it.
//!
//! The wire types and the [`KrakenApi`] trait live here on their own so the
//! adapter can be exercised against a mock and the real transport can be
//! swapped without touching either. The live implementation is
//! [`super::kraken_rest::KrakenRestApi`].

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
