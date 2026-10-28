use async_trait::async_trait;

use super::bridge_backend::BridgeError;

/// Classified failure from a CEX trade submission or reconciliation request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CexBackendError {
    /// The venue sees the funds or order but cannot trade or reconcile it yet.
    #[error("cex pending settlement: {0}")]
    PendingSettlement(String),
    /// The amount is under a minimum on the route, so no later attempt can
    /// succeed with it.
    #[error("{0}")]
    BelowMinimum(String),
    /// A call may have moved funds, and no answer decides it. Only an operator
    /// can say what happened, so no automatic retry is safe.
    #[error("{0}")]
    Ambiguous(String),
    /// The request never reached the venue (a failed connection), so nothing
    /// happened there. The leg waits, and the wait spends no retry budget.
    #[error("{0}")]
    Unreachable(String),
    /// Any backend failure that follows the normal retry/error policy.
    #[error("{0}")]
    Other(String),
}

impl From<String> for CexBackendError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

impl From<&str> for CexBackendError {
    fn from(message: &str) -> Self {
        Self::Other(message.to_string())
    }
}

/// A bridge failure inside a CEX leg step keeps the kinds the leg decides on.
impl From<BridgeError> for CexBackendError {
    fn from(error: BridgeError) -> Self {
        match error {
            BridgeError::BelowMinimum(message) => Self::BelowMinimum(message),
            BridgeError::Ambiguous(message) => Self::Ambiguous(message),
            BridgeError::Deferred(message)
            | BridgeError::Unreadable(message)
            | BridgeError::Refused(message)
            | BridgeError::Other(message) => Self::Other(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DepositAddress {
    pub asset: String,
    pub network: String,
    pub address: String,
    pub tag: Option<String>, // for exchanges that need memo/tag
}

/// Why a withdrawal request failed. The kind tells the caller what is safe next.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WithdrawError {
    /// The exchange may have executed the request; only its history decides.
    #[error("{0}")]
    Ambiguous(String),
    /// The amount is below the exchange minimum, so the same request fails again.
    #[error("{0}")]
    BelowMinimum(String),
    /// The exchange refused the request before it acted on it.
    #[error("{0}")]
    Refused(String),
    /// The request never reached the exchange, so a new request later is safe.
    #[error("{0}")]
    Unreachable(String),
}

#[derive(Debug, Clone)]
pub struct WithdrawalReceipt {
    pub asset: String,
    pub network: String,
    pub amount: f64,
    pub txid: Option<String>,
    pub internal_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    /// The account's taker fee on this market, in bps. A fill comes out this
    /// much below the book, so a preview takes it off the same way.
    pub taker_fee_bps: f64,
}

#[derive(Debug, Clone)]
pub struct SwapFillReport {
    /// Actual input amount consumed by the exchange for this order.
    pub input_consumed: f64,
    /// Actual output amount received from the exchange for this order.
    pub output_received: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyOrderInputMode {
    Auto,
    QuoteOrderQty,
    BaseQuantity,
}

/// A price bound that the exchange enforces on one order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceLimit {
    /// The preview mid price that the bound is measured from.
    pub reference_price: f64,
    /// The worst fill allowed, in bps away from the reference.
    pub max_slippage_bps: f64,
}

#[derive(Debug, Clone)]
pub struct SwapExecutionOptions {
    /// Optional deterministic client order id for retry-safe submissions.
    pub client_order_id: Option<String>,
    /// Buy-side input mode selection.
    pub buy_mode: BuyOrderInputMode,
    /// Optional quote overspend cap (in bps) used by base-quantity buy mode.
    pub max_quote_overspend_bps: Option<f64>,
    /// With a limit the order is an IOC limit order: nothing fills worse than the
    /// limit. If the exchange refuses IOC outright, or without a limit, it is a market order.
    pub price_limit: Option<PriceLimit>,
}

impl Default for SwapExecutionOptions {
    fn default() -> Self {
        Self {
            client_order_id: None,
            buy_mode: BuyOrderInputMode::Auto,
            max_quote_overspend_bps: None,
            price_limit: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithdrawStatus {
    Pending,
    Completed,
    Failed,
    Canceled,
    Unknown,
}

/// What a withdrawal of one coin on one network costs and needs, as the
/// exchange publishes it before the withdrawal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WithdrawTerms {
    /// Taken from the withdrawn amount, in the coin's units.
    pub fee: f64,
    /// The smallest amount the exchange accepts.
    pub minimum: f64,
}

/// One route through an exchange, as a plan would run it.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingRoute {
    pub deposit_asset: String,
    pub deposit_network: String,
    /// What reaches the exchange, after transfer and bridge fees.
    pub deposit_amount: f64,
    pub withdraw_asset: String,
    pub withdraw_network: String,
    pub withdraw_address: String,
}

/// Why an exchange cannot settle a route.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FundingRouteError {
    /// The deposit is under the exchange's minimum, so it would not be credited.
    #[error("{0}")]
    BelowMinimum(String),
    /// The exchange cannot settle the route as configured, for example to an
    /// address it has not verified.
    #[error("{0}")]
    Unsupported(String),
    /// A read failed, so nothing is known about the route.
    #[error("{0}")]
    Unavailable(String),
}

#[derive(Debug, Clone)]
pub struct WithdrawStatusSnapshot {
    pub status: WithdrawStatus,
    pub txid: Option<String>,
    pub transaction_fee: Option<f64>,
}

#[mockall::automock]
#[async_trait]
pub trait CexBackend: Send + Sync {
    // trading
    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexBackendError>;

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String>;

    // deposits
    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String>;

    // withdrawals
    /// `withdraw_order_id` is the caller's id for this request. The exchange
    /// stores it, so a lookup can find a request whose reply was lost.
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
        withdraw_order_id: &str,
    ) -> Result<WithdrawalReceipt, WithdrawError>;

    /// The fee and minimum of a withdrawal, known before it is sent.
    async fn get_withdraw_terms(&self, asset: &str, network: &str) -> Result<WithdrawTerms, String>;

    /// Read-only checks, before any funds move, that the exchange takes this
    /// deposit and sends this withdrawal.
    async fn check_funding_route(&self, route: &FundingRoute) -> Result<(), FundingRouteError>;

    // balance
    async fn get_balance(&self, asset: &str) -> Result<f64, String>;

    // withdrawal status with optional transfer metadata (txid, fee)
    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_error_display_formats_classification_without_changing_the_message() {
        assert_eq!(
            CexBackendError::PendingSettlement("matching engine is catching up".to_string()).to_string(),
            "cex pending settlement: matching engine is catching up"
        );
        assert_eq!(
            CexBackendError::Other("order rejected".to_string()).to_string(),
            "order rejected"
        );
    }
}
