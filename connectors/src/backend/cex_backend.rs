use async_trait::async_trait;
use thiserror::Error;

/// Marks a venue error that means "not ready yet", not "this order failed".
///
/// A CEX credits a deposit into its account service and into its matching
/// engine at different moments, so the balance API can report funds the order
/// book still refuses to trade. Only the venue knows when that gap closes, and
/// it says so by rejecting the order. Errors carrying this prefix must be
/// waited out under a deadline rather than counted against a retry budget.
pub const CEX_PENDING_SETTLEMENT_PREFIX: &str = "cex pending settlement: ";

/// Classification for calls that may have moved money before returning.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CexSubmissionError {
    #[error("submission rejected: {0}")]
    Rejected(String),
    #[error("submission pending settlement: {0}")]
    PendingSettlement(String),
    #[error("submission outcome is ambiguous: {0}")]
    Ambiguous(String),
}

/// Reports whether a backend error asks the caller to wait for the venue.
pub fn is_cex_pending_settlement_error(message: &str) -> bool {
    message.starts_with(CEX_PENDING_SETTLEMENT_PREFIX)
}

pub fn classify_cex_submission_error(message: &str) -> CexSubmissionError {
    if is_cex_pending_settlement_error(message) {
        CexSubmissionError::PendingSettlement(message.to_string())
    } else {
        CexSubmissionError::Rejected(message.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct DepositAddress {
    pub asset: String,
    pub network: String,
    pub address: String,
    pub tag: Option<String>, // for exchanges that need memo/tag
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

#[derive(Debug, Clone)]
pub struct SwapExecutionOptions {
    /// Optional deterministic client order id for retry-safe submissions.
    pub client_order_id: Option<String>,
    /// Buy-side input mode selection.
    pub buy_mode: BuyOrderInputMode,
    /// Optional quote overspend cap (in bps) used by base-quantity buy mode.
    pub max_quote_overspend_bps: Option<f64>,
}

impl Default for SwapExecutionOptions {
    fn default() -> Self {
        Self {
            client_order_id: None,
            buy_mode: BuyOrderInputMode::Auto,
            max_quote_overspend_bps: None,
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

#[derive(Debug, Clone)]
pub struct WithdrawStatusSnapshot {
    pub status: WithdrawStatus,
    pub txid: Option<String>,
    pub transaction_fee: Option<f64>,
}

#[mockall::automock]
#[async_trait]
pub trait CexBackend: Send + Sync {
    /// Converts a backend-owned error into the decision the venue adapter must
    /// persist. The backend owns this mapping because only it knows whether a
    /// failed request may have reached the exchange.
    fn classify_submission_error(&self, error: &str) -> CexSubmissionError {
        classify_cex_submission_error(error)
    }

    /// Read-only preflight performed before a venue quote can be committed.
    /// Backends with destination allowlists use it to prove later settlement
    /// is possible for the exact route and address.
    async fn validate_funding_route(
        &self,
        _deposit_asset: &str,
        _deposit_network: &str,
        _withdraw_asset: &str,
        _withdraw_network: &str,
        _withdraw_address: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    // trading
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64) -> Result<SwapFillReport, String>;

    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, String>;

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String>;

    // deposits
    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String>;

    // withdrawals
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String>;

    // balance
    async fn get_balance(&self, asset: &str) -> Result<f64, String>;

    // withdrawal status
    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String>;

    // withdrawal status with optional transfer metadata (txid, fee)
    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String>;
}
