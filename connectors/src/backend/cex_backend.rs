use async_trait::async_trait;
use thiserror::Error;

pub const CEX_PENDING_SETTLEMENT_PREFIX: &str = "cex pending settlement: ";

/// Classification for calls that may have moved money before returning.
///
/// `Ambiguous` is the reason this stays a three-variant enum: a submission whose
/// outcome the venue will not confirm cannot be retried like a rejection without
/// risking a second order against the same funds.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CexSubmissionError {
    /// Rendered verbatim. Callers still match sentinel prefixes against this
    /// message — the permanent amount floor, and each backend's own ambiguity
    /// marker — so decorating it here would silently defeat those checks.
    #[error("{0}")]
    Rejected(String),
    /// Rendered with the shared wire prefix so a classification made here still
    /// reads as pending after a round trip through `last_error` and back through
    /// [`classify_cex_submission_error`].
    #[error("{CEX_PENDING_SETTLEMENT_PREFIX}{0}")]
    PendingSettlement(String),
    #[error("submission outcome is ambiguous: {0}")]
    Ambiguous(String),
}

/// An unclassified backend message is a plain rejection: the two states that
/// need care are only ever reached by deliberate classification, never by
/// defaulting into them.
impl From<String> for CexSubmissionError {
    fn from(message: String) -> Self {
        Self::Rejected(message)
    }
}

impl From<&str> for CexSubmissionError {
    fn from(message: &str) -> Self {
        Self::Rejected(message.to_string())
    }
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

/// Exact, side-effect-free funding requirements for one planned CEX route.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingRoutePreflight {
    pub deposit_asset: String,
    pub deposit_network: String,
    pub withdraw_asset: String,
    pub withdraw_network: String,
    pub withdraw_address: String,
    pub deposit_amount: f64,
    pub withdraw_amount: f64,
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
    async fn validate_funding_route(&self, _preflight: &FundingRoutePreflight) -> Result<(), String> {
        Ok(())
    }

    /// Amount-aware, read-only validation for one planned trade leg. This is
    /// separate from order submission so venue minimums can reject a preview
    /// before collateral is transferred to the exchange.
    async fn validate_trade_amounts(
        &self,
        _market: &str,
        _side: &str,
        _amount_in: f64,
        _amount_out: f64,
    ) -> Result<(), String> {
        Ok(())
    }

    // trading
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64) -> Result<SwapFillReport, String>;

    /// Submits a market order with venue-specific execution controls and
    /// returns the amounts that the exchange actually filled.
    ///
    /// `market` identifies the venue market, while `side` must select either a
    /// buy or a sell. For sells, `amount_in` is the available base-asset
    /// quantity. For buys, `amount_in` is the quote-asset budget, even when the
    /// backend must translate that budget into a base quantity for its API.
    ///
    /// `options.client_order_id` should be forwarded when the venue supports
    /// client-assigned order IDs. `options.buy_mode` controls how a buy budget
    /// is expressed to the exchange, and `options.max_quote_overspend_bps`
    /// bounds extra quote consumption when a base-quantity buy is required.
    /// Implementations must reject unsupported modes or invalid limits before
    /// submitting an order.
    ///
    /// The returned [`SwapFillReport`] contains actual consumed input and
    /// received output rather than estimates. Because an error may occur after
    /// the exchange accepted the order, callers must pass failures through
    /// [`CexBackend::classify_submission_error`] before deciding whether a
    /// submission is safe to retry.
    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexSubmissionError>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_error_display_formats_classification_without_changing_the_message() {
        assert_eq!(
            CexSubmissionError::PendingSettlement("matching engine is catching up".to_string()).to_string(),
            "cex pending settlement: matching engine is catching up"
        );
        assert_eq!(
            CexSubmissionError::Rejected("order rejected".to_string()).to_string(),
            "order rejected"
        );
    }
}
