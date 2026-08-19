use async_trait::async_trait;
use thiserror::Error;

pub const CEX_PENDING_SETTLEMENT_PREFIX: &str = "cex pending settlement: ";

/// Marker a backend renders into a message whose request never left the host.
/// Only the backend can tell that apart from a venue's answer, so it decides
/// once and the marker carries the decision across the `Result<_, String>`
/// boundaries in this trait.
pub const CEX_VENUE_UNREACHABLE_PREFIX: &str = "cex venue unreachable: ";

/// Classification for calls that may have moved money before returning.
///
/// `Ambiguous` is the reason this is an enum at all: a submission whose outcome
/// the venue will not confirm cannot be retried like a rejection without risking
/// a second order against the same funds.
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
    /// The request never reached the venue, so there is no outcome to judge.
    /// Rendered with the shared marker for the same round trip as
    /// `PendingSettlement`.
    #[error("{CEX_VENUE_UNREACHABLE_PREFIX}{0}")]
    Unreachable(String),
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

/// Reports whether a backend marked this message as never having been sent.
///
/// Matched anywhere in the message rather than at the front: backends wrap a
/// rendered error in their own context (`Get_order err: ...`), and the marker
/// has to survive that. It is our own constant either way, so rewording the
/// human part of a message still cannot change the decision.
pub fn is_cex_venue_unreachable_error(message: &str) -> bool {
    message.contains(CEX_VENUE_UNREACHABLE_PREFIX)
}

pub fn classify_cex_submission_error(message: &str) -> CexSubmissionError {
    // Payloads are stored without their marker, because `Display` puts it
    // back. A backend that flattens a classification into its message and
    // hands it here again would otherwise stack one marker per round trip.
    if is_cex_venue_unreachable_error(message) {
        // The marker may sit inside a backend's own wrapping rather than at
        // the front, so the first copy is removed wherever it is.
        CexSubmissionError::Unreachable(message.replacen(CEX_VENUE_UNREACHABLE_PREFIX, "", 1))
    } else if let Some(payload) = message.strip_prefix(CEX_PENDING_SETTLEMENT_PREFIX) {
        CexSubmissionError::PendingSettlement(payload.to_string())
    } else {
        CexSubmissionError::Rejected(message.to_string())
    }
}

/// Why a venue refused a withdrawal.
///
/// The state machine keeps or writes off a leg's output on this, so the venue
/// that read the refusal names it here once; the message is only for humans.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CexWithdrawError {
    /// The amount is under the venue's minimum for this asset and network.
    /// Nothing was sent.
    #[error("{0}")]
    BelowMinimum(String),
    #[error("{0}")]
    Other(String),
}

impl From<String> for CexWithdrawError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

impl From<&str> for CexWithdrawError {
    fn from(message: &str) -> Self {
        Self::Other(message.to_string())
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
    ) -> Result<WithdrawalReceipt, CexWithdrawError>;

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

    #[test]
    fn an_unreachable_marker_survives_the_round_trip_through_a_message() {
        let original = CexSubmissionError::Unreachable("dns lookup failed".to_string());
        let rendered = original.to_string();
        let reclassified = classify_cex_submission_error(&rendered);
        assert_eq!(reclassified, original);
        // Rendered again it carries one marker, not one per pass.
        assert_eq!(reclassified.to_string(), rendered);
    }

    #[test]
    fn the_marker_is_found_inside_a_backends_own_wrapping() {
        let wrapped = format!("Get_order err: {CEX_VENUE_UNREACHABLE_PREFIX}connection refused");
        let classified = classify_cex_submission_error(&wrapped);
        assert_eq!(
            classified,
            CexSubmissionError::Unreachable("Get_order err: connection refused".to_string())
        );
        // And the wrapping is kept while the marker moves to the front once.
        assert_eq!(
            classified.to_string(),
            format!("{CEX_VENUE_UNREACHABLE_PREFIX}Get_order err: connection refused")
        );
    }

    /// Rendering a classification and reading it back has to land on the same
    /// error, not on one carrying a second copy of the marker. Kraken flattens
    /// typed errors into their message (`execute_swap_detailed`) and classifies
    /// the result again, so this round trip runs on a live path.
    #[test]
    fn a_pending_classification_survives_rendering_without_stacking_its_marker() {
        let original = CexSubmissionError::PendingSettlement("matching engine is catching up".to_string());
        let reclassified = classify_cex_submission_error(&original.to_string());

        assert_eq!(reclassified, original);
        assert_eq!(
            reclassified.to_string(),
            "cex pending settlement: matching engine is catching up"
        );
    }

    /// A venue's own answer must never be read as absence of a request.
    #[test]
    fn a_venue_answer_stays_a_rejection() {
        for message in [
            "EAccount:Invalid permissions:USDT trading restricted for RO.",
            "Reqwest error: operation timed out",
            "quantity 0.0001 below min_qty 0.2 for CKUSDT_USDT",
        ] {
            assert!(matches!(
                classify_cex_submission_error(message),
                CexSubmissionError::Rejected(_)
            ));
        }
    }
}
