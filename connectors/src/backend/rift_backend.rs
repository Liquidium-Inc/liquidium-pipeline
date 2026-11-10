//! Rift: an execution API that swaps one asset on one chain for another and
//! pays out to an address. A quote, then an order with a deposit address, then
//! the state of that order. There is no account, no balance and no order book,
//! so this is not a `CexBackend`.
use async_trait::async_trait;

/// An asset as Rift names it: `chain.symbol` in lower case, `ethereum.usdt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiftAsset(String);

impl RiftAsset {
    pub fn new(chain: &str, symbol: &str) -> Self {
        Self(format!(
            "{}.{}",
            chain.trim().to_ascii_lowercase(),
            symbol.trim().to_ascii_lowercase()
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What to quote. The amount is decimal text in the asset's own units, as Rift
/// reads and returns it in `formatted` mode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RiftVenueLists {
    /// Only these venues may route the order. Empty means any.
    pub allow: Vec<String>,
    /// These venues never route the order.
    pub block: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiftQuoteRequest {
    pub from: RiftAsset,
    pub to: RiftAsset,
    pub amount_in: String,
    /// The order inherits the quote's route, so the lists go on the quote.
    pub venues: RiftVenueLists,
}

/// A quote. It is indicative, it carries no price impact and no size limits,
/// and it expires ten minutes after creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiftQuote {
    pub id: String,
    pub from: RiftAsset,
    pub to: RiftAsset,
    pub amount_in: String,
    /// What reaches the payout address, net of Rift's execution cost.
    pub estimated_amount_out: String,
    /// The network costs the router pays, in the `to` asset.
    pub estimated_execution_cost: String,
    /// RFC 3339.
    pub expires_at: String,
    /// The venues on the route, in order.
    pub venues: Vec<String>,
}

/// An order. Funds sent to `deposit_address` before `deposit_deadline` start it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiftOrder {
    pub id: String,
    pub quote_id: String,
    pub deposit_address: String,
    pub amount_in: String,
    /// RFC 3339, seven days after creation.
    pub deposit_deadline: String,
    pub status: RiftOrderStatus,
}

/// Where an order stands. Every state but `AwaitingDeposit` and `Expired`
/// means Rift holds or has moved our funds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiftOrderStatus {
    AwaitingDeposit,
    Funded,
    /// The deposit was too small to fill. Rift says it refunds; the docs also
    /// say to raise it with support. An operator decides.
    Underfunded,
    Executing,
    Delivered,
    Refunded,
    /// The deposit deadline passed with no deposit watched.
    Expired,
    /// Halted by policy, or a refund failed. Only Rift support can move it.
    Frozen,
    /// A status this build does not know. Treated as money in flight.
    Unknown(String),
}

impl RiftOrderStatus {
    pub fn from_api(status: &str) -> Self {
        match status.trim().to_ascii_lowercase().as_str() {
            "awaiting_deposit" => Self::AwaitingDeposit,
            "funded" => Self::Funded,
            "underfunded" => Self::Underfunded,
            "executing" => Self::Executing,
            "delivered" => Self::Delivered,
            "refunded" => Self::Refunded,
            "expired" => Self::Expired,
            "frozen" => Self::Frozen,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// The status as the API names it; the inverse of `from_api`.
    pub fn as_api(&self) -> String {
        match self {
            Self::AwaitingDeposit => "awaiting_deposit".to_string(),
            Self::Funded => "funded".to_string(),
            Self::Underfunded => "underfunded".to_string(),
            Self::Executing => "executing".to_string(),
            Self::Delivered => "delivered".to_string(),
            Self::Refunded => "refunded".to_string(),
            Self::Expired => "expired".to_string(),
            Self::Frozen => "frozen".to_string(),
            Self::Unknown(other) => other.clone(),
        }
    }

    /// Whether Rift holds or has moved our funds. A caller must never treat
    /// such an order as if nothing happened.
    pub fn funds_moved(&self) -> bool {
        !matches!(self, Self::AwaitingDeposit | Self::Expired)
    }
}

/// The state of an order as Rift reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiftOrderState {
    pub id: String,
    pub status: RiftOrderStatus,
    /// What was paid out, once delivered.
    pub amount_out: Option<String>,
    /// The payout transaction, once delivered.
    pub payout_tx: Option<String>,
    pub refund_address: Option<String>,
}

/// Why a Rift call did not give an answer. The caller matches on the variant;
/// the text is for the operator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RiftError {
    /// The request never reached Rift, or Rift could not serve it, or the
    /// answer was lost. A repeat is safe: a quote creates nothing, and an
    /// order is the same for the same quote id.
    #[error("{0}")]
    Unreachable(String),
    /// Rift refused the request before acting: an unknown asset, a bad amount
    /// or a bad address. The same request fails again.
    #[error("{0}")]
    Rejected(String),
    /// An address failed Rift's sanctions screening. The same request fails again.
    #[error("{0}")]
    Screened(String),
    /// No venue could quote the pair at this amount.
    #[error("{0}")]
    NoVenue(String),
    /// The quote expired. A new quote is needed before an order.
    #[error("{0}")]
    QuoteExpired(String),
    /// Rift throttled the request.
    #[error("Rift throttled the request; retry after {retry_after_secs:?} s")]
    RateLimited { retry_after_secs: Option<u64> },
    /// Rift answered, but the answer could not be read.
    #[error("{0}")]
    Malformed(String),
}

#[mockall::automock]
#[async_trait]
pub trait RiftBackend: Send + Sync {
    /// An indicative quote, valid ten minutes.
    async fn quote(&self, request: &RiftQuoteRequest) -> Result<RiftQuote, RiftError>;

    /// The order for a quote. Rift returns the same order for the same quote id
    /// and addresses, so a repeat is safe. A changed address is refused.
    async fn create_order(
        &self,
        quote_id: &str,
        to_address: &str,
        refund_address: Option<String>,
    ) -> Result<RiftOrder, RiftError>;

    async fn order_state(&self, order_id: &str) -> Result<RiftOrderState, RiftError>;
}
