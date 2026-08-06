use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{persistance::WalStore, stages::executor::ExecutionReceipt, swappers::model::SwapExecution};

/// Why a finalization failed, and what the pipeline must do about the row.
///
/// The variant *is* the decision. A finalizer knows which case it is raising at
/// the point it raises it, so that knowledge travels with the error instead of
/// being re-derived downstream — rewording a message must never change how a
/// row is scheduled.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FinalizerError {
    /// A transient failure. Retry until the budget runs out.
    #[error("{0}")]
    Retryable(String),
    /// Retryable, but raised while a venue leg may still hold the liquidation's
    /// funds. Exhausting the retry budget must park the row for an operator
    /// rather than fail it permanently, because a permanently failed row leaves
    /// the runnable queue and nothing would ever return that custody.
    #[error("{0}")]
    VenueCustody(String),
    /// This liquidation can never be finalized. Fail it and stop.
    #[error("{0}")]
    Permanent(String),
    /// A bad-debt purchase whose output fell under a venue's amount floor. The
    /// stage accepts it as finalized when the row was bought as bad debt.
    #[error("{0}")]
    BadDebtAmountFloor(String),
    /// The committed row cannot be reconstructed by this binary or
    /// configuration — a version or invariant the current code cannot read.
    ///
    /// Nothing about the liquidation is wrong, so it must not be failed: the
    /// row is parked for an operator, who can downgrade or fix config and
    /// requeue it. Startup already treats the same condition this way in
    /// `park_unresumable_committed_rows`.
    #[error("{0}")]
    Unresumable(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizerResult {
    // Optional swap; non-swap finalizers can leave this as None
    pub swap_result: Option<SwapExecution>,
    pub finalized: bool,
    #[serde(default)]
    pub operator_required: bool,
    #[serde(default)]
    pub swapper: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl FinalizerResult {
    pub fn noop() -> Self {
        Self {
            swap_result: None,
            finalized: false,
            operator_required: false,
            swapper: None,
            reason: None,
        }
    }
}

/// Plumbing failures — a WAL write, an encode, a decode — carry no decision of
/// their own, so they default to retryable. Deciding a row's fate is done by
/// naming a variant explicitly; nothing is inferred from the message.
impl From<String> for FinalizerError {
    fn from(message: String) -> Self {
        FinalizerError::Retryable(message)
    }
}

impl FinalizerError {
    /// The variant name alone, for logs that already print the message.
    pub fn kind(&self) -> &'static str {
        match self {
            FinalizerError::Retryable(_) => "Retryable",
            FinalizerError::VenueCustody(_) => "VenueCustody",
            FinalizerError::Permanent(_) => "Permanent",
            FinalizerError::BadDebtAmountFloor(_) => "BadDebtAmountFloor",
            FinalizerError::Unresumable(_) => "Unresumable",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            FinalizerError::Retryable(message)
            | FinalizerError::VenueCustody(message)
            | FinalizerError::Permanent(message)
            | FinalizerError::BadDebtAmountFloor(message)
            | FinalizerError::Unresumable(message) => message,
        }
    }
}

#[async_trait]
pub trait Finalizer: Send + Sync {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt)
    -> Result<FinalizerResult, FinalizerError>;
}
