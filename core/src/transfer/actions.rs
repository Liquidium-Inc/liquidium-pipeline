use async_trait::async_trait;
use candid::Nat;

use crate::account::model::ChainAccount;
use crate::tokens::chain_token::ChainToken;

/// Why a transfer failed, and — the part that matters — whether it is known to
/// have left the funds untouched.
///
/// This decides who has to do what next. A ledger that evaluated the request
/// and refused it moved nothing, so a caller can act on the failure directly.
/// A submission that never produced a decided answer may or may not have been
/// applied, and only someone comparing balances can say which.
///
/// When in doubt, classify as `Ambiguous`: treating a real transfer as if it
/// never happened is the expensive mistake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferFailure {
    /// The transfer was evaluated and refused, or was never submitted at all.
    /// No funds moved.
    Rejected(String),
    /// The submission produced no decided answer. The transfer may or may not
    /// have been applied.
    Ambiguous(String),
}

impl TransferFailure {
    pub fn message(&self) -> &str {
        match self {
            TransferFailure::Rejected(message) | TransferFailure::Ambiguous(message) => message,
        }
    }

    /// Whether the funds are known not to have moved.
    pub fn is_rejected(&self) -> bool {
        matches!(self, TransferFailure::Rejected(_))
    }
}

impl std::fmt::Display for TransferFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferFailure::Rejected(message) => write!(f, "{message}"),
            TransferFailure::Ambiguous(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for TransferFailure {}

#[mockall::automock]
#[async_trait]
pub trait TransferActions: Send + Sync {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
    ) -> Result<String, TransferFailure>; // return tx hash/id

    // Optional ICRC-2-style approval for the current account (used to bump ledger activity).
    async fn approve(&self, token: &ChainToken, spender: &ChainAccount, amount_native: Nat) -> Result<String, String>;
}
