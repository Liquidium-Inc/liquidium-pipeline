//! Outbound sends: calls that move our funds out of an account we control.
//! The pipeline saves a send before the call, and only the chain or an operator decides it.
use async_trait::async_trait;
use candid::Nat;
use serde::{Deserialize, Serialize};

use crate::account::model::ChainAccount;
use crate::tokens::chain_token::ChainToken;

/// What one submit, or one read of the chain, says about a send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    /// The send happened. `receipt` is the chain's proof of it: a block index,
    /// a signature or a transaction hash.
    Landed { receipt: String },
    /// The send did not happen and cannot happen later. A new send is safe.
    NeverLanded { reason: String },
    /// The receiver refused the amount as too small. The send did not happen,
    /// and the same send fails again.
    BelowMinimum { reason: String },
    /// The sender was not ready, for example a busy wallet. The send did not
    /// happen, and a new send later is safe.
    Deferred { reason: String },
    /// No decided answer yet.
    Open { reason: String },
}

/// The funds to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundIntent {
    pub token: ChainToken,
    pub to: ChainAccount,
    pub amount_native: Nat,
}

/// A send whose identity exists before it runs. The caller saves it before
/// `submit`, so a retry or a restart sends exactly this payload again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedSend {
    /// The sender that built it. Only that sender can submit or settle it.
    pub sender: String,
    /// What the chain knows the send by: a signature, a hash or a creation time.
    pub identity: String,
    /// The exact send, encoded by its sender.
    pub payload: String,
    /// True when a second submit of this payload cannot move funds twice.
    /// A crash during a submit leaves the record unsent; only a safe replay
    /// may then resume without an operator.
    #[serde(default)]
    pub replay_safe: bool,
}

#[mockall::automock]
#[async_trait]
pub trait OutboundSender: Send + Sync {
    /// Builds the send, signed where the chain needs it. It has no side
    /// effect, so an error is a refusal.
    async fn prepare(&self, intent: &OutboundIntent) -> Result<PreparedSend, String>;

    /// Sends exactly the prepared payload. A second call with the same payload
    /// cannot move the funds twice.
    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome;

    /// Decides the send from the chain. It may send the same payload again,
    /// which `submit` guarantees is safe.
    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome;

    /// How long a submitted send may stay open before an operator must decide.
    fn open_window_secs(&self, prepared: &PreparedSend) -> i64 {
        let _ = prepared;
        15 * 60
    }
}
