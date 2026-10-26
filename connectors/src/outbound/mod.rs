//! One `OutboundSender` per chain, and a router that picks the sender.
mod cex_withdraw;
mod evm;
mod icp;
mod router;
mod solana;
mod unsettled;

pub use cex_withdraw::{AcceptedWithdrawal, CexWithdrawals, WithdrawRequest, prepare_cex_withdraw};
pub use evm::EvmSender;
pub(crate) use evm::prepared_send as evm_prepared_send;
pub use icp::IcpSender;
pub use router::{OutboundRouter, SignedSendRouter};
pub use solana::SolanaSender;
pub(crate) use solana::prepared_send as solana_prepared_send;
pub use unsettled::UnsettledSender;

/// Unix time in nanoseconds, the unit ICP ledgers use for `created_at_time`.
fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

fn encode<T: serde::Serialize>(payload: &T) -> Result<String, String> {
    serde_json::to_string(payload).map_err(|error| format!("encode outbound payload: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(payload: &str) -> Result<T, String> {
    serde_json::from_str(payload).map_err(|error| format!("decode outbound payload: {error}"))
}
