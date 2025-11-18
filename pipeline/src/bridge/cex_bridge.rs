use std::fmt::Debug;

use async_trait::async_trait;

use crate::swappers::model::{SwapExecution, SwapRequest};

#[derive(Debug, thiserror::Error)]
pub enum CexBridgeError {
    // IC side
    #[error("ic transfer failed: {0}")]
    IcTransfer(String),

    // Credit / watcher side
    #[error("cex credit timeout")]
    CreditTimeout,

    // CEX trading
    #[error("cex swap failed: {0}")]
    Swap(String),

    // CEX withdraw
    #[error("cex withdraw failed: {0}")]
    Withdraw(String),

    // Persistence / infra
    #[error("wal error: {0}")]
    Wal(String),

    // Generic
    #[error("other bridge error: {0}")]
    Other(String),
}

#[async_trait]
pub trait CexBridge: Send + Sync + Debug {
    async fn execute_roundtrip(
        &self,
        req: &SwapRequest,
    ) -> Result<SwapExecution, CexBridgeError>;
}