use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{persistance::WalStore, stages::executor::ExecutionReceipt, swappers::model::SwapExecution};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizerResult {
    // Optional swap; non-swap finalizers can leave this as None
    pub swap_result: Option<SwapExecution>,
    pub finalized: bool,
}

impl FinalizerResult {
    pub fn noop() -> Self {
        Self {
            swap_result: None,
            finalized: false,
        }
    }
}

#[async_trait]
pub trait Finalizer: Send + Sync {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String>;
}
