use async_trait::async_trait;

use crate::{persistance::WalStore, stages::executor::ExecutionReceipt, swappers::model::SwapExecution};

#[derive(Debug, Clone)]
pub struct FinalizerResult {
    // Optional swap; non-swap finalizers can leave this as None
    pub swap_result: Option<SwapExecution>,
    // You can add more fields later (tx ids, extra fees, etc) if needed
}

impl FinalizerResult {
    pub fn noop() -> Self {
        Self { swap_result: None }
    }
}

#[async_trait]
pub trait Finalizer: Send + Sync {
    async fn finalize(&self, wal: &dyn WalStore, receipt: Vec<ExecutionReceipt>) -> Result<Vec<FinalizerResult>, String>;
}
