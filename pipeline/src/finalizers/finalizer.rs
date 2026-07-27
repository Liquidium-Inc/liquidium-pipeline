use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{persistance::WalStore, stages::executor::ExecutionReceipt, swappers::model::SwapExecution};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizerErrorKind {
    Retryable,
    Permanent,
    BadDebtAmountFloor,
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

#[async_trait]
pub trait Finalizer: Send + Sync {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String>;

    /// Classify an error without exposing implementation-specific sentinels to
    /// the pipeline stage that owns retry scheduling.
    fn classify_error(&self, _error: &str) -> FinalizerErrorKind {
        FinalizerErrorKind::Retryable
    }
}
