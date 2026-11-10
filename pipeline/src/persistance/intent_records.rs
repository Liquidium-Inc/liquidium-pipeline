//! Liquidation intents: saved before the executor call, decided after it.
use anyhow::Result;
use async_trait::async_trait;

use crate::executors::executor::ExecutorRequest;
use crate::stages::executor::ExecutionReceipt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum LiquidationIntentStatus {
    Submitting = 0,
    Accepted = 1,
    Failed = 2,
    Ambiguous = 3,
}

impl TryFrom<i32> for LiquidationIntentStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Submitting),
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Ambiguous),
            _ => anyhow::bail!("unknown liquidation intent status {value}"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LiquidationIntentRecord {
    pub intent_id: String,
    pub liquidation_id: Option<String>,
    pub status: LiquidationIntentStatus,
    pub request_json: String,
    pub receipt_json: Option<String>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[allow(dead_code)]
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LiquidationIntentStore: Send + Sync {
    async fn create_submitting(&self, intent_id: &str, request: &ExecutorRequest) -> Result<()>;
    async fn mark_accepted(&self, intent_id: &str, liquidation_id: &str, receipt: &ExecutionReceipt) -> Result<()>;
    async fn mark_failed(
        &self,
        intent_id: &str,
        liquidation_id: Option<String>,
        receipt: Option<ExecutionReceipt>,
        error: &str,
    ) -> Result<()>;
    async fn mark_ambiguous(&self, intent_id: &str, error: &str, receipt: Option<ExecutionReceipt>) -> Result<()>;
    async fn recover_submitting_as_ambiguous(&self, reason: &str) -> Result<usize>;
    async fn get_intent(&self, intent_id: &str) -> Result<Option<LiquidationIntentRecord>>;
}
