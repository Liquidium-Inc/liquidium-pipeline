use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;

use crate::stages::executor::ExecutionReceipt;
pub mod sqlite;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ResultStatus {
    Enqueued = 0,
    InFlight = 1,
    Succeeded = 2,
    FailedRetryable = 3,
    FailedPermanent = 4,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LiqMetaWrapper {
    pub receipt: ExecutionReceipt,
    pub meta: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct LiqResultRecord {
    pub id: String,
    pub status: ResultStatus,
    pub attempt: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub meta_json: String,
}

#[allow(dead_code)]
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait WalStore: Send + Sync {
    async fn upsert_result(&self, row: LiqResultRecord) -> Result<()>;
    async fn get_result(&self, liq_id: &str) -> Result<Option<LiqResultRecord>>;
    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> Result<Vec<LiqResultRecord>>;
    async fn get_pending(&self, limit: usize) -> Result<Vec<LiqResultRecord>>;
    async fn update_status(&self, liq_id: &str, next: ResultStatus, bump_attempt: bool) -> Result<()>;
    async fn delete(&self, liq_id: &str) -> Result<()>;
}

pub fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}
