use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::stages::executor::ExecutionReceipt;
pub mod sqlite;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ResultStatus {
    Enqueued = 0,
    InFlight = 1,
    Succeeded = 2,
    FailedRetryable = 3,
    FailedPermanent = 4,
    WaitingCollateral = 5,
    WaitingProfit = 6,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LiqMetaWrapper {
    pub receipt: ExecutionReceipt,
    pub meta: Vec<u8>,
    #[serde(default)]
    pub finalizer_decision: Option<FinalizerDecisionSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalizerDecisionSnapshot {
    pub mode: String,
    pub chosen: String,
    pub reason: String,
    pub min_required_bps: f64,
    pub dex_preview_gross_bps: Option<f64>,
    pub dex_preview_net_bps: Option<f64>,
    pub cex_preview_gross_bps: Option<f64>,
    pub cex_preview_net_bps: Option<f64>,
    pub ts: i64,
}

#[derive(Debug, Clone)]
pub struct LiqResultRecord {
    pub id: String,
    pub status: ResultStatus,
    pub attempt: i32,
    pub error_count: i32,
    pub last_error: Option<String>,
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
    async fn update_failure(
        &self,
        liq_id: &str,
        next: ResultStatus,
        last_error: String,
        bump_attempt: bool,
    ) -> Result<()>;
    async fn delete(&self, liq_id: &str) -> Result<()>;
}

pub fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}
