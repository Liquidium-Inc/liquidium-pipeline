use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::executors::executor::ExecutorRequest;
use crate::stages::executor::ExecutionReceipt;
pub mod finalizer_meta_v2;
pub mod liquidation_intake;
pub mod sqlite;

pub use finalizer_meta_v2::*;
pub use liquidation_intake::*;

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
    /// External custody is ambiguous and must be reconciled before retrying.
    /// Pending selection deliberately excludes this status.
    OperatorRequired = 7,
    /// The committed route cannot be reconstructed with the current binary or
    /// configuration. The row is visible to operators but never auto-polled.
    Unresumable = 8,
}

impl From<i32> for ResultStatus {
    /// Unknown discriminants decode as permanently failed so a row written by a
    /// newer binary is never picked up as pending work.
    fn from(value: i32) -> Self {
        match value {
            0 => ResultStatus::Enqueued,
            1 => ResultStatus::InFlight,
            2 => ResultStatus::Succeeded,
            3 => ResultStatus::FailedRetryable,
            4 => ResultStatus::FailedPermanent,
            5 => ResultStatus::WaitingCollateral,
            6 => ResultStatus::WaitingProfit,
            7 => ResultStatus::OperatorRequired,
            8 => ResultStatus::Unresumable,
            _ => ResultStatus::FailedPermanent,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LiqMetaWrapper {
    pub receipt: ExecutionReceipt,
    pub meta: Vec<u8>,
    #[serde(default)]
    pub finalizer_decision: Option<FinalizerDecisionSnapshot>,
    #[serde(default)]
    pub profit_snapshot: Option<WalProfitSnapshot>,
    #[serde(default)]
    pub venue_execution: Option<VenueExecutionState>,
    #[serde(default)]
    pub meta_v2: Option<FinalizerMetaV2>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VenueExecutionState {
    pub venue: String,
    pub state: serde_json::Value,
}

impl VenueExecutionState {
    pub fn new<T: Serialize>(venue: impl Into<String>, state: &T) -> Result<Self, String> {
        Ok(Self {
            venue: venue.into(),
            state: serde_json::to_value(state).map_err(|error| format!("failed to encode venue state: {error}"))?,
        })
    }

    pub fn is_venue(&self, venue: &str) -> bool {
        self.venue == venue
    }

    pub fn decode<T: DeserializeOwned>(&self, venue: &str) -> Result<Option<T>, String> {
        if !self.is_venue(venue) {
            return Ok(None);
        }
        serde_json::from_value(self.state.clone())
            .map(Some)
            .map_err(|error| format!("failed to decode {venue} execution state: {error}"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalProfitSnapshot {
    pub expected_profit_raw: String,
    pub realized_profit_raw: Option<String>,
    pub debt_symbol: String,
    pub debt_decimals: u8,
    pub updated_at: i64,
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
    #[serde(default)]
    pub multi_venue_allocation: Option<MultiVenueAllocationSnapshot>,
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

#[derive(Debug, Clone)]
pub struct LiquidationHandoff {
    pub sequence: i64,
    pub intent: LiquidationIntentRecord,
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
    async fn mark_ambiguous(&self, intent_id: &str, error: &str) -> Result<()>;
    async fn recover_submitting_as_ambiguous(&self, reason: &str) -> Result<usize>;
    async fn get_intent(&self, intent_id: &str) -> Result<Option<LiquidationIntentRecord>>;
    async fn list_handoffs_after(&self, sequence: i64, limit: usize) -> Result<Vec<LiquidationHandoff>>;
}

pub fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}
