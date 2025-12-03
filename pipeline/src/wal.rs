use liquidium_pipeline_core::types::protocol_types::LiquidationResult;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    persistance::{LiqResultRecord, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
};

pub fn decode_meta<T: DeserializeOwned>(row: &LiqResultRecord) -> Result<Option<T>, String> {
    if row.meta_json.is_empty() || row.meta_json == "{}" {
        return Ok(None);
    }

    serde_json::from_str(&row.meta_json).map_err(|e| format!("invalid meta_json for {}: {}", row.id, e))
}

pub fn encode_meta<T: Serialize>(row: &mut LiqResultRecord, meta: &T) -> Result<(), String> {
    row.meta_json =
        serde_json::to_string(meta).map_err(|e| format!("failed to serialize meta_json for {}: {}", row.id, e))?;
    Ok(())
}

//
// Helper to extract liq_id from ExecutionReceipt
//

pub fn liq_id_from_receipt(receipt: &ExecutionReceipt) -> Result<String, String> {
    let liq: &LiquidationResult = receipt
        .liquidation_result
        .as_ref()
        .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;

    Ok(liq.id.to_string())
}

//
// WAL wrappers for finalizer
//
pub async fn wal_load(wal: &dyn WalStore, liq_id: &str) -> Result<Option<LiqResultRecord>, String> {
    wal.get_result(liq_id).await.map_err(|e| e.to_string())
}

pub async fn wal_mark_inflight(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::InFlight, true)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_succeeded(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::Succeeded, true)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_retryable_failed(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::FailedRetryable, true)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_permanent_failed(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::FailedPermanent, true)
        .await
        .map_err(|e| e.to_string())
}


pub async fn wal_mark_enqueued(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::Enqueued, true)
        .await
        .map_err(|e| e.to_string())
}
