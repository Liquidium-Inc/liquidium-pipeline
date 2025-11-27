use liquidium_pipeline_core::types::protocol_types::LiquidationResult;

use crate::{
    finalizers::liquidation_outcome::LiquidationOutcome,
    persistance::{LiqResultRecord, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
};

pub fn decode_meta(row: &LiqResultRecord) -> Result<Option<LiquidationOutcome>, String> {
    if row.meta_json.is_empty() || row.meta_json == "{}" {
        return Ok(None);
    }

    serde_json::from_str(&row.meta_json).map_err(|e| format!("invalid meta_json for {}: {}", row.liq_id, e))
}

pub fn encode_meta(row: &mut LiqResultRecord, meta: &LiquidationOutcome) -> Result<(), String> {
    row.meta_json =
        serde_json::to_string(meta).map_err(|e| format!("failed to serialize meta_json for {}: {}", row.liq_id, e))?;
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

pub async fn wal_mark_succeeded(
    wal: &dyn WalStore,
    mut row: LiqResultRecord,
    outcome: LiquidationOutcome,
) -> Result<(), String> {
    row.status = ResultStatus::Succeeded;

    encode_meta(&mut row, &outcome)?;

    wal.upsert_result(row).await.map_err(|e| e.to_string())
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
