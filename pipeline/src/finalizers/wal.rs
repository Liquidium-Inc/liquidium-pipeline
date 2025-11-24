use std::sync::Arc;

use liquidium_pipeline_core::types::protocol_types::LiquidationResult;
use serde::{Deserialize, Serialize};

use crate::{
    persistance::{LiqResultRecord, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
    swappers::model::SwapExecution,
};

//
// Meta stored inside LiqResultRecord.meta_json
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FinalizerMeta {
    pub swap: Option<SwapExecution>,
}

pub fn decode_meta(row: &LiqResultRecord) -> Result<FinalizerMeta, String> {
    if row.meta_json.is_empty() || row.meta_json == "{}" {
        return Ok(FinalizerMeta::default());
    }

    serde_json::from_str(&row.meta_json).map_err(|e| format!("invalid meta_json for {}: {}", row.liq_id, e))
}

pub fn encode_meta(row: &mut LiqResultRecord, meta: &FinalizerMeta) -> Result<(), String> {
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
    swap: Option<SwapExecution>,
) -> Result<(), String> {
    row.status = ResultStatus::Succeeded;

    let meta = FinalizerMeta { swap };
    encode_meta(&mut row, &meta)?;

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

pub struct FinalizerWal<D: WalStore> {
    db: Arc<D>,
}

impl<D: WalStore> FinalizerWal<D> {
    pub fn new(db: Arc<D>) -> Self {
        Self { db }
    }

    pub async fn load(&self, liq_id: &str) -> Result<Option<LiqResultRecord>, String> {
        self.db.get_result(liq_id).await.map_err(|e| e.to_string())
    }

    // Mark the record as "in flight" for finalization (retryable)
    pub async fn mark_finalizing(&self, liq_id: &str) -> Result<(), String> {
        self.db
            .update_status(liq_id, ResultStatus::InFlight, true)
            .await
            .map_err(|e| e.to_string())
    }

    // Mark the record as succeeded and persist the swap result into meta_json
    pub async fn mark_finalized(&self, mut row: LiqResultRecord, swap: Option<SwapExecution>) -> Result<(), String> {
        row.status = ResultStatus::Succeeded;

        let meta = FinalizerMeta { swap };
        encode_meta(&mut row, &meta)?;

        self.db.upsert_result(row).await.map_err(|e| e.to_string())
    }

    // Mark the record as a retryable failure for finalization
    pub async fn mark_failed(&self, liq_id: &str) -> Result<(), String> {
        self.db
            .update_status(liq_id, ResultStatus::FailedRetryable, true)
            .await
            .map_err(|e| e.to_string())
    }
}
