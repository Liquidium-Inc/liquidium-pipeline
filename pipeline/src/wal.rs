use liquidium_pipeline_commons::error::{CodedError, ErrorCode, format_with_code};
use liquidium_pipeline_core::types::protocol_types::LiquidationResult;
use serde::Serialize;
use thiserror::Error;

use crate::{
    persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
};

pub type WalResult<T> = Result<T, WalError>;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("invalid meta_json for {liq_id}: wrapper_err={wrapper_err}; receipt_err={receipt_err}")]
    DecodeMeta {
        liq_id: String,
        wrapper_err: String,
        receipt_err: String,
    },
    #[error("failed to serialize meta_json for {liq_id}: {details}")]
    EncodeMeta { liq_id: String, details: String },
    #[error("missing liquidation_result in receipt")]
    MissingLiquidationResult,
    #[error("wal operation failed ({action}): {details}")]
    Store { action: &'static str, details: String },
}

impl WalError {
    fn store(action: &'static str, err: impl std::fmt::Display) -> Self {
        WalError::Store {
            action,
            details: err.to_string(),
        }
    }
}

impl CodedError for WalError {
    fn code(&self) -> ErrorCode {
        match self {
            WalError::DecodeMeta { .. } => ErrorCode::PipelineWal,
            WalError::EncodeMeta { .. } => ErrorCode::PipelineWal,
            WalError::MissingLiquidationResult => ErrorCode::PipelineWal,
            WalError::Store { .. } => ErrorCode::PipelineWal,
        }
    }
}

impl From<WalError> for String {
    fn from(value: WalError) -> Self {
        format_with_code(&value)
    }
}

pub fn decode_receipt_wrapper(row: &LiqResultRecord) -> WalResult<Option<LiqMetaWrapper>> {
    if row.meta_json.is_empty() || row.meta_json == "{}" {
        return Ok(None);
    }

    match serde_json::from_str::<LiqMetaWrapper>(&row.meta_json) {
        Ok(wrapper) => Ok(Some(wrapper)),
        Err(wrapper_err) => match serde_json::from_str::<ExecutionReceipt>(&row.meta_json) {
            Ok(receipt) => Ok(Some(LiqMetaWrapper {
                receipt,
                meta: Vec::new(),
                finalizer_decision: None,
            })),
            Err(receipt_err) => Err(WalError::DecodeMeta {
                liq_id: row.id.clone(),
                wrapper_err: wrapper_err.to_string(),
                receipt_err: receipt_err.to_string(),
            }),
        },
    }
}

pub fn encode_meta<T: Serialize>(row: &mut LiqResultRecord, meta: &T) -> WalResult<()> {
    row.meta_json = serde_json::to_string(meta).map_err(|e| WalError::EncodeMeta {
        liq_id: row.id.clone(),
        details: e.to_string(),
    })?;
    Ok(())
}

//
// Helper to extract liq_id from ExecutionReceipt
//

pub fn liq_id_from_receipt(receipt: &ExecutionReceipt) -> WalResult<String> {
    let liq: &LiquidationResult = receipt
        .liquidation_result
        .as_ref()
        .ok_or(WalError::MissingLiquidationResult)?;

    Ok(liq.id.to_string())
}

//
// WAL wrappers for finalizer
//
pub async fn wal_load(wal: &dyn WalStore, liq_id: &str) -> WalResult<Option<LiqResultRecord>> {
    wal.get_result(liq_id)
        .await
        .map_err(|e| WalError::store("load result", e))
}

pub async fn wal_mark_inflight(wal: &dyn WalStore, liq_id: &str) -> WalResult<()> {
    wal.update_status(liq_id, ResultStatus::InFlight, true)
        .await
        .map_err(|e| WalError::store("mark inflight", e))
}

pub async fn wal_mark_succeeded(wal: &dyn WalStore, liq_id: &str) -> WalResult<()> {
    wal.update_status(liq_id, ResultStatus::Succeeded, true)
        .await
        .map_err(|e| WalError::store("mark succeeded", e))
}

pub async fn wal_mark_retryable_failed(wal: &dyn WalStore, liq_id: &str, last_error: String) -> WalResult<()> {
    wal.update_failure(liq_id, ResultStatus::FailedRetryable, last_error, true)
        .await
        .map_err(|e| WalError::store("mark retryable failed", e))
}

pub async fn wal_mark_permanent_failed(wal: &dyn WalStore, liq_id: &str, last_error: String) -> WalResult<()> {
    wal.update_failure(liq_id, ResultStatus::FailedPermanent, last_error, true)
        .await
        .map_err(|e| WalError::store("mark permanent failed", e))
}

pub async fn wal_mark_enqueued(wal: &dyn WalStore, liq_id: &str) -> WalResult<()> {
    wal.update_status(liq_id, ResultStatus::Enqueued, true)
        .await
        .map_err(|e| WalError::store("mark enqueued", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use serde_json::json;

    use crate::executors::executor::ExecutorRequest;
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::SwapRequest;

    fn make_receipt() -> ExecutionReceipt {
        let collateral = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(100u64),
        };
        let debt = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(100u64),
        };

        let request = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(1_000u64),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: Some(SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_formatted(collateral.clone(), 1.0),
                receive_asset: debt.asset_id(),
                receive_address: Some("dest".to_string()),
                max_slippage_bps: Some(100),
                venue_hint: None,
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 1,
            ref_price: Nat::from(1u64),
            debt_approval_needed: false,
        };

        ExecutionReceipt {
            request,
            liquidation_result: Some(LiquidationResult {
                id: 42,
                timestamp: 0,
                amounts: LiquidationAmounts {
                    collateral_received: Nat::from(2_000u64),
                    debt_repaid: Nat::from(1_000u64),
                },
                collateral_asset: AssetType::Unknown,
                debt_asset: AssetType::Unknown,
                status: LiquidationStatus::Success,
                change_tx: TxStatus {
                    tx_id: None,
                    status: TransferStatus::Success,
                },
                collateral_tx: TxStatus {
                    tx_id: None,
                    status: TransferStatus::Success,
                },
            }),
            status: ExecutionStatus::Success,
            change_received: true,
        }
    }

    fn make_row(meta_json: String) -> LiqResultRecord {
        LiqResultRecord {
            id: "42".to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json,
        }
    }

    #[test]
    fn decode_wrapper_without_finalizer_decision_defaults_to_none() {
        let receipt = make_receipt();
        let row = make_row(
            json!({
                "receipt": receipt,
                "meta": []
            })
            .to_string(),
        );

        let wrapper = decode_receipt_wrapper(&row)
            .expect("wrapper decode should succeed")
            .expect("wrapper should exist");
        assert!(wrapper.finalizer_decision.is_none());
    }

    #[test]
    fn decode_receipt_only_fallback_sets_finalizer_decision_none() {
        let receipt = make_receipt();
        let row = make_row(serde_json::to_string(&receipt).expect("receipt json should serialize"));

        let wrapper = decode_receipt_wrapper(&row)
            .expect("fallback decode should succeed")
            .expect("wrapper should exist");
        assert!(wrapper.finalizer_decision.is_none());
        assert!(wrapper.meta.is_empty());
    }
}
