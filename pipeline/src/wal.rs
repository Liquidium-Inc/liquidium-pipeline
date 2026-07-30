use liquidium_pipeline_core::types::protocol_types::LiquidationResult;
use serde::Serialize;

use crate::{
    persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
};

pub fn decode_receipt_wrapper(row: &LiqResultRecord) -> Result<Option<LiqMetaWrapper>, String> {
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
                profit_snapshot: None,
                venue_execution: None,
                meta_v2: None,
            })),
            Err(receipt_err) => Err(format!(
                "invalid meta_json for {}: wrapper_err={}; receipt_err={}",
                row.id, wrapper_err, receipt_err
            )),
        },
    }
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

pub async fn wal_mark_retryable_failed(wal: &dyn WalStore, liq_id: &str, last_error: String) -> Result<(), String> {
    wal.update_failure(liq_id, ResultStatus::FailedRetryable, last_error, true)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_permanent_failed(wal: &dyn WalStore, liq_id: &str, last_error: String) -> Result<(), String> {
    wal.update_failure(liq_id, ResultStatus::FailedPermanent, last_error, true)
        .await
        .map_err(|e| e.to_string())
}

/// Parks a row this build cannot resume, keeping the reason visible.
///
/// Distinct from a permanent failure: nothing about the liquidation is wrong,
/// so the row stays available for an operator to requeue once the binary or
/// configuration can read it again.
pub async fn wal_mark_unresumable(wal: &dyn WalStore, liq_id: &str, last_error: String) -> Result<(), String> {
    wal.update_failure(liq_id, ResultStatus::Unresumable, last_error, false)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_enqueued(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::Enqueued, true)
        .await
        .map_err(|e| e.to_string())
}

pub async fn wal_mark_operator_required(wal: &dyn WalStore, liq_id: &str) -> Result<(), String> {
    wal.update_status(liq_id, ResultStatus::OperatorRequired, false)
        .await
        .map_err(|e| e.to_string())
}

/// Parks a row for an operator while preserving the failure that caused it.
/// Used when the retry budget runs out on a row whose venue leg may still hold
/// funds, where the diagnostic matters as much as the status change.
pub async fn wal_mark_operator_required_with_error(
    wal: &dyn WalStore,
    liq_id: &str,
    last_error: String,
) -> Result<(), String> {
    wal.update_failure(liq_id, ResultStatus::OperatorRequired, last_error, false)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use serde_json::json;

    use crate::executors::executor::ExecutorRequest;
    use crate::persistance::VenueExecutionState;
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::icpswap::{
        identity::IcpswapExecutionIdentity,
        transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
        types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep},
    };
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
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
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
        assert!(wrapper.profit_snapshot.is_none());
        assert!(wrapper.venue_execution.is_none());
        assert!(wrapper.meta_v2.is_none());
    }

    #[test]
    fn decode_receipt_only_fallback_sets_finalizer_decision_none() {
        let receipt = make_receipt();
        let row = make_row(serde_json::to_string(&receipt).expect("receipt json should serialize"));

        let wrapper = decode_receipt_wrapper(&row)
            .expect("fallback decode should succeed")
            .expect("wrapper should exist");
        assert!(wrapper.finalizer_decision.is_none());
        assert!(wrapper.profit_snapshot.is_none());
        assert!(wrapper.venue_execution.is_none());
        assert!(wrapper.meta_v2.is_none());
        assert!(wrapper.meta.is_empty());
    }

    #[test]
    fn icpswap_execution_state_round_trips_in_existing_meta_json() {
        #[derive(serde::Deserialize)]
        #[serde(tag = "venue", content = "state", rename_all = "snake_case")]
        enum TaggedVenueExecutionState {
            Icpswap(IcpswapExecutionState),
        }

        let receipt = make_receipt();
        let token_in = ChainToken::Icp {
            ledger: Principal::from_slice(&[1]),
            symbol: "INPUT".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        };
        let token_out = ChainToken::Icp {
            ledger: Principal::from_slice(&[2]),
            symbol: "OUTPUT".to_string(),
            decimals: 6,
            fee: Nat::from(20u64),
        };
        let plan = IcpswapExecutionPlan::new(
            Principal::from_slice(&[3]),
            Principal::from_slice(&[1]),
            Principal::from_slice(&[2]),
            Nat::from(3_000u64),
            ChainTokenAmount::from_raw(token_in.clone(), Nat::from(100_000u64)),
            ChainTokenAmount::from_raw(token_in.clone(), Nat::from(10u64)),
            ChainTokenAmount::from_raw(token_out.clone(), Nat::from(120_000u64)),
            ChainTokenAmount::from_raw(token_out.clone(), Nat::from(20u64)),
            100,
        )
        .expect("plan");
        let (identity, _) = IcpswapExecutionIdentity::derive(
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            "42",
        )
        .expect("identity");
        let owner = Account {
            owner: identity.principal,
            subaccount: None,
        };
        let mut state = IcpswapExecutionState::prepare(
            "42",
            plan.clone(),
            identity,
            IcpswapFundingState::new(
                Account {
                    owner: Principal::from_slice(&[4]),
                    subaccount: None,
                },
                owner,
                plan.input_ledger_fee.clone(),
            ),
            IcpswapSettlementState {
                kind: None,
                destination: Account {
                    owner: Principal::from_slice(&[5]),
                    subaccount: None,
                },
                fee: plan.output_ledger_fee.clone(),
                transfer: IcpswapLedgerTransferState::default(),
                interrupted_transfer: None,
                interrupted_observed_debit: None,
                recovery_credit: None,
                residual_dust: None,
            },
        )
        .expect("state");
        state.step = IcpswapStep::TradePending;
        state.transfer.block_index = Some(Nat::from(77u64));
        state.trade.input_pool_balance_before = Some(Nat::from(100_000u64));
        state.trade.output_pool_balance_before = Some(Nat::from(42u64));
        state.trade.gross_output_amount = Some(Nat::from(119_500u64));
        state.last_error = Some("waiting for asynchronous output".to_string());

        let mut row = make_row("{}".to_string());
        let wrapper = LiqMetaWrapper {
            receipt,
            meta: vec![1, 2, 3],
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: Some(VenueExecutionState::new(crate::swappers::icpswap::VENUE_ID, &state).unwrap()),
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode wrapper");
        let encoded: serde_json::Value = serde_json::from_str(&row.meta_json).expect("encoded wrapper json");
        assert_eq!(encoded["venue_execution"]["venue"], "icpswap");
        assert!(encoded["venue_execution"]["state"].is_object());
        let tagged: TaggedVenueExecutionState = serde_json::from_value(encoded["venue_execution"].clone())
            .expect("the previous tagged-enum schema must decode the new representation");
        let TaggedVenueExecutionState::Icpswap(tagged_state) = tagged;
        assert_eq!(tagged_state, state);

        let decoded = decode_receipt_wrapper(&row)
            .expect("decode wrapper")
            .expect("wrapper exists");
        assert_eq!(decoded.meta, vec![1, 2, 3]);
        let encoded_state = VenueExecutionState::new(crate::swappers::icpswap::VENUE_ID, &state).unwrap();
        assert_eq!(decoded.venue_execution, Some(encoded_state));
        let decoded_state: IcpswapExecutionState = decoded
            .venue_execution
            .expect("ICPSwap state should exist")
            .decode(crate::swappers::icpswap::VENUE_ID)
            .expect("decode state")
            .expect("wrong venue");
        assert_eq!(decoded_state.plan, plan);
    }

    #[test]
    fn decode_wrapper_with_profit_snapshot_round_trips() {
        let receipt = make_receipt();
        let row = make_row(
            json!({
                "receipt": receipt,
                "meta": [],
                "finalizer_decision": null,
                "profit_snapshot": {
                    "expected_profit_raw": "1000",
                    "realized_profit_raw": "1200",
                    "debt_symbol": "ckUSDT",
                    "debt_decimals": 6,
                    "updated_at": 123
                }
            })
            .to_string(),
        );

        let wrapper = decode_receipt_wrapper(&row)
            .expect("wrapper decode should succeed")
            .expect("wrapper should exist");
        let snapshot = wrapper.profit_snapshot.expect("profit snapshot should exist");
        assert_eq!(snapshot.expected_profit_raw, "1000");
        assert_eq!(snapshot.realized_profit_raw.as_deref(), Some("1200"));
        assert_eq!(snapshot.debt_symbol, "ckUSDT");
        assert_eq!(snapshot.debt_decimals, 6);
        assert_eq!(snapshot.updated_at, 123);
    }

    #[test]
    fn decode_legacy_receipt_defaults_missing_request_fields() {
        let receipt = make_receipt();
        let mut legacy = serde_json::to_value(&receipt).expect("receipt json value should serialize");
        let request = legacy["request"]
            .as_object_mut()
            .expect("legacy request should be object");
        request.remove("min_collateral_amount");
        request.remove("debt_ref_price");

        let row = make_row(legacy.to_string());
        let wrapper = decode_receipt_wrapper(&row)
            .expect("legacy decode should succeed")
            .expect("wrapper should exist");

        assert_eq!(wrapper.receipt.request.min_collateral_amount, Nat::from(0u8));
        assert_eq!(wrapper.receipt.request.debt_ref_price, Nat::from(0u8));
    }
}
