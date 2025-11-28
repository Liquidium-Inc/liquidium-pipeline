use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    finalizers::dex_finalizer::DexFinalizerLogic,
    swappers::{
        model::{SwapExecution, SwapRequest},
        swap_interface::SwapInterface,
    },
};

pub struct KongSwapFinalizer<S>
where
    S: SwapInterface,
{
    pub swapper: Arc<S>,
}

impl<S> KongSwapFinalizer<S>
where
    S: SwapInterface,
{
    pub fn new(swapper: Arc<S>) -> Self {
        Self { swapper }
    }
}

#[async_trait]
impl<S> DexFinalizerLogic for KongSwapFinalizer<S>
where
    S: SwapInterface + Send + Sync,
{
    async fn swap(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        self.swapper.execute(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::finalizer::Finalizer;
    use crate::persistance::{LiqResultRecord, MockWalStore, ResultStatus};
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
    use crate::swappers::model::{SwapExecution, SwapRequest};
    use crate::swappers::swap_interface::MockSwapInterface;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::asset_id::AssetId;
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;
    use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };

    fn make_test_swap_execution() -> SwapExecution {
        SwapExecution {
            swap_id: 1,
            request_id: 1,
            status: "ok".to_string(),

            pay_asset: AssetId {
                chain: "icp".to_string(),
                address: "ledger".to_string(),
                symbol: "ckBTC".to_string(),
            },
            pay_amount: Nat::from(1_000_000u64),

            receive_asset: AssetId {
                chain: "icp".to_string(),
                address: "ledger-usdt".to_string(),
                symbol: "ckUSDT".to_string(),
            },
            receive_amount: Nat::from(2_000_000u64),

            mid_price: 1.0,
            exec_price: 1.0,
            slippage: 0.0,

            legs: vec![],
            ts: 0,
        }
    }

    fn make_default_request() -> SwapRequest {
        // Build a structurally valid SwapRequest for tests
        let pay_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let pay_amount = ChainTokenAmount::from_raw(
            pay_token.clone(),
            Nat::from(1_000_000u64), // 0.01 if 8 decimals
        );

        let pay_asset: AssetId = pay_token.asset_id();

        let receive_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };

        let receive_asset: AssetId = receive_token.asset_id();

        SwapRequest {
            pay_asset,
            pay_amount,
            receive_asset,
            receive_address: Some("test-address".to_string()),
            max_slippage_bps: Some(100),          // 1%
            venue_hint: Some("kong".to_string()), // matches venue
        }
    }

    fn make_execution_receipt(liq_id: u128) -> ExecutionReceipt {
        // Minimal ExecutorRequest: we only care about swap_args for the finalizer
        let req = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(0u32),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: Some(make_default_request()),
            debt_asset: ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckBTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            },
            collateral_asset: ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckUSDT".to_string(),
                decimals: 6,
                fee: Nat::from(1_000u64),
            },
            expected_profit: 0,
        };

        let liq = LiquidationResult {
            id: liq_id,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(0u32),
                debt_repaid: Nat::from(0u32),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
        };

        ExecutionReceipt {
            request: req,
            liquidation_result: Some(liq),
            status: ExecutionStatus::Success,
            change_received: false,
        }
    }

    #[tokio::test]
    async fn kong_swap_finalizer_delegates_to_swapper_success() {
        let mut mock_swapper = MockSwapInterface::new();

        // We don't care about the exact request here, just that execute is called once
        mock_swapper
            .expect_execute()
            .times(1)
            .returning(|_req: &SwapRequest| Ok(make_test_swap_execution()));

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(mock_swapper),
        };

        let req = make_default_request();

        let res = finalizer.swap(&req).await.expect("swap should succeed");

        // Basic sanity: we got some SwapExecution back
        // (structure is validated elsewhere, here we only check delegation works)
        let _ = res;
    }

    #[tokio::test]
    async fn kong_swap_finalizer_propagates_errors() {
        let mut mock_swapper = MockSwapInterface::new();

        mock_swapper
            .expect_execute()
            .times(1)
            .returning(|_req: &SwapRequest| Err("boom".to_string()));

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(mock_swapper),
        };

        let req = make_default_request();

        let err = finalizer.swap(&req).await.expect_err("swap should fail");

        assert_eq!(err, "boom".to_string());
    }

    #[tokio::test]
    async fn kong_finalize_idempotent_succeeded_skips_swap_and_reads_wal() {
        use mockall::predicate::eq;

        let mut wal = MockWalStore::new();
        let mut swapper = MockSwapInterface::new();

        // No swap should be executed when WAL already has Succeeded
        swapper.expect_execute().times(0);

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(swapper),
        };

        let receipt = make_execution_receipt(42);
        let swap_exec = make_test_swap_execution();

        // Encode meta_json as production does
        let meta_json = serde_json::json!({
            "swap": swap_exec
        })
        .to_string();

        wal.expect_get_result().with(eq("42")).returning(move |_| {
            Ok(Some(LiqResultRecord {
                liq_id: "42".to_string(),
                status: ResultStatus::Succeeded,
                attempt: 3,
                created_at: 0,
                updated_at: 0,
                meta_json: meta_json.clone(),
            }))
        });

        // Call batch finalizer with a single receipt
        let results = finalizer
            .finalize(&wal, std::slice::from_ref(&receipt).to_vec())
            .await
            .expect("finalize should succeed");

        assert_eq!(results.len(), 1);
        // On idempotent path we expect to read back the swap from WAL
        assert_eq!(results[0].swap_result.as_ref().unwrap().swap_id, swap_exec.swap_id);
    }

    #[tokio::test]
    async fn kong_finalize_retryable_then_success_updates_wal_and_stores_meta() {
        use mockall::predicate::eq;

        let mut wal = MockWalStore::new();
        let mut swapper = MockSwapInterface::new();

        let swap_exec = make_test_swap_execution();

        // 1. WAL: get_result returns FailedRetryable row for liq_id 43
        wal.expect_get_result().with(eq("43")).returning(|_| {
            Ok(Some(LiqResultRecord {
                liq_id: "43".to_string(),
                status: ResultStatus::FailedRetryable,
                attempt: 1,
                created_at: 0,
                updated_at: 0,
                meta_json: "{}".to_string(),
            }))
        });

        // 2. WAL: mark InFlight
        wal.expect_update_status()
            .with(eq("43"), eq(ResultStatus::InFlight), eq(true))
            .returning(|_, _, _| Ok(()));

        // 3. WAL: upsert_result with Succeeded and non-empty meta_json
        wal.expect_upsert_result().times(1).returning(|row: LiqResultRecord| {
            assert_eq!(row.status, ResultStatus::Succeeded);
            assert!(!row.meta_json.is_empty());
            Ok(())
        });

        // 4. Swap: execute once and return swap_exec
        swapper
            .expect_execute()
            .times(1)
            .returning(move |_req: &SwapRequest| Ok(swap_exec.clone()));

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(swapper),
        };

        let receipt = make_execution_receipt(43);

        let results = finalizer
            .finalize(&wal, std::slice::from_ref(&receipt).to_vec())
            .await
            .expect("finalize should succeed");

        assert_eq!(results.len(), 1);
        assert!(results[0].swap_result.is_some());
    }

    #[tokio::test]
    async fn kong_finalize_marks_failed_permanent_after_too_many_attempts() {
        use mockall::predicate::eq;

        let mut wal = MockWalStore::new();
        let mut swapper = MockSwapInterface::new();

        // WAL: get_result returns a FailedRetryable row with high attempt count for liq_id 44
        wal.expect_get_result().with(eq("44")).returning(|_| {
            Ok(Some(LiqResultRecord {
                liq_id: "44".to_string(),
                status: ResultStatus::FailedRetryable,
                attempt: 5, // assuming MAX_FINALIZER_ATTEMPTS = 5
                created_at: 0,
                updated_at: 0,
                meta_json: "{}".to_string(),
            }))
        });

        // First mark InFlight
        wal.expect_update_status()
            .with(eq("44"), eq(ResultStatus::InFlight), eq(true))
            .returning(|_, _, _| Ok(()));

        // Then mark FailedPermanent after swap error
        wal.expect_update_status()
            .with(eq("44"), eq(ResultStatus::FailedPermanent), eq(true))
            .returning(|_, _, _| Ok(()));

        // Swap: execute fails once
        swapper
            .expect_execute()
            .times(1)
            .returning(|_req: &SwapRequest| Err("boom".to_string()));

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(swapper),
        };

        let receipt = make_execution_receipt(44);

        let err = finalizer
            .finalize(&wal, std::slice::from_ref(&receipt).to_vec())
            .await
            .expect_err("finalize should fail");

        assert_eq!(err, "boom".to_string());
    }
}
