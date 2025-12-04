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
    use crate::persistance::MockWalStore;
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
            ref_price: Nat::from(0u8),
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
    async fn kong_finalize_uses_swap_args_and_sets_swap_result() {
        let wal = MockWalStore::new();
        let mut swapper = MockSwapInterface::new();

        swapper
            .expect_execute()
            .times(1)
            .returning(|req: &SwapRequest| {
                // basic sanity: finalize passes the swap_args through to the swapper
                assert_eq!(req.pay_asset.symbol, "ckBTC");
                assert_eq!(req.receive_asset.symbol, "ckUSDT");
                Ok(make_test_swap_execution())
            });

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(swapper),
        };

        let receipt = make_execution_receipt(42);

        let result = finalizer
            .finalize(&wal, receipt)
            .await
            .expect("finalize should succeed");

        let swap = result.swap_result.expect("swap_result should be present");
        assert_eq!(swap.swap_id, 1);
        assert_eq!(swap.pay_asset.symbol, "ckBTC");
        assert_eq!(swap.receive_asset.symbol, "ckUSDT");
    }

    #[tokio::test]
    async fn kong_finalize_propagates_swap_errors() {
        let wal = MockWalStore::new();
        let mut swapper = MockSwapInterface::new();

        swapper
            .expect_execute()
            .returning(|_req: &SwapRequest| Err("boom".to_string()));

        let finalizer = KongSwapFinalizer {
            swapper: Arc::new(swapper),
        };

        let receipt = make_execution_receipt(44);

        let err = finalizer
            .finalize(&wal, receipt)
            .await
            .expect_err("finalize should fail");

        assert_eq!(err, "boom".to_string());
    }
}
