use super::*;

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::transfer::actions::MockTransferActions;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus, TxStatus,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::config::MockConfigTrait;
use crate::error::{AppError, AppResult};
use crate::executors::executor::ExecutorRequest;
use crate::finalizers::cex_finalizer::{CexFinalizerLogic, CexRoutePreview, CexState};
use crate::finalizers::dex_finalizer::DexFinalizerLogic;
use crate::finalizers::finalizer::Finalizer;
use crate::finalizers::hybrid::utils::{RAY_PRICE_SCALE, RouteCandidate, RouteVenue, choose_best_route};
use crate::persistance::{FinalizerDecisionSnapshot, LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};
use crate::swappers::swap_interface::MockSwapInterface;
use crate::wal::{encode_meta, liq_id_from_receipt};

struct NoopDexFinalizer;

#[async_trait]
impl DexFinalizerLogic for NoopDexFinalizer {
    async fn swap(&self, _req: &SwapRequest) -> AppResult<SwapExecution> {
        Err("dex finalizer should not run".into())
    }
}

struct RecordingDexFinalizer {
    swap_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl DexFinalizerLogic for RecordingDexFinalizer {
    async fn swap(&self, req: &SwapRequest) -> AppResult<SwapExecution> {
        self.swap_calls.fetch_add(1, Ordering::SeqCst);
        Ok(SwapExecution {
            swap_id: 1,
            request_id: 1,
            status: "completed".to_string(),
            pay_asset: req.pay_asset.clone(),
            pay_amount: req.pay_amount.value.clone(),
            receive_asset: req.receive_asset.clone(),
            receive_amount: Nat::from(1_100u64),
            mid_price: 1.0,
            exec_price: 1.0,
            slippage: 0.0,
            legs: vec![],
            approval_count: None,
            ts: 0,
        })
    }
}

struct StubCexFinalizer {
    preview: AppResult<CexRoutePreview>,
}

#[async_trait]
impl CexFinalizerLogic for StubCexFinalizer {
    async fn prepare(&self, _liq_id: &str, _receipt: &ExecutionReceipt) -> AppResult<CexState> {
        Err("unused in hybrid tests".into())
    }

    async fn deposit(&self, _state: &mut CexState) -> AppResult<()> {
        Err("unused in hybrid tests".into())
    }

    async fn trade(&self, _state: &mut CexState) -> AppResult<()> {
        Err("unused in hybrid tests".into())
    }

    async fn withdraw(&self, _state: &mut CexState) -> AppResult<()> {
        Err("unused in hybrid tests".into())
    }

    async fn finish(&self, _receipt: &ExecutionReceipt, _state: &CexState) -> AppResult<SwapExecution> {
        Err("unused in hybrid tests".into())
    }

    async fn preview_route(&self, _receipt: &ExecutionReceipt) -> AppResult<CexRoutePreview> {
        self.preview.clone()
    }
}

struct TestWal {
    row: Mutex<Option<LiqResultRecord>>,
}

impl TestWal {
    fn empty() -> Self {
        Self { row: Mutex::new(None) }
    }

    fn with_receipt(receipt: &ExecutionReceipt) -> Self {
        Self::with_wrapper(LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
        })
    }

    fn with_receipt_and_meta(receipt: &ExecutionReceipt, meta: Vec<u8>) -> Self {
        Self::with_wrapper(LiqMetaWrapper {
            receipt: receipt.clone(),
            meta,
            finalizer_decision: None,
            profit_snapshot: None,
        })
    }

    fn with_wrapper(wrapper: LiqMetaWrapper) -> Self {
        let liq_id = liq_id_from_receipt(&wrapper.receipt).expect("liq id should exist");
        let mut row = LiqResultRecord {
            id: liq_id,
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: "{}".to_string(),
        };
        encode_meta(&mut row, &wrapper).expect("meta encode should succeed");
        Self {
            row: Mutex::new(Some(row)),
        }
    }

    fn snapshot(&self) -> Option<LiqResultRecord> {
        self.row.lock().expect("row lock poisoned").clone()
    }
}

#[async_trait]
impl WalStore for TestWal {
    async fn upsert_result(&self, row: LiqResultRecord) -> AppResult<()> {
        *self.row.lock().expect("row lock poisoned") = Some(row);
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> AppResult<Option<LiqResultRecord>> {
        Ok(self
            .row
            .lock()
            .expect("row lock poisoned")
            .clone()
            .filter(|row| row.id == liq_id))
    }

    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> AppResult<Vec<LiqResultRecord>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        Ok(self
            .row
            .lock()
            .expect("row lock poisoned")
            .clone()
            .filter(|row| row.status == status)
            .into_iter()
            .collect())
    }

    async fn get_pending(&self, limit: usize) -> AppResult<Vec<LiqResultRecord>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        Ok(self
            .row
            .lock()
            .expect("row lock poisoned")
            .clone()
            .filter(|row| {
                matches!(
                    row.status,
                    ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable
                )
            })
            .into_iter()
            .collect())
    }

    async fn update_status(&self, liq_id: &str, next: ResultStatus, bump_attempt: bool) -> AppResult<()> {
        if let Some(row) = self
            .row
            .lock()
            .expect("row lock poisoned")
            .as_mut()
            .filter(|row| row.id == liq_id)
        {
            row.status = next;
            if bump_attempt {
                row.attempt += 1;
            }
        }
        Ok(())
    }

    async fn update_failure(
        &self,
        liq_id: &str,
        next: ResultStatus,
        last_error: AppError,
        bump_attempt: bool,
    ) -> AppResult<()> {
        if let Some(row) = self
            .row
            .lock()
            .expect("row lock poisoned")
            .as_mut()
            .filter(|row| row.id == liq_id)
        {
            row.status = next;
            row.last_error = Some(last_error.to_string());
            if bump_attempt {
                row.attempt += 1;
            }
        }
        Ok(())
    }

    async fn delete(&self, liq_id: &str) -> AppResult<()> {
        let mut row = self.row.lock().expect("row lock poisoned");
        if row.as_ref().is_some_and(|existing| existing.id == liq_id) {
            *row = None;
        }
        Ok(())
    }
}

fn make_receipt(est_value_usd: f64) -> ExecutionReceipt {
    make_receipt_with_params(est_value_usd, 1_000u64, 100u64)
}

fn make_receipt_with_params(est_value_usd: f64, collateral_received: u64, collateral_fee: u64) -> ExecutionReceipt {
    let collateral = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(collateral_fee),
    };
    make_receipt_with_collateral(est_value_usd, collateral, collateral_received)
}

fn make_receipt_with_collateral(
    est_value_usd: f64,
    collateral_asset: ChainToken,
    collateral_received: u64,
) -> ExecutionReceipt {
    let debt = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let swap_req = SwapRequest {
        pay_asset: collateral_asset.asset_id(),
        pay_amount: ChainTokenAmount::from_formatted(collateral_asset.clone(), 1.0),
        receive_asset: debt.asset_id(),
        receive_address: Some("dest".to_string()),
        max_slippage_bps: Some(100),
        venue_hint: None,
    };

    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(1_000u64),
        receiver_address: Principal::anonymous(),
        buy_bad_debt: false,
    };

    let liq_result = LiquidationResult {
        id: 42,
        timestamp: 0,
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(collateral_received),
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
    };

    let ref_price_ray = (est_value_usd * RAY_PRICE_SCALE).round() as u128;
    let req = ExecutorRequest {
        liquidation,
        swap_args: Some(swap_req),
        debt_asset: debt,
        collateral_asset,
        expected_profit: 1,
        ref_price: Nat::from(ref_price_ray),
        debt_approval_needed: false,
    };

    ExecutionReceipt {
        request: req,
        liquidation_result: Some(liq_result),
        status: ExecutionStatus::Success,
        change_received: true,
    }
}

fn quote_from_req(req: &SwapRequest, receive_amount: u64, slippage: f64) -> SwapQuote {
    SwapQuote {
        pay_asset: req.pay_asset.clone(),
        pay_amount: req.pay_amount.value.clone(),
        receive_asset: req.receive_asset.clone(),
        receive_amount: Nat::from(receive_amount),
        mid_price: 1.0,
        exec_price: 1.0,
        slippage,
        legs: vec![],
    }
}

fn cex_preview(estimated_receive_native: f64) -> CexRoutePreview {
    CexRoutePreview {
        is_executable: true,
        // CEX preview returns decimal token units; tests define inputs in native debt units.
        estimated_receive_amount: estimated_receive_native / 1_000_000.0,
        estimated_slippage_bps: 0.0,
        reason: None,
    }
}

fn wal_wrapper(wal: &TestWal) -> LiqMetaWrapper {
    let row = wal.snapshot().expect("wal row should exist");
    serde_json::from_str::<LiqMetaWrapper>(&row.meta_json).expect("wrapper decode should succeed")
}

fn wal_snapshot(wal: &TestWal) -> FinalizerDecisionSnapshot {
    wal_wrapper(wal)
        .finalizer_decision
        .expect("finalizer decision snapshot should exist")
}

#[tokio::test]
async fn forced_dex_non_positive_preview_routes_to_recovery_and_uses_recovery_account() {
    let recovery_account = Account {
        owner: Principal::from_slice(&[1u8; 29]),
        subaccount: Some([9u8; 32]),
    };
    let expected_recovery = recovery_account;

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(expected_recovery);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 10.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .withf(move |_token, to, amount| {
            matches!(to, ChainAccount::Icp(acc) if acc == &expected_recovery) && amount.clone() == 900u64
        })
        .returning(|_, _, _| Ok("tx-1".to_string()));
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let res = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect("forced dex non-positive should route to recovery");
    assert!(res.finalized);
    assert_eq!(res.swapper.as_deref(), Some("recovery"));
}

#[tokio::test]
async fn forced_cex_non_positive_preview_routes_to_recovery_and_uses_recovery_account() {
    let recovery_account = Account {
        owner: Principal::from_slice(&[2u8; 29]),
        subaccount: Some([3u8; 32]),
    };
    let expected_recovery = recovery_account;

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Cex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(expected_recovery);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper.expect_quote().times(0);
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .withf(move |_token, to, amount| {
            matches!(to, ChainAccount::Icp(acc) if acc == &expected_recovery) && amount.clone() == 900u64
        })
        .returning(|_, _, _| Ok("tx-1".to_string()));
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(900.0)),
        })),
    };

    let res = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect("forced cex non-positive should route to recovery");
    assert_eq!(res.swapper.as_deref(), Some("recovery"));
}

#[tokio::test]
async fn hybrid_recovery_non_icp_does_not_transfer() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 4.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let collateral = ChainToken::EvmNative {
        chain: "eth".to_string(),
        symbol: "ETH".to_string(),
        decimals: 18,
        fee: Nat::from(100u64),
    };
    let receipt = make_receipt_with_collateral(10.0, collateral, 1_000u64);
    let wal = TestWal::empty();

    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let res = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("non-ICP recovery should skip transfer and succeed");
    assert_eq!(res.swapper.as_deref(), Some("recovery"));
}

#[tokio::test]
async fn forced_dex_positive_preview_executes_dex() {
    let swap_calls = Arc::new(AtomicUsize::new(0));

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 1_100u64, 12.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(RecordingDexFinalizer {
            swap_calls: swap_calls.clone(),
        }),
        cex_finalizer: None,
    };

    let res = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect("forced dex positive should execute dex");
    assert_eq!(res.swapper.as_deref(), Some("kong"));
    assert_eq!(swap_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn hybrid_both_preview_errors_returns_error() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(100u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|_| Err("dex unavailable".into()));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Err("cex unavailable".into()),
        })),
    };

    let err = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect_err("both preview failures should bubble as route preview failure");
    assert!(err.contains("route preview failed on both venues"));
}

#[tokio::test]
async fn hybrid_recovery_transfer_failure_returns_error() {
    let recovery_account = Account {
        owner: Principal::anonymous(),
        subaccount: None,
    };

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(recovery_account);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .returning(|_, _, _| Err("boom".into()));
    transfers.expect_approve().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 5.0)));
    dex_swapper.expect_execute().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let err = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect_err("recovery transfer should fail");
    assert!(err.contains("recovery transfer failed"));
}

#[tokio::test]
async fn hybrid_recovery_zero_transfer_amount_succeeds_without_transfer() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_recovery_account().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 4.0)));
    dex_swapper.expect_execute().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let res = finalizer
        .finalize(&wal, make_receipt_with_params(10.0, 1_000u64, 1_000u64))
        .await
        .expect("recovery with zero transferable amount should succeed");

    assert!(res.finalized);
    assert_eq!(res.swapper.as_deref(), Some("recovery"));
    assert!(res.swap_result.is_none());
}

#[tokio::test]
async fn forced_preview_error_persists_snapshot_and_errors() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|_| Err("dex api timeout".into()));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let err = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("forced preview failure should return retryable error");
    assert!(err.contains("forced dex preview failed"));

    let snapshot = wal_snapshot(&wal);
    assert_eq!(snapshot.mode, "forced");
    assert_eq!(snapshot.chosen, "error");
    assert!(snapshot.reason.contains("forced_preview_unavailable"));
}

#[tokio::test]
async fn forced_preview_none_persists_snapshot_and_errors() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Cex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper.expect_quote().times(0);
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(CexRoutePreview {
                is_executable: false,
                estimated_receive_amount: 0.0,
                estimated_slippage_bps: 0.0,
                reason: Some("book empty".to_string()),
            }),
        })),
    };

    let err = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("forced preview unavailable should return retryable error");
    assert!(err.contains("forced cex preview unavailable"));

    let snapshot = wal_snapshot(&wal);
    assert_eq!(snapshot.mode, "forced");
    assert_eq!(snapshot.chosen, "error");
    assert!(snapshot.reason.contains("forced_preview_unavailable"));
}

#[tokio::test]
async fn forced_cex_positive_preview_executes_cex() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Cex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper.expect_quote().times(0);
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(1_200.0)),
        })),
    };

    let res = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect("forced cex positive should execute cex");
    assert_eq!(res.swapper.as_deref(), Some("mexc"));
}

#[tokio::test]
async fn hybrid_both_non_positive_routes_to_recovery_and_uses_recovery_account() {
    let recovery_account = Account {
        owner: Principal::from_slice(&[4u8; 29]),
        subaccount: Some([5u8; 32]),
    };
    let expected_recovery = recovery_account;

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(expected_recovery);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 8.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .withf(move |_token, to, amount| {
            matches!(to, ChainAccount::Icp(acc) if acc == &expected_recovery) && amount.clone() == 900u64
        })
        .returning(|_, _, _| Ok("tx-1".to_string()));
    transfers.expect_approve().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(900.0)),
        })),
    };

    let res = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("both non-positive should route to recovery");
    assert_eq!(res.swapper.as_deref(), Some("recovery"));
}

#[tokio::test]
async fn hybrid_positive_but_below_min_returns_no_viable_route() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(1_000u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config.expect_get_recovery_account().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 1_050u64, 5.0)));
    dex_swapper.expect_execute().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(1_040.0)),
        })),
    };

    let err = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("positive but below min net edge should not execute");
    assert!(err.contains("no viable route"));
}

#[tokio::test]
async fn hybrid_one_preview_error_uses_other_preview() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(100u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config.expect_get_recovery_account().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|_| Err("dex unavailable".into()));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let wal = TestWal::empty();
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(1_200.0)),
        })),
    };

    let res = finalizer
        .finalize(&wal, make_receipt(10.0))
        .await
        .expect("cex should be used when dex preview fails");
    assert_eq!(res.swapper.as_deref(), Some("mexc"));
}

#[tokio::test]
async fn hybrid_recovery_persists_snapshot() {
    let recovery_account = Account {
        owner: Principal::anonymous(),
        subaccount: None,
    };

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(recovery_account);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 5.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .withf(|_token, _to, amount| amount.clone() == 900u64)
        .returning(|_, _, _| Ok("tx-1".to_string()));
    transfers.expect_approve().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(900.0)),
        })),
    };

    let res = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("non-positive preview should route to recovery");
    assert_eq!(res.swapper.as_deref(), Some("recovery"));

    let snapshot = wal_snapshot(&wal);
    assert_eq!(snapshot.mode, "hybrid");
    assert_eq!(snapshot.chosen, "recovery");
    assert!(snapshot.reason.contains("hybrid_non_positive"));
}

#[tokio::test]
async fn hybrid_no_viable_persists_snapshot() {
    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
    config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
    config.expect_get_cex_min_net_edge_bps().return_const(1_000u32);
    config.expect_get_cex_route_fee_bps().return_const(0u32);
    config.expect_get_cex_delay_buffer_bps().return_const(0u32);
    config.expect_get_recovery_account().times(0);

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 1_050u64, 4.0)));
    dex_swapper.expect_execute().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt(&receipt);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: Some(Arc::new(StubCexFinalizer {
            preview: Ok(cex_preview(1_040.0)),
        })),
    };

    let err = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("positive but below min should return no viable route");
    assert!(err.contains("no viable route"));

    let snapshot = wal_snapshot(&wal);
    assert_eq!(snapshot.mode, "hybrid");
    assert_eq!(snapshot.chosen, "error");
    assert!(snapshot.reason.contains("hybrid_positive_below_min"));
}

#[tokio::test]
async fn decision_snapshot_preserves_existing_meta_bytes() {
    let recovery_account = Account {
        owner: Principal::anonymous(),
        subaccount: None,
    };

    let mut config = MockConfigTrait::new();
    config.expect_get_swapper_mode().return_const(SwapperMode::Dex);
    config.expect_get_cex_min_net_edge_bps().return_const(150u32);
    config
        .expect_get_recovery_account()
        .times(1)
        .return_const(recovery_account);

    let mut dex_swapper = MockSwapInterface::new();
    dex_swapper
        .expect_quote()
        .times(1)
        .returning(|req| Ok(quote_from_req(req, 900u64, 1.0)));
    dex_swapper.expect_execute().times(0);

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .withf(|_token, _to, amount| amount.clone() == 900u64)
        .returning(|_, _, _| Ok("tx-1".to_string()));
    transfers.expect_approve().times(0);

    let receipt = make_receipt(10.0);
    let wal = TestWal::with_receipt_and_meta(&receipt, vec![7u8, 8u8, 9u8]);
    let finalizer = HybridFinalizer {
        config: Arc::new(config),
        trader_transfers: Arc::new(transfers),
        dex_swapper: Arc::new(dex_swapper),
        dex_finalizer: Arc::new(NoopDexFinalizer),
        cex_finalizer: None,
    };

    let _ = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("forced non-positive should recover");

    let wrapper = wal_wrapper(&wal);
    assert_eq!(wrapper.meta, vec![7u8, 8u8, 9u8]);
    assert!(wrapper.finalizer_decision.is_some());
}

#[test]
fn choose_best_route_prefers_higher_net_edge_candidate() {
    const DEX_NET_EDGE_BPS: f64 = 125.0;
    const CEX_NET_EDGE_BPS: f64 = 126.0;

    let dex_candidate = RouteCandidate {
        venue: RouteVenue::Dex,
        gross_edge_bps: DEX_NET_EDGE_BPS,
        net_edge_bps: DEX_NET_EDGE_BPS,
        reason: "dex".to_string(),
    };
    let cex_candidate = RouteCandidate {
        venue: RouteVenue::Cex,
        gross_edge_bps: CEX_NET_EDGE_BPS,
        net_edge_bps: CEX_NET_EDGE_BPS,
        reason: "cex".to_string(),
    };

    let selected =
        choose_best_route(Some(dex_candidate), Some(cex_candidate)).expect("one candidate should be selected");
    assert_eq!(selected.venue, RouteVenue::Cex);
    assert_eq!(selected.net_edge_bps, CEX_NET_EDGE_BPS);
}

#[test]
fn choose_best_route_returns_available_candidate_when_other_missing() {
    const DEX_NET_EDGE_BPS: f64 = 125.0;
    const CEX_NET_EDGE_BPS: f64 = 118.0;

    let dex_candidate = RouteCandidate {
        venue: RouteVenue::Dex,
        gross_edge_bps: DEX_NET_EDGE_BPS,
        net_edge_bps: DEX_NET_EDGE_BPS,
        reason: "dex".to_string(),
    };
    let cex_candidate = RouteCandidate {
        venue: RouteVenue::Cex,
        gross_edge_bps: CEX_NET_EDGE_BPS,
        net_edge_bps: CEX_NET_EDGE_BPS,
        reason: "cex".to_string(),
    };

    let selected = choose_best_route(Some(dex_candidate.clone()), None).expect("dex candidate should be selected");
    assert_eq!(selected.venue, RouteVenue::Dex);

    let selected =
        choose_best_route(None, Some(cex_candidate)).expect("candidate should be selected when only one side exists");
    assert_eq!(selected.venue, RouteVenue::Cex);
}
