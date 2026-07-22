use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    },
};

use crate::{
    executors::executor::ExecutorRequest,
    finalizers::{
        dex_finalizer::{DexRouteFinalizer, DexRoutePreview},
        finalizer::Finalizer,
        icpswap::finalizer::{ICPSWAP_FINALIZER_PERMANENT_PREFIX, IcpswapFinalizer},
    },
    persistance::{
        FinalizerDecisionSnapshot, LiqMetaWrapper, LiqResultRecord, ResultStatus, VenueExecutionState, WalStore,
    },
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{
            client::{
                IcpswapExecutionClient, IcpswapReconciliationClient, IcpswapRecoveryClient,
                IcpswapRecoveryTransferClient, MockIcpswapExecutionClient, MockIcpswapReconciliationClient,
                MockIcpswapRecoveryClient, MockIcpswapRecoveryTransferClient,
            },
            plan::IcpswapPlanner,
            types::{
                IcpswapApprovalRequest, IcpswapDepositAndSwapArgs, IcpswapExecutionClientError, IcpswapExecutionPhase,
                IcpswapExecutionPlan, IcpswapExecutionState, IcpswapQuoteError, IcpswapQuoteResult,
                IcpswapRecoveryClientError, IcpswapRecoveryTransferClientError, IcpswapRecoveryTransferOutcome,
                IcpswapRecoveryTransferRequest, IcpswapTransaction, IcpswapUnusedBalance, IcpswapWithdrawArgs,
            },
        },
        model::{SwapQuote, SwapRequest},
    },
    wal::{decode_receipt_wrapper, encode_meta},
};

fn p(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn trader() -> Account {
    Account {
        owner: p(4),
        subaccount: None,
    }
}

fn recovery_destination() -> Account {
    Account {
        owner: p(4),
        subaccount: Some([7; 32]),
    }
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn state(phase: IcpswapExecutionPhase) -> IcpswapExecutionState {
    let input = token(p(1), "IN", 10);
    let output = token(p(2), "OUT", 5);
    let plan = IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input, Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(token(p(1), "IN", 10), Nat::from(10u64)),
        ChainTokenAmount::from_raw(output, Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(token(p(2), "OUT", 5), Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan");
    let mut state = IcpswapExecutionState::planned(plan);
    state.phase = phase;
    state.pool_transaction_start = Some(Nat::from(42u64));
    state.pool_transaction_id = Some(Nat::from(42u64));
    state.submitted_at = Some(100);
    state
}

fn receipt() -> ExecutionReceipt {
    let collateral = token(p(1), "IN", 10);
    let debt = token(p(2), "OUT", 5);
    ExecutionReceipt {
        request: ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: p(6),
                debt_pool_id: p(7),
                collateral_pool_id: p(8),
                debt_amount: Nat::from(100_000u64),
                receiver_address: p(4),
                buy_bad_debt: false,
            },
            swap_args: Some(SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(collateral.clone(), Nat::from(100_000u64)),
                receive_asset: debt.asset_id(),
                receive_address: None,
                max_slippage_bps: Some(100),
                venue_hint: Some("icpswap".to_string()),
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 1,
            ref_price: Nat::from(1u8),
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            id: 42,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(100_000u64),
                debt_repaid: Nat::from(90_000u64),
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

struct TestWal(Mutex<LiqResultRecord>);

impl TestWal {
    fn new(receipt: &ExecutionReceipt, state: IcpswapExecutionState) -> Self {
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: Some(VenueExecutionState::new(crate::swappers::icpswap::VENUE_ID, &state).unwrap()),
        };
        let mut row = LiqResultRecord {
            id: "42".to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: String::new(),
        };
        encode_meta(&mut row, &wrapper).expect("encode WAL");
        Self(Mutex::new(row))
    }

    fn state(&self) -> IcpswapExecutionState {
        let row = self.0.lock().unwrap().clone();
        let wrapper = decode_receipt_wrapper(&row).unwrap().unwrap();
        wrapper
            .venue_execution
            .expect("missing ICPSwap state")
            .decode(crate::swappers::icpswap::VENUE_ID)
            .expect("decode state")
            .expect("wrong venue")
    }

    fn clear_execution_state(&self) {
        let mut row = self.0.lock().unwrap();
        let mut wrapper = decode_receipt_wrapper(&row).unwrap().unwrap();
        wrapper.venue_execution = None;
        encode_meta(&mut row, &wrapper).expect("encode WAL");
    }

    fn wrapper(&self) -> LiqMetaWrapper {
        decode_receipt_wrapper(&self.0.lock().unwrap()).unwrap().unwrap()
    }
}

#[async_trait]
impl WalStore for TestWal {
    async fn upsert_result(&self, row: LiqResultRecord) -> anyhow::Result<()> {
        *self.0.lock().unwrap() = row;
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> anyhow::Result<Option<LiqResultRecord>> {
        let row = self.0.lock().unwrap();
        Ok((row.id == liq_id).then(|| row.clone()))
    }

    async fn list_by_status(&self, _: ResultStatus, _: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(Vec::new())
    }

    async fn get_pending(&self, _: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(Vec::new())
    }

    async fn update_status(&self, _: &str, _: ResultStatus, _: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_failure(&self, _: &str, _: ResultStatus, _: String, _: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

struct TestIcpswapClient {
    execution: MockIcpswapExecutionClient,
    settlement: MockIcpswapReconciliationClient,
    recovery: MockIcpswapRecoveryClient,
    transfer: MockIcpswapRecoveryTransferClient,
}

#[async_trait]
impl IcpswapPlanner for TestIcpswapClient {
    async fn quote_with_plan(&self, _request: &SwapRequest) -> Result<IcpswapQuoteResult, IcpswapQuoteError> {
        Err(IcpswapQuoteError::NoUsablePools {
            failures: vec!["preview is not used by finalizer phase tests".to_string()],
        })
    }
}

#[async_trait]
impl IcpswapExecutionClient for TestIcpswapClient {
    async fn latest_transaction_id(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Option<Nat>, IcpswapExecutionClientError> {
        self.execution.latest_transaction_id(pool, owner).await
    }

    async fn allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapExecutionClientError> {
        self.execution.allowance(ledger, owner, spender).await
    }

    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapExecutionClientError> {
        self.execution.approve(request).await
    }

    async fn deposit_from_and_swap(
        &self,
        pool: Principal,
        args: &IcpswapDepositAndSwapArgs,
    ) -> Result<Nat, IcpswapExecutionClientError> {
        self.execution.deposit_from_and_swap(pool, args).await
    }
}

#[async_trait]
impl IcpswapReconciliationClient for TestIcpswapClient {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String> {
        IcpswapReconciliationClient::transactions_by_owner(&self.settlement, pool, owner).await
    }

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String> {
        IcpswapReconciliationClient::unused_balance(&self.settlement, pool, owner).await
    }
}

#[async_trait]
impl IcpswapRecoveryClient for TestIcpswapClient {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String> {
        IcpswapRecoveryClient::transactions_by_owner(&self.recovery, pool, owner).await
    }

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String> {
        IcpswapRecoveryClient::unused_balance(&self.recovery, pool, owner).await
    }

    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapRecoveryClientError> {
        self.recovery.withdraw(pool, args).await
    }
}

#[async_trait]
impl IcpswapRecoveryTransferClient for TestIcpswapClient {
    async fn transfer_recovered_funds(
        &self,
        request: &IcpswapRecoveryTransferRequest,
    ) -> Result<IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferClientError> {
        self.transfer.transfer_recovered_funds(request).await
    }
}

fn finalizer(
    execution: MockIcpswapExecutionClient,
    settlement: MockIcpswapReconciliationClient,
    recovery: MockIcpswapRecoveryClient,
    transfer: MockIcpswapRecoveryTransferClient,
) -> IcpswapFinalizer {
    IcpswapFinalizer::from_venue_with_clock(
        Arc::new(TestIcpswapClient {
            execution,
            settlement,
            recovery,
            transfer,
        }),
        trader(),
        recovery_destination(),
        120,
        300,
        Arc::new(|| 1_000_000_000),
    )
}

#[tokio::test]
async fn committing_dex_preview_creates_icpswap_state_inside_finalizer() {
    let receipt = receipt();
    let plan = state(IcpswapExecutionPhase::Planned).plan;
    let wal = TestWal::new(&receipt, IcpswapExecutionState::planned(plan.clone()));
    wal.clear_execution_state();
    let finalizer = finalizer(
        MockIcpswapExecutionClient::new(),
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );
    let decision = FinalizerDecisionSnapshot {
        mode: "hybrid".to_string(),
        chosen: "dex".to_string(),
        reason: "best route".to_string(),
        min_required_bps: 100.0,
        dex_preview_gross_bps: Some(200.0),
        dex_preview_net_bps: Some(200.0),
        cex_preview_gross_bps: Some(150.0),
        cex_preview_net_bps: Some(140.0),
        ts: 123,
    };
    let preview = DexRoutePreview::new(
        SwapQuote {
            pay_asset: receipt.request.swap_args.as_ref().unwrap().pay_asset.clone(),
            pay_amount: plan.amount_in.value.clone(),
            receive_asset: receipt.request.swap_args.as_ref().unwrap().receive_asset.clone(),
            receive_amount: plan.net_expected_output.value.clone(),
            mid_price: 0.0,
            exec_price: 0.0,
            slippage: 0.0,
            legs: Vec::new(),
        },
        plan.clone(),
    );

    finalizer
        .commit_route(&wal, &receipt, decision.clone(), preview)
        .await
        .expect("commit route");

    let wrapper = wal.wrapper();
    assert_eq!(wrapper.finalizer_decision, Some(decision));
    let state: IcpswapExecutionState = wrapper
        .venue_execution
        .expect("missing ICPSwap execution state")
        .decode(crate::swappers::icpswap::VENUE_ID)
        .expect("decode state")
        .expect("wrong venue");
    assert_eq!(state.plan, plan);
    assert_eq!(state.phase, IcpswapExecutionPhase::Planned);
    assert_eq!(state.recovery_destination, Some(recovery_destination()));
}

#[tokio::test]
async fn completed_state_builds_net_execution_with_exact_attribution() {
    let receipt = receipt();
    let mut state = state(IcpswapExecutionPhase::Completed);
    state.gross_swap_output = Some(ChainTokenAmount::from_raw(
        state.plan.gross_quoted_out.token.clone(),
        Nat::from(119_500u64),
    ));
    state.settlement_ledger_block_index = Some(Nat::from(900u64));
    state.approval_block_index = Some(Nat::from(77u64));
    let wal = TestWal::new(&receipt, state);
    let finalizer = finalizer(
        MockIcpswapExecutionClient::new(),
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );

    let result = finalizer.finalize(&wal, receipt).await.expect("completed");
    let execution = result.swap_result.expect("swap execution");
    assert!(result.finalized);
    assert_eq!(result.swapper.as_deref(), Some("icpswap"));
    assert_eq!(execution.receive_amount, Nat::from(119_495u64));
    assert_eq!(execution.swap_id, 42);
    assert_eq!(execution.request_id, 900);
    assert!(execution.legs[0].route_id.contains("transaction=42:ledger_block=900"));
}

#[tokio::test]
async fn recovered_state_finalizes_without_swap() {
    let receipt = receipt();
    let mut state = state(IcpswapExecutionPhase::Recovered);
    state.recovery_transaction_id = Some(Nat::from(44u64));
    let wal = TestWal::new(&receipt, state);
    let finalizer = finalizer(
        MockIcpswapExecutionClient::new(),
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );

    let result = finalizer.finalize(&wal, receipt).await.expect("recovered");
    assert!(result.finalized);
    assert!(result.swap_result.is_none());
    assert_eq!(result.swapper.as_deref(), Some("recovery"));
    assert!(result.reason.unwrap().contains("pool withdrawal 44"));
}

#[tokio::test]
async fn terminal_state_returns_explicit_permanent_error() {
    let receipt = receipt();
    let mut state = state(IcpswapExecutionPhase::FailedTerminal);
    state.last_error = Some("ambiguous withdrawal".to_string());
    let wal = TestWal::new(&receipt, state);
    let finalizer = finalizer(
        MockIcpswapExecutionClient::new(),
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );

    let error = finalizer.finalize(&wal, receipt).await.expect_err("permanent");
    assert!(error.starts_with(ICPSWAP_FINALIZER_PERMANENT_PREFIX));
    assert!(error.contains("ambiguous withdrawal"));
}

#[tokio::test]
async fn post_submission_query_error_is_persisted_and_returns_noop() {
    let receipt = receipt();
    let wal = TestWal::new(&receipt, state(IcpswapExecutionPhase::SubmissionUnknown));
    let mut execution = MockIcpswapExecutionClient::new();
    execution.expect_deposit_from_and_swap().times(0);
    let mut settlement = MockIcpswapReconciliationClient::new();
    settlement
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Err("pool query unavailable".to_string()));
    let finalizer = finalizer(
        execution,
        settlement,
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );

    let result = finalizer.finalize(&wal, receipt).await.expect("pending noop");
    assert!(!result.finalized);
    let state = wal.state();
    assert_eq!(state.phase, IcpswapExecutionPhase::SubmissionUnknown);
    assert!(state.last_error.unwrap().contains("pool query unavailable"));
}

#[tokio::test]
async fn planned_state_submits_once_then_returns_noop_for_settlement() {
    let receipt = receipt();
    let mut planned = state(IcpswapExecutionPhase::Planned);
    planned.pool_transaction_start = None;
    planned.pool_transaction_id = None;
    let wal = TestWal::new(&receipt, planned);
    let mut execution = MockIcpswapExecutionClient::new();
    execution
        .expect_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(200_000u64)));
    execution
        .expect_latest_transaction_id()
        .times(1)
        .return_once(|_, _| Ok(Some(Nat::from(41u64))));
    execution
        .expect_deposit_from_and_swap()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(119_500u64)));
    execution.expect_approve().times(0);
    let finalizer = finalizer(
        execution,
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        MockIcpswapRecoveryTransferClient::new(),
    );

    let result = finalizer.finalize(&wal, receipt).await.expect("submitted");
    assert!(!result.finalized);
    let state = wal.state();
    assert_eq!(state.phase, IcpswapExecutionPhase::AwaitingOutput);
    assert_eq!(state.pool_transaction_start, Some(Nat::from(42u64)));
    assert_eq!(state.submitted_at, Some(1_000_000_000));
}

#[tokio::test]
async fn refunded_state_dispatches_deduplicated_recovery_transfer() {
    let receipt = receipt();
    let mut state = state(IcpswapExecutionPhase::Refunded);
    state.refund_transaction_id = Some(Nat::from(43u64));
    state.returned_gross_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        Nat::from(100_000u64),
    ));
    let wal = TestWal::new(&receipt, state);
    let mut transfer = MockIcpswapRecoveryTransferClient::new();
    transfer
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(|request| {
            assert_eq!(request.amount, Nat::from(99_980u64));
            assert_eq!(request.created_at_time, 1_000_000_000);
            Ok(IcpswapRecoveryTransferOutcome::Completed(Nat::from(902u64)))
        });
    let finalizer = finalizer(
        MockIcpswapExecutionClient::new(),
        MockIcpswapReconciliationClient::new(),
        MockIcpswapRecoveryClient::new(),
        transfer,
    );

    let result = finalizer.finalize(&wal, receipt).await.expect("recovered");
    assert!(result.finalized);
    assert_eq!(result.swapper.as_deref(), Some("recovery"));
    assert_eq!(wal.state().phase, IcpswapExecutionPhase::Recovered);
}
