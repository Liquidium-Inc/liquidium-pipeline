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
use mockall::Sequence;

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
            client::{IcpswapManualClient, MockIcpswapManualClient},
            execution::IcpswapExecutionStateStore,
            manual::{deposit_step, operator_step, recover_step, trade_step, withdraw_step},
            types::{
                IcpswapApprovalRequest, IcpswapDepositArgs, IcpswapExecutionPlan, IcpswapExecutionState,
                IcpswapManualClientError, IcpswapQuoteError, IcpswapRoutePreview, IcpswapStep, IcpswapSwapArgs,
                IcpswapUnusedBalance, IcpswapWithdrawArgs,
            },
            venue::IcpswapFinalizerLogic,
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

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn plan() -> IcpswapExecutionPlan {
    IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(token(p(1), "IN", 10), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(token(p(1), "IN", 10), Nat::from(10u64)),
        ChainTokenAmount::from_raw(token(p(2), "OUT", 5), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(token(p(2), "OUT", 5), Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan")
}

fn state() -> IcpswapExecutionState {
    IcpswapExecutionState::prepare("42", plan(), trader())
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
        let wrapper = decode_receipt_wrapper(&self.0.lock().unwrap()).unwrap().unwrap();
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
    manual: MockIcpswapManualClient,
}

#[async_trait]
impl IcpswapFinalizerLogic for TestIcpswapClient {
    async fn preview_route(&self, _request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        Err(IcpswapQuoteError::NoUsablePools {
            failures: vec!["preview is not used by this test".to_string()],
        })
    }

    async fn deposit(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<(), String> {
        deposit_step(self, store, execution_id, state, now_nanos).await
    }

    async fn trade(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<(), String> {
        trade_step(self, store, execution_id, state, now_nanos).await
    }

    async fn withdraw(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<(), String> {
        withdraw_step(self, store, execution_id, state, now_nanos).await
    }

    async fn recover(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<(), String> {
        recover_step(self, store, execution_id, state, now_nanos).await
    }

    async fn reconcile_operator(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapExecutionState,
    ) -> Result<(), String> {
        operator_step(self, store, execution_id, state).await
    }
}

#[async_trait]
impl IcpswapManualClient for TestIcpswapClient {
    async fn quote_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError> {
        self.manual.quote_manual(pool, args).await
    }

    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapManualClientError> {
        self.manual.ledger_balance(ledger, account).await
    }

    async fn manual_allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapManualClientError> {
        self.manual.manual_allowance(ledger, owner, spender).await
    }

    async fn manual_approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapManualClientError> {
        self.manual.manual_approve(request).await
    }

    async fn manual_unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapManualClientError> {
        self.manual.manual_unused_balance(pool, owner).await
    }

    async fn deposit_from(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapManualClientError> {
        self.manual.deposit_from(pool, args).await
    }

    async fn swap_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError> {
        self.manual.swap_manual(pool, args).await
    }

    async fn withdraw_manual(
        &self,
        pool: Principal,
        args: &IcpswapWithdrawArgs,
    ) -> Result<Nat, IcpswapManualClientError> {
        self.manual.withdraw_manual(pool, args).await
    }
}

fn finalizer(manual: MockIcpswapManualClient) -> IcpswapFinalizer {
    IcpswapFinalizer::from_workflow_with_clock(
        Arc::new(TestIcpswapClient { manual }),
        trader(),
        Arc::new(|| 1_000_000_000),
    )
}

#[tokio::test]
async fn committing_dex_preview_creates_manual_icpswap_state() {
    let receipt = receipt();
    let plan = plan();
    let wal = TestWal::new(&receipt, state());
    wal.clear_execution_state();
    let finalizer = finalizer(MockIcpswapManualClient::new());
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
        crate::swappers::icpswap::VENUE_ID,
        &plan,
    )
    .expect("encode route preview");

    finalizer
        .commit_route(&wal, &receipt, decision.clone(), preview)
        .await
        .expect("commit route");

    assert_eq!(wal.wrapper().finalizer_decision, Some(decision));
    let state = wal.state();
    assert_eq!(state.plan, plan);
    assert_eq!(state.step, IcpswapStep::Deposit);
    assert_eq!(state.owner, trader());
}

#[tokio::test]
async fn finalizer_runs_deposit_trade_withdraw_and_builds_execution() {
    let receipt = receipt();
    let wal = TestWal::new(&receipt, state());
    let mut manual = MockIcpswapManualClient::new();
    manual
        .expect_manual_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(100_010u64)));
    manual.expect_manual_approve().times(0);
    manual.expect_quote_manual().times(0);

    let mut unused_sequence = Sequence::new();
    for balance in [
        IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(0u8),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(100_000u64),
            balance1: Nat::from(0u8),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(100_000u64),
            balance1: Nat::from(0u8),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(119_500u64),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(119_500u64),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(0u8),
        },
    ] {
        manual
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(balance));
    }
    manual
        .expect_deposit_from()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone()));
    manual.expect_swap_manual().times(1).return_once(|_, args| {
        assert_eq!(args.amount_out_minimum, "118800");
        Ok(Nat::from(119_500u64))
    });
    let mut balance_sequence = Sequence::new();
    manual
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(10u64)));
    manual
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(119_505u64)));
    manual
        .expect_withdraw_manual()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone()));
    let finalizer = finalizer(manual);

    assert!(!finalizer.finalize(&wal, receipt.clone()).await.unwrap().finalized);
    assert_eq!(wal.state().step, IcpswapStep::Trade);
    assert!(!finalizer.finalize(&wal, receipt.clone()).await.unwrap().finalized);
    assert_eq!(wal.state().step, IcpswapStep::Withdraw);

    let result = finalizer.finalize(&wal, receipt).await.expect("withdraw");
    let execution = result.swap_result.expect("execution");
    assert!(result.finalized);
    assert_eq!(execution.receive_amount, Nat::from(119_495u64));
    assert!(execution.legs[0].route_id.contains("manual=42"));
}

#[tokio::test]
async fn failed_state_returns_explicit_permanent_error() {
    let receipt = receipt();
    let mut state = state();
    state.step = IcpswapStep::Failed;
    state.last_error = Some("ambiguous withdrawal".to_string());
    let wal = TestWal::new(&receipt, state);

    let error = finalizer(MockIcpswapManualClient::new())
        .finalize(&wal, receipt)
        .await
        .expect_err("permanent");
    assert!(error.starts_with(ICPSWAP_FINALIZER_PERMANENT_PREFIX));
    assert!(error.contains("ambiguous withdrawal"));
}
