use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use ic_agent::Identity;
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use icrc_ledger_types::icrc2::approve::ApproveArgs;
use liquidium_pipeline_connectors::backend::icp_backend::{IcpBackend, IcrcTransferError};
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
            identity::IcpswapExecutionIdentity,
            session::{IcpswapExecutionSession, IcpswapExecutionSessionFactory},
            state::MAX_DEPOSIT_OBSERVATION_ATTEMPTS,
            transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
            types::{
                IcpswapClientError, IcpswapDepositArgs, IcpswapExecutionPlan, IcpswapExecutionState, IcpswapQuoteError,
                IcpswapRoutePreview, IcpswapStep, IcpswapSwapArgs, IcpswapUnusedBalance, IcpswapWithdrawArgs,
            },
            venue::IcpswapFinalizerLogic,
        },
        model::{SwapQuote, SwapRequest},
    },
    utils::{CKUSDC_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL},
    wal::{decode_receipt_wrapper, encode_meta},
    watchdog::{Watchdog, WatchdogEvent},
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

fn receiver() -> Account {
    Account {
        owner: p(5),
        subaccount: None,
    }
}

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

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
    )
    .expect("plan")
}

fn execution_state(execution_id: &str, plan: IcpswapExecutionPlan) -> IcpswapExecutionState {
    let (identity, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "42").expect("identity");
    let child = Account {
        owner: identity.principal,
        subaccount: None,
    };
    let funding = IcpswapFundingState::new(trader(), child, plan.input_ledger_fee.clone());
    let settlement = IcpswapSettlementState {
        kind: None,
        destination: receiver(),
        fee: plan.output_ledger_fee.clone(),
        transfer: IcpswapLedgerTransferState::default(),
        interrupted_transfer: None,
        interrupted_observed_debit: None,
        recovery_credit: None,
        residual_dust: None,
    };
    IcpswapExecutionState::prepare(execution_id, plan, identity, funding, settlement).expect("state")
}

fn pool_state(execution_id: &str, plan: IcpswapExecutionPlan) -> IcpswapExecutionState {
    let mut state = execution_state(execution_id, plan);
    state.step = IcpswapStep::Transfer;
    state
}

fn state() -> IcpswapExecutionState {
    pool_state("42", plan())
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
                receive_address: Some(receiver().owner.to_text()),
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
    preview: Option<IcpswapRoutePreview>,
}

#[derive(Default)]
struct RecordingWatchdog(Mutex<Vec<(String, String, String, String, String)>>);

impl RecordingWatchdog {
    fn events(&self) -> Vec<(String, String, String, String, String)> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl Watchdog for RecordingWatchdog {
    async fn notify(&self, event: WatchdogEvent<'_>) {
        if let WatchdogEvent::OperatorRequired {
            execution_id,
            venue,
            pending_step,
            owner,
            details,
        } = event
        {
            self.0
                .lock()
                .unwrap()
                .push((execution_id, venue, pending_step, owner, details));
        }
    }
}

#[async_trait]
impl IcpswapFinalizerLogic for TestIcpswapClient {
    async fn preview_route(&self, _request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        self.preview.clone().ok_or_else(|| IcpswapQuoteError::NoUsablePools {
            failures: vec!["preview is not configured by this test".to_string()],
        })
    }
}

#[async_trait]
impl IcpswapManualClient for TestIcpswapClient {
    async fn requote(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError> {
        self.manual.requote(pool, args).await
    }

    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapClientError> {
        self.manual.ledger_balance(ledger, account).await
    }

    async fn ledger_transfer(&self, ledger: Principal, args: TransferArg) -> Result<Nat, IcpswapClientError> {
        self.manual.ledger_transfer(ledger, args).await
    }

    async fn unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapClientError> {
        self.manual.unused_balance(pool, owner).await
    }

    async fn deposit(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapClientError> {
        self.manual.deposit(pool, args).await
    }

    async fn swap(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError> {
        self.manual.swap(pool, args).await
    }

    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapClientError> {
        self.manual.withdraw(pool, args).await
    }
}

#[async_trait]
impl IcpBackend for TestIcpswapClient {
    async fn icrc1_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String> {
        IcpswapManualClient::ledger_balance(self, ledger, account)
            .await
            .map_err(|error| error.to_string())
    }

    async fn icp_account_balance(&self, _: Principal, _: &str) -> Result<Nat, String> {
        Err("native ICP backend balance is not used by this finalizer test double".to_string())
    }

    async fn icrc1_transfer(&self, _: Principal, _: &Account, _: &Account, _: Nat) -> Result<Nat, String> {
        Err("plain ICRC-1 transfer is not used by this finalizer test double".to_string())
    }

    async fn icrc1_transfer_with_args(&self, ledger: Principal, args: TransferArg) -> Result<Nat, IcrcTransferError> {
        IcpswapManualClient::ledger_transfer(self, ledger, args)
            .await
            .map_err(|error| match error {
                IcpswapClientError::LedgerTransferTooOld { .. } => IcrcTransferError::TooOld,
                IcpswapClientError::LedgerTransferCreatedInFuture { .. } => {
                    IcrcTransferError::CreatedInFuture { ledger_time: 0 }
                }
                other => IcrcTransferError::Other(other.to_string()),
            })
    }

    async fn icp_transfer(&self, _: Principal, _: &str, _: Nat) -> Result<u64, String> {
        Err("legacy ICP transfer is not used by this finalizer test double".to_string())
    }

    async fn icrc1_decimals(&self, _: Principal) -> Result<u8, String> {
        Err("ledger decimals are not used by this finalizer test double".to_string())
    }

    async fn icrc1_fee(&self, _: Principal) -> Result<Nat, String> {
        Err("ledger fees are not used by this finalizer test double".to_string())
    }

    async fn icrc2_allowance(&self, _: Principal, _: &Account, _: &Account) -> Result<Nat, String> {
        Err("ICRC-2 allowance is not used by this finalizer test double".to_string())
    }

    async fn icrc2_approve(&self, _: Principal, _: ApproveArgs) -> Result<Nat, String> {
        Err("ICRC-2 approval is not used by this finalizer test double".to_string())
    }
}

struct TestSessionFactory {
    client: Arc<TestIcpswapClient>,
}

impl IcpswapExecutionSessionFactory for TestSessionFactory {
    fn descriptor(&self, liquidation_id: &str) -> Result<IcpswapExecutionIdentity, String> {
        IcpswapExecutionIdentity::derive(TEST_MNEMONIC, liquidation_id).map(|(descriptor, _)| descriptor)
    }

    fn derive_identity(&self, descriptor: &IcpswapExecutionIdentity) -> Result<Arc<dyn Identity>, String> {
        descriptor
            .validate_and_derive(TEST_MNEMONIC)
            .map(|identity| Arc::new(identity) as Arc<dyn Identity>)
    }

    fn open(&self, descriptor: &IcpswapExecutionIdentity) -> Result<IcpswapExecutionSession, String> {
        self.derive_identity(descriptor)?;
        Ok(IcpswapExecutionSession {
            funder: self.client.clone(),
            child_ledger: self.client.clone(),
            child: self.client.clone(),
        })
    }
}

fn test_finalizer(client: Arc<TestIcpswapClient>, trader: Account) -> IcpswapFinalizer {
    let sessions = Arc::new(TestSessionFactory { client: client.clone() });
    IcpswapFinalizer::from_workflow_with_clock(client, trader, sessions, Arc::new(|| 1_000_000_000))
}

fn finalizer(manual: MockIcpswapManualClient) -> IcpswapFinalizer {
    test_finalizer(Arc::new(TestIcpswapClient { manual, preview: None }), trader())
}

fn native_plan() -> IcpswapExecutionPlan {
    let native_ledger = Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("native ICP ledger");
    let ckusdc_ledger = Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).expect("ckUSDC ledger");
    let input = token(native_ledger, "ICP", 10);
    let output = token(ckusdc_ledger, "ckUSDC", 5);
    IcpswapExecutionPlan::new(
        p(9),
        native_ledger,
        ckusdc_ledger,
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
    )
    .expect("native plan")
}

fn native_request(route: &IcpswapExecutionPlan) -> SwapRequest {
    SwapRequest {
        pay_asset: route.amount_in.token.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(route.amount_in.token.clone(), Nat::from(100_030u64)),
        receive_asset: route.gross_quoted_out.token.asset_id(),
        receive_address: Some(receiver().owner.to_text()),
        max_slippage_bps: Some(100),
        venue_hint: Some(crate::swappers::icpswap::VENUE_ID.to_string()),
    }
}

#[tokio::test]
async fn committing_dex_preview_creates_manual_icpswap_state() {
    let mut receipt = receipt();
    let plan = native_plan();
    let request = native_request(&plan);
    receipt.request.collateral_asset = request.pay_amount.token.clone();
    receipt.request.debt_asset = plan.gross_quoted_out.token.clone();
    receipt.request.swap_args = Some(request);
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
            receive_amount: plan.net_expected_output().value,
            mid_price: 0.0,
            exec_price: 0.0,
            estimated_price_impact_bps: 0.0,
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
    assert_eq!(state.step, IcpswapStep::Funding);
    assert_ne!(state.owner, trader());
    assert_eq!(state.funding.source, trader());
}

#[tokio::test]
async fn finalizer_drives_immediately_runnable_steps_and_builds_execution() {
    let receipt = receipt();
    let wal = TestWal::new(&receipt, state());
    let mut manual = MockIcpswapManualClient::new();
    manual.expect_ledger_transfer().times(1).return_once(|ledger, args| {
        assert_eq!(ledger, p(1));
        assert_eq!(args.amount, Nat::from(100_010u64));
        assert_eq!(args.fee, Some(Nat::from(10u64)));
        assert_eq!(args.created_at_time, Some(1_000_000_000));
        Ok(Nat::from(77u64))
    });
    manual.expect_requote().times(0);

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
            balance0: Nat::from(0u8),
            balance1: Nat::from(119_500u64),
        },
        IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(0u8),
        },
    ] {
        manual
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(balance));
    }
    manual
        .expect_deposit()
        .times(1)
        // The pool sweeps the subaccount and credits `amount - fee`, so this is
        // what a deposit of exactly the planned input looks like.
        .return_once(|_, args| Ok(args.amount.clone() - args.fee.clone()));
    manual.expect_swap().times(1).return_once(|_, args| {
        assert_eq!(args.amount_out_minimum, "118800");
        Ok(Nat::from(119_500u64))
    });
    let mut balance_sequence = Sequence::new();
    manual
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));
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
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(119_495u64)));
    manual
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(0u8)));
    manual
        .expect_withdraw()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone()));
    manual.expect_ledger_transfer().times(1).return_once(|ledger, args| {
        assert_eq!(ledger, p(2));
        assert_eq!(args.to, receiver());
        assert_eq!(args.amount, Nat::from(119_490u64));
        Ok(Nat::from(78u64))
    });
    let finalizer = finalizer(manual);

    // Every successful phase remains immediately runnable, so the finalizer
    // drives the complete workflow while persisting every side-effect boundary.
    let mut calls = 0;
    let execution = loop {
        calls += 1;
        assert!(calls <= 8, "execution never reached a terminal state");
        let outcome = finalizer.finalize(&wal, receipt.clone()).await.expect("advance");
        if let Some(execution) = outcome.swap_result {
            assert!(outcome.finalized);
            break execution;
        }
    };

    assert_eq!(calls, 1);
    assert_eq!(wal.state().step, IcpswapStep::Completed);
    assert_eq!(execution.receive_amount, Nat::from(119_490u64));
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

#[tokio::test]
async fn operator_required_realerts_every_cycle_while_parked() {
    let receipt = receipt();
    let mut pending = state();
    pending.step = IcpswapStep::DepositPending;
    pending.deposit.input_pool_balance_before = Some(Nat::from(0u8));
    pending.deposit.observation_attempts = MAX_DEPOSIT_OBSERVATION_ATTEMPTS - 1;
    let wal = TestWal::new(&receipt, pending);
    let mut manual = MockIcpswapManualClient::new();
    // One poll only: the failed step arms a retry cooldown, so the next cycle
    // re-alerts without spending another IC call re-observing the same balance.
    manual.expect_unused_balance().times(1).returning(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(0u8),
            balance1: Nat::from(0u8),
        })
    });
    let watchdog = Arc::new(RecordingWatchdog::default());
    let finalizer = finalizer(manual).with_watchdog(watchdog.clone());

    assert!(!finalizer.finalize(&wal, receipt.clone()).await.unwrap().finalized);
    assert_eq!(wal.state().step, IcpswapStep::OperatorRequired);
    assert_eq!(watchdog.events().len(), 1);
    assert_eq!(watchdog.events()[0].0, "42");
    assert_eq!(watchdog.events()[0].1, "icpswap");
    assert_eq!(watchdog.events()[0].2, "DepositPending");

    // The finalizer re-alerts on every parked cycle and leaves throttling to the
    // watchdog's cooldown key. Alerting only once would let a dropped webhook
    // hide custody that still requires intervention.
    assert!(!finalizer.finalize(&wal, receipt).await.unwrap().finalized);
    assert_eq!(
        watchdog.events().len(),
        2,
        "a still-parked execution must keep escalating"
    );
    assert_eq!(watchdog.events()[1].2, "DepositPending");
}
