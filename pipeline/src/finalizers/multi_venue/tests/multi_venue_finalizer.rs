use super::super::*;

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use candid::{Nat, Principal};
use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::{MockTransferActions, TransferFailure},
    types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    },
};
use num_traits::ToPrimitive;

use crate::{
    executors::executor::ExecutorRequest,
    finalizers::{
        finalizer::{Finalizer, FinalizerError, FinalizerResult},
        multi_venue::{ICPSWAP_VENUE_ID, KRAKEN_VENUE_ID, MEXC_VENUE_ID, VenueLegProgress, VenueRoutePreview},
        profit_calculator::SimpleProfitCalculator,
    },
    persistance::{
        FINALIZER_META_V2_VERSION, FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper, LiqResultRecord,
        MultiVenueExecutionOutcome, MultiVenueExecutionState, RecoverySweepState, RecoverySweepStatus, ResultStatus,
        VenueExecutionState, VenueLegState, VenueLegStatus, WalStore,
    },
    stage::PipelineStage,
    stages::{
        executor::{ExecutionReceipt, ExecutionStatus},
        finalize::{FinalizeStage, MAX_FINALIZER_ERRORS},
    },
    swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest},
    utils::{CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL},
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt},
    watchdog::{Watchdog, WatchdogEvent},
};

const TOTAL_PAY: u64 = 100_000_000;
const DEBT_REPAID: u64 = 190_000_000;

#[derive(Default)]
struct RecordingWatchdog(Mutex<Vec<(String, String)>>);

impl RecordingWatchdog {
    /// Captured operator escalations as `(execution_id, pending_step)`.
    fn alerts(&self) -> Vec<(String, String)> {
        self.0.lock().expect("watchdog lock").clone()
    }
}

#[async_trait]
impl Watchdog for RecordingWatchdog {
    async fn notify(&self, event: WatchdogEvent<'_>) {
        if let WatchdogEvent::OperatorRequired {
            execution_id,
            pending_step,
            ..
        } = event
        {
            self.0.lock().expect("watchdog lock").push((execution_id, pending_step));
        }
    }
}

struct TestWal {
    row: Mutex<Option<LiqResultRecord>>,
}

impl TestWal {
    fn with_receipt(receipt: &ExecutionReceipt) -> Self {
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        };
        Self::with_wrapper(wrapper)
    }

    fn with_wrapper(wrapper: LiqMetaWrapper) -> Self {
        let mut row = LiqResultRecord {
            id: liq_id_from_receipt(&wrapper.receipt).expect("liquidation id"),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: String::new(),
        };
        encode_meta(&mut row, &wrapper).expect("encode wrapper");
        Self {
            row: Mutex::new(Some(row)),
        }
    }

    /// Seeds a row whose recovery sweep was committed by an earlier process, so
    /// the finalizer under test can only reach it by loading it back.
    fn with_recovery_sweep(receipt: &ExecutionReceipt, status: RecoverySweepStatus) -> Self {
        let collateral = receipt.request.collateral_asset.clone();
        let state = RecoverySweepState {
            liquidation_id: liq_id_from_receipt(receipt).expect("liquidation id"),
            reason: "no viable swap route: MEXC amount is below its minimum".to_string(),
            amount: ChainTokenAmount::from_raw(collateral, Nat::from(TOTAL_PAY - 10_000)),
            destination: ChainAccount::IcpLedger(
                ic_ledger_types::AccountIdentifier::new(
                    &recovery_account().owner,
                    &ic_ledger_types::Subaccount(recovery_account().subaccount.expect("recovery subaccount")),
                )
                .to_hex(),
            ),
            status,
            txid: match status {
                RecoverySweepStatus::Completed => Some("earlier-recovery-block".to_string()),
                _ => None,
            },
            last_error: None,
        };
        Self::with_wrapper(LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: Some(FinalizerMetaV2 {
                version: FINALIZER_META_V2_VERSION,
                payload: FinalizerMetaPayload::RecoverySweep(state),
            }),
        })
    }

    fn wrapper(&self) -> LiqMetaWrapper {
        let row = self.row.lock().expect("WAL lock").clone().expect("WAL row");
        decode_receipt_wrapper(&row)
            .expect("decode wrapper")
            .expect("wrapper exists")
    }

    /// Rewrites the committed envelope in place, standing in for a row written
    /// by a different build of the binary.
    fn replace_wrapper(&self, wrapper: &LiqMetaWrapper) {
        let mut guard = self.row.lock().expect("WAL lock");
        let row = guard.as_mut().expect("WAL row");
        encode_meta(row, wrapper).expect("re-encode wrapper");
    }

    fn leg_statuses(&self) -> Vec<VenueLegStatus> {
        let wrapper = self.wrapper();
        let meta = wrapper.meta_v2.expect("committed meta_v2");
        let FinalizerMetaPayload::MultiVenueSwap(state) = meta.payload else {
            panic!("expected multi-venue state")
        };
        state.legs.into_iter().map(|leg| leg.status).collect()
    }
}

#[async_trait]
impl WalStore for TestWal {
    async fn upsert_result(&self, row: LiqResultRecord) -> anyhow::Result<()> {
        *self.row.lock().expect("WAL lock") = Some(row);
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> anyhow::Result<Option<LiqResultRecord>> {
        Ok(self
            .row
            .lock()
            .expect("WAL lock")
            .clone()
            .filter(|row| row.id == liq_id))
    }

    async fn list_by_status(&self, _status: ResultStatus, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(Vec::new())
    }

    async fn get_pending(&self, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(Vec::new())
    }

    async fn update_status(&self, _liq_id: &str, _next: ResultStatus, _bump_attempt: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_failure(
        &self,
        _liq_id: &str,
        _next: ResultStatus,
        _last_error: String,
        _bump_attempt: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete(&self, _liq_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Multi-row WAL used to prove that one parked or retryable venue execution
/// cannot prevent the outer finalize batch from advancing the following row.
struct BatchWal {
    rows: Mutex<Vec<LiqResultRecord>>,
}

impl BatchWal {
    fn new(receipts: impl IntoIterator<Item = ExecutionReceipt>) -> Self {
        Self {
            rows: Mutex::new(receipts.into_iter().map(row_for_receipt).collect()),
        }
    }

    fn status(&self, liquidation_id: u128) -> ResultStatus {
        self.rows
            .lock()
            .expect("batch WAL lock")
            .iter()
            .find(|row| row.id == liquidation_id.to_string())
            .expect("batch WAL row")
            .status
    }

    fn set_status(&self, liquidation_id: u128, status: ResultStatus) {
        self.rows
            .lock()
            .expect("batch WAL lock")
            .iter_mut()
            .find(|row| row.id == liquidation_id.to_string())
            .expect("batch WAL row")
            .status = status;
    }

    fn set_error_count(&self, liquidation_id: u128, error_count: i32) {
        self.rows
            .lock()
            .expect("batch WAL lock")
            .iter_mut()
            .find(|row| row.id == liquidation_id.to_string())
            .expect("batch WAL row")
            .error_count = error_count;
    }

    fn make_retry_due(&self, liquidation_id: u128) {
        self.rows
            .lock()
            .expect("batch WAL lock")
            .iter_mut()
            .find(|row| row.id == liquidation_id.to_string())
            .expect("batch WAL row")
            .updated_at = 0;
    }

    fn committed_state(&self, liquidation_id: u128) -> MultiVenueExecutionState {
        let rows = self.rows.lock().expect("batch WAL lock");
        let row = rows
            .iter()
            .find(|row| row.id == liquidation_id.to_string())
            .expect("batch WAL row");
        let wrapper = decode_receipt_wrapper(row)
            .expect("decode batch WAL row")
            .expect("batch WAL wrapper");
        let FinalizerMetaPayload::MultiVenueSwap(state) = wrapper.meta_v2.expect("committed meta_v2").payload else {
            panic!("expected multi-venue state")
        };
        state
    }
}

#[async_trait]
impl WalStore for BatchWal {
    async fn upsert_result(&self, row: LiqResultRecord) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("batch WAL lock");
        if let Some(existing) = rows.iter_mut().find(|existing| existing.id == row.id) {
            *existing = row;
        } else {
            rows.push(row);
        }
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> anyhow::Result<Option<LiqResultRecord>> {
        Ok(self
            .rows
            .lock()
            .expect("batch WAL lock")
            .iter()
            .find(|row| row.id == liq_id)
            .cloned())
    }

    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(self
            .rows
            .lock()
            .expect("batch WAL lock")
            .iter()
            .filter(|row| row.status == status)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_pending(&self, limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
        Ok(self
            .rows
            .lock()
            .expect("batch WAL lock")
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable
                )
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn update_status(&self, liq_id: &str, next: ResultStatus, bump_attempt: bool) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("batch WAL lock");
        let row = rows.iter_mut().find(|row| row.id == liq_id).expect("batch WAL row");
        row.status = next;
        if bump_attempt {
            row.attempt += 1;
        }
        Ok(())
    }

    async fn update_failure(
        &self,
        liq_id: &str,
        next: ResultStatus,
        last_error: String,
        bump_attempt: bool,
    ) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("batch WAL lock");
        let row = rows.iter_mut().find(|row| row.id == liq_id).expect("batch WAL row");
        row.status = next;
        row.last_error = Some(last_error);
        row.error_count += 1;
        if bump_attempt {
            row.attempt += 1;
        }
        Ok(())
    }

    async fn delete(&self, liq_id: &str) -> anyhow::Result<()> {
        self.rows.lock().expect("batch WAL lock").retain(|row| row.id != liq_id);
        Ok(())
    }
}

struct ScriptedAdapter {
    venue_id: &'static str,
    safe_through: Option<u64>,
    progresses: Mutex<VecDeque<VenueLegStatus>>,
    retryable_errors: Mutex<VecDeque<Option<String>>>,
    recovery_progresses: Mutex<VecDeque<VenueLegStatus>>,
    preview_calls: AtomicUsize,
    advance_calls: AtomicUsize,
    recover_calls: AtomicUsize,
    wal: Mutex<Weak<TestWal>>,
    observed_statuses: Mutex<Vec<Vec<VenueLegStatus>>>,
    /// Makes `advance` fail structurally, the way a decode or dispatch error
    /// does: the leg is never touched, so it cannot be holding funds.
    advance_failure: Option<String>,
    /// Makes `preview` fail the way a venue outage does, so the planner sees
    /// the venue as unavailable rather than as quoted.
    preview_failure: Option<String>,
}

impl ScriptedAdapter {
    fn new(venue_id: &'static str, safe_through: Option<u64>, progresses: Vec<VenueLegStatus>) -> Self {
        Self {
            venue_id,
            safe_through,
            progresses: Mutex::new(progresses.into()),
            retryable_errors: Mutex::new(VecDeque::new()),
            recovery_progresses: Mutex::new(VecDeque::new()),
            preview_calls: AtomicUsize::new(0),
            advance_calls: AtomicUsize::new(0),
            recover_calls: AtomicUsize::new(0),
            wal: Mutex::new(Weak::new()),
            observed_statuses: Mutex::new(Vec::new()),
            advance_failure: None,
            preview_failure: None,
        }
    }

    fn with_advance_failure(mut self, error: &str) -> Self {
        self.advance_failure = Some(error.to_string());
        self
    }

    fn with_preview_failure(mut self, error: &str) -> Self {
        self.preview_failure = Some(error.to_string());
        self
    }

    fn observe(&self, wal: &Arc<TestWal>) {
        *self.wal.lock().expect("observer lock") = Arc::downgrade(wal);
    }

    fn with_retryable_errors(self, errors: Vec<Option<String>>) -> Self {
        *self.retryable_errors.lock().expect("retry errors lock") = errors.into();
        self
    }

    fn with_recovery_progresses(self, progresses: Vec<VenueLegStatus>) -> Self {
        *self.recovery_progresses.lock().expect("recovery progress lock") = progresses.into();
        self
    }

    fn calls(&self) -> usize {
        self.advance_calls.load(Ordering::SeqCst)
    }

    fn recovery_calls(&self) -> usize {
        self.recover_calls.load(Ordering::SeqCst)
    }

    fn previews(&self) -> usize {
        self.preview_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MultiVenueAdapter for ScriptedAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    async fn preview(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        self.preview_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = &self.preview_failure {
            return Err(error.clone());
        }
        let pay = request
            .pay_amount
            .value
            .0
            .to_u64()
            .ok_or_else(|| "test pay amount does not fit u64".to_string())?;
        let impact = match self.safe_through {
            Some(limit) if pay > limit && self.venue_id == ICPSWAP_VENUE_ID => 200.0,
            Some(limit) if pay > limit => 201.0,
            _ => 50.0,
        };
        let receive = Nat::from(pay * 2);
        Ok(VenueRoutePreview {
            venue_id: self.venue_id.to_string(),
            request: request.clone(),
            quote: SwapQuote {
                pay_asset: request.pay_asset.clone(),
                pay_amount: request.pay_amount.value.clone(),
                receive_asset: request.receive_asset.clone(),
                receive_amount: receive.clone(),
                mid_price: 2.0,
                exec_price: 2.0,
                estimated_price_impact_bps: impact,
                legs: Vec::new(),
            },
            conservative_receive: ChainTokenAmount::from_raw(debt_token(), receive),
            initial_execution_state: VenueExecutionState {
                venue: self.venue_id.to_string(),
                state: serde_json::json!({ "step": "planned" }),
            },
        })
    }

    async fn advance(
        &self,
        leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = &self.advance_failure {
            return Err(error.clone());
        }
        if let Some(wal) = self.wal.lock().expect("observer lock").upgrade() {
            self.observed_statuses
                .lock()
                .expect("statuses lock")
                .push(wal.leg_statuses());
        }
        let status = self
            .progresses
            .lock()
            .expect("progress lock")
            .pop_front()
            .unwrap_or(VenueLegStatus::Running);
        let result = (status == VenueLegStatus::Completed).then(|| execution_for(leg));
        let retryable_error = self
            .retryable_errors
            .lock()
            .expect("retry errors lock")
            .pop_front()
            .flatten();
        Ok(VenueLegProgress {
            execution: VenueExecutionState {
                venue: self.venue_id.to_string(),
                state: serde_json::json!({ "status": format!("{status:?}") }),
            },
            status,
            result,
            last_error: retryable_error.clone(),
            retryable_error,
        })
    }

    async fn recover(
        &self,
        leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.recover_calls.fetch_add(1, Ordering::SeqCst);
        let status = self
            .recovery_progresses
            .lock()
            .expect("recovery progress lock")
            .pop_front()
            .unwrap_or(VenueLegStatus::OperatorRequired);
        Ok(VenueLegProgress {
            execution: leg.execution.clone(),
            status,
            result: (status == VenueLegStatus::Completed).then(|| execution_for(leg)),
            last_error: leg.last_error.clone(),
            retryable_error: None,
        })
    }
}

fn native_icp() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("native ICP ledger"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    }
}

fn debt_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).expect("ckUSDC ledger"),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10u64),
    }
}

fn ckusdt() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(CKUSDT_LEDGER_PRINCIPAL).expect("ckUSDT ledger"),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

fn receipt() -> ExecutionReceipt {
    receipt_with_id(7)
}

fn receipt_with_id(liquidation_id: u128) -> ExecutionReceipt {
    let collateral = native_icp();
    let debt = debt_token();
    ExecutionReceipt {
        request: ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(DEBT_REPAID),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: Some(SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(collateral.clone(), Nat::from(1u8)),
                receive_asset: debt.asset_id(),
                receive_address: Some("receiver".to_string()),
                max_slippage_bps: Some(500),
                venue_hint: None,
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 0,
            ref_price: Nat::from(10_000_000_000_000_000_000_000_000_000u128),
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            id: liquidation_id,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(TOTAL_PAY),
                debt_repaid: Nat::from(DEBT_REPAID),
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

fn row_for_receipt(receipt: ExecutionReceipt) -> LiqResultRecord {
    let mut row = LiqResultRecord {
        id: liq_id_from_receipt(&receipt).expect("liquidation id"),
        status: ResultStatus::Enqueued,
        attempt: 0,
        error_count: 0,
        last_error: None,
        created_at: 0,
        updated_at: 0,
        meta_json: String::new(),
    };
    encode_meta(
        &mut row,
        &LiqMetaWrapper {
            receipt,
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        },
    )
    .expect("encode batch WAL row");
    row
}

fn execution_for(leg: &VenueLegState) -> SwapExecution {
    SwapExecution {
        swap_id: 1,
        request_id: 1,
        status: "completed".to_string(),
        pay_asset: leg.request.pay_asset.clone(),
        pay_amount: leg.request.pay_amount.value.clone(),
        receive_asset: leg.request.receive_asset.clone(),
        receive_amount: leg.quote.estimated_receive.value.clone(),
        mid_price: 2.0,
        exec_price: 2.0,
        realized_slippage_bps: 0.0,
        legs: vec![SwapQuoteLeg {
            venue: leg.venue_id.clone(),
            route_id: leg.quote.route_id.clone(),
            pay_chain: leg.request.pay_amount.token.chain(),
            pay_symbol: leg.request.pay_amount.token.symbol(),
            pay_amount: leg.request.pay_amount.value.clone(),
            receive_chain: leg.quote.estimated_receive.token.chain(),
            receive_symbol: leg.quote.estimated_receive.token.symbol(),
            receive_amount: leg.quote.estimated_receive.value.clone(),
            price: 2.0,
            lp_fee: Nat::from(0u8),
            gas_fee: Nat::from(0u8),
        }],
        approval_count: None,
        ts: 0,
    }
}

fn planner_config() -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        max_price_impact_bps: 100.0,
        max_search_iterations: 16,
        dust_fallback_max_price_impact_bps: 150.0,
        max_cex_price_impact_bps: 200.0,
        cex_min_exec_usd: 0.01,
        min_net_edge_bps: 150,
        bad_debt_min_net_edge_bps: 150,
        max_oracle_discount_bps: 250,
        oracle_snapshot_max_age_secs: 300,
        icpswap_test_allocation_usd: None,
        mexc_test_allocation_usd: None,
    }
}

fn finalizer(adapters: Vec<Arc<dyn MultiVenueAdapter>>) -> MultiVenueFinalizer {
    MultiVenueFinalizer::new(adapters, planner_config()).expect("valid finalizer")
}

fn below_mexc_minimum_config() -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        cex_min_exec_usd: 11.0,
        ..planner_config()
    }
}

fn recovery_account() -> icrc_ledger_types::icrc1::account::Account {
    icrc_ledger_types::icrc1::account::Account {
        owner: Principal::from_slice(&[42]),
        subaccount: Some([7; 32]),
    }
}

fn committed_recovery_state(wal: &TestWal) -> crate::persistance::RecoverySweepState {
    let meta = wal.wrapper().meta_v2.expect("committed meta_v2");
    let FinalizerMetaPayload::RecoverySweep(state) = meta.payload else {
        panic!("expected recovery sweep state")
    };
    state
}

#[tokio::test]
async fn below_minimum_route_sweeps_exact_collateral_minus_fee_to_recovery() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let destination = recovery_account();
    let expected_account_id = ic_ledger_types::AccountIdentifier::new(
        &destination.owner,
        &ic_ledger_types::Subaccount(destination.subaccount.unwrap()),
    )
    .to_hex();
    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .withf(move |token, account, amount| {
            token.symbol() == "ICP"
                && account == &ChainAccount::IcpLedger(expected_account_id.clone())
                && amount == &Nat::from(TOTAL_PAY - 10_000)
        })
        .times(1)
        .returning(|_, _, _| Ok("recovery-block-123".to_string()));
    let finalizer = MultiVenueFinalizer::new(vec![mexc.clone()], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_recovery_sweep(Arc::new(transfers), destination);

    let completed = finalizer.finalize(&wal, receipt).await.expect("sweep to recovery");

    assert!(completed.finalized);
    assert!(!completed.operator_required);
    assert_eq!(completed.swapper.as_deref(), Some("recovery"));
    // No venue is touched: the sweep exists because none could execute.
    assert_eq!(mexc.calls(), 0);
    let state = committed_recovery_state(&wal);
    assert_eq!(state.status, RecoverySweepStatus::Completed);
    assert_eq!(state.amount.value, Nat::from(TOTAL_PAY - 10_000));
    assert_eq!(state.txid.as_deref(), Some("recovery-block-123"));
}

/// The transfer must be durable before it is submitted, so that a reader who
/// finds it can tell a planned sweep apart from an unrecorded one.
#[tokio::test]
async fn recovery_transfer_is_persisted_as_ready_before_it_is_submitted() {
    let receipt = receipt();
    let wal = Arc::new(TestWal::with_receipt(&receipt));
    let observed = Arc::new(Mutex::new(None));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));

    let mut transfers = MockTransferActions::new();
    let seen = observed.clone();
    let wal_during_transfer = wal.clone();
    transfers.expect_transfer().times(1).returning(move |_, _, _| {
        *seen.lock().expect("observation lock") = Some(committed_recovery_state(&wal_during_transfer));
        Ok("recovery-block-123".to_string())
    });

    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_recovery_sweep(Arc::new(transfers), recovery_account());
    finalizer
        .finalize(wal.as_ref(), receipt)
        .await
        .expect("sweep to recovery");

    let during = observed
        .lock()
        .expect("observation lock")
        .clone()
        .expect("transfer ran");
    assert_eq!(during.status, RecoverySweepStatus::ReadyToSubmit);
    assert_eq!(during.amount.value, Nat::from(TOTAL_PAY - 10_000));
    assert!(during.txid.is_none());
}

#[tokio::test]
async fn seven_dollar_ckusdt_is_swept_from_trader_to_the_icrc_recovery_account() {
    const RECEIVED: u64 = 7_125_292;
    const TRANSFERRED: u64 = 7_115_292;
    let mut receipt = receipt();
    let collateral = ckusdt();
    receipt.request.collateral_asset = collateral.clone();
    receipt.request.ref_price = Nat::from(1_000_000_000_000_000_000_000_000_000u128);
    let swap = receipt.request.swap_args.as_mut().expect("swap request");
    swap.pay_asset = collateral.asset_id();
    swap.pay_amount = ChainTokenAmount::from_raw(collateral, Nat::from(RECEIVED));
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .collateral_received = Nat::from(RECEIVED);

    let wal = TestWal::with_receipt(&receipt);
    let destination = recovery_account();
    let expected_destination = destination;
    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .withf(move |token, account, amount| {
            token.symbol() == "ckUSDT"
                && account == &ChainAccount::Icp(expected_destination)
                && amount == &Nat::from(TRANSFERRED)
        })
        .times(1)
        .returning(|_, _, _| Ok("ckusdt-recovery-block".to_string()));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let mut config = planner_config();
    config.cex_min_exec_usd = 8.0;
    let finalizer = MultiVenueFinalizer::new(vec![mexc], config)
        .expect("valid finalizer")
        .with_recovery_sweep(Arc::new(transfers), destination);

    let completed = finalizer.finalize(&wal, receipt).await.expect("sweep to recovery");

    assert!(completed.finalized);
    let state = committed_recovery_state(&wal);
    assert_eq!(state.amount.value, Nat::from(TRANSFERRED));
    assert_eq!(state.txid.as_deref(), Some("ckusdt-recovery-block"));
}

/// A ready transfer read back from the WAL is ambiguous: the process that wrote
/// it may have submitted it and died, or never reached the ledger at all. It is
/// never repeated automatically, whichever process finds it.
#[tokio::test]
async fn ready_recovery_sweep_loaded_from_the_wal_is_parked_without_transfer() {
    let receipt = receipt();
    let wal = TestWal::with_recovery_sweep(&receipt, RecoverySweepStatus::ReadyToSubmit);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let watchdog = Arc::new(RecordingWatchdog::default());

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_watchdog(watchdog.clone())
        .with_recovery_sweep(Arc::new(transfers), recovery_account());

    let result = finalizer.finalize(&wal, receipt).await.expect("park ambiguous sweep");

    assert!(result.operator_required);
    assert!(!result.finalized);
    let state = committed_recovery_state(&wal);
    assert_eq!(state.status, RecoverySweepStatus::OperatorRequired);
    assert!(state.txid.is_none());
    assert!(
        state.last_error.is_some(),
        "the ambiguity must be recorded for an operator"
    );
    // The alert has to name the liquidation an operator needs to reconcile.
    assert_eq!(
        watchdog.alerts(),
        vec![("recovery-7".to_string(), "recovery_transfer_reconciliation".to_string())]
    );
}

/// A completed sweep is terminal: reprocessing the row must not transfer twice.
#[tokio::test]
async fn completed_recovery_sweep_is_not_transferred_again() {
    let receipt = receipt();
    let wal = TestWal::with_recovery_sweep(&receipt, RecoverySweepStatus::Completed);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_recovery_sweep(Arc::new(transfers), recovery_account());

    let result = finalizer.finalize(&wal, receipt).await.expect("terminal sweep");

    assert!(result.finalized);
    assert!(!result.operator_required);
    assert_eq!(committed_recovery_state(&wal).status, RecoverySweepStatus::Completed);
}

/// An error from the ledger may or may not have applied the transfer, so the
/// sweep is escalated rather than retried on the next cycle.
#[tokio::test]
async fn ambiguous_recovery_transfer_error_parks_the_sweep() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let watchdog = Arc::new(RecordingWatchdog::default());

    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer()
        .times(1)
        .returning(|_, _, _| Err(TransferFailure::Ambiguous("ledger call timed out".to_string())));
    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_watchdog(watchdog.clone())
        .with_recovery_sweep(Arc::new(transfers), recovery_account());

    let result = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("park ambiguous transfer");

    assert!(result.operator_required);
    assert!(!result.finalized);
    let state = committed_recovery_state(&wal);
    assert_eq!(state.status, RecoverySweepStatus::OperatorRequired);
    assert!(
        state
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("ledger call timed out")),
        "the ledger failure must survive for an operator: {:?}",
        state.last_error
    );
    assert!(
        state
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("may already have been applied")),
        "a lost answer must send the operator to compare balances: {:?}",
        state.last_error
    );
    assert_eq!(watchdog.alerts().len(), 1);
}

/// A refusal the ledger decided is not ambiguous: nothing moved, and the
/// operator must not be sent to reconcile balances over it.
#[tokio::test]
async fn rejected_recovery_transfer_is_parked_as_having_moved_nothing() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let watchdog = Arc::new(RecordingWatchdog::default());

    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(1).returning(|_, _, _| {
        Err(TransferFailure::Rejected(
            "icp_transfer error: InsufficientFunds { balance: Tokens { e8s: 1 } }".to_string(),
        ))
    });
    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_watchdog(watchdog.clone())
        .with_recovery_sweep(Arc::new(transfers), recovery_account());

    let result = finalizer.finalize(&wal, receipt).await.expect("park refused transfer");

    assert!(result.operator_required);
    let state = committed_recovery_state(&wal);
    assert_eq!(state.status, RecoverySweepStatus::OperatorRequired);
    assert!(state.txid.is_none());
    let reported = state.last_error.expect("refusal must be recorded");
    assert!(
        reported.contains("InsufficientFunds"),
        "the ledger's reason must survive: {reported}"
    );
    assert!(
        reported.contains("moved nothing"),
        "a decided refusal must say the balance is intact: {reported}"
    );
    assert!(
        !reported.contains("may already have been applied"),
        "a decided refusal must not be reported as ambiguous: {reported}"
    );
    assert_eq!(watchdog.alerts().len(), 1);
}

/// A venue outage alongside a below-minimum venue must never be mistaken for
/// "this amount can never be traded". The planner's summary still contains the
/// below-minimum wording, so this is exactly the case that a text match on that
/// wording would sweep: the collateral would leave the swap path permanently
/// because ICPSwap happened to be down for a moment.
#[tokio::test]
async fn transient_icpswap_outage_below_minimum_is_retried_instead_of_swept() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, Vec::new()).with_preview_failure("canister is out of cycles"),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    let finalizer = MultiVenueFinalizer::new(vec![icpswap, mexc], below_mexc_minimum_config())
        .expect("valid finalizer")
        .with_recovery_sweep(Arc::new(transfers), recovery_account());

    let error = finalizer.finalize(&wal, receipt).await.expect_err("no route yet");

    // The wording a text match would have keyed on is present...
    assert!(
        error.message().contains("amount is below its minimum"),
        "expected the below-minimum wording to still appear: {error}"
    );
    // ...but the outage, not the amount, is what blocked the route.
    assert!(
        error.message().contains("canister is out of cycles"),
        "expected the outage to be reported: {error}"
    );
    assert!(
        matches!(error, FinalizerError::Retryable(_)),
        "unexpected kind: {error:?}"
    );
    assert!(
        wal.wrapper().meta_v2.is_none(),
        "an outage must not commit a recovery sweep"
    );
}

/// Committed state this build cannot read is a code or config problem, not a
/// dead liquidation. Bumping the meta version — or tightening any invariant —
/// must not permanently fail rows whose venue legs may still hold funds; they
/// are parked for an operator, the way startup already handles this.
#[tokio::test]
async fn committed_state_this_build_cannot_read_is_parked_not_failed() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));

    // Commit a plan, then age it out from under the running binary.
    let finalizer = finalizer(vec![mexc.clone()]);
    let _ = finalizer.finalize(&wal, receipt.clone()).await;
    let mut wrapper = wal.wrapper();
    let meta = wrapper.meta_v2.as_mut().expect("committed meta_v2");
    meta.version = FINALIZER_META_V2_VERSION + 1;
    wal.replace_wrapper(&wrapper);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("unreadable committed state cannot be planned");

    assert!(
        matches!(error, FinalizerError::Unresumable(_)),
        "an unreadable row must be parked, not failed: {error:?}"
    );
    assert!(
        error.message().contains("cannot be read by this build"),
        "the reason must point at the binary, not the liquidation: {error}"
    );
}

/// A receipt that cannot be planned from is broken in a way no retry can
/// repair, so it must not spend the row's retry budget before saying so.
#[tokio::test]
async fn a_receipt_that_cannot_be_planned_from_fails_permanently_on_the_first_attempt() {
    let mut receipt = receipt();
    receipt.status = ExecutionStatus::SwapFailed("an earlier finalizer failure".to_string());
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![mexc.clone()]);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("an unsuccessful execution cannot be planned");

    assert!(
        error.message().contains("receipt execution is not successful"),
        "the reason must survive: {error}"
    );
    assert!(
        matches!(error, FinalizerError::Permanent(_)),
        "unexpected kind: {error:?}"
    );
    assert_eq!(mexc.previews(), 0, "no venue should be quoted for an unusable receipt");
    assert!(wal.wrapper().meta_v2.is_none(), "nothing may be committed");
}

#[tokio::test]
async fn below_minimum_route_without_recovery_runtime_is_terminal() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = MultiVenueFinalizer::new(vec![mexc], below_mexc_minimum_config()).expect("valid finalizer");

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("missing recovery runtime");
    assert!(error.message().contains("amount is below its minimum"));
    assert!(
        matches!(error, FinalizerError::Permanent(_)),
        "unexpected kind: {error:?}"
    );
}

fn committed_state(wal: &TestWal) -> MultiVenueExecutionState {
    let meta = wal.wrapper().meta_v2.expect("committed meta_v2");
    let FinalizerMetaPayload::MultiVenueSwap(state) = meta.payload else {
        panic!("expected multi-venue state")
    };
    state
}

#[tokio::test]
async fn bad_debt_below_minimum_edge_never_advances_a_venue_leg() {
    let mut receipt = receipt();
    receipt.request.liquidation.buy_bad_debt = true;
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .debt_repaid = Nat::from(199_000_000u64);
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, Vec::new()));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("a bad-debt receipt does not bypass the swap edge floor");

    assert!(error.message().contains("below required 150 bps"));
    assert_eq!(icpswap.previews(), 1);
    assert_eq!(mexc.previews(), 0);
    assert_eq!(icpswap.calls(), 0);
    assert_eq!(mexc.calls(), 0);
    assert!(wal.wrapper().meta_v2.is_none(), "a rejected plan must not be committed");
}

/// Bad debt repays more than the collateral is worth by construction, so a
/// non-negative floor strands it forever. A negative floor opts into recycling
/// that collateral, and applies only to rows actually bought as bad debt.
#[tokio::test]
async fn a_negative_bad_debt_floor_recycles_collateral_a_positive_one_strands() {
    async fn plan_with(bad_debt_floor: i32, buy_bad_debt: bool) -> Result<FinalizerResult, FinalizerError> {
        let mut receipt = receipt();
        receipt.request.liquidation.buy_bad_debt = buy_bad_debt;
        receipt
            .liquidation_result
            .as_mut()
            .expect("liquidation result")
            .amounts
            .debt_repaid = Nat::from(199_000_000u64);
        let wal = TestWal::with_receipt(&receipt);
        let icpswap = Arc::new(ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, Vec::new()));
        let config = IcpswapFirstPlannerConfig {
            bad_debt_min_net_edge_bps: bad_debt_floor,
            ..planner_config()
        };
        let finalizer = MultiVenueFinalizer::new(vec![icpswap], config).expect("valid finalizer");
        finalizer.finalize(&wal, receipt).await
    }

    // The row from the existing test: underwater, and rejected while the
    // bad-debt floor still demands a profit.
    let error = plan_with(150, true).await.expect_err("a positive floor strands it");
    assert!(error.message().contains("below required 150 bps"));

    // The same row clears once the floor states how much shortfall may recycle.
    plan_with(-6000, true).await.expect("a negative floor recycles it");

    // A normal liquidation is untouched by the bad-debt floor: it keeps being
    // measured against min_net_edge_bps, so it is still rejected.
    let error = plan_with(-6000, false)
        .await
        .expect_err("a profitable-liquidation row keeps its own floor");
    assert!(error.message().contains("below required 150 bps"));
}

#[tokio::test]
async fn commits_plan_before_effects_and_journals_legs_in_vector_order() {
    let receipt = receipt();
    let wal = Arc::new(TestWal::with_receipt(&receipt));
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        Some(TOTAL_PAY / 2),
        vec![VenueLegStatus::Completed],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::Running, VenueLegStatus::Completed],
    ));
    icpswap.observe(&wal);
    mexc.observe(&wal);
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let first = finalizer
        .finalize(wal.as_ref(), receipt.clone())
        .await
        .expect("first cycle");
    assert!(!first.finalized);
    assert_eq!(
        icpswap.observed_statuses.lock().expect("statuses lock").as_slice(),
        &[vec![VenueLegStatus::Planned, VenueLegStatus::Planned]]
    );
    assert_eq!(
        mexc.observed_statuses.lock().expect("statuses lock").as_slice(),
        &[vec![VenueLegStatus::Completed, VenueLegStatus::Planned]]
    );
    let first_state = committed_state(&wal);
    assert_eq!(first_state.legs[0].status, VenueLegStatus::Completed);
    assert_eq!(first_state.legs[1].status, VenueLegStatus::Running);
    assert_eq!(first_state.outcome, MultiVenueExecutionOutcome::Running);
    let allocations = first_state
        .legs
        .iter()
        .map(|leg| leg.request.pay_amount.value.clone())
        .collect::<Vec<_>>();

    let second = finalizer.finalize(wal.as_ref(), receipt).await.expect("second cycle");
    assert!(second.finalized);
    let aggregate = second.swap_result.expect("split result should be aggregated");
    assert_eq!(aggregate.pay_amount, Nat::from(TOTAL_PAY));
    assert_eq!(aggregate.receive_amount, Nat::from(TOTAL_PAY * 2));
    assert_eq!(
        aggregate.legs.iter().map(|leg| leg.venue.as_str()).collect::<Vec<_>>(),
        vec![ICPSWAP_VENUE_ID, MEXC_VENUE_ID]
    );
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(mexc.calls(), 2);
    let second_state = committed_state(&wal);
    assert_eq!(second_state.outcome, MultiVenueExecutionOutcome::Completed);
    assert_eq!(
        second_state
            .legs
            .iter()
            .map(|leg| leg.request.pay_amount.value.clone())
            .collect::<Vec<_>>(),
        allocations
    );
}

#[tokio::test]
async fn three_venue_waterfall_is_committed_before_execution_and_aggregated_in_order() {
    let receipt = receipt();
    let wal = Arc::new(TestWal::with_receipt(&receipt));
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        Some(40_000_000),
        vec![VenueLegStatus::Completed],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        Some(30_000_000),
        vec![VenueLegStatus::Completed],
    ));
    let kraken = Arc::new(ScriptedAdapter::new(
        KRAKEN_VENUE_ID,
        None,
        vec![VenueLegStatus::Completed],
    ));
    icpswap.observe(&wal);
    mexc.observe(&wal);
    kraken.observe(&wal);
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone(), kraken.clone()]);

    let result = finalizer
        .finalize(wal.as_ref(), receipt)
        .await
        .expect("three venue execution");

    assert!(result.finalized);
    assert_eq!(
        icpswap.observed_statuses.lock().expect("statuses lock").as_slice(),
        &[vec![
            VenueLegStatus::Planned,
            VenueLegStatus::Planned,
            VenueLegStatus::Planned,
        ]]
    );
    assert_eq!(
        mexc.observed_statuses.lock().expect("statuses lock").as_slice(),
        &[vec![
            VenueLegStatus::Completed,
            VenueLegStatus::Planned,
            VenueLegStatus::Planned,
        ]]
    );
    assert_eq!(
        kraken.observed_statuses.lock().expect("statuses lock").as_slice(),
        &[vec![
            VenueLegStatus::Completed,
            VenueLegStatus::Completed,
            VenueLegStatus::Planned,
        ]]
    );
    let aggregate = result.swap_result.expect("aggregated waterfall result");
    assert_eq!(aggregate.pay_amount, Nat::from(TOTAL_PAY));
    assert_eq!(aggregate.receive_amount, Nat::from(TOTAL_PAY * 2));
    assert_eq!(
        aggregate.legs.iter().map(|leg| leg.venue.as_str()).collect::<Vec<_>>(),
        vec![ICPSWAP_VENUE_ID, MEXC_VENUE_ID, KRAKEN_VENUE_ID]
    );
}

#[tokio::test]
async fn operator_required_mexc_leg_is_parked_across_restarts() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::OperatorRequired],
    ));
    let watchdog = Arc::new(RecordingWatchdog::default());
    let finalizer = MultiVenueFinalizer::new(vec![mexc.clone()], planner_config())
        .expect("valid MEXC-only finalizer")
        .with_watchdog(watchdog.clone());

    assert!(
        !finalizer
            .finalize(&wal, receipt.clone())
            .await
            .expect("first cycle")
            .finalized
    );

    // `meta_v2` is authoritative even if stale legacy fields coexist in the
    // wrapper, which also makes this restart precedence explicit.
    let mut row = wal.row.lock().expect("WAL lock").clone().expect("WAL row");
    let mut wrapper = decode_receipt_wrapper(&row)
        .expect("decode wrapper")
        .expect("wrapper exists");
    wrapper.meta = vec![1];
    wrapper.venue_execution = Some(VenueExecutionState {
        venue: ICPSWAP_VENUE_ID.to_string(),
        state: serde_json::json!({ "stale": true }),
    });
    encode_meta(&mut row, &wrapper).expect("encode wrapper");
    *wal.row.lock().expect("WAL lock") = Some(row);

    assert!(
        !finalizer
            .finalize(&wal, receipt)
            .await
            .expect("restart cycle")
            .finalized
    );
    assert_eq!(mexc.calls(), 1);
    assert_eq!(mexc.recovery_calls(), 0);
    assert_eq!(
        watchdog.0.lock().expect("watchdog lock").as_slice(),
        &[("mexc-0".to_string(), "venue_reconciliation".to_string())]
    );
    assert!(matches!(
        committed_state(&wal).outcome,
        MultiVenueExecutionOutcome::OperatorRequired { .. }
    ));
}

#[tokio::test]
async fn permanent_failure_is_not_readvanced_or_rerouted() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, vec![VenueLegStatus::FailedPermanent])
            .with_retryable_errors(vec![Some("deposit outcome could not be proven".to_string())]),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let first_error = finalizer
        .finalize(&wal, receipt.clone())
        .await
        .expect_err("permanent failure");
    assert!(
        matches!(first_error, FinalizerError::Permanent(_)),
        "unexpected kind: {first_error:?}"
    );
    assert!(
        first_error
            .message()
            .contains("icpswap-0: deposit outcome could not be proven")
    );
    finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("restart remains failed");
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(mexc.calls(), 0);
    assert!(matches!(
        committed_state(&wal).outcome,
        MultiVenueExecutionOutcome::PartialRecovered { .. }
    ));
}

#[tokio::test]
async fn partial_recovery_still_reports_the_completed_leg_proceeds() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    // A split plan where ICPSwap completes and MEXC fails permanently.
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        Some(TOTAL_PAY / 2),
        vec![VenueLegStatus::Completed],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::FailedPermanent],
    ));
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let result = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("partial recovery keeps the completed proceeds");

    let state = committed_state(&wal);
    assert!(matches!(
        state.outcome,
        MultiVenueExecutionOutcome::PartialRecovered { .. }
    ));
    // The permanent failure is reported as context, not as a lost execution.
    assert!(result.finalized);
    assert!(!result.operator_required);
    assert!(
        result
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed permanently"))
    );
    let aggregate = result.swap_result.expect("completed leg proceeds are aggregated");
    assert_eq!(aggregate.pay_amount, Nat::from(TOTAL_PAY / 2));
    assert_eq!(
        aggregate.legs.iter().map(|leg| leg.venue.as_str()).collect::<Vec<_>>(),
        vec![ICPSWAP_VENUE_ID]
    );
}

#[tokio::test]
async fn failed_icpswap_leg_does_not_prevent_the_split_mexc_leg_from_completing() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        Some(TOTAL_PAY / 2),
        vec![VenueLegStatus::FailedPermanent],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::Completed],
    ));
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let result = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("the completed MEXC proceeds should finish the partial result");

    assert!(result.finalized);
    assert!(!result.operator_required);
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(mexc.calls(), 1);
    assert!(matches!(
        committed_state(&wal).outcome,
        MultiVenueExecutionOutcome::PartialRecovered { .. }
    ));
    let aggregate = result.swap_result.expect("completed MEXC result");
    assert_eq!(aggregate.pay_amount, Nat::from(TOTAL_PAY / 2));
    assert_eq!(
        aggregate.legs.iter().map(|leg| leg.venue.as_str()).collect::<Vec<_>>(),
        vec![MEXC_VENUE_ID]
    );
}

#[tokio::test]
async fn committed_legacy_state_is_rejected_without_being_upgraded() {
    let receipt = receipt();
    let mut wrapper = LiqMetaWrapper {
        receipt: receipt.clone(),
        meta: vec![1],
        finalizer_decision: None,
        profit_snapshot: None,
        venue_execution: None,
        meta_v2: None,
    };
    wrapper.venue_execution = Some(VenueExecutionState {
        venue: MEXC_VENUE_ID.to_string(),
        state: serde_json::json!({ "step": "deposit" }),
    });
    let wal = TestWal::with_wrapper(wrapper);
    let adapter = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        None,
        vec![VenueLegStatus::Completed],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![adapter.clone(), mexc.clone()]);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("legacy state must be rejected");
    assert!(
        matches!(error, FinalizerError::Permanent(_)),
        "unexpected kind: {error:?}"
    );
    assert_eq!(adapter.calls(), 0);
    assert_eq!(mexc.calls(), 0);
    assert!(wal.wrapper().meta_v2.is_none());
}

#[tokio::test]
async fn adapter_error_is_persisted_before_retry_backoff_is_requested() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, vec![VenueLegStatus::Running])
            .with_retryable_errors(vec![Some("temporary pool outage".to_string())]),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap, mexc]);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("operational error should reach FinalizeStage backoff");

    // The leg has begun, so the venue may already hold the funds. The error
    // stays retryable, but is tagged so that exhausting the retry budget parks
    // the row for an operator instead of failing it permanently.
    assert!(
        matches!(error, FinalizerError::VenueCustody(_)),
        "unexpected kind: {error:?}"
    );
    let state = committed_state(&wal);
    assert_eq!(state.legs[0].status, VenueLegStatus::Running);
    assert_eq!(state.legs[0].last_error.as_deref(), Some("temporary pool outage"));
}

/// A failure raised before any leg leaves `Planned` cannot have moved funds, so
/// it must stay an ordinary retryable error whose budget still ends in a
/// permanent failure rather than an operator park.
#[tokio::test]
async fn error_before_any_leg_starts_is_not_tagged_as_holding_custody() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, Vec::new()).with_advance_failure("venue dispatch failed"),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap, mexc]);

    let error = finalizer
        .finalize(&wal, receipt)
        .await
        .expect_err("a structural adapter failure reaches FinalizeStage");

    assert!(
        matches!(error, FinalizerError::Retryable(_)),
        "unexpected kind: {error:?}"
    );
    assert_eq!(committed_state(&wal).legs[0].status, VenueLegStatus::Planned);
}

#[tokio::test]
async fn mexc_only_registry_commits_a_single_mexc_leg() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::Completed],
    ));
    let finalizer = MultiVenueFinalizer::new(vec![mexc], planner_config()).expect("valid finalizer");

    let result = finalizer.finalize(&wal, receipt).await.expect("MEXC execution");

    assert!(result.finalized);
    let state = committed_state(&wal);
    assert_eq!(state.plan.strategy_id, "icpswap_first");
    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn retryable_mexc_row_does_not_block_the_next_liquidation_in_the_finalize_batch() {
    const RETRYING_ID: u128 = 801;
    const SUCCEEDING_ID: u128 = 802;
    let wal = Arc::new(BatchWal::new([
        receipt_with_id(RETRYING_ID),
        receipt_with_id(SUCCEEDING_ID),
    ]));
    let mexc = Arc::new(
        ScriptedAdapter::new(
            MEXC_VENUE_ID,
            None,
            vec![VenueLegStatus::Running, VenueLegStatus::Completed],
        )
        .with_retryable_errors(vec![Some("temporary MEXC outage".to_string()), None]),
    );
    let finalizer = Arc::new(MultiVenueFinalizer::new(vec![mexc.clone()], planner_config()).expect("valid finalizer"));
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    let outcomes = stage.process(&()).await.expect("the finalize batch should continue");

    assert_eq!(wal.status(RETRYING_ID), ResultStatus::FailedRetryable);
    assert_eq!(wal.status(SUCCEEDING_ID), ResultStatus::Succeeded);
    assert_eq!(mexc.calls(), 2);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0]
            .execution_receipt
            .liquidation_result
            .as_ref()
            .expect("successful liquidation result")
            .id,
        SUCCEEDING_ID
    );
}

#[tokio::test]
async fn operator_required_mexc_row_does_not_block_the_next_liquidation_in_the_finalize_batch() {
    const PARKED_ID: u128 = 803;
    const SUCCEEDING_ID: u128 = 804;
    let wal = Arc::new(BatchWal::new([
        receipt_with_id(PARKED_ID),
        receipt_with_id(SUCCEEDING_ID),
    ]));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::OperatorRequired, VenueLegStatus::Completed],
    ));
    let finalizer = Arc::new(MultiVenueFinalizer::new(vec![mexc.clone()], planner_config()).expect("valid finalizer"));
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    let outcomes = stage.process(&()).await.expect("the finalize batch should continue");

    assert_eq!(wal.status(PARKED_ID), ResultStatus::OperatorRequired);
    assert_eq!(wal.status(SUCCEEDING_ID), ResultStatus::Succeeded);
    assert_eq!(mexc.calls(), 2);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0]
            .execution_receipt
            .liquidation_result
            .as_ref()
            .expect("successful liquidation result")
            .id,
        SUCCEEDING_ID
    );
}

#[tokio::test]
async fn retryable_mexc_row_resumes_its_committed_plan_after_backoff() {
    const LIQUIDATION_ID: u128 = 805;
    let wal = Arc::new(BatchWal::new([receipt_with_id(LIQUIDATION_ID)]));
    let mexc = Arc::new(
        ScriptedAdapter::new(
            MEXC_VENUE_ID,
            None,
            vec![VenueLegStatus::Running, VenueLegStatus::Completed],
        )
        .with_retryable_errors(vec![Some("temporary MEXC outage".to_string()), None]),
    );
    let finalizer = Arc::new(MultiVenueFinalizer::new(vec![mexc.clone()], planner_config()).expect("valid finalizer"));
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    assert!(stage.process(&()).await.expect("first finalize cycle").is_empty());
    assert_eq!(wal.status(LIQUIDATION_ID), ResultStatus::FailedRetryable);
    let committed_before_retry = wal.committed_state(LIQUIDATION_ID);
    wal.make_retry_due(LIQUIDATION_ID);

    let outcomes = stage.process(&()).await.expect("retry cycle");

    assert_eq!(wal.status(LIQUIDATION_ID), ResultStatus::Succeeded);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(mexc.calls(), 2);
    assert_eq!(mexc.previews(), 1, "a committed retry must not quote or plan again");
    let completed = wal.committed_state(LIQUIDATION_ID);
    assert_eq!(completed.plan, committed_before_retry.plan);
    assert_eq!(
        completed
            .legs
            .iter()
            .map(|leg| (&leg.leg_id, &leg.venue_id, &leg.request.pay_amount))
            .collect::<Vec<_>>(),
        committed_before_retry
            .legs
            .iter()
            .map(|leg| (&leg.leg_id, &leg.venue_id, &leg.request.pay_amount))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn reenqueuing_the_parent_row_does_not_rearm_an_operator_required_mexc_leg() {
    const LIQUIDATION_ID: u128 = 806;
    let wal = Arc::new(BatchWal::new([receipt_with_id(LIQUIDATION_ID)]));
    let mexc = Arc::new(
        ScriptedAdapter::new(
            MEXC_VENUE_ID,
            None,
            vec![VenueLegStatus::OperatorRequired, VenueLegStatus::Completed],
        )
        .with_recovery_progresses(vec![VenueLegStatus::Running]),
    );
    let finalizer = Arc::new(MultiVenueFinalizer::new(vec![mexc.clone()], planner_config()).expect("valid finalizer"));
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    assert!(stage.process(&()).await.expect("parking cycle").is_empty());
    assert_eq!(wal.status(LIQUIDATION_ID), ResultStatus::OperatorRequired);
    assert_eq!(mexc.calls(), 1);

    wal.set_status(LIQUIDATION_ID, ResultStatus::Enqueued);
    assert!(stage.process(&()).await.expect("polling cycle").is_empty());
    assert_eq!(wal.status(LIQUIDATION_ID), ResultStatus::OperatorRequired);
    assert_eq!(mexc.recovery_calls(), 0);
    assert_eq!(mexc.calls(), 1);
    assert_eq!(mexc.previews(), 1);
}

#[tokio::test]
async fn operator_required_isolated_icpswap_row_does_not_block_the_next_batch_row() {
    const PARKED_ID: u128 = 807;
    const SUCCEEDING_ID: u128 = 808;
    let wal = Arc::new(BatchWal::new([
        receipt_with_id(PARKED_ID),
        receipt_with_id(SUCCEEDING_ID),
    ]));
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        None,
        vec![VenueLegStatus::OperatorRequired, VenueLegStatus::Completed],
    ));
    let finalizer = Arc::new(
        MultiVenueFinalizer::new(vec![icpswap.clone()], planner_config()).expect("valid ICPSwap-only finalizer"),
    );
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    let outcomes = stage.process(&()).await.expect("the next ICPSwap row should continue");

    assert_eq!(wal.status(PARKED_ID), ResultStatus::OperatorRequired);
    assert_eq!(wal.status(SUCCEEDING_ID), ResultStatus::Succeeded);
    assert_eq!(icpswap.calls(), 2);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0]
            .execution_receipt
            .liquidation_result
            .as_ref()
            .expect("successful liquidation result")
            .id,
        SUCCEEDING_ID
    );
}

#[tokio::test]
async fn mexc_custody_row_exhausting_retries_parks_without_blocking_the_next_batch_row() {
    const EXHAUSTED_ID: u128 = 809;
    const SUCCEEDING_ID: u128 = 810;
    let wal = Arc::new(BatchWal::new([
        receipt_with_id(EXHAUSTED_ID),
        receipt_with_id(SUCCEEDING_ID),
    ]));
    wal.set_error_count(EXHAUSTED_ID, MAX_FINALIZER_ERRORS - 1);
    let mexc = Arc::new(
        ScriptedAdapter::new(
            MEXC_VENUE_ID,
            None,
            vec![VenueLegStatus::Running, VenueLegStatus::Completed],
        )
        .with_retryable_errors(vec![Some("last retryable MEXC failure".to_string()), None]),
    );
    let finalizer = Arc::new(MultiVenueFinalizer::new(vec![mexc.clone()], planner_config()).expect("valid finalizer"));
    let stage = FinalizeStage::new(
        wal.clone(),
        finalizer,
        Arc::new(SimpleProfitCalculator),
        Arc::new(MockPipelineAgent::new()),
        Principal::anonymous(),
        1,
        60,
    );

    let outcomes = stage.process(&()).await.expect("failure-limit batch should continue");

    assert_eq!(wal.status(EXHAUSTED_ID), ResultStatus::OperatorRequired);
    assert_eq!(wal.status(SUCCEEDING_ID), ResultStatus::Succeeded);
    assert_eq!(mexc.calls(), 2);
    assert_eq!(outcomes.len(), 1);
}
