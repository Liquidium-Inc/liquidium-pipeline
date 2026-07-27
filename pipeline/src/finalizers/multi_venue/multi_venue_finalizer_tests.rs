use super::*;

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use candid::{Nat, Principal};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    },
};
use num_traits::ToPrimitive;

use crate::{
    executors::executor::ExecutorRequest,
    finalizers::multi_venue::{
        ICPSWAP_VENUE_ID, MEXC_VENUE_ID, VenueExecutionLock, VenueLegProgress, VenueRoutePreview,
    },
    persistance::{LiqResultRecord, ResultStatus, VenueExecutionState},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest},
    utils::ICP_LEDGER_PRINCIPAL,
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt},
    watchdog::{Watchdog, WatchdogEvent},
};

const TOTAL_PAY: u64 = 100_000_000;
const DEBT_REPAID: u64 = 190_000_000;

#[derive(Default)]
struct RecordingWatchdog(Mutex<Vec<(String, String)>>);

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
    lock_available: AtomicBool,
    lock_release_available: AtomicBool,
    lock_acquisitions: AtomicUsize,
    lock_releases: AtomicUsize,
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
            lock_available: AtomicBool::new(true),
            lock_release_available: AtomicBool::new(true),
            lock_acquisitions: AtomicUsize::new(0),
            lock_releases: AtomicUsize::new(0),
        }
    }

    fn wrapper(&self) -> LiqMetaWrapper {
        let row = self.row.lock().expect("WAL lock").clone().expect("WAL row");
        decode_receipt_wrapper(&row)
            .expect("decode wrapper")
            .expect("wrapper exists")
    }

    fn leg_statuses(&self) -> Vec<VenueLegStatus> {
        let wrapper = self.wrapper();
        let meta = wrapper.meta_v2.expect("committed meta_v2");
        let FinalizerMetaPayload::MultiVenueSwap(state) = meta.payload;
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

    async fn acquire_icpswap_execution_lock(
        &self,
        _owner: &str,
        _execution_id: &str,
    ) -> anyhow::Result<bool> {
        self.lock_acquisitions.fetch_add(1, Ordering::SeqCst);
        Ok(self.lock_available.load(Ordering::SeqCst))
    }

    async fn release_icpswap_execution_lock(
        &self,
        _owner: &str,
        _execution_id: &str,
    ) -> anyhow::Result<()> {
        self.lock_releases.fetch_add(1, Ordering::SeqCst);
        if !self.lock_release_available.load(Ordering::SeqCst) {
            anyhow::bail!("injected lock release failure");
        }
        Ok(())
    }
}

struct ScriptedAdapter {
    venue_id: &'static str,
    safe_through: Option<u64>,
    progresses: Mutex<VecDeque<VenueLegStatus>>,
    retryable_errors: Mutex<VecDeque<Option<String>>>,
    advance_calls: AtomicUsize,
    recover_calls: AtomicUsize,
    wal: Mutex<Weak<TestWal>>,
    observed_statuses: Mutex<Vec<Vec<VenueLegStatus>>>,
    execution_lock: Option<VenueExecutionLock>,
}

impl ScriptedAdapter {
    fn new(venue_id: &'static str, safe_through: Option<u64>, progresses: Vec<VenueLegStatus>) -> Self {
        Self {
            venue_id,
            safe_through,
            progresses: Mutex::new(progresses.into()),
            retryable_errors: Mutex::new(VecDeque::new()),
            advance_calls: AtomicUsize::new(0),
            recover_calls: AtomicUsize::new(0),
            wal: Mutex::new(Weak::new()),
            observed_statuses: Mutex::new(Vec::new()),
            execution_lock: None,
        }
    }

    fn observe(&self, wal: &Arc<TestWal>) {
        *self.wal.lock().expect("observer lock") = Arc::downgrade(wal);
    }

    fn with_retryable_errors(self, errors: Vec<Option<String>>) -> Self {
        *self.retryable_errors.lock().expect("retry errors lock") = errors.into();
        self
    }

    fn with_execution_lock(mut self, owner_key: &str, execution_id: &str) -> Self {
        self.execution_lock = Some(VenueExecutionLock {
            owner_key: owner_key.to_string(),
            execution_id: execution_id.to_string(),
        });
        self
    }

    fn calls(&self) -> usize {
        self.advance_calls.load(Ordering::SeqCst)
    }

    fn recovery_calls(&self) -> usize {
        self.recover_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MultiVenueAdapter for ScriptedAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    async fn preview(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        let pay = request
            .pay_amount
            .value
            .0
            .to_u64()
            .ok_or_else(|| "test pay amount does not fit u64".to_string())?;
        let impact = match self.safe_through {
            Some(limit) if pay > limit => 200.0,
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

    fn execution_lock(&self, _leg: &VenueLegState) -> Result<Option<VenueExecutionLock>, String> {
        Ok(self.execution_lock.clone())
    }

    async fn advance(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        self.advance_calls.fetch_add(1, Ordering::SeqCst);
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

    async fn recover(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        self.recover_calls.fetch_add(1, Ordering::SeqCst);
        Ok(VenueLegProgress {
            execution: leg.execution.clone(),
            status: VenueLegStatus::OperatorRequired,
            result: None,
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
        ledger: Principal::from_slice(&[2]),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10u64),
    }
}

fn receipt() -> ExecutionReceipt {
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
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            id: 7,
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
        cex_min_exec_usd: 0.01,
        min_net_edge_bps: 150,
        overflow_venue_ids: vec![MEXC_VENUE_ID.to_string()],
    }
}

fn finalizer(adapters: Vec<Arc<dyn MultiVenueAdapter>>) -> MultiVenueFinalizer {
    MultiVenueFinalizer::new(adapters, planner_config()).expect("valid finalizer")
}

fn committed_state(wal: &TestWal) -> MultiVenueExecutionState {
    let meta = wal.wrapper().meta_v2.expect("committed meta_v2");
    let FinalizerMetaPayload::MultiVenueSwap(state) = meta.payload;
    state
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
async fn operator_required_leg_is_parked_across_restarts() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        None,
        vec![VenueLegStatus::OperatorRequired],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap.clone(), mexc]);

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
        venue: MEXC_VENUE_ID.to_string(),
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
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(icpswap.recovery_calls(), 1);
    assert!(matches!(
        committed_state(&wal).outcome,
        MultiVenueExecutionOutcome::OperatorRequired { .. }
    ));
}

#[tokio::test]
async fn permanent_failure_is_not_readvanced_or_rerouted() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(ScriptedAdapter::new(
        ICPSWAP_VENUE_ID,
        None,
        vec![VenueLegStatus::FailedPermanent],
    ));
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap.clone(), mexc.clone()]);

    let first_error = finalizer
        .finalize(&wal, receipt.clone())
        .await
        .expect_err("permanent failure");
    assert_eq!(finalizer.classify_error(&first_error), FinalizerErrorKind::Permanent);
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
    assert_eq!(finalizer.classify_error(&error), FinalizerErrorKind::Permanent);
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

    assert_eq!(finalizer.classify_error(&error), FinalizerErrorKind::Retryable);
    let state = committed_state(&wal);
    assert_eq!(state.legs[0].status, VenueLegStatus::Running);
    assert_eq!(state.legs[0].last_error.as_deref(), Some("temporary pool outage"));
}

#[tokio::test]
async fn forced_cex_mode_commits_a_single_mexc_leg() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    let icpswap = Arc::new(ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, Vec::new()));
    let mexc = Arc::new(ScriptedAdapter::new(
        MEXC_VENUE_ID,
        None,
        vec![VenueLegStatus::Completed],
    ));
    let finalizer = MultiVenueFinalizer::new(vec![icpswap, mexc], planner_config())
        .expect("valid finalizer")
        .with_routing(MultiVenueRouting::ForcedVenue(MEXC_VENUE_ID.to_string()))
        .expect("registered forced venue");

    let result = finalizer.finalize(&wal, receipt).await.expect("forced MEXC execution");

    assert!(result.finalized);
    let state = committed_state(&wal);
    assert_eq!(state.plan.strategy_id, "forced_mexc");
    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn exclusive_venue_leg_waits_for_its_durable_owner_lock() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    wal.lock_available.store(false, Ordering::SeqCst);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, vec![VenueLegStatus::Completed])
            .with_execution_lock("trader", "icpswap-execution"),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let finalizer = finalizer(vec![icpswap.clone(), mexc]);

    let waiting = finalizer
        .finalize(&wal, receipt.clone())
        .await
        .expect("lock contention is not a finalizer failure");
    assert!(!waiting.finalized);
    assert_eq!(icpswap.calls(), 0);
    assert_eq!(wal.lock_acquisitions.load(Ordering::SeqCst), 1);

    wal.lock_available.store(true, Ordering::SeqCst);
    let completed = finalizer.finalize(&wal, receipt).await.expect("lock owner may advance");
    assert!(completed.finalized);
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(wal.lock_releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn terminal_leg_retries_lock_cleanup_without_readvancing_the_venue() {
    let receipt = receipt();
    let wal = TestWal::with_receipt(&receipt);
    wal.lock_release_available.store(false, Ordering::SeqCst);
    let icpswap = Arc::new(
        ScriptedAdapter::new(ICPSWAP_VENUE_ID, None, vec![VenueLegStatus::Completed])
            .with_execution_lock("trader", "icpswap-execution"),
    );
    let mexc = Arc::new(ScriptedAdapter::new(MEXC_VENUE_ID, None, Vec::new()));
    let watchdog = Arc::new(RecordingWatchdog::default());
    let finalizer = finalizer(vec![icpswap.clone(), mexc]).with_watchdog(watchdog.clone());

    let first = finalizer
        .finalize(&wal, receipt.clone())
        .await
        .expect_err("failed cleanup must keep the parent WAL row retryable");
    assert_eq!(finalizer.classify_error(&first), FinalizerErrorKind::LockCleanup);
    assert_eq!(wal.leg_statuses(), vec![VenueLegStatus::Completed]);
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(watchdog.0.lock().expect("watchdog lock").as_slice(), &[(
        "icpswap-execution".to_string(),
        "lock_cleanup".to_string(),
    )]);

    wal.lock_release_available.store(true, Ordering::SeqCst);
    let second = finalizer
        .finalize(&wal, receipt)
        .await
        .expect("the next cycle should retry only lock cleanup");
    assert!(second.finalized);
    assert_eq!(icpswap.calls(), 1);
    assert_eq!(wal.lock_releases.load(Ordering::SeqCst), 2);
}
