use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use candid::{Encode, Principal};
use futures::FutureExt;
use tokio::time::{sleep, timeout};
use tracing::instrument;
use tracing::{error, info, warn};

use crate::persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore};
use crate::stages::executor::ExecutionReceipt;
use crate::stages::executor::ExecutionStatus;
use crate::utils::now_ts;
use crate::wal::{decode_receipt_wrapper, encode_meta};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, ProtocolError, TransferStatus};

/// Upper bound on a single reconciliation sweep.
///
/// Auditor notes: the watcher issues IC queries through an agent configured
/// without a transport deadline, so an accepted-but-unanswered request would
/// otherwise park reconciliation forever while the daemon keeps logging healthy.
const TICK_TIMEOUT: Duration = Duration::from_secs(120);

/// Minimum cooldown after a panicking sweep before the next attempt.
const PANIC_RECOVERY_DELAY: Duration = Duration::from_secs(1);

/// Liveness cadence, so a silent watcher is distinguishable from an idle one.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);

/// Result of one guarded sweep, used to drive backoff and liveness logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickOutcome {
    /// Sweep ran to completion.
    Completed,
    /// Sweep returned an error; the loop is healthy, the work is not.
    Failed,
    /// Sweep exceeded [`TICK_TIMEOUT`] and was abandoned.
    TimedOut,
    /// Sweep panicked and was contained.
    Panicked,
}

pub struct SettlementWatcher<A, D>
where
    A: PipelineAgent,
    D: WalStore,
{
    pub wal: Arc<D>,
    pub agent: Arc<A>,
    pub lending_canister: Principal,
    pub poll_interval: Duration,
}

impl<A, D> SettlementWatcher<A, D>
where
    A: PipelineAgent + Send + Sync,
    D: WalStore + Send + Sync,
{
    pub fn new(wal: Arc<D>, agent: Arc<A>, lending_canister: Principal, poll_interval: Duration) -> Self {
        Self {
            wal,
            agent,
            lending_canister,
            poll_interval,
        }
    }

    /// Drives reconciliation forever.
    ///
    /// Auditor notes: this loop is the only thing advancing already-started
    /// liquidations to a terminal state, so it must survive every per-sweep
    /// fault. Both ways a sweep can break the loop — an unwinding panic and an
    /// unbounded await — are contained in [`Self::guarded_tick`].
    pub async fn run(self) {
        let mut last_heartbeat = Instant::now();
        let mut consecutive_stalls: u32 = 0;

        loop {
            let outcome = self.guarded_tick().await;
            match outcome {
                TickOutcome::Completed | TickOutcome::Failed => consecutive_stalls = 0,
                TickOutcome::TimedOut | TickOutcome::Panicked => {
                    consecutive_stalls = consecutive_stalls.saturating_add(1);
                    warn!(
                        consecutive_stalls,
                        ?outcome,
                        "[settlement] sweep did not complete; watcher stays alive and retries"
                    );
                }
            }

            if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                info!(consecutive_stalls, "[settlement] watcher alive");
                last_heartbeat = Instant::now();
            }

            let delay = if matches!(outcome, TickOutcome::Panicked) {
                self.poll_interval.max(PANIC_RECOVERY_DELAY)
            } else {
                self.poll_interval
            };
            sleep(delay).await;
        }
    }

    /// Runs one sweep under a deadline and a panic guard.
    async fn guarded_tick(&self) -> TickOutcome {
        // `tick` only touches `Arc` handles and owned rows, so a panic cannot
        // leave the watcher observing torn state.
        let tick = AssertUnwindSafe(self.tick()).catch_unwind();
        match timeout(TICK_TIMEOUT, tick).await {
            Ok(Ok(Ok(()))) => TickOutcome::Completed,
            Ok(Ok(Err(err))) => {
                warn!("[settlement] tick error: {}", err);
                TickOutcome::Failed
            }
            Ok(Err(_)) => {
                error!("[settlement] tick panicked; recovering and continuing reconciliation");
                TickOutcome::Panicked
            }
            Err(_) => {
                error!(
                    timeout_secs = TICK_TIMEOUT.as_secs(),
                    "[settlement] tick timed out; abandoning sweep and retrying"
                );
                TickOutcome::TimedOut
            }
        }
    }

    async fn tick(&self) -> Result<(), String> {
        let mut rows = self
            .wal
            .list_by_status(ResultStatus::WaitingCollateral, 100)
            .await
            .map_err(|e| e.to_string())?;
        let mut profit_rows = self
            .wal
            .list_by_status(ResultStatus::WaitingProfit, 100)
            .await
            .map_err(|e| e.to_string())?;
        rows.append(&mut profit_rows);

        for row in rows {
            if let Err(err) = self.process_row(row).await {
                warn!("[settlement] row processing failed: {}", err);
            }
        }
        Ok(())
    }

    #[instrument(name = "settlement.process_row", skip_all, err, fields(row_id = %row.id))]
    async fn process_row(&self, row: LiqResultRecord) -> Result<(), String> {
        let meta = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id))?;
        let mut receipt: ExecutionReceipt = meta.receipt;

        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

        let fresh = match self.refresh_liquidation(liq.id).await {
            Ok(liq) => liq,
            Err(err) => {
                warn!("[settlement] get_liquidation failed liq_id={} err={}", liq.id, err);
                return Ok(());
            }
        };

        let mut updated = false;
        if fresh != *liq {
            receipt.liquidation_result = Some(fresh.clone());
            updated = true;
        }
        if matches!(fresh.collateral_tx.status, TransferStatus::Success)
            && matches!(receipt.status, ExecutionStatus::CollateralTransferFailed(_))
        {
            receipt.status = ExecutionStatus::Success;
            updated = true;
        }
        if updated {
            let touch_meta = row.status != ResultStatus::WaitingProfit;
            self.update_receipt_meta(&row, &receipt, touch_meta).await?;
        }

        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

        if !matches!(liq.collateral_tx.status, TransferStatus::Success) {
            if row.status != ResultStatus::WaitingCollateral {
                self.wal
                    .update_status(&row.id, ResultStatus::WaitingCollateral, false)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            return Ok(());
        }

        if receipt.request.swap_args.is_none() {
            self.wal
                .update_status(&row.id, ResultStatus::Succeeded, true)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }

        self.wal
            .update_status(&row.id, ResultStatus::Enqueued, true)
            .await
            .map_err(|e| e.to_string())?;
        info!("[settlement] ✅ liq_id={} -> enqueued for multi-venue planning", liq.id);
        Ok(())
    }

    async fn refresh_liquidation(&self, liq_id: u128) -> Result<LiquidationResult, String> {
        let args = Encode!(&liq_id).map_err(|e| format!("get_liquidation encode error: {e}"))?;
        let res = self
            .agent
            .call_query::<Result<LiquidationResult, ProtocolError>>(&self.lending_canister, "get_liquidation", args)
            .await?;
        match res {
            Ok(liq) => Ok(liq),
            Err(err) => Err(format!("get_liquidation error: {err:?}")),
        }
    }

    async fn update_receipt_meta(
        &self,
        row: &LiqResultRecord,
        receipt: &ExecutionReceipt,
        touch: bool,
    ) -> Result<(), String> {
        let mut row = row.clone();
        let mut wrapper = decode_receipt_wrapper(&row)?.unwrap_or(LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        });
        wrapper.receipt = receipt.clone();
        encode_meta(&mut row, &wrapper)?;
        if touch {
            row.updated_at = now_ts();
        }
        self.wal.upsert_result(row).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::executors::executor::ExecutorRequest;
    use crate::persistance::{
        FinalizerDecisionSnapshot, LiqMetaWrapper, MockWalStore, ResultStatus, WalProfitSnapshot,
    };
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::SwapRequest;
    use candid::Nat;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::asset_id::AssetId;
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;
    use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use mockall::predicate::eq;

    fn make_request(buy_bad_debt: bool, swap_args: Option<SwapRequest>) -> ExecutorRequest {
        let debt_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        let collateral_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };
        ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(0u32),
                receiver_address: Principal::anonymous(),
                buy_bad_debt,
            },
            swap_args,
            debt_asset,
            collateral_asset,
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        }
    }

    fn make_swap_args() -> SwapRequest {
        let pay_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        let pay_amount = ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(1_000_000u64));
        SwapRequest {
            pay_asset: pay_token.asset_id(),
            pay_amount,
            receive_asset: AssetId {
                chain: "icp".to_string(),
                address: "ledger-usdt".to_string(),
                symbol: "ckUSDT".to_string(),
            },
            receive_address: Some("test-address".to_string()),
            max_slippage_bps: Some(100),
            venue_hint: Some("kong".to_string()),
        }
    }

    fn make_liq_result(liq_id: u128, collateral_status: TransferStatus) -> LiquidationResult {
        LiquidationResult {
            id: liq_id,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(0u32),
                debt_repaid: Nat::from(1_000_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 0,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: collateral_status,
            },
        }
    }

    fn make_row(status: ResultStatus, receipt: ExecutionReceipt) -> LiqResultRecord {
        let mut row = LiqResultRecord {
            id: receipt.liquidation_result.as_ref().unwrap().id.to_string(),
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now_ts(),
            updated_at: now_ts(),
            meta_json: "{}".to_string(),
        };
        let wrapper = LiqMetaWrapper {
            receipt,
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode_meta should succeed");
        row
    }

    #[tokio::test]
    async fn watcher_enqueues_for_multi_venue_planning() {
        let liq_id = 9u128;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(100usize))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(100usize))
            .times(1)
            .returning(|_, _| Ok(vec![]));
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher.tick().await.expect("tick should succeed");
    }

    #[tokio::test]
    async fn watcher_enqueues_settled_swap_rows_without_a_quote_dependency() {
        let liq_id = 10u128;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(100usize))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(100usize))
            .times(1)
            .returning(|_, _| Ok(vec![]));
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher.tick().await.expect("tick should succeed");
    }

    /// A ready settled row is handed to the finalizer without venue-specific routing.
    #[tokio::test]
    async fn watcher_enqueue_behavior_is_independent_of_venue_selection() {
        // given
        const LIQUIDATION_ID: u128 = 11;
        const WAL_BATCH_LIMIT: usize = 100;
        let liq_id = LIQUIDATION_ID;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(WAL_BATCH_LIMIT))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(WAL_BATCH_LIMIT))
            .times(1)
            .returning(|_, _| Ok(vec![]));
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        // when
        watcher.tick().await.expect("tick should succeed");

        // Expectations above assert the Enqueued transition.
    }

    #[tokio::test]
    async fn watcher_reenqueues_legacy_waiting_profit_rows() {
        let liq_id = 12u128;
        let swap_args = make_swap_args();
        let row = make_row(
            ResultStatus::WaitingProfit,
            ExecutionReceipt {
                request: make_request(false, Some(swap_args.clone())),
                liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
                status: ExecutionStatus::Success,
                change_received: true,
            },
        );
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(100usize))
            .times(1)
            .returning(|_, _| Ok(vec![]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(100usize))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher.tick().await.expect("tick should succeed");
    }

    #[tokio::test]
    async fn update_receipt_meta_preserves_wrapper_extensions() {
        let liq_id = 13u128;
        let old_receipt = ExecutionReceipt {
            request: make_request(false, Some(make_swap_args())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Pending)),
            status: ExecutionStatus::CollateralTransferFailed("pending".to_string()),
            change_received: true,
        };
        let new_receipt = ExecutionReceipt {
            request: make_request(false, Some(make_swap_args())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
        };

        let mut row = make_row(ResultStatus::WaitingCollateral, old_receipt.clone());
        let wrapper = LiqMetaWrapper {
            receipt: old_receipt,
            meta: vec![7, 8, 9],
            finalizer_decision: Some(FinalizerDecisionSnapshot {
                mode: "hybrid".to_string(),
                chosen: "cex".to_string(),
                reason: "test".to_string(),
                min_required_bps: 25.0,
                dex_preview_gross_bps: Some(40.0),
                dex_preview_net_bps: Some(28.0),
                cex_preview_gross_bps: Some(33.0),
                cex_preview_net_bps: Some(26.0),
                ts: 123,
                multi_venue_allocation: None,
            }),
            profit_snapshot: Some(WalProfitSnapshot {
                expected_profit_raw: "1000".to_string(),
                realized_profit_raw: Some("900".to_string()),
                debt_symbol: "ckBTC".to_string(),
                debt_decimals: 8,
                updated_at: 123,
            }),
            venue_execution: None,
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode wrapper");

        let mut wal = MockWalStore::new();
        wal.expect_upsert_result().times(1).returning(move |updated_row| {
            let updated_wrapper = decode_receipt_wrapper(&updated_row)
                .expect("decode wrapper")
                .expect("wrapper exists");
            assert_eq!(updated_wrapper.meta, vec![7, 8, 9]);
            assert!(updated_wrapper.finalizer_decision.is_some());
            assert!(updated_wrapper.profit_snapshot.is_some());
            assert!(
                matches!(updated_wrapper.receipt.status, ExecutionStatus::Success),
                "receipt status should be updated"
            );
            Ok(())
        });

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher
            .update_receipt_meta(&row, &new_receipt, true)
            .await
            .expect("receipt meta update should succeed");
    }

    /// How a [`FaultyWal`] sweep misbehaves.
    enum Fault {
        /// Never resolves — an IC query accepted and then never answered.
        Hang,
        /// Unwinds on every sweep.
        Panic,
    }

    /// WAL double for the sweep-fault containment paths.
    ///
    /// Hand-rolled rather than reusing `MockWalStore` because mockall guards its
    /// call state with a mutex that the first panic poisons; every later sweep
    /// would then fail inside the mock instead of exercising the watcher.
    struct FaultyWal {
        fault: Fault,
        sweeps: Arc<AtomicUsize>,
    }

    impl FaultyWal {
        fn new(fault: Fault) -> Self {
            Self {
                fault,
                sweeps: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn sweep_counter(&self) -> Arc<AtomicUsize> {
            self.sweeps.clone()
        }
    }

    #[async_trait::async_trait]
    impl WalStore for FaultyWal {
        async fn list_by_status(&self, _status: ResultStatus, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
            self.sweeps.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                Fault::Hang => std::future::pending().await,
                Fault::Panic => panic!("simulated sweep panic"),
            }
        }

        async fn upsert_result(&self, _row: LiqResultRecord) -> anyhow::Result<()> {
            unreachable!("sweep faults before reaching this")
        }

        async fn get_result(&self, _liq_id: &str) -> anyhow::Result<Option<LiqResultRecord>> {
            unreachable!("sweep faults before reaching this")
        }

        async fn get_pending(&self, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
            unreachable!("sweep faults before reaching this")
        }

        async fn update_status(&self, _liq_id: &str, _next: ResultStatus, _bump: bool) -> anyhow::Result<()> {
            unreachable!("sweep faults before reaching this")
        }

        async fn update_failure(
            &self,
            _liq_id: &str,
            _next: ResultStatus,
            _last_error: String,
            _bump: bool,
        ) -> anyhow::Result<()> {
            unreachable!("sweep faults before reaching this")
        }

        async fn delete(&self, _liq_id: &str) -> anyhow::Result<()> {
            unreachable!("sweep faults before reaching this")
        }
    }

    fn watcher_with_wal<D: WalStore + Send + Sync + 'static>(wal: D) -> SettlementWatcher<MockPipelineAgent, D> {
        SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            Duration::from_millis(1),
        )
    }

    #[tokio::test]
    async fn guarded_tick_contains_a_panicking_sweep() {
        let watcher = watcher_with_wal(FaultyWal::new(Fault::Panic));

        assert_eq!(watcher.guarded_tick().await, TickOutcome::Panicked);
    }

    #[tokio::test]
    async fn guarded_tick_reports_a_sweep_error_without_unwinding() {
        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .returning(|_, _| Err(anyhow::anyhow!("wal unavailable")));

        let watcher = watcher_with_wal(wal);

        assert_eq!(watcher.guarded_tick().await, TickOutcome::Failed);
    }

    /// A stalled sweep must be abandoned rather than parking reconciliation.
    /// The paused clock makes `TICK_TIMEOUT` elapse in virtual time.
    #[tokio::test(start_paused = true)]
    async fn guarded_tick_abandons_a_stalled_sweep() {
        let watcher = watcher_with_wal(FaultyWal::new(Fault::Hang));

        assert_eq!(watcher.guarded_tick().await, TickOutcome::TimedOut);
    }

    /// The loop is the only thing advancing started liquidations to a terminal
    /// state, so a panicking sweep must not end it.
    #[tokio::test]
    async fn run_keeps_sweeping_after_a_panicking_sweep() {
        let wal = FaultyWal::new(Fault::Panic);
        let sweeps = wal.sweep_counter();

        let handle = tokio::spawn(watcher_with_wal(wal).run());

        // Poll rather than sleeping a fixed span so the test does not depend on
        // wall-clock scheduling.
        for _ in 0..1_000 {
            if sweeps.load(Ordering::SeqCst) >= 3 {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }

        assert!(
            sweeps.load(Ordering::SeqCst) >= 3,
            "watcher stopped sweeping after a panic"
        );
        assert!(!handle.is_finished(), "watcher task died on a panicking sweep");
        handle.abort();
    }
}
