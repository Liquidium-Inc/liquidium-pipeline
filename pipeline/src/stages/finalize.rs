use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use candid::{Encode, Principal};
use tracing::{debug, info, warn};

use crate::error::AppResult;
use crate::finalizers::finalizer::{Finalizer, FinalizerResult};
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;

use crate::persistance::{LiqMetaWrapper, ResultStatus, WalStore};
use crate::stage::PipelineStage;
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::wal::{
    decode_receipt_wrapper, encode_meta, wal_mark_enqueued, wal_mark_inflight, wal_mark_permanent_failed,
    wal_mark_retryable_failed, wal_mark_succeeded,
};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, ProtocolError, TransferStatus};

const MAX_FINALIZER_ERRORS: i32 = 5;
/// Maximum safe left-shift for `u64` multipliers in retry backoff.
const MAX_U64_SHIFT: u32 = 63;

/// Exponential retry delay with cap: min(max, base * 2^(errors-1)).
fn retry_delay_secs(base: u64, max: u64, error_count: i32) -> u64 {
    if base == 0 {
        return 0;
    }
    let capped_max = max.max(base);
    let exponent = error_count.saturating_sub(1).max(0) as u32;
    let multiplier = if exponent >= MAX_U64_SHIFT {
        u64::MAX
    } else {
        1u64 << exponent
    };
    base.saturating_mul(multiplier).min(capped_max)
}

//
// FinalizeStage: pipeline stage over a concrete Finalizer
//
pub struct FinalizeStage<F, D, P, A>
where
    F: Finalizer,
    D: WalStore,
    P: ProfitCalculator,
    A: PipelineAgent,
{
    pub wal: Arc<D>,
    pub finalizer: Arc<F>,
    pub profit_calc: Arc<P>,
    pub agent: Arc<A>,
    pub lending_canister: Principal,
    /// Base retry delay for retryable finalizer failures, in seconds.
    pub cex_retry_base_secs: u64,
    /// Maximum retry delay cap for retryable finalizer failures, in seconds.
    pub cex_retry_max_secs: u64,
}

impl<F, D, P, A> FinalizeStage<F, D, P, A>
where
    F: Finalizer,
    D: WalStore,
    P: ProfitCalculator,
    A: PipelineAgent,
{
    pub fn new(
        wal: Arc<D>,
        finalizer: Arc<F>,
        profit_calc: Arc<P>,
        agent: Arc<A>,
        lending_canister: Principal,
        cex_retry_base_secs: u64,
        cex_retry_max_secs: u64,
    ) -> Self {
        Self {
            wal,
            finalizer,
            profit_calc,
            agent,
            lending_canister,
            cex_retry_base_secs,
            cex_retry_max_secs,
        }
    }

    async fn refresh_liquidation(&self, liq_id: u128) -> AppResult<LiquidationResult> {
        let args = Encode!(&liq_id).map_err(|e| format!("get_liquidation encode error: {e}"))?;
        let res = self
            .agent
            .call_query::<Result<LiquidationResult, ProtocolError>>(&self.lending_canister, "get_liquidation", args)
            .await?;
        match res {
            Ok(liq) => Ok(liq),
            Err(err) => Err(format!("get_liquidation error: {err:?}").into()),
        }
    }

    async fn update_receipt_meta(&self, liq_id: &str, receipt: &ExecutionReceipt) -> AppResult<()> {
        let row = self.wal.get_result(liq_id).await?;
        if let Some(mut row) = row {
            let wrapper = LiqMetaWrapper {
                receipt: receipt.clone(),
                meta: Vec::new(),
                finalizer_decision: None,
            };
            encode_meta(&mut row, &wrapper)?;
            row.updated_at = now_ts();
            self.wal.upsert_result(row).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, F, D, P, A> PipelineStage<'a, (), Vec<LiquidationOutcome>> for FinalizeStage<F, D, P, A>
where
    F: Finalizer + Sync + Send,
    D: WalStore + Sync + Send,
    P: ProfitCalculator + Sync + Send,
    A: PipelineAgent + Sync + Send,
{
    async fn process(&self, _: &'a ()) -> AppResult<Vec<LiquidationOutcome>> {
        // Load pending entries from WAL
        let rows = self.wal.get_pending(100).await?;
        debug!("Finalizing rows {:?}", rows);
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Decode all receipts and build mappings:
        // - liq_id -> wal_row_id
        // - liq_id -> created_at
        // Then collect receipts for per-receipt processing.
        let mut wal_id_by_liq: HashMap<u128, String> = HashMap::new();
        let mut created_at_by_liq: HashMap<u128, i64> = HashMap::new();
        let mut updated_at_by_liq: HashMap<u128, i64> = HashMap::new();
        let mut status_by_liq: HashMap<u128, ResultStatus> = HashMap::new();
        let mut error_count_by_liq: HashMap<u128, i32> = HashMap::new();
        let mut receipts: Vec<ExecutionReceipt> = vec![];

        for row in rows {
            let meta = decode_receipt_wrapper(&row)?
                .ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id))?;
            let receipt: ExecutionReceipt = meta.receipt;

            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

            let liq_id = liq.id;
            wal_id_by_liq.insert(liq_id, row.id.clone());
            created_at_by_liq.insert(liq_id, row.created_at);
            updated_at_by_liq.insert(liq_id, row.updated_at);
            status_by_liq.insert(liq_id, row.status);
            error_count_by_liq.insert(liq_id, row.error_count);

            receipts.push(receipt);
        }

        let mut fin_results: Vec<(FinalizerResult, ExecutionReceipt)> = vec![];

        // Run each receipt independently (no batching).
        for receipt in receipts {
            let mut receipt = receipt;
            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;
            let liq_id = liq.id;

            if !matches!(liq.collateral_tx.status, TransferStatus::Success) {
                match self.refresh_liquidation(liq_id).await {
                    Ok(fresh) => {
                        if fresh != *liq {
                            receipt.liquidation_result = Some(fresh.clone());
                            if matches!(receipt.status, ExecutionStatus::CollateralTransferFailed(_))
                                && matches!(fresh.collateral_tx.status, TransferStatus::Success)
                            {
                                receipt.status = ExecutionStatus::Success;
                            }
                            if let Some(wal_id) = wal_id_by_liq.get(&liq_id)
                                && let Err(err) = self.update_receipt_meta(wal_id, &receipt).await
                            {
                                warn!("Failed to update WAL meta for liq_id {}: {}", liq_id, err);
                            }
                        }
                    }
                    Err(err) => {
                        warn!("[finalize] get_liquidation failed liq_id={} err={}", liq_id, err);
                    }
                }
            }

            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;

            if !matches!(liq.collateral_tx.status, TransferStatus::Success) {
                info!(
                    "[finalize] ⏳ collateral_tx status={:?} liq_id={}",
                    liq.collateral_tx.status, liq_id
                );
                continue;
            }

            info!(
                "[finalize] 🧾 executing receipt: liq_id={} pay={} recv={} debt_repaid={} collateral_received={} swap={}",
                liq_id,
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol(),
                liq.amounts.debt_repaid,
                liq.amounts.collateral_received,
                receipt.request.swap_args.is_some()
            );

            if receipt.request.swap_args.is_none() {
                let wal_id = wal_id_by_liq
                    .get(&liq_id)
                    .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;
                let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                fin_results.push((
                    FinalizerResult {
                        swap_result: None,
                        finalized: true,
                        swapper: Some("none".to_string()),
                    },
                    receipt,
                ));
                continue;
            }

            // Apply bounded retry backoff for retryable failures to avoid thrashing thin books.
            if matches!(status_by_liq.get(&liq_id), Some(ResultStatus::FailedRetryable)) {
                let recorded_errors = error_count_by_liq.get(&liq_id).copied().unwrap_or(1).max(1);
                let delay_secs = retry_delay_secs(self.cex_retry_base_secs, self.cex_retry_max_secs, recorded_errors);
                let last_update = updated_at_by_liq.get(&liq_id).copied().unwrap_or(0);
                let due_at = last_update.saturating_add(delay_secs.min(i64::MAX as u64) as i64);
                let now = now_ts();
                if now < due_at {
                    debug!(
                        "[finalize] ⏳ retry backoff liq_id={} errors={} delay={}s remaining={}s",
                        liq_id,
                        recorded_errors,
                        delay_secs,
                        due_at.saturating_sub(now)
                    );
                    continue;
                }
            }

            if let Some(wal_id) = wal_id_by_liq.get(&liq_id) {
                wal_mark_inflight(&*self.wal, wal_id).await?;
            }

            match self.finalizer.finalize(&*self.wal, receipt.clone()).await {
                Ok(res) => {
                    if res.finalized {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                        fin_results.push((res.clone(), receipt));
                    } else {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        let _ = wal_mark_enqueued(&*self.wal, wal_id).await;
                    }
                }
                Err(e) => {
                    let base_errors = error_count_by_liq.get(&liq_id).copied().unwrap_or(0);
                    let next_errors = base_errors + 1;
                    let err_msg = e.to_string();

                    let wal_id = wal_id_by_liq
                        .get(&liq_id)
                        .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                    debug!("Failed finalization {}", err_msg);
                    if next_errors >= MAX_FINALIZER_ERRORS {
                        let _ = wal_mark_permanent_failed(&*self.wal, wal_id, err_msg.clone().into()).await;

                        let mut failed_receipt = receipt.clone();
                        failed_receipt.status =
                            ExecutionStatus::SwapFailed(format!("finalizer permanent failure: {}", err_msg));

                        fin_results.push((
                            FinalizerResult {
                                swap_result: None,
                                finalized: true,
                                swapper: None,
                            },
                            failed_receipt,
                        ));
                    } else {
                        let _ = wal_mark_retryable_failed(&*self.wal, wal_id, err_msg.clone().into()).await;
                    }

                    debug!("Finalization failed for receipt; continuing: {}", err_msg);
                }
            }
        }

        let mut outcomes = vec![];

        for (fin_res, receipt) in fin_results {
            let req = &receipt.request;
            let liq = receipt
                .liquidation_result
                .as_ref()
                .ok_or_else(|| format!("missing liquidation_result for request: {:?}", req))?;

            let round_trip_secs = if fin_res.finalized {
                created_at_by_liq
                    .get(&liq.id)
                    .map(|created| now_ts().saturating_sub(*created))
            } else {
                None
            };

            let expected_profit = self.profit_calc.expected(req, Some(liq));
            let realized_profit = self.profit_calc.realized(req, liq, fin_res.swap_result.as_ref());

            outcomes.push(LiquidationOutcome {
                request: req.clone(),
                execution_receipt: receipt.clone(),
                finalizer_result: fin_res.clone(),
                status: receipt.status.clone(),
                expected_profit,
                realized_profit,
                round_trip_secs,
            });
        }

        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::finalizer::{Finalizer, FinalizerResult};
    use crate::finalizers::profit_calculator::SimpleProfitCalculator;
    use crate::persistance::{LiqResultRecord, MockWalStore, ResultStatus};
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::SwapRequest;
    use candid::{Encode, Nat};
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use mockall::predicate::eq;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct NoopFinalizer {
        calls: Arc<Mutex<usize>>,
    }

    #[async_trait::async_trait]
    impl Finalizer for NoopFinalizer {
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> AppResult<FinalizerResult> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            Ok(FinalizerResult {
                swap_result: None,
                finalized: true,
                swapper: Some("noop".to_string()),
            })
        }
    }

    fn make_request() -> ExecutorRequest {
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
                buy_bad_debt: false,
            },
            swap_args: None,
            debt_asset,
            collateral_asset,
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_approval_needed: false,
        }
    }

    fn make_liq_result(liq_id: u128, collateral_status: TransferStatus, ts: u64) -> LiquidationResult {
        LiquidationResult {
            id: liq_id,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(0u32),
                debt_repaid: Nat::from(0u32),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: ts,
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

    fn make_swap_request(request: &ExecutorRequest) -> SwapRequest {
        SwapRequest {
            pay_asset: request.collateral_asset.asset_id(),
            pay_amount: ChainTokenAmount::from_formatted(request.collateral_asset.clone(), 1.0),
            receive_asset: request.debt_asset.asset_id(),
            receive_address: Some("dest".to_string()),
            max_slippage_bps: Some(100),
            venue_hint: None,
        }
    }

    fn make_row(liq_id: u128, receipt: ExecutionReceipt) -> LiqResultRecord {
        let mut row = LiqResultRecord {
            id: liq_id.to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: "{}".to_string(),
        };
        let wrapper = LiqMetaWrapper {
            receipt,
            meta: Vec::new(),
            finalizer_decision: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode_meta should succeed");
        row
    }

    #[test]
    fn retry_delay_secs_progresses_exponentially() {
        assert_eq!(retry_delay_secs(5, 120, 1), 5);
        assert_eq!(retry_delay_secs(5, 120, 2), 10);
        assert_eq!(retry_delay_secs(5, 120, 3), 20);
        assert_eq!(retry_delay_secs(5, 120, 4), 40);
        assert_eq!(retry_delay_secs(5, 120, 5), 80);
    }

    #[test]
    fn retry_delay_secs_caps_at_max() {
        assert_eq!(retry_delay_secs(5, 120, 6), 120);
        assert_eq!(retry_delay_secs(5, 120, 7), 120);
        assert_eq!(retry_delay_secs(10, 10, 4), 10);
    }

    #[tokio::test]
    async fn finalize_refreshes_pending_collateral_and_skips_if_still_pending() {
        let liq_id = 42u128;
        let initial_liq = make_liq_result(liq_id, TransferStatus::Pending, 0);
        let receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(initial_liq.clone()),
            status: ExecutionStatus::CollateralTransferFailed("collateral pending".to_string()),
            change_received: true,
        };
        let row = make_row(liq_id, receipt.clone());
        let row_pending = row.clone();
        let row_for_get = row.clone();
        let liq_id_str = liq_id.to_string();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_get_result()
            .withf(move |id| id == liq_id_str.as_str())
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));

        wal.expect_upsert_result().times(1).returning(|_| Ok(()));
        wal.expect_update_status().times(0);

        let fresh_liq = make_liq_result(liq_id, TransferStatus::Pending, 1);
        let args = Encode!(&liq_id).expect("encode should succeed");

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh_liq.clone())));

        let finalizer = NoopFinalizer {
            calls: Arc::new(Mutex::new(0)),
        };

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(finalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(agent),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert!(outcomes.is_empty(), "no outcomes while collateral pending");
    }

    #[tokio::test]
    async fn finalize_proceeds_after_collateral_success_refresh() {
        let liq_id = 77u128;
        let initial_liq = make_liq_result(liq_id, TransferStatus::Pending, 0);
        let receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(initial_liq.clone()),
            status: ExecutionStatus::CollateralTransferFailed("collateral pending".to_string()),
            change_received: true,
        };
        let row = make_row(liq_id, receipt.clone());
        let row_pending = row.clone();
        let row_for_get = row.clone();
        let liq_id_str = liq_id.to_string();
        let liq_id_str_status = liq_id_str.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_get_result()
            .withf(move |id| id == liq_id_str.as_str())
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));

        wal.expect_upsert_result().times(1).returning(|_| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| {
                id == liq_id_str_status.as_str() && *status == ResultStatus::Succeeded && *bump
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let fresh_liq = make_liq_result(liq_id, TransferStatus::Success, 1);
        let args = Encode!(&liq_id).expect("encode should succeed");

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh_liq.clone())));

        let finalizer = NoopFinalizer {
            calls: Arc::new(Mutex::new(0)),
        };

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(finalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(agent),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1, "should finalize after collateral success");
        assert!(matches!(outcomes[0].status, ExecutionStatus::Success));
    }

    /// Given: A retryable row is not yet due under exponential backoff.
    /// When: Finalize stage processes pending rows.
    /// Then: The row is skipped and finalizer is not invoked.
    #[tokio::test]
    async fn finalize_skips_retryable_rows_until_backoff_window_expires() {
        // given
        const LIQUIDATION_ID: u128 = 88;
        const RECORDED_ERROR_COUNT: i32 = 3;
        const RETRY_BASE_DELAY_SECS: u64 = 5;
        const RETRY_MAX_DELAY_SECS: u64 = 120;
        const WAL_BATCH_LIMIT: usize = 100;

        let liq_id = LIQUIDATION_ID;
        let mut request = make_request();
        request.swap_args = Some(make_swap_request(&request));
        let receipt = ExecutionReceipt {
            request,
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success, 0)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let mut row = make_row(liq_id, receipt);
        row.status = ResultStatus::FailedRetryable;
        row.error_count = RECORDED_ERROR_COUNT;
        row.updated_at = now_ts();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(WAL_BATCH_LIMIT))
            .times(1)
            .returning(move |_| Ok(vec![row.clone()]));
        wal.expect_update_status().times(0);
        wal.expect_get_result().times(0);
        wal.expect_upsert_result().times(0);

        let finalize_calls = Arc::new(Mutex::new(0usize));
        let finalizer = NoopFinalizer {
            calls: finalize_calls.clone(),
        };

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(finalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            RETRY_BASE_DELAY_SECS,
            RETRY_MAX_DELAY_SECS,
        );

        // when
        let outcomes = stage.process(&()).await.expect("process should succeed");

        // then
        assert!(outcomes.is_empty(), "backoff-gated rows should not finalize");
        assert_eq!(*finalize_calls.lock().expect("calls lock"), 0);
    }

    /// Given: A retryable row is already due under exponential backoff.
    /// When: Finalize stage processes pending rows.
    /// Then: The row is moved in-flight and finalized successfully.
    #[tokio::test]
    async fn finalize_processes_retryable_rows_after_backoff_window_expires() {
        // given
        const LIQUIDATION_ID: u128 = 89;
        const RECORDED_ERROR_COUNT: i32 = 3;
        const RETRY_BASE_DELAY_SECS: u64 = 5;
        const RETRY_MAX_DELAY_SECS: u64 = 120;
        const WAL_BATCH_LIMIT: usize = 100;
        const ALREADY_ELAPSED_WINDOW_SECS: i64 = 1_000;
        const EXPECTED_FINALIZE_CALLS: usize = 1;
        const EXPECTED_OUTCOME_COUNT: usize = 1;

        let liq_id = LIQUIDATION_ID;
        let mut request = make_request();
        request.swap_args = Some(make_swap_request(&request));
        let receipt = ExecutionReceipt {
            request,
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success, 0)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let mut row = make_row(liq_id, receipt);
        row.status = ResultStatus::FailedRetryable;
        row.error_count = RECORDED_ERROR_COUNT;
        row.updated_at = now_ts().saturating_sub(ALREADY_ELAPSED_WINDOW_SECS);

        let row_for_pending = row.clone();
        let row_id = row.id.clone();
        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(WAL_BATCH_LIMIT))
            .times(1)
            .returning(move |_| Ok(vec![row_for_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, _| id == row_id && *status == ResultStatus::InFlight)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(|_, status, _| *status == ResultStatus::Succeeded)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_get_result().times(0);
        wal.expect_upsert_result().times(0);

        let finalize_calls = Arc::new(Mutex::new(0usize));
        let finalizer = NoopFinalizer {
            calls: finalize_calls.clone(),
        };

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(finalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            RETRY_BASE_DELAY_SECS,
            RETRY_MAX_DELAY_SECS,
        );

        // when
        let outcomes = stage.process(&()).await.expect("process should succeed");

        // then
        assert_eq!(
            outcomes.len(),
            EXPECTED_OUTCOME_COUNT,
            "backoff-expired row should finalize"
        );
        assert_eq!(*finalize_calls.lock().expect("calls lock"), EXPECTED_FINALIZE_CALLS);
    }
}
