use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use async_trait::async_trait;
use candid::{Encode, Principal};
use futures::FutureExt;
use tracing::{debug, error, info, warn};

use crate::finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult};
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;

use crate::persistance::{LiqMetaWrapper, ResultStatus, WalProfitSnapshot, WalStore};
use crate::stage::PipelineStage;
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::wal::{
    decode_receipt_wrapper, encode_meta, wal_mark_enqueued, wal_mark_inflight, wal_mark_operator_required,
    wal_mark_operator_required_with_error, wal_mark_permanent_failed, wal_mark_retryable_failed, wal_mark_succeeded,
};
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, ProtocolError, TransferStatus};

pub(crate) const MAX_FINALIZER_ERRORS: i32 = 5;
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
    /// Escalation channel for rows this stage removes from the runnable queue
    /// while a venue may still hold their funds.
    pub watchdog: Arc<dyn Watchdog>,
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
            watchdog: noop_watchdog(),
        }
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
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

    async fn update_receipt_meta(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<(), String> {
        let row = self.wal.get_result(liq_id).await.map_err(|e| e.to_string())?;
        if let Some(mut row) = row {
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
            row.updated_at = now_ts();
            self.wal.upsert_result(row).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Escalates a liquidation parked because its retry budget ran out while a
    /// venue leg may still hold the funds.
    ///
    /// The venue itself is not known here -- this stage is generic over the
    /// finalizer -- but the tagged error carries the venue and leg, so it goes
    /// in `details` while `venue` names the component that made the decision.
    async fn escalate_custody_park(&self, receipt: &ExecutionReceipt, liq_id: u128, error: &str) {
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: liq_id.to_string(),
                venue: "finalizer".to_string(),
                pending_step: "retry_budget_exhausted".to_string(),
                owner: receipt.request.liquidation.borrower.to_text(),
                details: format!(
                    "Liquidation {liq_id} was parked after {MAX_FINALIZER_ERRORS} failed finalize attempts while a venue leg may still hold its funds. It will not be retried automatically. Last error: {error}"
                ),
            })
            .await;
    }

    async fn persist_profit_snapshot(
        &self,
        liq_id: &str,
        receipt: &ExecutionReceipt,
        expected_profit: i128,
        realized_profit: i128,
    ) -> Result<(), String> {
        let row = self.wal.get_result(liq_id).await.map_err(|e| e.to_string())?;
        let Some(mut row) = row else {
            return Err(format!("missing WAL row for liq_id {liq_id}"));
        };

        let mut wrapper = decode_receipt_wrapper(&row)?.unwrap_or(LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        });
        wrapper.receipt = receipt.clone();
        wrapper.profit_snapshot = Some(WalProfitSnapshot {
            expected_profit_raw: expected_profit.to_string(),
            realized_profit_raw: Some(realized_profit.to_string()),
            debt_symbol: receipt.request.debt_asset.symbol().to_string(),
            debt_decimals: receipt.request.debt_asset.decimals(),
            updated_at: now_ts(),
        });

        encode_meta(&mut row, &wrapper)?;
        row.updated_at = now_ts();
        self.wal.upsert_result(row).await.map_err(|e| e.to_string())
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
    async fn process(&self, _: &'a ()) -> Result<Vec<LiquidationOutcome>, String> {
        // Load pending entries from WAL
        let rows = self.wal.get_pending(100).await.map_err(|e| e.to_string())?;
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
            let meta = match decode_receipt_wrapper(&row) {
                Ok(Some(meta)) => meta,
                Ok(None) => {
                    let error = format!("receipt not found in WAL meta_json for {}", row.id);
                    if let Err(mark_error) = wal_mark_permanent_failed(&*self.wal, &row.id, error.clone()).await {
                        warn!("Failed to quarantine malformed WAL row {}: {}", row.id, mark_error);
                    }
                    error!("[finalize] quarantined malformed WAL row {}: {}", row.id, error);
                    continue;
                }
                Err(error) => {
                    if let Err(mark_error) = wal_mark_permanent_failed(&*self.wal, &row.id, error.clone()).await {
                        warn!("Failed to quarantine malformed WAL row {}: {}", row.id, mark_error);
                    }
                    error!("[finalize] quarantined malformed WAL row {}: {}", row.id, error);
                    continue;
                }
            };
            let receipt: ExecutionReceipt = meta.receipt;

            let Some(liq) = receipt.liquidation_result.as_ref() else {
                let error = format!("missing liquidation_result for WAL id {}", row.id);
                if let Err(mark_error) = wal_mark_permanent_failed(&*self.wal, &row.id, error.clone()).await {
                    warn!("Failed to quarantine malformed WAL row {}: {}", row.id, mark_error);
                }
                error!("[finalize] quarantined malformed WAL row {}: {}", row.id, error);
                continue;
            };

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
                "[finalize] 🧾 executing receipt: liq_id={} debt_asset={} collateral_asset={} debt_repaid={} collateral_received={} swap={} swap_pay={} swap_recv={}",
                liq_id,
                receipt.request.debt_asset.symbol(),
                receipt.request.collateral_asset.symbol(),
                liq.amounts.debt_repaid,
                liq.amounts.collateral_received,
                receipt.request.swap_args.is_some(),
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol()
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
                        operator_required: false,
                        swapper: Some("none".to_string()),
                        reason: None,
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

            // A panic in one row must not abort the batch. Without this the
            // unwind escapes `process()` entirely, taking export, heartbeat and
            // every remaining row with it -- and `tokio::time::timeout` does not
            // catch panics, so a deterministic one would repeat every cycle.
            let outcome = AssertUnwindSafe(self.finalizer.finalize(&*self.wal, receipt.clone()))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    let detail = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!("[finalize] 💥 finalizer panicked liq_id={} detail={}", liq_id, detail);
                    Err(format!("finalizer panicked: {detail}"))
                });

            match outcome {
                Ok(res) => {
                    if res.operator_required {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;
                        if let Err(error) = wal_mark_operator_required(&*self.wal, wal_id).await {
                            warn!("Failed to park operator-required WAL row {}: {}", wal_id, error);
                        } else {
                            warn!(
                                "[finalize] operator reconciliation required liq_id={} reason={}",
                                liq_id,
                                res.reason.as_deref().unwrap_or("unspecified")
                            );
                        }
                    } else if res.finalized {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        let _ = wal_mark_succeeded(&*self.wal, wal_id).await;
                        fin_results.push((res.clone(), receipt));
                    } else {
                        let wal_id = wal_id_by_liq
                            .get(&liq_id)
                            .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                        if let Err(error) = wal_mark_enqueued(&*self.wal, wal_id).await {
                            warn!("Failed to re-enqueue unfinished WAL row {}: {}", wal_id, error);
                        }
                    }
                }
                Err(e) => {
                    let base_errors = error_count_by_liq.get(&liq_id).copied().unwrap_or(0);
                    let next_errors = base_errors + 1;
                    let err_msg = e.to_string();
                    let error_kind = self.finalizer.classify_error(&err_msg);

                    let wal_id = wal_id_by_liq
                        .get(&liq_id)
                        .ok_or_else(|| format!("missing WAL id for liquidation {}", liq_id))?;

                    debug!("Failed finalization {}", err_msg);
                    if error_kind == FinalizerErrorKind::BadDebtAmountFloor && receipt.request.liquidation.buy_bad_debt
                    {
                        let _ = wal_mark_succeeded(&*self.wal, wal_id).await;

                        fin_results.push((
                            FinalizerResult {
                                swap_result: None,
                                finalized: true,
                                operator_required: false,
                                swapper: None,
                                reason: Some(format!("bad debt finalizer amount floor accepted: {}", err_msg)),
                            },
                            receipt.clone(),
                        ));
                    } else if error_kind == FinalizerErrorKind::LockCleanup {
                        // Terminal lock deletion is idempotent and cannot replay
                        // venue effects. Keep retrying under capped backoff;
                        // watchdog notifications provide operator escalation.
                        let _ = wal_mark_retryable_failed(&*self.wal, wal_id, err_msg.clone()).await;
                    } else if error_kind == FinalizerErrorKind::VenueCustody
                        && next_errors >= MAX_FINALIZER_ERRORS
                    {
                        // The retry budget is spent, but a venue leg may still
                        // hold this liquidation's funds. Failing permanently
                        // would drop the row out of the runnable queue and
                        // leave that custody with nothing tracking it, so park
                        // it for an operator instead. Parking is terminal until
                        // someone requeues the row, which is deliberate: the
                        // venue side has to be understood before a retry.
                        if let Err(error) =
                            wal_mark_operator_required_with_error(&*self.wal, wal_id, err_msg.clone()).await
                        {
                            warn!("Failed to park custody-holding WAL row {}: {}", wal_id, error);
                        } else {
                            error!(
                                "[finalize] 🅿️ retry budget exhausted while a venue leg holds funds; parked for operator liq_id={} err={}",
                                liq_id, err_msg
                            );
                            // A parked row produces no finalized outcome, so it
                            // reaches neither the CSV export nor the
                            // liquidation-finalized notification. Without this
                            // the only trace of stranded custody is a log line.
                            self.escalate_custody_park(&receipt, liq_id, &err_msg).await;
                        }
                    } else if matches!(
                        error_kind,
                        FinalizerErrorKind::Permanent | FinalizerErrorKind::BadDebtAmountFloor
                    ) || next_errors >= MAX_FINALIZER_ERRORS
                    {
                        let _ = wal_mark_permanent_failed(&*self.wal, wal_id, err_msg.clone()).await;

                        let mut failed_receipt = receipt.clone();
                        failed_receipt.status =
                            ExecutionStatus::SwapFailed(format!("finalizer permanent failure: {}", err_msg));

                        fin_results.push((
                            FinalizerResult {
                                swap_result: None,
                                finalized: true,
                                operator_required: false,
                                swapper: None,
                                reason: Some(err_msg.clone()),
                            },
                            failed_receipt,
                        ));
                    } else {
                        let _ = wal_mark_retryable_failed(&*self.wal, wal_id, err_msg.clone()).await;
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

            if let Some(wal_id) = wal_id_by_liq.get(&liq.id) {
                if let Err(err) = self
                    .persist_profit_snapshot(wal_id, &receipt, expected_profit, realized_profit)
                    .await
                {
                    warn!("Failed to persist WAL profit snapshot for liq_id {}: {}", liq.id, err);
                }
            } else {
                warn!(
                    "Skipping WAL profit snapshot persistence: missing WAL id for liq_id {}",
                    liq.id
                );
            }

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
    use crate::persistance::{
        FinalizerDecisionSnapshot, LiqMetaWrapper, LiqResultRecord, MockWalStore, ResultStatus, WalProfitSnapshot,
    };
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::SwapRequest;
    use candid::{Encode, Nat};
    use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use mockall::predicate::eq;
    use std::sync::{Arc, Mutex};

    /// Captures operator escalations as `(execution_id, pending_step, details)`.
    #[derive(Default)]
    struct RecordingWatchdog(Mutex<Vec<(String, String, String)>>);

    impl RecordingWatchdog {
        fn operator_alerts(&self) -> Vec<(String, String, String)> {
            self.0.lock().expect("watchdog lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl Watchdog for RecordingWatchdog {
        async fn notify(&self, event: WatchdogEvent<'_>) {
            if let WatchdogEvent::OperatorRequired {
                execution_id,
                pending_step,
                details,
                ..
            } = event
            {
                self.0
                    .lock()
                    .expect("watchdog lock")
                    .push((execution_id, pending_step, details));
            }
        }
    }

    #[derive(Clone)]
    struct NoopFinalizer {
        calls: Arc<Mutex<usize>>,
    }

    #[async_trait::async_trait]
    impl Finalizer for NoopFinalizer {
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, String> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            Ok(FinalizerResult {
                swap_result: None,
                finalized: true,
                operator_required: false,
                swapper: Some("noop".to_string()),
                reason: None,
            })
        }
    }

    #[derive(Clone)]
    struct ErrorFinalizer {
        error: String,
        kind: FinalizerErrorKind,
    }

    struct OperatorRequiredFinalizer;

    #[async_trait::async_trait]
    impl Finalizer for OperatorRequiredFinalizer {
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, String> {
            Ok(FinalizerResult {
                swap_result: None,
                finalized: false,
                operator_required: true,
                swapper: Some("mexc".to_string()),
                reason: Some("ambiguous MEXC submission".to_string()),
            })
        }
    }

    #[async_trait::async_trait]
    impl Finalizer for ErrorFinalizer {
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, String> {
            Err(self.error.clone())
        }

        fn classify_error(&self, _error: &str) -> FinalizerErrorKind {
            self.kind
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
            min_collateral_amount: Nat::from(0u8),
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

    fn make_swapping_receipt(liq_id: u128) -> ExecutionReceipt {
        let mut request = make_request();
        request.swap_args = Some(make_swap_request(&request));
        ExecutionReceipt {
            request,
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success, 0)),
            status: ExecutionStatus::Success,
            change_received: true,
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
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode_meta should succeed");
        row
    }

    #[tokio::test]
    async fn malformed_row_is_quarantined_without_blocking_valid_rows() {
        let malformed_id = "malformed-row".to_string();
        let malformed = LiqResultRecord {
            id: malformed_id.clone(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: "{not-json".to_string(),
        };
        let valid_liq_id = 920u128;
        let valid_receipt = make_swapping_receipt(valid_liq_id);
        let valid = make_row(valid_liq_id, valid_receipt);
        let valid_for_profit = valid.clone();
        let valid_id = valid.id.clone();
        let valid_id_for_success = valid_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![malformed, valid]));
        wal.expect_update_failure()
            .withf(move |id, status, error, bump| {
                id == malformed_id
                    && *status == ResultStatus::FailedPermanent
                    && error.contains("invalid meta_json")
                    && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == valid_id && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == valid_id_for_success && *status == ResultStatus::Succeeded && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_get_result()
            .times(1)
            .returning(move |_| Ok(Some(valid_for_profit.clone())));
        wal.expect_upsert_result().times(1).returning(|_| Ok(()));

        let calls = Arc::new(Mutex::new(0usize));
        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(NoopFinalizer { calls: calls.clone() }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("valid row should still finalize");

        assert_eq!(outcomes.len(), 1);
        assert_eq!(*calls.lock().expect("calls lock"), 1);
    }

    #[tokio::test]
    async fn operator_required_result_is_parked_outside_the_runnable_queue() {
        let liq_id = 921u128;
        let row = make_row(liq_id, make_swapping_receipt(liq_id));
        let row_id = row.id.clone();
        let row_id_for_operator = row_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| {
                id == row_id_for_operator && *status == ResultStatus::OperatorRequired && !*bump
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(OperatorRequiredFinalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("operator row should park cleanly");
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn lock_cleanup_remains_retryable_after_the_ordinary_error_limit() {
        let liq_id = 922u128;
        let mut row = make_row(liq_id, make_swapping_receipt(liq_id));
        row.error_count = MAX_FINALIZER_ERRORS - 1;
        let row_id = row.id.clone();
        let row_id_for_retry = row_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, error, bump| {
                id == row_id_for_retry
                    && *status == ResultStatus::FailedRetryable
                    && error.contains("lock cleanup unavailable")
                    && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: "lock cleanup unavailable".to_string(),
                kind: FinalizerErrorKind::LockCleanup,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("cleanup failure should remain retryable");
        assert!(outcomes.is_empty());
    }

    /// A permanently failed row leaves the runnable queue for good. When the
    /// budget runs out on a row whose venue leg may still hold the funds, that
    /// would strand the custody with nothing tracking it, so the row must be
    /// parked for an operator instead -- and, because a parked row produces no
    /// finalized outcome, the park has to raise its own alert.
    #[tokio::test]
    async fn exhausted_retries_park_a_custody_holding_row_and_escalate_it() {
        let liq_id = 923u128;
        let mut row = make_row(liq_id, make_swapping_receipt(liq_id));
        row.error_count = MAX_FINALIZER_ERRORS - 1;
        let row_id = row.id.clone();
        let row_id_for_park = row_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, error, _| {
                id == row_id_for_park
                    && *status == ResultStatus::OperatorRequired
                    && error.contains("venue still holds the input")
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let watchdog = Arc::new(RecordingWatchdog::default());
        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: "venue still holds the input".to_string(),
                kind: FinalizerErrorKind::VenueCustody,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        )
        .with_watchdog(watchdog.clone());

        let outcomes = stage.process(&()).await.expect("custody row should park cleanly");
        assert!(outcomes.is_empty());

        let alerts = watchdog.operator_alerts();
        assert_eq!(alerts.len(), 1, "a parked custody row must raise exactly one alert");
        let (execution_id, pending_step, details) = &alerts[0];
        assert_eq!(execution_id, &liq_id.to_string());
        assert_eq!(pending_step, "retry_budget_exhausted");
        assert!(details.contains("venue still holds the input"));
        assert!(details.contains("will not be retried automatically"));
    }

    /// Below the limit the same error is an ordinary retry: parking early would
    /// bury a row that a transient venue outage would have resolved by itself,
    /// and alerting on it would train operators to ignore the alert.
    #[tokio::test]
    async fn custody_error_below_the_limit_stays_retryable_and_silent() {
        let liq_id = 924u128;
        let row = make_row(liq_id, make_swapping_receipt(liq_id));
        let row_id = row.id.clone();
        let row_id_for_retry = row_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, _, bump| {
                id == row_id_for_retry && *status == ResultStatus::FailedRetryable && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let watchdog = Arc::new(RecordingWatchdog::default());
        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: "venue still holds the input".to_string(),
                kind: FinalizerErrorKind::VenueCustody,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        )
        .with_watchdog(watchdog.clone());

        let outcomes = stage.process(&()).await.expect("custody row should retry");
        assert!(outcomes.is_empty());
        assert!(watchdog.operator_alerts().is_empty());
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
    async fn finalize_marks_below_minimum_bridge_error_permanent_immediately() {
        let liq_id = 910u128;
        let receipt = make_swapping_receipt(liq_id);
        let row = make_row(liq_id, receipt.clone());
        let row_pending = row.clone();
        let row_for_profit = row.clone();
        let liq_id_str = liq_id.to_string();
        let liq_id_for_failure = liq_id_str.clone();

        let err = format!(
            "{}: ckETH@ICP -> ETH amount below minimum withdrawal (amount=0.0049 minimum=0.005)",
            FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX
        );
        let err_for_failure = err.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, last_error, bump| {
                id == liq_id_for_failure.as_str()
                    && *status == ResultStatus::FailedPermanent
                    && last_error == &err_for_failure
                    && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        wal.expect_get_result()
            .withf(move |id| id == liq_id.to_string().as_str())
            .times(1)
            .returning(move |_| Ok(Some(row_for_profit.clone())));
        wal.expect_upsert_result().times(1).returning(|_| Ok(()));

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: err,
                kind: FinalizerErrorKind::BadDebtAmountFloor,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0].status, ExecutionStatus::SwapFailed(_)));
    }

    #[tokio::test]
    async fn finalize_accepts_below_minimum_bridge_error_for_bad_debt() {
        let liq_id = 912u128;
        let mut receipt = make_swapping_receipt(liq_id);
        receipt.request.liquidation.buy_bad_debt = true;
        let row = make_row(liq_id, receipt.clone());
        let row_pending = row.clone();
        let row_for_profit = row.clone();
        let liq_id_str = liq_id.to_string();
        let liq_id_for_success = liq_id_str.clone();

        let err = format!(
            "{}: ckETH@ICP -> ETH amount below minimum withdrawal (amount=0.0049 minimum=0.005)",
            FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX
        );

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| {
                id == liq_id_for_success.as_str() && *status == ResultStatus::Succeeded && *bump
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure().times(0);
        wal.expect_get_result()
            .withf(move |id| id == liq_id.to_string().as_str())
            .times(1)
            .returning(move |_| Ok(Some(row_for_profit.clone())));
        wal.expect_upsert_result().times(1).returning(|_| Ok(()));

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: err.clone(),
                kind: FinalizerErrorKind::BadDebtAmountFloor,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0].status, ExecutionStatus::Success));
        assert!(outcomes[0].finalizer_result.finalized);
        assert!(
            outcomes[0]
                .finalizer_result
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("bad debt finalizer amount floor accepted"))
        );
    }

    #[tokio::test]
    async fn finalize_keeps_ordinary_finalizer_error_retryable() {
        let liq_id = 911u128;
        let receipt = make_swapping_receipt(liq_id);
        let row = make_row(liq_id, receipt);
        let row_pending = row.clone();
        let liq_id_str = liq_id.to_string();
        let liq_id_for_failure = liq_id_str.clone();
        let err = "temporary bridge transport failure".to_string();
        let err_for_failure = err.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, last_error, bump| {
                id == liq_id_for_failure.as_str()
                    && *status == ResultStatus::FailedRetryable
                    && last_error == &err_for_failure
                    && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        wal.expect_get_result().times(0);
        wal.expect_upsert_result().times(0);

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(ErrorFinalizer {
                error: err,
                kind: FinalizerErrorKind::Retryable,
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert!(outcomes.is_empty());
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
            .times(2)
            .returning(move |_| Ok(Some(row_for_get.clone())));

        wal.expect_upsert_result().times(2).returning(|_| Ok(()));
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
        let row_for_get = row.clone();
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
        wal.expect_get_result()
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));
        wal.expect_upsert_result().times(1).returning(|_| Ok(()));

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

    #[tokio::test]
    async fn update_receipt_meta_preserves_wrapper_extensions() {
        let liq_id = 901u128;
        let old_receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Pending, 0)),
            status: ExecutionStatus::CollateralTransferFailed("pending".to_string()),
            change_received: true,
        };
        let new_receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Pending, 1)),
            status: ExecutionStatus::CollateralTransferFailed("pending".to_string()),
            change_received: true,
        };

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
            receipt: old_receipt,
            meta: vec![1, 2, 3],
            finalizer_decision: Some(FinalizerDecisionSnapshot {
                mode: "hybrid".to_string(),
                chosen: "dex".to_string(),
                reason: "test".to_string(),
                min_required_bps: 10.0,
                dex_preview_gross_bps: Some(12.0),
                dex_preview_net_bps: Some(11.5),
                cex_preview_gross_bps: None,
                cex_preview_net_bps: None,
                ts: 1,
                multi_venue_allocation: None,
            }),
            profit_snapshot: Some(WalProfitSnapshot {
                expected_profit_raw: "10".to_string(),
                realized_profit_raw: Some("9".to_string()),
                debt_symbol: "ckBTC".to_string(),
                debt_decimals: 8,
                updated_at: 1,
            }),
            venue_execution: None,
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode wrapper");

        let row_for_get = row.clone();
        let mut wal = MockWalStore::new();
        wal.expect_get_result()
            .withf(move |id| id == liq_id.to_string())
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));
        wal.expect_upsert_result().times(1).returning(|row| {
            let wrapper = decode_receipt_wrapper(&row)
                .expect("decode wrapper")
                .expect("wrapper exists");
            assert_eq!(wrapper.meta, vec![1, 2, 3]);
            assert!(wrapper.finalizer_decision.is_some());
            assert!(wrapper.profit_snapshot.is_some());
            Ok(())
        });

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(NoopFinalizer {
                calls: Arc::new(Mutex::new(0)),
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        stage
            .update_receipt_meta(&liq_id.to_string(), &new_receipt)
            .await
            .expect("receipt meta update should succeed");
    }

    #[tokio::test]
    async fn finalize_persists_profit_snapshot_in_wal_meta() {
        let liq_id = 902u128;
        let receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success, 0)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let row = make_row(liq_id, receipt.clone());
        let row_for_pending = row.clone();
        let row_for_get = row.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_for_pending.clone()]));
        wal.expect_update_status()
            .withf(|_, status, bump| *status == ResultStatus::Succeeded && *bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_get_result()
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));
        wal.expect_upsert_result().times(1).returning(|row| {
            let wrapper = decode_receipt_wrapper(&row)
                .expect("decode wrapper")
                .expect("wrapper exists");
            let snapshot = wrapper.profit_snapshot.expect("profit snapshot should be present");
            assert_eq!(snapshot.expected_profit_raw, "0");
            assert!(snapshot.realized_profit_raw.is_some());
            assert_eq!(snapshot.debt_symbol, "ckBTC");
            assert_eq!(snapshot.debt_decimals, 8);
            Ok(())
        });

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(NoopFinalizer {
                calls: Arc::new(Mutex::new(0)),
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
    }
}
