use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use async_trait::async_trait;
use candid::Principal;
use futures::FutureExt;
use tracing::{debug, error, info, warn};

use crate::finalizers::finalizer::{Finalizer, FinalizerError, FinalizerResult};
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::profit_calculator::ProfitCalculator;
use crate::liquidation::lending_queries::get_liquidation;

use crate::persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalProfitSnapshot, WalStore};
use crate::stage::PipelineStage;
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::wal::{
    decode_receipt_wrapper, encode_meta, wal_mark_enqueued, wal_mark_inflight, wal_mark_no_route,
    wal_mark_operator_required, wal_mark_operator_required_with_error, wal_mark_permanent_failed,
    wal_mark_retryable_failed, wal_mark_succeeded, wal_mark_unresumable, wal_save_receipt,
};
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::TransferStatus;

pub(crate) const MAX_FINALIZER_ERRORS: i32 = 5;
/// How many times one liquidation may be worked before an operator looks at it.
/// Strikes restart when a row moves again, so only this bounds a row that fails
/// slowly forever (liquidation 1630). A search that finds no route is no work.
pub(crate) const MAX_FINALIZER_ATTEMPTS: i32 = 20;
/// The retry pace of a row with no acceptable swap once its normal budget is spent.
const NO_ROUTE_SLOW_RETRY_SECS: u64 = 30 * 60;
/// Maximum safe left-shift for `u64` multipliers in retry backoff.
const MAX_U64_SHIFT: u32 = 63;

/// Why a row was parked for an operator while a venue leg may still hold its funds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CustodyParkReason {
    AttemptBudgetExhausted,
    RetryBudgetExhausted,
    UnresumableCommittedRow,
}

impl CustodyParkReason {
    /// The key the watchdog dedupes alerts on.
    fn pending_step(self) -> &'static str {
        match self {
            Self::AttemptBudgetExhausted => "attempt_budget_exhausted",
            Self::RetryBudgetExhausted => "retry_budget_exhausted",
            Self::UnresumableCommittedRow => "unresumable_committed_row",
        }
    }
}

impl std::fmt::Display for CustodyParkReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttemptBudgetExhausted => write!(f, "worked {MAX_FINALIZER_ATTEMPTS} times without finishing"),
            Self::RetryBudgetExhausted => write!(f, "after {MAX_FINALIZER_ERRORS} failed finalize attempts"),
            Self::UnresumableCommittedRow => f.write_str("this build cannot resume the committed row"),
        }
    }
}

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

/// A liquidation the stage finished, with the receipt its outcome reports.
type Finished = (FinalizerResult, ExecutionReceipt);

/// An outcome is published only after its terminal status is durable. A failed
/// write leaves the row to run again, so it is reported once (audit L18).
fn published(
    write: Result<(), String>,
    liq_id: u128,
    result: FinalizerResult,
    receipt: ExecutionReceipt,
) -> Option<Finished> {
    match write {
        Ok(()) => Some((result, receipt)),
        Err(error) => {
            warn!(
                "[finalize] terminal WAL write failed; outcome not published yet liq_id={} err={}",
                liq_id, error
            );
            None
        }
    }
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

    async fn update_receipt_meta(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<(), String> {
        match self.wal.get_result(liq_id).await.map_err(|e| e.to_string())? {
            Some(row) => wal_save_receipt(&*self.wal, row, receipt, true).await,
            None => Ok(()),
        }
    }

    async fn alert_operator(&self, receipt: &ExecutionReceipt, liq_id: u128, pending_step: &str, details: String) {
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: liq_id.to_string(),
                venue: "finalizer".to_string(),
                pending_step: pending_step.to_string(),
                owner: receipt.request.liquidation.borrower.to_text(),
                details,
            })
            .await;
    }

    /// Escalates a liquidation parked while a venue leg may still hold the funds.
    ///
    /// The venue itself is not known here -- this stage is generic over the
    /// finalizer -- but the tagged error carries the venue and leg, so it goes
    /// in `details` while `venue` names the component that made the decision.
    async fn escalate_custody_park(
        &self,
        receipt: &ExecutionReceipt,
        liq_id: u128,
        reason: CustodyParkReason,
        error: &str,
    ) {
        let details = format!(
            "Liquidation {liq_id} was parked ({reason}) while a venue leg may still hold its funds. It will not be retried automatically. Last error: {error}"
        );
        self.alert_operator(receipt, liq_id, reason.pending_step(), details)
            .await;
    }

    /// Alerts once when a row with no acceptable swap drops to the slow retry
    /// pace. The row keeps retrying; the alert makes a lasting cause visible.
    async fn escalate_slow_retry(&self, receipt: &ExecutionReceipt, liq_id: u128, error: &str) {
        let details = format!(
            "Liquidation {liq_id} found no acceptable swap in {MAX_FINALIZER_ERRORS} tries. It keeps retrying every {} minutes on its own. Last error: {error}",
            NO_ROUTE_SLOW_RETRY_SECS / 60
        );
        self.alert_operator(receipt, liq_id, "no_route_slow_retry", details)
            .await;
    }

    /// Parks a row whose WAL meta cannot be interpreted. The row is already
    /// unusable, so a failure to mark it is logged rather than propagated.
    async fn quarantine_row(&self, row_id: &str, error: String) {
        if let Err(mark_error) = wal_mark_permanent_failed(&*self.wal, row_id, error.clone()).await {
            warn!("Failed to quarantine malformed WAL row {}: {}", row_id, mark_error);
        }
        error!("[finalize] quarantined malformed WAL row {}: {}", row_id, error);
    }

    /// Records the PnL of one finished liquidation.
    ///
    /// The committed receipt is deliberately left alone. Callers pass their own
    /// copy, and the permanent-failure path hands over one whose status has been
    /// rewritten to `SwapFailed` for reporting. Persisting that copy would
    /// overwrite the execution outcome the row is replanned from, so a requeued
    /// row could never be finalized again. A receipt that genuinely changed is
    /// already journalled by `update_receipt_meta`.
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

        let mut wrapper = decode_receipt_wrapper(&row)?.unwrap_or(LiqMetaWrapper::new(receipt.clone()));
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

    /// Rows with a readable receipt and liquidation result. Any other row is
    /// quarantined, so it cannot block the rest.
    async fn decode_rows(&self, rows: Vec<LiqResultRecord>) -> Vec<(LiqResultRecord, ExecutionReceipt)> {
        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let meta = decode_receipt_wrapper(&row)
                .and_then(|meta| meta.ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id)));
            match meta {
                Ok(meta) if meta.receipt.liquidation_result.is_some() => decoded.push((row, meta.receipt)),
                Ok(_) => {
                    let error = format!("missing liquidation_result for WAL id {}", row.id);
                    self.quarantine_row(&row.id, error).await;
                }
                Err(error) => self.quarantine_row(&row.id, error).await,
            }
        }
        decoded
    }

    /// One row through the stage. `Some` is a finished liquidation whose
    /// terminal status is durable, ready to publish.
    async fn finalize_row(
        &self,
        row: &LiqResultRecord,
        mut receipt: ExecutionReceipt,
    ) -> Result<Option<Finished>, String> {
        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;
        let liq_id = liq.id;
        let refreshed = !matches!(liq.collateral_tx.status, TransferStatus::Success);
        if refreshed {
            self.refresh_liquidation(&row.id, liq_id, &mut receipt).await;
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
            return Ok(None);
        }

        debug!(
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
            self.refresh_pending_change(&row.id, liq_id, &mut receipt, refreshed)
                .await;
            let result = FinalizerResult::finished_without_swap(Some("none".to_string()), None);
            let write = wal_mark_succeeded(&*self.wal, &row.id).await;
            return Ok(published(write, liq_id, result, receipt));
        }
        if row.attempt >= MAX_FINALIZER_ATTEMPTS {
            let err_msg = format!(
                "liquidation was worked {MAX_FINALIZER_ATTEMPTS} times without finishing; last error: {}",
                row.last_error.as_deref().unwrap_or("none recorded")
            );
            self.park_custody(
                row,
                liq_id,
                &receipt,
                CustodyParkReason::AttemptBudgetExhausted,
                &err_msg,
            )
            .await;
            return Ok(None);
        }
        if self.retry_not_due(row, liq_id) {
            return Ok(None);
        }
        self.refresh_pending_change(&row.id, liq_id, &mut receipt, refreshed)
            .await;

        wal_mark_inflight(&*self.wal, &row.id).await?;
        Ok(match self.run_finalizer(liq_id, &receipt).await {
            Ok(result) => self.handle_result(row, liq_id, result, receipt).await,
            Err(error) => self.handle_error(row, liq_id, error, receipt).await,
        })
    }

    /// A row whose collateral settled at once is read only here, so this is where
    /// its pending change can land. It runs only for a row this cycle works.
    async fn refresh_pending_change(
        &self,
        row_id: &str,
        liq_id: u128,
        receipt: &mut ExecutionReceipt,
        refreshed: bool,
    ) {
        let change_pending = receipt
            .liquidation_result
            .as_ref()
            .is_some_and(|liq| matches!(liq.change_tx.status, TransferStatus::Pending));
        if change_pending && !refreshed {
            self.refresh_liquidation(row_id, liq_id, receipt).await;
        }
    }

    /// Asks the pool again while the collateral transfer is open, and saves a
    /// receipt that changed.
    async fn refresh_liquidation(&self, row_id: &str, liq_id: u128, receipt: &mut ExecutionReceipt) {
        match get_liquidation(self.agent.as_ref(), &self.lending_canister, liq_id).await {
            Ok(fresh) => {
                if receipt.apply_pool_view(fresh)
                    && let Err(err) = self.update_receipt_meta(row_id, receipt).await
                {
                    warn!("Failed to update WAL meta for liq_id {}: {}", liq_id, err);
                }
            }
            Err(err) => warn!("[finalize] get_liquidation failed liq_id={} err={}", liq_id, err),
        }
    }

    /// True while a failed row waits out its backoff, so thin books are not thrashed.
    fn retry_not_due(&self, row: &LiqResultRecord, liq_id: u128) -> bool {
        if row.status != ResultStatus::FailedRetryable {
            return false;
        }
        let recorded_errors = row.error_count.max(1);
        // Only a `NoRoute` row stays retryable past the budget, and it waits longer.
        let delay_secs = if recorded_errors >= MAX_FINALIZER_ERRORS {
            NO_ROUTE_SLOW_RETRY_SECS
        } else {
            retry_delay_secs(self.cex_retry_base_secs, self.cex_retry_max_secs, recorded_errors)
        };
        let due_at = row.updated_at.saturating_add(delay_secs.min(i64::MAX as u64) as i64);
        let now = now_ts();
        if now >= due_at {
            return false;
        }
        debug!(
            "[finalize] ⏳ retry backoff liq_id={} errors={} delay={}s remaining={}s",
            liq_id,
            recorded_errors,
            delay_secs,
            due_at.saturating_sub(now)
        );
        true
    }

    async fn run_finalizer(&self, liq_id: u128, receipt: &ExecutionReceipt) -> Result<FinalizerResult, FinalizerError> {
        // A panic in one row must not abort the batch. Without this the
        // unwind escapes `process()` entirely, taking export, heartbeat and
        // every remaining row with it -- and `tokio::time::timeout` does not
        // catch panics, so a deterministic one would repeat every cycle.
        AssertUnwindSafe(self.finalizer.finalize(&*self.wal, receipt.clone()))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                let detail = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                error!("[finalize] 💥 finalizer panicked liq_id={} detail={}", liq_id, detail);
                // A panic says nothing about whether a retry would succeed.
                Err(FinalizerError::Retryable(format!("finalizer panicked: {detail}")))
            })
    }

    async fn handle_result(
        &self,
        row: &LiqResultRecord,
        liq_id: u128,
        result: FinalizerResult,
        receipt: ExecutionReceipt,
    ) -> Option<Finished> {
        if result.operator_required {
            match wal_mark_operator_required(&*self.wal, &row.id).await {
                Err(error) => warn!("Failed to park operator-required WAL row {}: {}", row.id, error),
                Ok(()) => warn!(
                    "[finalize] operator reconciliation required liq_id={} reason={}",
                    liq_id,
                    result.reason.as_deref().unwrap_or("unspecified")
                ),
            }
            return None;
        }
        if result.finalized {
            let write = wal_mark_succeeded(&*self.wal, &row.id).await;
            return published(write, liq_id, result, receipt);
        }
        if let Err(error) = wal_mark_enqueued(&*self.wal, &row.id).await {
            warn!("Failed to re-enqueue unfinished WAL row {}: {}", row.id, error);
        }
        None
    }

    /// The error variant decides the row's fate; the attempt count decides
    /// when a retryable error has spent its budget.
    async fn handle_error(
        &self,
        row: &LiqResultRecord,
        liq_id: u128,
        error: FinalizerError,
        receipt: ExecutionReceipt,
    ) -> Option<Finished> {
        let attempt = row.error_count + 1;
        let exhausted = attempt >= MAX_FINALIZER_ERRORS;
        let err_msg = error.message().to_string();
        warn!(
            "[finalize] finalization failed liq_id={} kind={} attempt={} err={}",
            liq_id,
            error.kind(),
            attempt,
            err_msg
        );

        match error {
            FinalizerError::Unresumable(_) => {
                // This build cannot read the committed row. The
                // liquidation is not wrong, so it must not be failed:
                // park it where an operator can see it, exactly as
                // startup does for the same condition.
                if let Err(mark_error) = wal_mark_unresumable(&*self.wal, &row.id, err_msg.clone()).await {
                    warn!("Failed to park unresumable WAL row {}: {}", row.id, mark_error);
                }
                error!(
                    "[finalize] 🅿️ committed row cannot be resumed by this build; parked for operator liq_id={} err={}",
                    liq_id, err_msg
                );
                self.escalate_custody_park(&receipt, liq_id, CustodyParkReason::UnresumableCommittedRow, &err_msg)
                    .await;
                None
            }
            FinalizerError::BadDebtAmountFloor(_) if receipt.request.liquidation.buy_bad_debt => {
                let reason = format!("bad debt finalizer amount floor accepted: {}", err_msg);
                let result = FinalizerResult::finished_without_swap(None, Some(reason));
                let write = wal_mark_succeeded(&*self.wal, &row.id).await;
                published(write, liq_id, result, receipt)
            }
            FinalizerError::VenueCustody(_) if exhausted => {
                self.park_custody(row, liq_id, &receipt, CustodyParkReason::RetryBudgetExhausted, &err_msg)
                    .await;
                None
            }
            FinalizerError::NoRoute(_) => {
                self.retry_no_route(row, liq_id, attempt, &receipt, &err_msg).await;
                None
            }
            FinalizerError::Permanent(_) | FinalizerError::BadDebtAmountFloor(_) => {
                self.fail_permanently(row, liq_id, receipt, err_msg).await
            }
            _ if exhausted => self.fail_permanently(row, liq_id, receipt, err_msg).await,
            _ => {
                if let Err(error) = wal_mark_retryable_failed(&*self.wal, &row.id, err_msg).await {
                    warn!("Failed to mark WAL row {} retryable: {}", row.id, error);
                }
                None
            }
        }
    }

    /// A budget is spent, but a venue leg may still
    /// hold this liquidation's funds. Failing permanently
    /// would drop the row out of the runnable queue and
    /// leave that custody with nothing tracking it, so park
    /// it for an operator instead. Parking is terminal until
    /// someone requeues the row, which is deliberate: the
    /// venue side has to be understood before a retry.
    async fn park_custody(
        &self,
        row: &LiqResultRecord,
        liq_id: u128,
        receipt: &ExecutionReceipt,
        reason: CustodyParkReason,
        err_msg: &str,
    ) {
        match wal_mark_operator_required_with_error(&*self.wal, &row.id, err_msg.to_string()).await {
            Err(error) => warn!("Failed to park custody-holding WAL row {}: {}", row.id, error),
            Ok(()) => error!(
                "[finalize] 🅿️ parked for operator ({}) liq_id={} err={}",
                reason, liq_id, err_msg
            ),
        }
        // A parked row produces no finalized outcome, so it
        // reaches neither the CSV export nor the
        // liquidation-finalized notification. Without this
        // the only trace of stranded custody is a log line.
        self.escalate_custody_park(receipt, liq_id, reason, err_msg).await;
    }

    /// The collateral is still ours, and most causes pass: a venue outage, a thin
    /// book, a price spike. So the row keeps retrying, at a slow pace once its
    /// budget is spent, with one alert then (audit L17).
    async fn retry_no_route(
        &self,
        row: &LiqResultRecord,
        liq_id: u128,
        attempt: i32,
        receipt: &ExecutionReceipt,
        err_msg: &str,
    ) {
        if let Err(error) = wal_mark_no_route(&*self.wal, &row.id, err_msg.to_string()).await {
            warn!("Failed to mark WAL row {} for a retry: {}", row.id, error);
        }
        if attempt == MAX_FINALIZER_ERRORS {
            self.escalate_slow_retry(receipt, liq_id, err_msg).await;
        }
    }

    /// Fails the row for good and reports it as `SwapFailed`.
    async fn fail_permanently(
        &self,
        row: &LiqResultRecord,
        liq_id: u128,
        receipt: ExecutionReceipt,
        err_msg: String,
    ) -> Option<Finished> {
        let write = wal_mark_permanent_failed(&*self.wal, &row.id, err_msg.clone()).await;
        let mut failed_receipt = receipt;
        failed_receipt.status = ExecutionStatus::SwapFailed(format!("finalizer permanent failure: {}", err_msg));
        let result = FinalizerResult::finished_without_swap(None, Some(err_msg));
        published(write, liq_id, result, failed_receipt)
    }

    /// The outcome of one finished liquidation, with its PnL saved on the row.
    async fn record_outcome(
        &self,
        row: &LiqResultRecord,
        result: FinalizerResult,
        receipt: ExecutionReceipt,
    ) -> Result<LiquidationOutcome, String> {
        let req = &receipt.request;
        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for request: {:?}", req))?;

        let round_trip_secs = result.finalized.then(|| now_ts().saturating_sub(row.created_at));
        let expected_profit = self.profit_calc.expected(req, Some(liq));
        let realized_profit = self.profit_calc.realized(req, liq, result.swap_result.as_ref());

        if let Err(err) = self
            .persist_profit_snapshot(&row.id, &receipt, expected_profit, realized_profit)
            .await
        {
            warn!("Failed to persist WAL profit snapshot for liq_id {}: {}", liq.id, err);
        }

        Ok(LiquidationOutcome {
            request: req.clone(),
            execution_receipt: receipt.clone(),
            finalizer_result: result,
            status: receipt.status.clone(),
            expected_profit,
            realized_profit,
            round_trip_secs,
        })
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
        let rows = self.wal.get_pending(100).await.map_err(|e| e.to_string())?;
        debug!("Finalizing rows {:?}", rows);

        // Each row runs on its own. Outcomes are built after the whole batch.
        let mut finished = vec![];
        for (row, receipt) in self.decode_rows(rows).await {
            if let Some(done) = self.finalize_row(&row, receipt).await? {
                finished.push((row, done));
            }
        }

        let mut outcomes = Vec::with_capacity(finished.len());
        for (row, (result, receipt)) in finished {
            outcomes.push(self.record_outcome(&row, result, receipt).await?);
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
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, ProtocolError,
        TransferStatus, TxStatus,
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
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, FinalizerError> {
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
        error: FinalizerError,
    }

    struct OperatorRequiredFinalizer;

    #[async_trait::async_trait]
    impl Finalizer for OperatorRequiredFinalizer {
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, FinalizerError> {
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
        async fn finalize(&self, _: &dyn WalStore, _: ExecutionReceipt) -> Result<FinalizerResult, FinalizerError> {
            Err(self.error.clone())
        }
    }

    /// A stage with the default test tunables and an agent that is never called.
    fn test_stage<F: Finalizer>(
        wal: MockWalStore,
        finalizer: F,
    ) -> FinalizeStage<F, MockWalStore, SimpleProfitCalculator, MockPipelineAgent> {
        FinalizeStage::new(
            Arc::new(wal),
            Arc::new(finalizer),
            Arc::new(SimpleProfitCalculator),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
            5,
            120,
        )
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
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
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
            // Settled, so a row is not re-read for its change; the test for
            // that re-read builds its own.
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
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
        let wrapper = LiqMetaWrapper::new(receipt);
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
            .withf(move |id, status, bump| id == valid_id && *status == ResultStatus::InFlight && !*bump)
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
        let stage = test_stage(wal, NoopFinalizer { calls: calls.clone() });

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
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && !*bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, bump| {
                id == row_id_for_operator && *status == ResultStatus::OperatorRequired && !*bump
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let stage = test_stage(wal, OperatorRequiredFinalizer);

        let outcomes = stage.process(&()).await.expect("operator row should park cleanly");
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
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && !*bump)
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
        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::VenueCustody("venue still holds the input".to_string()),
            },
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

    #[tokio::test]
    async fn exhausted_custody_retries_escalate_when_the_park_write_fails() {
        let liq_id = 925u128;
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
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && !*bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, error, _| {
                id == row_id_for_park
                    && *status == ResultStatus::OperatorRequired
                    && error.contains("venue still holds the input")
            })
            .times(1)
            .returning(|_, _, _, _| Err(anyhow::anyhow!("WAL unavailable")));

        let watchdog = Arc::new(RecordingWatchdog::default());
        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::VenueCustody("venue still holds the input".to_string()),
            },
        )
        .with_watchdog(watchdog.clone());

        let outcomes = stage
            .process(&())
            .await
            .expect("failed park write should not suppress processing");
        assert!(outcomes.is_empty());

        let alerts = watchdog.operator_alerts();
        assert_eq!(alerts.len(), 1, "a failed park write must still alert an operator");
        let (execution_id, pending_step, details) = &alerts[0];
        assert_eq!(execution_id, &liq_id.to_string());
        assert_eq!(pending_step, "retry_budget_exhausted");
        assert!(details.contains("venue still holds the input"));
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
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && !*bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, _, bump| {
                id == row_id_for_retry && *status == ResultStatus::FailedRetryable && *bump
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let watchdog = Arc::new(RecordingWatchdog::default());
        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::VenueCustody("venue still holds the input".to_string()),
            },
        )
        .with_watchdog(watchdog.clone());

        let outcomes = stage.process(&()).await.expect("custody row should retry");
        assert!(outcomes.is_empty());
        assert!(watchdog.operator_alerts().is_empty());
    }

    /// Audit L17: a route search that keeps failing must not fail the row while
    /// the collateral is still ours. It drops to the slow pace with one alert.
    #[tokio::test]
    async fn exhausted_route_search_retries_slowly_and_alerts_once() {
        let liq_id = 926u128;
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
            .withf(move |id, status, bump| id == row_id && *status == ResultStatus::InFlight && !*bump)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(move |id, status, _, _| id == row_id_for_retry && *status == ResultStatus::FailedRetryable)
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let watchdog = Arc::new(RecordingWatchdog::default());
        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::NoRoute("no viable ICPSwap-first route: MEXC unavailable".to_string()),
            },
        )
        .with_watchdog(watchdog.clone());

        let outcomes = stage.process(&()).await.expect("route failure should retry");
        assert!(outcomes.is_empty(), "a row still retrying reports no outcome");

        let alerts = watchdog.operator_alerts();
        assert_eq!(alerts.len(), 1);
        let (execution_id, pending_step, details) = &alerts[0];
        assert_eq!(execution_id, &liq_id.to_string());
        assert_eq!(pending_step, "no_route_slow_retry");
        assert!(details.contains("MEXC unavailable"));
    }

    /// Past the budget a route failure waits the slow interval, not the normal cap.
    #[tokio::test]
    async fn slow_retry_row_waits_longer_than_the_normal_backoff() {
        let liq_id = 927u128;
        let mut row = make_row(liq_id, make_swapping_receipt(liq_id));
        row.status = ResultStatus::FailedRetryable;
        row.error_count = MAX_FINALIZER_ERRORS;
        row.updated_at = now_ts() - 600;

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));

        let calls = Arc::new(Mutex::new(0usize));
        let stage = test_stage(wal, NoopFinalizer { calls: calls.clone() });

        let outcomes = stage.process(&()).await.expect("waiting row is skipped");
        assert!(outcomes.is_empty());
        assert_eq!(
            *calls.lock().unwrap(),
            0,
            "ten minutes is inside the slow retry interval"
        );
    }

    /// Audit L18: an outcome is published only after its terminal status is
    /// durable. A failed write leaves the row to run again.
    #[tokio::test]
    async fn failed_terminal_write_publishes_no_outcome() {
        let liq_id = 928u128;
        let row = make_row(liq_id, make_swapping_receipt(liq_id));
        let row_id = row.id.clone();
        let row_id_for_success = row_id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .return_once(move |_| Ok(vec![row]));
        wal.expect_update_status()
            .withf(move |id, status, _| id == row_id && *status == ResultStatus::InFlight)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_status()
            .withf(move |id, status, _| id == row_id_for_success && *status == ResultStatus::Succeeded)
            .times(1)
            .returning(|_, _, _| Err(anyhow::anyhow!("database locked")));

        let stage = test_stage(
            wal,
            NoopFinalizer {
                calls: Arc::new(Mutex::new(0usize)),
            },
        );

        let outcomes = stage.process(&()).await.expect("a failed write is not a stage error");
        assert!(
            outcomes.is_empty(),
            "no export or notification before the status is durable"
        );
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

        let err = "ckETH@ICP -> ETH amount below minimum withdrawal (amount=0.0049 minimum=0.005)".to_string();
        let err_for_failure = err.clone();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && !*bump)
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

        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::BadDebtAmountFloor(err),
            },
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

        let err = "ckETH@ICP -> ETH amount below minimum withdrawal (amount=0.0049 minimum=0.005)".to_string();

        let mut wal = MockWalStore::new();
        wal.expect_get_pending()
            .with(eq(100usize))
            .times(1)
            .returning(move |_| Ok(vec![row_pending.clone()]));
        wal.expect_update_status()
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && !*bump)
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

        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::BadDebtAmountFloor(err.clone()),
            },
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
            .withf(move |id, status, bump| id == liq_id_str.as_str() && *status == ResultStatus::InFlight && !*bump)
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

        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::Retryable(err),
            },
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

    /// A row whose collateral settled at once never passes the settlement
    /// watcher, so only finalize can see its change land.
    #[tokio::test]
    async fn finalize_re_reads_a_row_whose_change_was_still_pending_and_corrects_the_flag() {
        let liq_id = 78u128;
        let mut initial_liq = make_liq_result(liq_id, TransferStatus::Success, 0);
        initial_liq.change_tx.status = TransferStatus::Pending;
        let receipt = ExecutionReceipt {
            request: make_request(),
            liquidation_result: Some(initial_liq),
            status: ExecutionStatus::Success,
            change_received: false,
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
            .returning(move |_| Ok(Some(row_for_get.clone())));
        // The corrected receipt is written back before the row is finalized.
        wal.expect_upsert_result()
            .withf(|row| {
                let wrapper = decode_receipt_wrapper(row)
                    .expect("stored meta decodes")
                    .expect("stored meta is present");
                wrapper.receipt.change_received
            })
            .times(1)
            .returning(|_| Ok(()));
        wal.expect_upsert_result().returning(|_| Ok(()));
        wal.expect_update_status().returning(|_, _, _| Ok(()));

        let fresh_liq = make_liq_result(liq_id, TransferStatus::Success, 0);
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(
                eq(Principal::anonymous()),
                eq("get_liquidation"),
                eq(Encode!(&liq_id).expect("encode should succeed")),
            )
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh_liq.clone())));

        let stage = FinalizeStage::new(
            Arc::new(wal),
            Arc::new(NoopFinalizer {
                calls: Arc::new(Mutex::new(0)),
            }),
            Arc::new(SimpleProfitCalculator),
            Arc::new(agent),
            Principal::anonymous(),
            5,
            120,
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].execution_receipt.change_received);
    }

    /// One finalize cycle over `row`; returns how often the finalizer ran.
    async fn finalize_calls_for(row: LiqResultRecord, mut wal: MockWalStore) -> usize {
        wal.expect_get_pending().returning(move |_| Ok(vec![row.clone()]));
        let calls = Arc::new(Mutex::new(0usize));
        let stage = test_stage(wal, NoopFinalizer { calls: calls.clone() });
        stage.process(&()).await.expect("process should succeed");
        *calls.lock().expect("calls lock")
    }

    /// Liquidation 1630 failed slowly for minutes: each release cleared its
    /// strikes, so only the lifetime attempt budget can stop such a row.
    #[tokio::test]
    async fn finalize_parks_a_row_that_exhausts_its_lifetime_attempt_budget() {
        let mut row = make_row(91, make_swapping_receipt(91));
        row.status = ResultStatus::Enqueued;
        row.attempt = MAX_FINALIZER_ATTEMPTS;

        let mut wal = MockWalStore::new();
        wal.expect_update_failure()
            .withf(|_, next, _, _| *next == ResultStatus::OperatorRequired)
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        wal.expect_update_status().times(0);
        wal.expect_upsert_result().times(0);

        assert_eq!(
            finalize_calls_for(row, wal).await,
            0,
            "an exhausted row is not worked again"
        );
    }

    /// The cap counts work, not waiting, so a patient healthy row keeps running.
    #[tokio::test]
    async fn finalize_does_not_park_a_long_running_row_that_has_not_failed() {
        let mut row = make_row(92, make_swapping_receipt(92));
        row.status = ResultStatus::Enqueued;
        row.attempt = 1;

        let mut wal = MockWalStore::new();
        wal.expect_update_failure()
            .withf(|_, next, _, _| *next == ResultStatus::OperatorRequired)
            .times(0);
        wal.expect_update_status().returning(|_, _, _| Ok(()));
        wal.expect_upsert_result().returning(|_| Ok(()));
        wal.expect_get_result().returning(|_| Ok(None));

        assert_eq!(finalize_calls_for(row, wal).await, 1);
    }

    /// A route search that finds nothing did no work, so it is a strike but not
    /// an attempt. Otherwise a row on the slow retry (audit L17) would be parked
    /// by the attempt cap as soon as a route appears.
    #[tokio::test]
    async fn a_no_route_failure_is_a_strike_but_not_an_attempt() {
        let mut row = make_row(93, make_swapping_receipt(93));
        row.status = ResultStatus::Enqueued;

        let mut wal = MockWalStore::new();
        wal.expect_get_pending().returning(move |_| Ok(vec![row.clone()]));
        wal.expect_update_status().returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(|_, next, _, bump| *next == ResultStatus::FailedRetryable && !*bump)
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::NoRoute("no acceptable swap".to_string()),
            },
        );

        assert!(stage.process(&()).await.expect("process should succeed").is_empty());
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
        let mut row = make_row(liq_id, make_swapping_receipt(liq_id));
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
        let mut row = make_row(liq_id, make_swapping_receipt(liq_id));
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

        let stage = test_stage(
            wal,
            NoopFinalizer {
                calls: Arc::new(Mutex::new(0)),
            },
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

        let stage = test_stage(
            wal,
            NoopFinalizer {
                calls: Arc::new(Mutex::new(0)),
            },
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
    }

    /// A permanent failure reports `SwapFailed` to the export and the operator,
    /// but the committed receipt has to keep describing what actually happened
    /// on chain. Overwriting it would leave the row unable to replan, because
    /// planning requires a successful execution, so a requeue could never
    /// finalize a liquidation whose collateral the trader already holds.
    #[tokio::test]
    async fn permanent_failure_reports_swap_failed_without_rewriting_the_committed_receipt() {
        let liq_id = 914u128;
        let receipt = ExecutionReceipt {
            request: make_swapping_receipt(liq_id).request,
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
            .withf(|_, status, _| *status == ResultStatus::InFlight)
            .times(1)
            .returning(|_, _, _| Ok(()));
        wal.expect_update_failure()
            .withf(|_, status, _, _| *status == ResultStatus::FailedPermanent)
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        wal.expect_get_result()
            .times(1)
            .returning(move |_| Ok(Some(row_for_get.clone())));
        wal.expect_upsert_result().times(1).returning(|row| {
            let wrapper = decode_receipt_wrapper(&row)
                .expect("decode wrapper")
                .expect("wrapper exists");
            assert!(
                matches!(wrapper.receipt.status, ExecutionStatus::Success),
                "the committed receipt must still be replannable, got {:?}",
                wrapper.receipt.status
            );
            assert!(wrapper.profit_snapshot.is_some(), "the PnL still has to be recorded");
            Ok(())
        });

        let stage = test_stage(
            wal,
            ErrorFinalizer {
                error: FinalizerError::Permanent("permanent multi-venue finalizer: no route".to_string()),
            },
        );

        let outcomes = stage.process(&()).await.expect("process should succeed");
        assert_eq!(outcomes.len(), 1);
        // The reported outcome still carries the failure for export and alerting.
        assert!(matches!(outcomes[0].status, ExecutionStatus::SwapFailed(_)));
    }
}
