use async_trait::async_trait;
use candid::Encode;

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use tracing::{debug, info, warn};

use crate::{
    executors::{basic::basic_executor::BasicExecutor, executor::ExecutorRequest},
    liquidation::lending_queries::get_liquidation_inflow,
    persistance::LiquidationIntentStore,
    stage::PipelineStage,
    watchdog::WatchdogEvent,
};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use liquidium_pipeline_core::types::protocol_types::{
    LiquidationResult, LiquidationStatus, ProtocolError, TransferStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Pending,
    Success,
    LiquidationCallFailed(String),
    FailedLiquidation(String),
    CollateralTransferFailed(String),
    ChangeTransferFailed(String),
    SwapFailed(String),
}

impl ExecutionStatus {
    pub fn description(&self) -> String {
        match self {
            ExecutionStatus::Pending => "Pending".to_string(),
            ExecutionStatus::Success => "Success".to_string(),
            ExecutionStatus::LiquidationCallFailed(msg) => format!("LiquidationCallFailed: {}", msg),
            ExecutionStatus::FailedLiquidation(msg) => format!("FailedLiquidation: {}", msg),
            ExecutionStatus::CollateralTransferFailed(msg) => format!("CollateralTransferFailed: {}", msg),
            ExecutionStatus::ChangeTransferFailed(msg) => format!("ChangeTransferFailed: {}", msg),
            ExecutionStatus::SwapFailed(msg) => format!("SwapFailed: {}", msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub request: ExecutorRequest,
    pub liquidation_result: Option<LiquidationResult>,
    pub status: ExecutionStatus,
    /// Whether the change transfer had settled at the last read of the
    /// liquidation. For display only; no retry or recovery reads it.
    pub change_received: bool,
}

impl ExecutionReceipt {
    /// Returns the receipt of a liquidation that the canister answered.
    /// The normal call and the recovery of an interrupted call both use it, so their rows are the same.
    pub fn from_result(request: ExecutorRequest, liquidation: LiquidationResult) -> Self {
        let status = match (&liquidation.status, &liquidation.collateral_tx.status) {
            (LiquidationStatus::FailedLiquidation(error), _) => ExecutionStatus::FailedLiquidation(error.clone()),
            (_, TransferStatus::Success) => ExecutionStatus::Success,
            (_, TransferStatus::Pending) => ExecutionStatus::CollateralTransferFailed("collateral pending".to_string()),
            (_, TransferStatus::Failed(error)) => ExecutionStatus::CollateralTransferFailed(error.clone()),
        };
        // A failed liquidation keeps the default, as before. No retry reads this flag.
        let change_received = matches!(status, ExecutionStatus::FailedLiquidation(_))
            || matches!(liquidation.change_tx.status, TransferStatus::Success);
        Self {
            request,
            liquidation_result: Some(liquidation),
            status,
            change_received,
        }
    }

    /// Takes the pool's current view of the liquidation. A collateral transfer
    /// that has landed clears its earlier failure. Returns whether anything changed.
    pub fn apply_pool_view(&mut self, mut fresh: LiquidationResult) -> bool {
        // For a share payout, the pool reports the seized value. The receipt keeps the amount that the withdraw paid.
        if let Some(current) = &self.liquidation_result
            && current.collateral_share_receiver.is_some()
        {
            fresh.amounts.collateral_received = current.amounts.collateral_received.clone();
        }
        let collateral_landed = matches!(fresh.collateral_tx.status, TransferStatus::Success);
        let change_received = matches!(fresh.change_tx.status, TransferStatus::Success);
        let mut changed = false;
        if self.change_received != change_received {
            self.change_received = change_received;
            changed = true;
        }
        if self.liquidation_result.as_ref() != Some(&fresh) {
            self.liquidation_result = Some(fresh);
            changed = true;
        }
        if collateral_landed && matches!(self.status, ExecutionStatus::CollateralTransferFailed(_)) {
            self.status = ExecutionStatus::Success;
            changed = true;
        }
        changed
    }
}

#[async_trait]
impl<'a, A: PipelineAgent, D: LiquidationIntentStore> PipelineStage<'a, Vec<ExecutorRequest>, Vec<ExecutionReceipt>>
    for BasicExecutor<A, D>
{
    #[instrument(name = "executor.process", skip_all, err, fields(request_count = executor_requests.len()))]
    async fn process(&self, executor_requests: &'a Vec<ExecutorRequest>) -> Result<Vec<ExecutionReceipt>, String> {
        debug!("Executing request {:?}", executor_requests);
        // One future per request, all run concurrently
        let futures = executor_requests.iter().map(|executor_request| {
            let executor_request = executor_request.clone();

            async move {
                let mut receipt = ExecutionReceipt {
                    request: executor_request.clone(),
                    liquidation_result: None,
                    status: ExecutionStatus::Success,
                    change_received: true,
                };

                let liq_req = executor_request.liquidation.clone();
                let summary = request_summary(&executor_request);

                info!("[executor] ⚡ liquidation req | {summary}");

                let args =
                    Encode!(&liq_req, &executor_request.min_collateral_amount).map_err(|e| e.to_string())?;
                // Every call gets a new intent, also for a borrower whose last call lost its answer
                // (audit L02). A repeat is a valid liquidation, or the pool rejects it and refunds the debt.
                let intent_id = uuid::Uuid::new_v4().to_string();
                self.intents
                    .create_submitting(&intent_id, &executor_request)
                    .await
                    .map_err(|error| format!("failed to persist liquidation intent {intent_id}: {error}"))?;

                let liq_call = match self
                    .agent
                    .call_update::<Result<LiquidationResult, ProtocolError>>(
                        &self.lending_canister,
                        "liquidate_with_slippage",
                        args,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        warn!("Liquidation call failed {err}");
                        receipt.status = ExecutionStatus::LiquidationCallFailed(err.clone());
                        // The lost reply held the liquidation id, so nothing can finalize the call.
                        self.alert_unrecorded(
                            &intent_id,
                            "liquidation-call-unanswered",
                            format!(
                                "liquidation call got no answer and may have run on chain: {err}; intent {intent_id} is ambiguous, and its collateral may be in the trader account with no WAL row | {summary}"
                            ),
                        )
                        .await;
                        self.intents
                            .mark_ambiguous(&intent_id, &err, None)
                            .await
                            .map_err(|store_error| {
                                format!("failed to mark liquidation intent {intent_id} ambiguous: {store_error}")
                            })?;
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                };

                let liq = match liq_call {
                    Ok(v) => v,
                    Err(err) => {
                        warn!("[executor] liquidate_with_slippage rejected by canister | {summary} | err={err:?}");
                        let error = format!("{:?}", err);
                        receipt.status = ExecutionStatus::FailedLiquidation(error.clone());
                        return self.save_failed(&intent_id, None, receipt, &error).await;
                    }
                };

                // A missing change must not stop the receipt: the WAL is the only handoff of the
                // collateral to the finalizer. The batch end reports the unpaid change (audit L04).
                let receipt = ExecutionReceipt::from_result(executor_request.clone(), liq.clone());
                if let ExecutionStatus::FailedLiquidation(err) = &receipt.status {
                    warn!(
                        "[executor] liquidate_with_slippage returned FailedLiquidation | liq_id={} | {summary} | err={err}",
                        liq.id
                    );
                    let err = err.clone();
                    return self.save_failed(&intent_id, Some(liq.id.to_string()), receipt, &err).await;
                }
                if matches!(receipt.status, ExecutionStatus::CollateralTransferFailed(_)) {
                    info!(
                        "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                        liq.collateral_tx.status, liq.id
                    );
                }

                debug!("Executed liquidation {:?}", liq);
                if let Err(details) =
                    persist_accepted_receipt(self.intents.as_ref(), &intent_id, &liq.id.to_string(), &receipt).await
                {
                    self.alert_unrecorded(&intent_id, "persist-liquidation-result", details.clone())
                        .await;
                    return Err(details);
                }

                Ok::<ExecutionReceipt, String>(receipt)
            }
        });

        let results = join_all(futures).await;

        let mut liquidations = Vec::with_capacity(results.len());
        for res in results {
            let receipt = res?;
            debug!("Receipt result {:?}", receipt);
            liquidations.push(receipt);
        }

        for liquidation in liquidations
            .iter()
            .filter_map(|receipt| receipt.liquidation_result.as_ref())
        {
            self.report_unpaid_change_or_refund(liquidation).await;
        }

        Ok(liquidations)
    }
}

impl<A: PipelineAgent, D: LiquidationIntentStore> BasicExecutor<A, D> {
    /// A liquidation that may have run has no WAL row, so nothing finalizes it.
    /// The intent id keys the alert, so two liquidations never share a cooldown.
    async fn alert_unrecorded(&self, intent_id: &str, pending_step: &str, details: String) {
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: intent_id.to_string(),
                venue: "protocol".to_string(),
                pending_step: pending_step.to_string(),
                owner: self.account_id.owner.to_text(),
                details,
            })
            .await;
    }

    /// Saves a rejected liquidation on its intent and returns its receipt.
    async fn save_failed(
        &self,
        intent_id: &str,
        liquidation_id: Option<String>,
        receipt: ExecutionReceipt,
        error: &str,
    ) -> Result<ExecutionReceipt, String> {
        self.intents
            .mark_failed(intent_id, liquidation_id, Some(receipt.clone()), error)
            .await
            .map_err(|store_error| format!("failed to mark liquidation intent {intent_id} failed: {store_error}"))?;
        Ok(receipt)
    }

    /// Logs a liquidation whose change or refund has not landed yet (audit L04).
    /// The pool retries a temporary failure for about nine hours, and only a
    /// pool admin can recover a payment that never lands, so the bot only reports it.
    async fn report_unpaid_change_or_refund(&self, liquidation: &LiquidationResult) {
        if matches!(liquidation.change_tx.status, TransferStatus::Success) {
            return;
        }
        // The pool keeps this record until it owes nothing. A failed deposit
        // that moved no money has none, so it needs no report.
        match get_liquidation_inflow(self.agent.as_ref(), &self.lending_canister, liquidation.id).await {
            Ok(None) => {}
            Ok(Some(inflow)) => tracing::error!(
                liquidation_id = %liquidation.id,
                debt_amount = %inflow.debt_amount,
                change_tx = ?liquidation.change_tx.status,
                "[executor] the pool has not returned the change or refund yet; it retries a temporary failure for about nine hours, and if the money never lands a pool admin must recover this liquidation"
            ),
            Err(error) => tracing::error!(
                liquidation_id = %liquidation.id,
                change_tx = ?liquidation.change_tx.status,
                "[executor] could not read what the pool still owes for this liquidation: {error}"
            ),
        }
    }
}

fn request_summary(request: &ExecutorRequest) -> String {
    let liquidation = &request.liquidation;
    format!(
        "borrower={} | debt_pool={} | collateral_pool={} | debt={} | bad_debt={} | min_collateral_amount={}",
        liquidation.borrower.to_text(),
        liquidation.debt_pool_id.to_text(),
        liquidation.collateral_pool_id.to_text(),
        liquidation.debt_amount,
        liquidation.buy_bad_debt,
        request.min_collateral_amount
    )
}

async fn persist_accepted_receipt<D: LiquidationIntentStore>(
    intents: &D,
    intent_id: &str,
    liquidation_id: &str,
    receipt: &ExecutionReceipt,
) -> Result<(), String> {
    // The chain has already moved funds. Retry only the atomic local write,
    // never the liquidation call. Keep retries bounded within the cycle timeout.
    let mut last_error = String::new();
    for attempt in 0..3 {
        match intents.mark_accepted(intent_id, liquidation_id, receipt).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    let mut details = format!(
        "liquidation {liquidation_id} succeeded on-chain but intent {intent_id} could not be accepted: {last_error}; operator reconciliation required; do not resubmit"
    );
    // Preserve the successful receipt separately from the failed execution-row
    // insert. Ambiguous intents are never picked up for automatic resubmission.
    if let Err(error) = intents.mark_ambiguous(intent_id, &details, Some(receipt.clone())).await {
        details.push_str(&format!("; could not persist ambiguous state: {error}"));
    }
    tracing::error!(intent_id, liquidation_id, receipt = ?receipt, "{details}");
    Err(details)
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
