use async_trait::async_trait;
use candid::Encode;
use liquidium_pipeline_commons::error::ErrorCode;

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use tracing::{debug, info, warn};

use crate::{
    executors::{basic::basic_executor::BasicExecutor, executor::ExecutorRequest},
    finalizers::{finalizer::FinalizerResult, liquidation_outcome::LiquidationOutcome},
    persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore},
    stage::PipelineStage,
    utils::now_ts,
    wal::{encode_meta, liq_id_from_receipt},
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

fn coded(code: ErrorCode, message: impl Into<String>) -> String {
    format!("{} (code={})", message.into(), code.as_u16())
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
    pub change_received: bool,
}

#[async_trait]
impl<'a, A: PipelineAgent, D: WalStore> PipelineStage<'a, Vec<ExecutorRequest>, Vec<ExecutionReceipt>>
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

                info!(
                    "[executor] ⚡ liquidation req | borrower={} | debt_pool={} | collateral_pool={} | debt={} | bad_debt={}",
                    liq_req.borrower.to_text(),
                    liq_req.debt_pool_id.to_text(),
                    liq_req.collateral_pool_id.to_text(),
                    liq_req.debt_amount,
                    liq_req.buy_bad_debt
                );

                let args = Encode!(&liq_req)
                    .map_err(|e| coded(ErrorCode::PipelineExecution, format!("liquidation encode failed: {e}")))?;

                let liq_call = match self
                    .agent
                    .call_update::<Result<LiquidationResult, ProtocolError>>(
                        &self.lending_canister,
                        "liquidate",
                        args,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        warn!("Liquidation call failed {err}");
                        receipt.status =
                            ExecutionStatus::LiquidationCallFailed(coded(ErrorCode::PipelineExecution, err.to_string()));
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                };

                let liq = match liq_call {
                    Ok(v) => v,
                    Err(err) => {
                        receipt.status =
                            ExecutionStatus::FailedLiquidation(coded(ErrorCode::PipelineExecution, format!("{err:?}")));
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                };

                receipt.liquidation_result = Some(liq.clone());
                if let LiquidationStatus::FailedLiquidation(err) = liq.status {
                    receipt.status = ExecutionStatus::FailedLiquidation(err);
                    return Ok::<ExecutionReceipt, String>(receipt);
                }

                if matches!(
                    liq.change_tx.status,
                    TransferStatus::Failed(_) | TransferStatus::Pending
                ) {
                    info!(
                        "[executor] 💱 change_tx status={:?} liq_id={}",
                        liq.change_tx.status, liq.id
                    );
                    receipt.change_received = false;
                    return Ok::<ExecutionReceipt, String>(receipt);
                }

                match &liq.collateral_tx.status {
                    TransferStatus::Success => {}
                    TransferStatus::Pending => {
                        info!(
                            "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                            liq.collateral_tx.status, liq.id
                        );
                        receipt.status = ExecutionStatus::CollateralTransferFailed(coded(
                            ErrorCode::PipelineExecution,
                            "collateral pending",
                        ));
                        if let Err(err) = self
                            .store_to_wal(&receipt, &executor_request, ResultStatus::WaitingCollateral)
                            .await
                        {
                            warn!("Failed to store to WAL: {}", err);
                        }
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                    TransferStatus::Failed(err) => {
                        info!(
                            "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                            liq.collateral_tx.status, liq.id
                        );
                        receipt.status = ExecutionStatus::CollateralTransferFailed(coded(
                            ErrorCode::PipelineExecution,
                            err.clone(),
                        ));
                        if let Err(err) = self
                            .store_to_wal(&receipt, &executor_request, ResultStatus::WaitingCollateral)
                            .await
                        {
                            warn!("Failed to store to WAL: {}", err);
                        }
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                }

                debug!("Executed liquidation {:?}", liq);
                if let Err(err) = self
                    .store_to_wal(&receipt, &executor_request, ResultStatus::Enqueued)
                    .await
                {
                    warn!("Failed to store to WAL: {}", err);
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

        Ok(liquidations)
    }
}

impl<A: PipelineAgent, D: WalStore> BasicExecutor<A, D> {
    async fn store_to_wal(
        &self,
        receipt: &ExecutionReceipt,
        executor_request: &ExecutorRequest,
        status: ResultStatus,
    ) -> Result<(), String> {
        debug!("Storing execution log...");
        let liq_id = liq_id_from_receipt(receipt)?;

        let _outcome = LiquidationOutcome {
            execution_receipt: receipt.clone(),
            expected_profit: executor_request.expected_profit,
            request: executor_request.clone(),
            finalizer_result: FinalizerResult::noop(),
            realized_profit: 0i128,
            status: ExecutionStatus::Pending,
            round_trip_secs: None,
        };

        let mut result_record = LiqResultRecord {
            id: liq_id,
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now_ts(),
            updated_at: now_ts(),
            meta_json: "{}".to_string(),
        };
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
        };
        if let Err(err) = encode_meta(&mut result_record, &wrapper) {
            warn!("Failed to encode WAL metadata: {}", String::from(err));
        }
        self.wal
            .upsert_result(result_record)
            .await
            .map_err(|e| coded(ErrorCode::PipelineWal, format!("wal upsert_result failed: {e}")))
    }
}
