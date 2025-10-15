use async_trait::async_trait;
use candid::Encode;

use log::{debug, warn};
use serde::{Deserialize, Serialize};

use crate::{
    executors::{basic::basic_executor::BasicExecutor, executor::ExecutorRequest},
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    types::protocol_types::{LiquidationResult, LiquidationStatus},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Success,
    LiquidationCallFailed(String),
    FailedLiquidation(String),
    CollateralTransferFailed(String),
    ChangeTransferFailed(String),
    SwapFailed(String) 
}

impl ExecutionStatus {
    pub fn description(&self) -> String {
        match self {
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
}

#[async_trait]
impl<'a, A: PipelineAgent> PipelineStage<'a, Vec<ExecutorRequest>, Vec<ExecutionReceipt>> for BasicExecutor<A> {
    async fn process(&self, executor_requests: &'a Vec<ExecutorRequest>) -> Result<Vec<ExecutionReceipt>, String> {
        let mut liquidations: Vec<ExecutionReceipt> = vec![];
        for executor_request in executor_requests {
            let mut receipt = ExecutionReceipt {
                request: executor_request.clone(),
                liquidation_result: None,
                status: ExecutionStatus::Success,
            };

            // Force receiver to the configured dex account to separate seized collateral from repay funds
            let liq_req = executor_request.liquidation.clone();

            let args = Encode!(&self.account_id.owner, &liq_req).map_err(|e| e.to_string())?;
            // Make the update call to the canister
            let liq_call = match self
                .agent
                .call_update::<Result<LiquidationResult, String>>(&self.lending_canister, "liquidate", args)
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    warn!("Liquidation call failed {err}");
                    receipt.status = ExecutionStatus::LiquidationCallFailed(err);
                    liquidations.push(receipt);
                    continue;
                }
            };

            let liq = match liq_call {
                Ok(v) => v,
                Err(err) => {
                    receipt.status = ExecutionStatus::FailedLiquidation(err);
                    liquidations.push(receipt);
                    continue;
                }
            };

            receipt.liquidation_result = Some(liq.clone());
            if let LiquidationStatus::FailedLiquidation(err) = liq.status {
                receipt.status = ExecutionStatus::FailedLiquidation(err);
                liquidations.push(receipt);
                continue;
            }

            if let LiquidationStatus::ChangeTransferFailed(err) = liq.status {
                receipt.status = ExecutionStatus::ChangeTransferFailed(err);
                liquidations.push(receipt);
                continue;
            }

            if let LiquidationStatus::CollateralTransferFailed(err) = liq.status {
                receipt.status = ExecutionStatus::CollateralTransferFailed(err);
                liquidations.push(receipt);
                continue;
            }

            debug!("Executed liquidation {:?}", liq);

            liquidations.push(receipt);
        }

        Ok(liquidations)
    }
}
