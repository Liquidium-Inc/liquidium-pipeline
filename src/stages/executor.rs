use async_trait::async_trait;
use candid::{Decode, Encode};
use lending::interface::liquidation::LiquidationResult;
use log::info;

use crate::{
    executors::{
        executor::{ExecutorRequest, IcrcSwapExecutor},
        kong_swap::kong_swap::KongSwapExecutor,
    },
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    types::SwapResult,
};

#[async_trait]
impl<A: PipelineAgent> PipelineStage<ExecutorRequest, SwapResult> for KongSwapExecutor<A> {
    async fn process(&self, executor_request: ExecutorRequest) -> Result<SwapResult, String> {
        let args = Encode!(
            &self.config.liquidator_principal,
            &executor_request.liquidation
        )
        .map_err(|e| e.to_string())?;

        // Make the update call to the canister
        let response = self
            .agent
            .call_update(&self.config.lending_canister, "liquidate", args)
            .await
            .map_err(|e| format!("Agent update error: {e}"))?;

        // Decode the candid response
        let result: Result<LiquidationResult, String> =
            Decode!(&response, Result<LiquidationResult, String>)
                .map_err(|e| format!("Candid decode error: {e}"))?;

        info!("Executed liquidation {:?}", result);

        let swap_result = self
            .swap(executor_request.swap_args)
            .await
            .expect("swap failed");

        info!("Executed swap {:?}", swap_result);
        Ok(SwapResult {
            received_asset: "USDT".into(),
            received_amount: 0,
        })
    }
}
