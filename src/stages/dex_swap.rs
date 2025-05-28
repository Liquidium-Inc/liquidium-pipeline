use async_trait::async_trait;

use crate::{
    executors::kong_swap::kong_swap::KongSwapExecutor,
    stage::PipelineStage,
    types::{ExecutionReceipt, SwapResult},
};

#[async_trait]
impl PipelineStage<ExecutionReceipt, SwapResult> for KongSwapExecutor {
    async fn process(&self, receipt: ExecutionReceipt) -> Result<SwapResult, String> {
        // TODO: Swap via DEX
        Ok(SwapResult {
            received_asset: "USDT".into(),
            received_amount: receipt.seized_collateral * 99 / 100,
        })
    }
}
