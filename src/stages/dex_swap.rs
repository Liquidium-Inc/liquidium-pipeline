use crate::stage::PipelineStage;
use crate::types::*;
use async_trait::async_trait;

pub struct DexSwapExecutor;

#[async_trait]
impl PipelineStage<ExecutionReceipt, SwapResult> for DexSwapExecutor {
    async fn process(&self, receipt: ExecutionReceipt) -> Result<SwapResult, String> {
        // TODO: Swap via DEX
        Ok(SwapResult {
            received_asset: "USDT".into(),
            received_amount: receipt.seized_collateral * 99 / 100,
        })
    }
}
