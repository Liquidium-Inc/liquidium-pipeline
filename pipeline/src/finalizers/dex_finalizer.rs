use async_trait::async_trait;
use log::debug;

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    persistance::WalStore,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapExecution, SwapRequest},
};

// Tunables
const BASE_SLIPPAGE_BPS: u32 = 125; // 1.25%
const STEP_SLIPPAGE_BPS: u32 = 50; // +0.5% per retry
const MAX_SLIPPAGE_BPS: u32 = 500; // 5.0% cap
const MAX_SLIPPAGE_RETRIES: u32 = 3; // total attempts = MAX_SLIPPAGE_RETRIES + 1

fn slippage_for_retry(retry: u32, explicit: Option<u32>) -> u32 {
    let base = explicit.unwrap_or(BASE_SLIPPAGE_BPS);
    let bump = STEP_SLIPPAGE_BPS.saturating_mul(retry);
    base.saturating_add(bump).min(MAX_SLIPPAGE_BPS)
}

#[async_trait]
pub trait DexFinalizerLogic: Send + Sync {
    async fn swap(&self, req: &SwapRequest) -> Result<SwapExecution, String>;

    async fn swap_with_slippage_retry(&self, swap_req: SwapRequest) -> Result<SwapExecution, String> {
        let mut last_err: Option<String> = None;

        for retry in 0..=MAX_SLIPPAGE_RETRIES {
            let mut req = swap_req.clone();

            let eff_slippage = slippage_for_retry(retry, req.max_slippage_bps);
            req.max_slippage_bps = Some(eff_slippage);

            debug!("[Slippage] {}", eff_slippage);

            match self.swap(&req).await {
                Ok(exec) => return Ok(exec),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "swap failed with no error".to_string()))
    }
}

#[async_trait]
impl Finalizer for dyn DexFinalizerLogic {
    async fn finalize(&self, _: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Only finalize successful executions
        if !matches!(receipt.status, ExecutionStatus::Success) {
            return Ok(FinalizerResult::noop());
        }

        // If no swap is needed, noop
        let Some(swap_req) = &receipt.request.swap_args else {
            return Ok(FinalizerResult::noop());
        };

        let swap_exec = self.swap_with_slippage_retry(swap_req.clone()).await?;

        let finlizer_result = FinalizerResult {
            swap_result: Some(swap_exec),
            finalized: true,
            swapper: None,
        };

        Ok(finlizer_result)
    }
}
