use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    persistance::WalStore,
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    stages::{
        bridge::{BridgeStatus, BridgeToIcReceipt},
        executor::ExecutionStatus,
        finalize::LiquidationOutcome,
    },
};

// Simplified Hyperliquid Finalizer that aggregates multi-stage pipeline results
//
// This finalizer is the final stage in the Hyperliquid pipeline. It receives
// BridgeToIcReceipts after the full cross-chain journey and converts them into
// LiquidationOutcomes for export.
//
// The full pipeline flow is:
// 1. Executor -> ExecutionReceipt
// 2. BridgeToHyperliquidStage -> BridgeToHyperliquidReceipt (IC -> HL)
// 3. HyperliquidSwapStage -> SwapReceipt (Swap on HL Core)
// 4. BridgeToIcStage -> BridgeToIcReceipt (HL -> IC)
// 5. HyperliquidFinalizer -> LiquidationOutcome (this stage)
pub struct HyperliquidFinalizerV2<D, C, P>
where
    D: WalStore,
    C: ConfigTrait,
    P: PipelineAgent,
{
    // Write-Ahead Log database for retry persistence
    pub db: Arc<D>,
    // Configuration
    pub config: Arc<C>,
    // Pipeline agent for IC canister calls
    pub agent: Arc<P>,
}

impl<D, C, P> HyperliquidFinalizerV2<D, C, P>
where
    D: WalStore,
    C: ConfigTrait,
    P: PipelineAgent,
{
    pub fn new(db: Arc<D>, config: Arc<C>, agent: Arc<P>) -> Self {
        Self { db, config, agent }
    }

    // Convert a BridgeToIcReceipt to a LiquidationOutcome
    fn convert_to_outcome(&self, ic_receipt: &BridgeToIcReceipt) -> LiquidationOutcome {
        // Determine execution status based on bridge status
        let execution_status = match &ic_receipt.status {
            BridgeStatus::Success => ExecutionStatus::Success,
            BridgeStatus::PartialSuccess { stage, warning } => {
                // Partial success is treated as success with warning logged
                log::warn!("Partial success at {}: {}", stage, warning);
                ExecutionStatus::Success
            }
            BridgeStatus::Failed { stage, error } => {
                ExecutionStatus::SwapFailed(format!("Bridge failed at {}: {}", stage, error))
            }
        };

        // Extract original request and liquidation result from the nested receipts
        let original_request = ic_receipt
            .swap_receipt
            .bridge_receipt
            .original_request
            .clone();
        let liquidation_result = ic_receipt
            .swap_receipt
            .bridge_receipt
            .liquidation_result
            .clone();

        // Create outcome
        LiquidationOutcome {
            request: original_request,
            liquidation_result,
            swap_result: None, // TODO: Convert MultiHopSwapResult to SwapReply if needed
            status: execution_status,
            expected_profit: 0i128, // TODO: Calculate profit
            realized_profit: ic_receipt.realized_profit,
        }
    }

    // Handle failed cross-chain transaction recovery
    // This would transfer stuck funds to the recovery account
    async fn handle_failed_transaction(
        &self,
        ic_receipt: &BridgeToIcReceipt,
    ) -> Result<(), String> {
        log::error!(
            "Handling failed cross-chain transaction for liquidation {}",
            ic_receipt.swap_receipt.bridge_receipt.original_request.liquidation.borrower
        );

        // TODO: Implement recovery logic
        // This would:
        // 1. Check where funds are stuck (IC, HL EVM, or HL Core)
        // 2. Attempt to transfer to recovery account
        // 3. Record in WAL for manual intervention if needed

        Ok(())
    }
}

#[async_trait]
impl<'a, D, C, P> PipelineStage<'a, Vec<BridgeToIcReceipt>, Vec<LiquidationOutcome>>
    for HyperliquidFinalizerV2<D, C, P>
where
    D: WalStore,
    C: ConfigTrait,
    P: PipelineAgent,
{
    async fn process(
        &self,
        ic_receipts: &'a Vec<BridgeToIcReceipt>,
    ) -> Result<Vec<LiquidationOutcome>, String> {
        log::info!("HyperliquidFinalizerV2 processing {} bridge receipts", ic_receipts.len());

        let mut outcomes = Vec::with_capacity(ic_receipts.len());

        for (i, ic_receipt) in ic_receipts.iter().enumerate() {
            log::info!("[{}/{}] Converting bridge receipt to liquidation outcome", i + 1, ic_receipts.len());

            // Convert to outcome
            let outcome = self.convert_to_outcome(ic_receipt);

            // Handle failures
            if matches!(&outcome.status, ExecutionStatus::SwapFailed(_)) {
                log::error!(
                    "[{}/{}] Cross-chain transaction failed: {:?}",
                    i + 1,
                    ic_receipts.len(),
                    outcome.status
                );

                // Attempt recovery
                if let Err(e) = self.handle_failed_transaction(ic_receipt).await {
                    log::error!("[{}/{}] Recovery failed: {}", i + 1, ic_receipts.len(), e);
                }
            } else {
                log::info!(
                    "[{}/{}] Success: realized profit = {}",
                    i + 1,
                    ic_receipts.len(),
                    outcome.realized_profit
                );
            }

            outcomes.push(outcome);
        }

        log::info!(
            "HyperliquidFinalizerV2 completed: {} successful, {} failed",
            outcomes
                .iter()
                .filter(|o| matches!(o.status, ExecutionStatus::Success))
                .count(),
            outcomes
                .iter()
                .filter(|o| !matches!(o.status, ExecutionStatus::Success))
                .count()
        );

        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_finalizer_creation() {
        // TODO: Add tests when implementation is complete
    }
}
