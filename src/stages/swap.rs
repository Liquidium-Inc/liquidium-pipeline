use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    stage::PipelineStage,
    stages::bridge::{BridgeToHyperliquidReceipt, SwapReceipt, SwapStatus},
    swappers::hyperliquid_swapper::HyperliquidSwapInterface,
    swappers::hyperliquid_types::{MultiHopSwapArgs, SwapLeg},
};

// Pipeline stage for executing swaps on Hyperliquid Core
pub struct HyperliquidSwapStage<S, C>
where
    S: HyperliquidSwapInterface,
    C: ConfigTrait,
{
    // Hyperliquid swapper for multi-hop swaps
    pub swapper: Arc<S>,
    // Configuration
    pub config: Arc<C>,
}

impl<S, C> HyperliquidSwapStage<S, C>
where
    S: HyperliquidSwapInterface,
    C: ConfigTrait,
{
    pub fn new(swapper: Arc<S>, config: Arc<C>) -> Self {
        Self { swapper, config }
    }

    // Process a single bridge receipt through the swap flow
    async fn process_bridge_receipt(&self, bridge_receipt: &BridgeToHyperliquidReceipt) -> Result<SwapReceipt, String> {
        let tx_id = format!("swap_{}", chrono::Utc::now().timestamp());

        log::info!(
            "[{}] Executing multi-hop swap on Hyperliquid Core: BTC -> USDC -> USDT",
            tx_id
        );

        // Get quote for BTC -> USDC -> USDT
        let quote = self
            .swapper
            .get_multi_hop_quote(U256::from(bridge_receipt.core_amount), 100) // 1% slippage
            .await
            .map_err(|e| format!("[{}] Failed to get quote: {}", tx_id, e))?;

        log::info!(
            "[{}] Quote: {} BTC -> {} USDT (price impact: {}bps)",
            tx_id,
            bridge_receipt.core_amount,
            quote.final_amount,
            quote.total_price_impact_bps
        );

        // Build swap arguments
        let deadline = chrono::Utc::now().timestamp() as u64 + 300; // 5 minutes

        // Get token addresses from minter info
        let collateral_address = self
            .config
            .get_erc20_address_by_ledger(&bridge_receipt.original_request.collateral_asset.ledger)
            .map_err(|e| format!("[{}] {}", tx_id, e))?;

        // TODO: USDC intermediate token should be configurable or discovered dynamically
        // For now, we need to add a way to specify the intermediate token ledger ID
        // Placeholder - this will fail until HYPERLIQUID_USDC_LEDGER is added to config
        let usdc_ledger = std::env::var("HYPERLIQUID_USDC_LEDGER")
            .ok()
            .and_then(|s| candid::Principal::from_text(s).ok())
            .ok_or(format!("[{}] HYPERLIQUID_USDC_LEDGER not configured", tx_id))?;

        let usdc_address = self
            .config
            .get_erc20_address_by_ledger(&usdc_ledger)
            .map_err(|e| format!("[{}] {}", tx_id, e))?;

        let first_leg = SwapLeg {
            token_in: collateral_address,
            token_out: usdc_address,
            amount_in: quote.first_leg.amount_in,
            min_amount_out: quote.first_leg.amount_out * U256::from(99) / U256::from(100), // 1% slippage
            pool_id: None,
        };

        let debt_address = self
            .config
            .get_erc20_address_by_ledger(&bridge_receipt.original_request.debt_asset.ledger)
            .map_err(|e| format!("[{}] {}", tx_id, e))?;

        let second_leg = SwapLeg {
            token_in: usdc_address,
            token_out: debt_address,
            amount_in: quote.second_leg.amount_in,
            min_amount_out: quote.second_leg.amount_out * U256::from(99) / U256::from(100), // 1% slippage
            pool_id: None,
        };

        let swap_args = MultiHopSwapArgs {
            first_leg,
            second_leg,
            max_slippage_bps: 100, // 1%
            deadline,
        };

        // Execute the multi-hop swap
        let swap_result = self
            .swapper
            .execute_multi_hop_swap(swap_args)
            .await
            .map_err(|e| format!("[{}] Swap execution failed: {}", tx_id, e))?;

        let final_amount = swap_result.final_amount;

        log::info!(
            "[{}] Swap completed successfully: {} BTC -> {} USDT (rate: {:.6})",
            tx_id,
            bridge_receipt.core_amount,
            final_amount,
            swap_result.overall_rate
        );

        Ok(SwapReceipt {
            bridge_receipt: bridge_receipt.clone(),
            swap_result,
            final_token_address: format!("{:?}", debt_address),
            final_amount,
            status: SwapStatus::Success,
        })
    }
}

#[async_trait]
impl<'a, S, C> PipelineStage<'a, Vec<BridgeToHyperliquidReceipt>, Vec<SwapReceipt>> for HyperliquidSwapStage<S, C>
where
    S: HyperliquidSwapInterface,
    C: ConfigTrait,
{
    async fn process(&self, bridge_receipts: &'a Vec<BridgeToHyperliquidReceipt>) -> Result<Vec<SwapReceipt>, String> {
        log::info!(
            "HyperliquidSwapStage processing {} bridge receipts",
            bridge_receipts.len()
        );

        let mut swap_receipts = Vec::with_capacity(bridge_receipts.len());

        for (i, bridge_receipt) in bridge_receipts.iter().enumerate() {
            log::info!(
                "[{}/{}] Executing swap on Hyperliquid Core",
                i + 1,
                bridge_receipts.len()
            );

            // Skip failed bridge receipts
            if let crate::stages::bridge::BridgeStatus::Failed { stage, error } = &bridge_receipt.status {
                log::warn!(
                    "[{}/{}] Skipping swap for failed bridge (stage: {}, error: {})",
                    i + 1,
                    bridge_receipts.len(),
                    stage,
                    error
                );
                // Create a failed swap receipt
                swap_receipts.push(SwapReceipt {
                    bridge_receipt: bridge_receipt.clone(),
                    swap_result: crate::swappers::hyperliquid_types::MultiHopSwapResult {
                        first_leg: crate::swappers::hyperliquid_types::SwapLegResult {
                            tx_hash: B256::ZERO,
                            token_in: Address::ZERO,
                            amount_in: U256::ZERO,
                            token_out: Address::ZERO,
                            amount_out: U256::ZERO,
                            price: 0.0,
                            gas_used: U256::ZERO,
                            block_number: 0,
                            timestamp: 0,
                        },
                        second_leg: crate::swappers::hyperliquid_types::SwapLegResult {
                            tx_hash: B256::ZERO,
                            token_in: Address::ZERO,
                            amount_in: U256::ZERO,
                            token_out: Address::ZERO,
                            amount_out: U256::ZERO,
                            price: 0.0,
                            gas_used: U256::ZERO,
                            block_number: 0,
                            timestamp: 0,
                        },
                        total_gas_used: U256::ZERO,
                        initial_amount: 0u128,
                        final_amount: 0u128,
                        overall_rate: 0.0,
                    },
                    final_token_address: String::new(),
                    final_amount: 0,
                    status: SwapStatus::Failed {
                        error: format!("Bridge failed at stage {}: {}", stage, error),
                    },
                });
                continue;
            }

            match self.process_bridge_receipt(bridge_receipt).await {
                Ok(swap_receipt) => {
                    log::info!(
                        "[{}/{}] Swap successful: {} USDT received",
                        i + 1,
                        bridge_receipts.len(),
                        swap_receipt.final_amount
                    );
                    swap_receipts.push(swap_receipt);
                }
                Err(e) => {
                    log::error!("[{}/{}] Swap failed: {}", i + 1, bridge_receipts.len(), e);
                    // Create failed swap receipt
                    swap_receipts.push(SwapReceipt {
                        bridge_receipt: bridge_receipt.clone(),
                        swap_result: crate::swappers::hyperliquid_types::MultiHopSwapResult {
                            first_leg: crate::swappers::hyperliquid_types::SwapLegResult {
                                tx_hash: B256::ZERO,
                                token_in: Address::ZERO,
                                amount_in: U256::ZERO,
                                token_out: Address::ZERO,
                                amount_out: U256::ZERO,
                                price: 0.0,
                                gas_used: U256::ZERO,
                                block_number: 0,
                                timestamp: 0,
                            },
                            second_leg: crate::swappers::hyperliquid_types::SwapLegResult {
                                tx_hash: B256::ZERO,
                                token_in: Address::ZERO,
                                amount_in: U256::ZERO,
                                token_out: Address::ZERO,
                                amount_out: U256::ZERO,
                                price: 0.0,
                                gas_used: U256::ZERO,
                                block_number: 0,
                                timestamp: 0,
                            },
                            total_gas_used: U256::ZERO,
                            initial_amount: 0u128,
                            final_amount:   0u128,
                            overall_rate: 0.0,
                        },
                        final_token_address: String::new(),
                        final_amount: 0,
                        status: SwapStatus::Failed { error: e },
                    });
                }
            }
        }

        log::info!(
            "HyperliquidSwapStage completed: {} successful, {} failed",
            swap_receipts
                .iter()
                .filter(|r| matches!(r.status, SwapStatus::Success))
                .count(),
            swap_receipts
                .iter()
                .filter(|r| matches!(r.status, SwapStatus::Failed { .. }))
                .count()
        );

        Ok(swap_receipts)
    }
}
