use crate::{
    bridge::{
        evm_to_icp_bridge::EvmBridge,
        evm_core_bridge::EvmCoreBridge,
        types::{BurnRequest, MintRequest},
    },
    config::ConfigTrait,
    executors::executor::ExecutorRequest,
    icrc_token::icrc_token_amount::IcrcTokenAmount,
    stage::PipelineStage,
    stages::executor::ExecutionReceipt,
    swappers::hyperliquid_types::MultiHopSwapResult,
    types::protocol_types::LiquidationResult,
};
use alloy::primitives::B256;
use async_trait::async_trait;
use num_traits::ToPrimitive;
use std::sync::Arc;

// Status of bridge operations
#[derive(Debug, Clone)]
pub enum BridgeStatus {
    Success,
    PartialSuccess { stage: String, warning: String },
    Failed { stage: String, error: String },
}

// Status of swap operations
#[derive(Debug, Clone)]
pub enum SwapStatus {
    Success,
    PartialSuccess { warning: String },
    Failed { error: String },
}

// Receipt after bridging from IC to Hyperliquid
#[derive(Debug, Clone)]
pub struct BridgeToHyperliquidReceipt {
    // Original execution request
    pub original_request: ExecutorRequest,
    // Liquidation result from executor
    pub liquidation_result: LiquidationResult,

    // Step 1: IC -> Hyperliquid EVM
    // Block index where ckBTC was burned on IC
    pub ic_burn_block: u64,
    // Transaction hash where BTC was received on Hyperliquid EVM
    pub evm_receive_tx: B256,
    // Amount received on EVM (after bridge fees)
    pub evm_amount: u128,

    // Step 2: Hyperliquid EVM -> Core
    // Transaction hash for EVM -> Core deposit
    pub evm_to_core_tx: B256,
    // Transaction ID on Hyperliquid Core
    pub core_receive_tx: String,
    // Amount received on Core (after transfer fees)
    pub core_amount: u128,

    // Token address on Hyperliquid Core ready for swapping
    pub core_token_address: String,

    // Overall bridge status
    pub status: BridgeStatus,
}

// Receipt after swapping on Hyperliquid Core
#[derive(Debug, Clone)]
pub struct SwapReceipt {
    // The bridge receipt that led to this swap
    pub bridge_receipt: BridgeToHyperliquidReceipt,

    // Multi-hop swap results (BTC -> USDC -> USDT)
    pub swap_result: MultiHopSwapResult,
    // Final token address on Core after swaps
    pub final_token_address: String,
    // Final amount on Core after swaps
    pub final_amount: u128,

    // Swap status
    pub status: SwapStatus,
}

// Receipt after bridging back from Hyperliquid to IC
#[derive(Debug, Clone)]
pub struct BridgeToIcReceipt {
    // The swap receipt that led to this return bridge
    pub swap_receipt: SwapReceipt,

    // Step 1: Hyperliquid Core -> EVM
    // Transaction ID for Core -> EVM withdrawal
    pub core_to_evm_tx: String,
    // Transaction hash where tokens were received on EVM
    pub evm_receive_tx: B256,
    // Amount received on EVM
    pub evm_amount: u128,

    // Step 2: Hyperliquid EVM -> IC
    // Transaction hash for wrapping on EVM
    pub evm_wrap_tx: B256,
    // Block index where ckUSDT was minted on IC
    pub ic_mint_block: u64,
    // Amount minted on IC (ckUSDT)
    pub ic_amount: u128,

    // Overall bridge status
    pub status: BridgeStatus,
    // Realized profit after all operations
    pub realized_profit: i128,
}

// Pipeline stage for bridging from IC to Hyperliquid
pub struct BridgeToHyperliquidStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    // EVM bridge for IC ↔ Hyperliquid EVM transfers
    pub evm_bridge: Arc<B>,
    // EVM-Core bridge for Hyperliquid EVM ↔ Core transfers
    pub evm_core_bridge: Arc<EC>,
    // Configuration
    pub config: Arc<Cfg>,
}

impl<B, EC, Cfg> BridgeToHyperliquidStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    pub fn new(evm_bridge: Arc<B>, evm_core_bridge: Arc<EC>, config: Arc<Cfg>) -> Self {
        Self {
            evm_bridge,
            evm_core_bridge,
            config,
        }
    }

    // Process a single execution receipt through IC -> Hyperliquid bridge
    async fn process_receipt(&self, receipt: &ExecutionReceipt) -> Result<BridgeToHyperliquidReceipt, String> {
        let tx_id = format!("bridge_to_hl_{}", chrono::Utc::now().timestamp());

        // Check if liquidation was successful
        let liquidation_result = receipt.liquidation_result.clone().ok_or("No liquidation result")?;

        log::info!(
            "[{}] Step 1: Burning liqBTC on IC and unwrapping to Hyperliquid EVM",
            tx_id
        );

        let burn_request = BurnRequest {
            ck_token: receipt.request.collateral_asset.clone(),
            amount: IcrcTokenAmount {
                token: receipt.request.collateral_asset.clone(),
                value: liquidation_result.amounts.collateral_received.clone(),
            },
            destination_address: self.evm_core_bridge.get_wallet_address(),
        };

        let burn_receipt = self
            .evm_bridge
            .burn_and_unwrap(burn_request)
            .await
            .map_err(|e| format!("[{}] Burn and unwrap failed: {}", tx_id, e))?;

        log::info!(
            "[{}] Burned {} ckBTC on IC (block {}), received {} BTC on EVM (tx {})",
            tx_id,
            burn_receipt.request.amount.formatted(),
            burn_receipt.ic_block_index,
            burn_receipt.received_amount,
            burn_receipt.evm_tx_hash
        );

        // Step 2: Transfer BTC from Hyperliquid EVM -> Core
        log::info!("[{}] Step 2: Transferring BTC from EVM to Core", tx_id);

        // Get ERC20 address for collateral asset from minter info
        let collateral_erc20_address = self
            .config
            .get_erc20_address_by_ledger(&receipt.request.collateral_asset.ledger)
            .map_err(|e| format!("[{}] {}", tx_id, e))?;

        let core_deposit = self
            .evm_core_bridge
            .deposit_evm_to_core(collateral_erc20_address, burn_receipt.received_amount)
            .await
            .map_err(|e| format!("[{}] EVM to Core transfer failed: {}", tx_id, e))?;

        log::info!(
            "[{}] Transferred {} BTC to Core (core tx: {})",
            tx_id,
            core_deposit.core_amount,
            core_deposit.core_tx_id
        );

        Ok(BridgeToHyperliquidReceipt {
            original_request: receipt.request.clone(),
            liquidation_result,
            ic_burn_block: burn_receipt.ic_block_index,
            evm_receive_tx: burn_receipt.evm_tx_hash,
            evm_amount: burn_receipt.received_amount,
            evm_to_core_tx: core_deposit.evm_tx_hash,
            core_receive_tx: core_deposit.core_tx_id,
            core_amount: core_deposit.core_amount.to::<u128>(),
            core_token_address: format!("{:?}", collateral_erc20_address),
            status: BridgeStatus::Success,
        })
    }
}

#[async_trait]
impl<'a, B, EC, Cfg> PipelineStage<'a, Vec<ExecutionReceipt>, Vec<BridgeToHyperliquidReceipt>>
    for BridgeToHyperliquidStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    async fn process(&self, receipts: &'a Vec<ExecutionReceipt>) -> Result<Vec<BridgeToHyperliquidReceipt>, String> {
        log::info!("BridgeToHyperliquidStage processing {} receipts", receipts.len());

        let mut bridge_receipts = Vec::with_capacity(receipts.len());

        for (i, receipt) in receipts.iter().enumerate() {
            log::info!("[{}/{}] Bridging to Hyperliquid", i + 1, receipts.len());

            match self.process_receipt(receipt).await {
                Ok(bridge_receipt) => {
                    log::info!("[{}/{}] Successfully bridged to Hyperliquid", i + 1, receipts.len());
                    bridge_receipts.push(bridge_receipt);
                }
                Err(e) => {
                    log::error!("[{}/{}] Bridge failed: {}", i + 1, receipts.len(), e);
                    // Create failed receipt
                    if let Some(liquidation_result) = &receipt.liquidation_result {
                        bridge_receipts.push(BridgeToHyperliquidReceipt {
                            original_request: receipt.request.clone(),
                            liquidation_result: liquidation_result.clone(),
                            ic_burn_block: 0,
                            evm_receive_tx: B256::ZERO,
                            evm_amount: 0,
                            evm_to_core_tx: B256::ZERO,
                            core_receive_tx: String::new(),
                            core_amount: 0,
                            core_token_address: String::new(),
                            status: BridgeStatus::Failed {
                                stage: "IC to Hyperliquid".to_string(),
                                error: e,
                            },
                        });
                    }
                }
            }
        }

        log::info!("BridgeToHyperliquidStage completed: {} receipts", bridge_receipts.len());
        Ok(bridge_receipts)
    }
}

// Pipeline stage for bridging from Hyperliquid back to IC
pub struct BridgeToIcStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    // EVM bridge for Hyperliquid EVM ↔ IC transfers
    pub evm_bridge: Arc<B>,
    // EVM-Core bridge for Hyperliquid EVM ↔ Core transfers
    pub evm_core_bridge: Arc<EC>,
    // Configuration
    pub config: Arc<Cfg>,
}

impl<B, EC, Cfg> BridgeToIcStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    pub fn new(evm_bridge: Arc<B>, evm_core_bridge: Arc<EC>, config: Arc<Cfg>) -> Self {
        Self {
            evm_bridge,
            evm_core_bridge,
            config,
        }
    }

    // Process a single swap receipt through Hyperliquid -> IC bridge
    async fn process_swap_receipt(&self, swap_receipt: &SwapReceipt) -> Result<BridgeToIcReceipt, String> {
        let tx_id = format!("bridge_to_ic_{}", chrono::Utc::now().timestamp());

        log::info!("[{}] Step 1: Transferring USDT from Core to EVM", tx_id);

        // Step 1: Transfer USDT from Hyperliquid Core -> EVM
        let core_withdraw = self
            .evm_core_bridge
            .withdraw_core_to_evm(
                swap_receipt.final_token_address.clone(),
                swap_receipt.final_amount,
            )
            .await
            .map_err(|e| format!("[{}] Core to EVM transfer failed: {}", tx_id, e))?;

        log::info!(
            "[{}] Transferred {} USDT to EVM (evm tx: {})",
            tx_id,
            core_withdraw.evm_amount,
            core_withdraw.evm_tx_hash
        );

        // Step 2: Wrap USDT on EVM and mint ckUSDT on IC
        log::info!("[{}] Step 2: Wrapping USDT on EVM and minting ckUSDT on IC", tx_id);

        // Get ERC20 address for debt asset (USDT) from minter info
        let debt_erc20_address = self
            .config
            .get_erc20_address_by_ledger(&swap_receipt.bridge_receipt.original_request.debt_asset.ledger)
            .map_err(|e| format!("[{}] {}", tx_id, e))?;

        let mint_request = MintRequest {
            native_token_address: debt_erc20_address,
            amount: core_withdraw.evm_amount.to::<u128>(),
            ck_token: swap_receipt.bridge_receipt.original_request.debt_asset.clone(),
            destination_principal: self.config.get_trader_principal(),
        };

        let mint_receipt = self
            .evm_bridge
            .wrap_and_mint(mint_request)
            .await
            .map_err(|e| format!("[{}] Wrap and mint failed: {}", tx_id, e))?;

        log::info!(
            "[{}] Minted {} ckUSDT on IC (block {})",
            tx_id,
            mint_receipt.minted_amount.formatted(),
            mint_receipt.ic_block_index
        );

        // Calculate realized profit
        let ic_amount = mint_receipt.minted_amount.value.0.to_u128().unwrap_or(0);
        let debt_repaid = swap_receipt
            .bridge_receipt
            .liquidation_result
            .amounts
            .debt_repaid
            .0
            .to_u128()
            .unwrap_or(0);
        let realized_profit = ic_amount as i128 - debt_repaid as i128;

        Ok(BridgeToIcReceipt {
            swap_receipt: swap_receipt.clone(),
            core_to_evm_tx: core_withdraw.core_tx_id,
            evm_receive_tx: core_withdraw.evm_tx_hash,
            evm_amount: core_withdraw.evm_amount.to::<u128>(),
            evm_wrap_tx: mint_receipt.evm_tx_hash,
            ic_mint_block: mint_receipt.ic_block_index,
            ic_amount,
            status: BridgeStatus::Success,
            realized_profit,
        })
    }
}

#[async_trait]
impl<'a, B, EC, Cfg> PipelineStage<'a, Vec<SwapReceipt>, Vec<BridgeToIcReceipt>> for BridgeToIcStage<B, EC, Cfg>
where
    B: EvmBridge,
    EC: EvmCoreBridge,
    Cfg: ConfigTrait,
{
    async fn process(&self, swap_receipts: &'a Vec<SwapReceipt>) -> Result<Vec<BridgeToIcReceipt>, String> {
        log::info!("BridgeToIcStage processing {} swap receipts", swap_receipts.len());

        let mut ic_receipts = Vec::with_capacity(swap_receipts.len());

        for (i, swap_receipt) in swap_receipts.iter().enumerate() {
            log::info!("[{}/{}] Bridging back to IC", i + 1, swap_receipts.len());

            match self.process_swap_receipt(swap_receipt).await {
                Ok(ic_receipt) => {
                    log::info!(
                        "[{}/{}] Successfully bridged to IC: realized profit = {}",
                        i + 1,
                        swap_receipts.len(),
                        ic_receipt.realized_profit
                    );
                    ic_receipts.push(ic_receipt);
                }
                Err(e) => {
                    log::error!("[{}/{}] Bridge to IC failed: {}", i + 1, swap_receipts.len(), e);
                    // Create failed receipt
                    ic_receipts.push(BridgeToIcReceipt {
                        swap_receipt: swap_receipt.clone(),
                        core_to_evm_tx: String::new(),
                        evm_receive_tx: B256::ZERO,
                        evm_amount: 0,
                        evm_wrap_tx: B256::ZERO,
                        ic_mint_block: 0,
                        ic_amount: 0,
                        status: BridgeStatus::Failed {
                            stage: "Hyperliquid to IC".to_string(),
                            error: e,
                        },
                        realized_profit: 0,
                    });
                }
            }
        }

        log::info!("BridgeToIcStage completed: {} receipts", ic_receipts.len());
        Ok(ic_receipts)
    }
}
