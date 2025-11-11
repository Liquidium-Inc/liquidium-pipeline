use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use evm_bridge_client::{EvmClient, IcpHypeBridge};
use std::sync::Arc;

use super::types::{BridgeError, BurnReceipt, BurnRequest, MintReceipt, MintRequest};
use crate::config::ConfigTrait;
use crate::pipeline_agent::PipelineAgent;

// Trait for bridging tokens between Internet Computer and Hyperliquid EVM
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmBridge: Send + Sync {
    // Burn ckTokens on IC and unwrap to native tokens on Hyperliquid EVM
    //
    // This performs a two-step process:
    // 1. Burns the ckTokens on IC ledger
    // 2. Waits for corresponding tokens to appear on Hyperliquid EVM
    async fn burn_and_unwrap(&self, request: BurnRequest) -> Result<BurnReceipt, BridgeError>;

    // Wrap native tokens on Hyperliquid EVM and mint ckTokens on IC
    //
    // This performs a two-step process:
    // 1. Calls the wrap contract on Hyperliquid EVM
    // 2. Waits for ckTokens to be minted on IC
    async fn wrap_and_mint(&self, request: MintRequest) -> Result<MintReceipt, BridgeError>;

    // Check if a burn transaction has completed
    async fn check_burn_status(&self, ic_block_index: u64) -> Result<Option<B256>, BridgeError>;

    // Check if a mint transaction has completed
    async fn check_mint_status(&self, evm_tx_hash: B256) -> Result<Option<u64>, BridgeError>;
}

// Implementation of EVM bridge for Hyperliquid
pub struct HyperliquidToIcpBridge<A: PipelineAgent, C: ConfigTrait> {
    // IC agent for calling IC canisters
    pub ic_agent: Arc<A>,
    // Configuration
    pub config: Arc<C>,
    // Optional EVM client for advanced operations
    pub bridge: IcpHypeBridge,
}

impl<A: PipelineAgent, C: ConfigTrait> HyperliquidToIcpBridge<A, C> {
    // Create a new Hyperliquid EVM bridge
    pub fn new(ic_agent: Arc<A>, evm_client: Arc<EvmClient>, config: Arc<C>) -> Result<Self, BridgeError> {
        let bridge_addr: Address = config
            .get_hyperliquid_bridge_address()
            .expect("bridge address not found")
            .parse()
            .map_err(|_| BridgeError::ConfigError("invalid bridge address".to_string()))?;

        let bridge = IcpHypeBridge::new(bridge_addr, evm_client.clone());
        Ok(Self {
            ic_agent,
            bridge,
            config,
        })
    }

    // Get the bridge contract address for a given ckToken
    fn get_bridge_contract(&self, ledger_id: &candid::Principal) -> Result<Address, BridgeError> {
        // TODO: This should come from config
        // For now, return a placeholder error
        Err(BridgeError::ConfigError(format!(
            "Bridge contract not configured for ledger {}",
            ledger_id
        )))
    }

    // Get the EVM token address for a given IC ledger
    fn get_evm_token_address(&self, ledger_id: &candid::Principal) -> Result<Address, BridgeError> {
        // TODO: This should come from config
        Err(BridgeError::ConfigError(format!(
            "EVM token not configured for ledger {}",
            ledger_id
        )))
    }
}

#[async_trait]
impl<A: PipelineAgent, C: ConfigTrait> EvmBridge for HyperliquidToIcpBridge<A, C> {
    async fn burn_and_unwrap(&self, request: BurnRequest) -> Result<BurnReceipt, BridgeError> {
        log::info!(
            "Burning {} {} and unwrapping to {}",
            request.amount.formatted(),
            request.ck_token.symbol,
            request.destination_address
        );

        // Step 1: Burn ckTokens on IC
        // This would call the ckToken minter canister to burn tokens
        // The minter then triggers a transfer on the EVM side

        // TODO: Implement actual burn logic
        // For now, return an error indicating this is not yet implemented
        Err(BridgeError::BurnFailed(
            "Burn and unwrap not yet implemented".to_string(),
        ))

        // Expected flow:
        // 1. Call ckToken minter canister with burn request
        // 2. Wait for burn transaction to be confirmed on IC
        // 3. Monitor Hyperliquid EVM for corresponding tokens
        // 4. Return receipt with both IC block index and EVM tx hash
    }

    async fn wrap_and_mint(&self, request: MintRequest) -> Result<MintReceipt, BridgeError> {
        log::info!(
            "Wrapping {} of token {:?} and minting {}",
            request.amount,
            request.native_token_address,
            request.ck_token.symbol
        );

        // Step 1: Wrap tokens on Hyperliquid EVM
        // This calls the bridge contract to lock tokens and trigger minting on IC

        // TODO: Implement actual wrap logic
        // For now, return an error indicating this is not yet implemented
        Err(BridgeError::WrapFailed("Wrap and mint not yet implemented".to_string()))

        // Expected flow:
        // 1. Approve EVM token for bridge contract
        // 2. Call bridge contract to wrap tokens
        // 3. Wait for EVM transaction confirmation
        // 4. Monitor IC ledger for minted ckTokens
        // 5. Return receipt with both EVM tx hash and IC block index
    }

    async fn check_burn_status(&self, _ic_block_index: u64) -> Result<Option<B256>, BridgeError> {
        // Check if the burn on IC has resulted in unwrapped tokens on EVM
        // Returns Some(tx_hash) if found, None if still pending

        // TODO: Implement status checking
        Err(BridgeError::NetworkError(
            "Status checking not yet implemented".to_string(),
        ))
    }

    async fn check_mint_status(&self, _evm_tx_hash: B256) -> Result<Option<u64>, BridgeError> {
        // Check if the wrap on EVM has resulted in minted ckTokens on IC
        // Returns Some(block_index) if found, None if still pending

        // TODO: Implement status checking
        Err(BridgeError::NetworkError(
            "Status checking not yet implemented".to_string(),
        ))
    }
}
