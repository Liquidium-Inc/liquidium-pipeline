// Wrapper around evm_bridge_client for the liquidium pipeline

use evm_bridge_client::{EvmClient, EvmError, RpcConfig, TxPolicyConfig};
use log::info;
use std::sync::Arc;

/// Initialize an EVM client from configuration
pub async fn init_evm_client(rpc_url: String, chain_id: u64, private_key: String) -> Result<Arc<EvmClient>, String> {
    // Parse private key
    let private_key = private_key.trim_start_matches("0x");
    let signer: evm_bridge_client::PrivateKeySigner = private_key
        .parse()
        .map_err(|e| format!("Invalid EVM private key: {}", e))?;

    let from = signer.address();

    // Create RPC config
    let rpc = RpcConfig { rpc_url, chain_id };

    // Create transaction policy with sensible defaults
    let policy = TxPolicyConfig {
        max_fee_per_gas_wei: None,          // Fetch from network
        max_priority_fee_per_gas_wei: None, // Fetch from network
        gas_limit_multiplier_bps: 12000,    // 120%
        max_retries: 3,
        confirm_blocks: 2,         // Wait for 2 confirmations
        receipt_timeout_secs: 300, // 5 minutes
    };

    // Create EVM client
    let client = EvmClient::new(rpc, signer, from, policy)
        .await
        .map_err(|e| format!("Failed to create EVM client: {}", e))?;

    info!("EVM client initialized with address: {:?}", client.from);

    Ok(Arc::new(client))
}

/// Helper to convert EvmError to String
pub fn evm_error_to_string(err: EvmError) -> String {
    match err {
        EvmError::Rpc(msg) => format!("RPC error: {}", msg),
        EvmError::Signing(msg) => format!("Signing error: {}", msg),
        EvmError::Revert(msg) => format!("Transaction reverted: {}", msg),
        EvmError::Timeout => "Transaction timeout".to_string(),
        EvmError::Dropped(msg) => format!("Transaction dropped: {}", msg),
        EvmError::Reorg => "Chain reorganization detected".to_string(),
        EvmError::Other(msg) => format!("Error: {}", msg),
    }
}
