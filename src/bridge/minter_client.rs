use candid::Principal;
use ic_agent::Agent;

use super::types::{BridgeError, MinterInfo, MinterInfoResponse};

// Fetch minter info from the Hyperliquid minter canister
//
// This queries the `get_minter_info` method which returns information about:
// - Supported ckERC20 tokens and their ERC20 contract addresses
// - Bridge contract address (smart_contract_address)
// - Minter address and helper contracts
// - Current balances and gas estimates
//
// # Arguments
// * `agent` - IC agent for making canister calls
// * `minter_canister` - Principal of the Hyperliquid minter canister
//
// # Returns
// Parsed MinterInfo with token address mappings
pub async fn fetch_minter_info(agent: &Agent, minter_canister: Principal) -> Result<MinterInfo, BridgeError> {
    log::info!(
        "Fetching minter info from canister: {}",
        minter_canister.to_text()
    );

    // Query the minter canister
    let response = agent
        .query(&minter_canister, "get_minter_info")
        .with_arg(candid::encode_args(()).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to encode get_minter_info args: {}", e))
        })?)
        .call()
        .await
        .map_err(|e| BridgeError::NetworkError(format!("Failed to call get_minter_info: {}", e)))?;

    // Decode the response
    let (minter_response,): (MinterInfoResponse,) = candid::decode_args(&response).map_err(|e| {
        BridgeError::NetworkError(format!("Failed to decode get_minter_info response: {}", e))
    })?;

    // Log discovered tokens
    if let Some(tokens) = &minter_response.supported_ckerc20_tokens {
        log::info!("Discovered {} supported ckERC20 tokens:", tokens.len());
        for token in tokens {
            log::info!(
                "  - {} ({}): {}",
                token.ckerc20_token_symbol,
                token.ledger_canister_id,
                token.erc20_contract_address
            );
        }
    }

    // Log bridge contract address
    if let Some(bridge_addr) = &minter_response.smart_contract_address {
        log::info!("Bridge contract address: {}", bridge_addr);
    }

    // Parse into MinterInfo
    let minter_info = MinterInfo::from_response(minter_response)?;

    log::info!(
        "Successfully parsed minter info with {} token mappings",
        minter_info.token_addresses.len()
    );

    Ok(minter_info)
}