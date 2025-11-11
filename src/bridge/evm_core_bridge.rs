use async_trait::async_trait;
use ethers::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Receipt from depositing tokens from Hyperliquid EVM to Hyperliquid Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvmToCoreReceipt {
    // Token address on EVM
    pub evm_token: Address,
    // Amount deposited from EVM
    pub evm_amount: U256,
    // Transaction hash on EVM initiating the deposit
    pub evm_tx_hash: H256,
    // Transaction ID on Hyperliquid Core confirming receipt
    pub core_tx_id: String,
    // Amount received on Core (after fees)
    pub core_amount: U256,
    // Block number on EVM
    pub evm_block: u64,
    // Timestamp of Core confirmation
    pub core_timestamp: u64,
}

// Receipt from withdrawing tokens from Hyperliquid Core to Hyperliquid EVM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreToEvmReceipt {
    // Token identifier on Core
    pub core_token: String,
    // Amount withdrawn from Core
    pub core_amount: U256,
    // Transaction ID on Core initiating withdrawal
    pub core_tx_id: String,
    // Transaction hash on EVM confirming receipt
    pub evm_tx_hash: H256,
    // Amount received on EVM (after fees)
    pub evm_amount: U256,
    // Block number on EVM
    pub evm_block: u64,
    // Timestamp of EVM confirmation
    pub evm_timestamp: u64,
}

// Status of an EVM ↔ Core transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferStatus {
    // Transfer initiated, waiting for confirmation
    Pending,
    // Transfer confirmed on destination layer
    Confirmed,
    // Transfer failed
    Failed(String),
}

// Errors specific to EVM ↔ Core bridge operations
#[derive(Debug, Clone)]
pub enum EvmCoreBridgeError {
    // Failed to deposit from EVM to Core
    DepositFailed { evm_tx: H256, reason: String },
    // Failed to withdraw from Core to EVM
    WithdrawFailed { core_tx: String, reason: String },
    // Insufficient balance on source layer
    InsufficientBalance { required: U256, available: U256 },
    // Bridge contract call failed
    ContractCallFailed(String),
    // Core API call failed
    CoreApiFailed(String),
    // Transaction not found on Core
    CoreTxNotFound(String),
    // Transaction not found on EVM
    EvmTxNotFound(H256),
    // Timeout waiting for transfer
    TransferTimeout { waited: u64, timeout: u64 },
    // Network error
    NetworkError(String),
}

impl std::fmt::Display for EvmCoreBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvmCoreBridgeError::DepositFailed { evm_tx, reason } => {
                write!(f, "Deposit failed (EVM tx {:?}): {}", evm_tx, reason)
            }
            EvmCoreBridgeError::WithdrawFailed { core_tx, reason } => {
                write!(f, "Withdrawal failed (Core tx {}): {}", core_tx, reason)
            }
            EvmCoreBridgeError::InsufficientBalance { required, available } => {
                write!(f, "Insufficient balance: required {}, available {}", required, available)
            }
            EvmCoreBridgeError::ContractCallFailed(e) => write!(f, "Bridge contract call failed: {}", e),
            EvmCoreBridgeError::CoreApiFailed(e) => write!(f, "Core API call failed: {}", e),
            EvmCoreBridgeError::CoreTxNotFound(tx) => write!(f, "Core transaction not found: {}", tx),
            EvmCoreBridgeError::EvmTxNotFound(tx) => write!(f, "EVM transaction not found: {:?}", tx),
            EvmCoreBridgeError::TransferTimeout { waited, timeout } => {
                write!(f, "Transfer timed out after {}s (timeout: {}s)", waited, timeout)
            }
            EvmCoreBridgeError::NetworkError(e) => write!(f, "Network error: {}", e),
        }
    }
}

impl std::error::Error for EvmCoreBridgeError {}

// Trait for bridging assets between Hyperliquid EVM and Hyperliquid Core
//
// Hyperliquid has a two-layer architecture:
// - EVM Layer (L1): EVM-compatible blockchain where tokens can be held
// - Core Layer (L2): High-performance trading layer where the DEX operates
//
// Assets must be transferred between these layers for trading.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmCoreBridge: Send + Sync {
    // Deposit tokens from Hyperliquid EVM to Hyperliquid Core
    //
    // This makes tokens available for trading on the Hyperliquid DEX.
    //
    // # Process
    // 1. Approve EVM token for bridge contract
    // 2. Call bridge contract's deposit function on EVM
    // 3. Wait for EVM transaction confirmation
    // 4. Poll Hyperliquid Core API until deposit is credited
    // 5. Return receipt with both transaction identifiers
    //
    // # Arguments
    // * `evm_token` - Token contract address on EVM
    // * `amount` - Amount to deposit (in token's smallest unit)
    //
    // # Returns
    // Receipt with EVM tx hash and Core tx ID
    async fn deposit_evm_to_core(&self, evm_token: Address, amount: U256) -> Result<EvmToCoreReceipt, EvmCoreBridgeError>;

    // Withdraw tokens from Hyperliquid Core to Hyperliquid EVM
    //
    // This moves tokens from the trading layer back to the EVM layer,
    // where they can be bridged to other chains.
    //
    // # Process
    // 1. Sign withdrawal request with wallet
    // 2. Submit withdrawal to Hyperliquid Core API
    // 3. Wait for Core transaction confirmation
    // 4. Monitor EVM for bridge contract to release tokens
    // 5. Return receipt with both transaction identifiers
    //
    // # Arguments
    // * `core_token` - Token identifier on Core (e.g., "BTC", "USDT")
    // * `amount` - Amount to withdraw (in token's smallest unit)
    //
    // # Returns
    // Receipt with Core tx ID and EVM tx hash
    async fn withdraw_core_to_evm(&self, core_token: String, amount: U256) -> Result<CoreToEvmReceipt, EvmCoreBridgeError>;

    // Check the status of an EVM -> Core deposit
    //
    // # Arguments
    // * `evm_tx_hash` - Transaction hash of the deposit on EVM
    //
    // # Returns
    // - `Some(core_tx_id)` if deposit is confirmed on Core
    // - `None` if still pending
    async fn check_deposit_status(&self, evm_tx_hash: H256) -> Result<Option<String>, EvmCoreBridgeError>;

    // Check the status of a Core -> EVM withdrawal
    //
    // # Arguments
    // * `core_tx_id` - Transaction ID of the withdrawal on Core
    //
    // # Returns
    // - `Some(evm_tx_hash)` if withdrawal is confirmed on EVM
    // - `None` if still pending
    async fn check_withdrawal_status(&self, core_tx_id: String) -> Result<Option<H256>, EvmCoreBridgeError>;

    // Get balance of a token on Hyperliquid Core
    //
    // # Arguments
    // * `core_token` - Token identifier on Core
    //
    // # Returns
    // Current balance on Core
    async fn get_core_balance(&self, core_token: String) -> Result<U256, EvmCoreBridgeError>;

    // Get balance of a token on Hyperliquid EVM
    //
    // # Arguments
    // * `evm_token` - Token contract address on EVM
    // * `address` - Wallet address to check
    //
    // # Returns
    // Current balance on EVM
    async fn get_evm_balance(&self, evm_token: Address, address: Address) -> Result<U256, EvmCoreBridgeError>;
}

// Implementation of the EVM ↔ Core bridge for Hyperliquid
pub struct HyperliquidEvmCoreBridge {
    // EVM provider for Hyperliquid EVM layer
    evm_provider: Arc<Provider<Http>>,
    // Wallet for signing transactions on both layers
    wallet: LocalWallet,
    // Bridge contract address on EVM
    bridge_contract: Address,
    // Hyperliquid Core API URL
    core_api_url: String,
    // Chain ID
    chain_id: u64,
    // Timeout for waiting for transfers (seconds)
    transfer_timeout: u64,
}

impl HyperliquidEvmCoreBridge {
    // Create a new EVM ↔ Core bridge
    //
    // # Arguments
    // * `evm_rpc_url` - RPC URL for Hyperliquid EVM
    // * `core_api_url` - API URL for Hyperliquid Core
    // * `wallet_private_key` - Private key for signing transactions
    // * `bridge_contract` - Address of the bridge contract on EVM
    // * `chain_id` - Hyperliquid chain ID
    pub fn new(
        evm_rpc_url: String,
        core_api_url: String,
        wallet_private_key: String,
        bridge_contract: Address,
        chain_id: u64,
    ) -> Result<Self, EvmCoreBridgeError> {
        let evm_provider = Provider::<Http>::try_from(evm_rpc_url)
            .map_err(|e| EvmCoreBridgeError::NetworkError(format!("Failed to create EVM provider: {}", e)))?;

        let wallet: LocalWallet = wallet_private_key
            .parse()
            .map_err(|e| EvmCoreBridgeError::NetworkError(format!("Invalid private key: {}", e)))?;

        Ok(Self {
            evm_provider: Arc::new(evm_provider),
            wallet,
            bridge_contract,
            core_api_url,
            chain_id,
            transfer_timeout: 300, // 5 minutes default
        })
    }

    // Set the transfer timeout (in seconds)
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.transfer_timeout = timeout_secs;
        self
    }
}

#[async_trait]
impl EvmCoreBridge for HyperliquidEvmCoreBridge {
    async fn deposit_evm_to_core(&self, evm_token: Address, amount: U256) -> Result<EvmToCoreReceipt, EvmCoreBridgeError> {
        log::info!("Depositing {} of token {:?} from EVM to Core", amount, evm_token);

        // TODO: Implement actual deposit logic
        // This requires:
        // 1. ERC20 approve for bridge contract
        // 2. Call bridge contract's deposit() function
        // 3. Wait for EVM tx confirmation
        // 4. Poll Hyperliquid Core API for deposit credit
        // 5. Return receipt

        Err(EvmCoreBridgeError::ContractCallFailed(
            "EVM to Core deposit not yet implemented - requires Hyperliquid bridge contract integration".to_string(),
        ))

        // Expected implementation:
        // ```rust
        // // 1. Approve ERC20 token
        // let approve_tx = erc20_contract.approve(self.bridge_contract, amount).send().await?;
        // approve_tx.await?;
        //
        // // 2. Deposit to bridge
        // let deposit_tx = bridge_contract.deposit(evm_token, amount).send().await?;
        // let receipt = deposit_tx.await?;
        //
        // // 3. Wait for Core confirmation
        // let core_tx_id = self.wait_for_core_deposit(receipt.transaction_hash).await?;
        //
        // // 4. Get Core balance to confirm
        // let core_balance = self.get_core_balance(token_symbol).await?;
        //
        // Ok(EvmToCoreReceipt { ... })
        // ```
    }

    async fn withdraw_core_to_evm(&self, core_token: String, amount: U256) -> Result<CoreToEvmReceipt, EvmCoreBridgeError> {
        log::info!("Withdrawing {} of token {} from Core to EVM", amount, core_token);

        // TODO: Implement actual withdrawal logic
        // This requires:
        // 1. Sign withdrawal request
        // 2. Submit to Hyperliquid Core API
        // 3. Wait for Core tx confirmation
        // 4. Monitor EVM for bridge contract withdrawal tx
        // 5. Return receipt

        Err(EvmCoreBridgeError::CoreApiFailed(
            "Core to EVM withdrawal not yet implemented - requires Hyperliquid Core API integration".to_string(),
        ))

        // Expected implementation:
        // ```rust
        // // 1. Create withdrawal request
        // let withdrawal_request = create_withdrawal_request(&self.wallet, core_token, amount)?;
        //
        // // 2. Submit to Core API
        // let core_tx_id = submit_to_core_api(&self.core_api_url, withdrawal_request).await?;
        //
        // // 3. Wait for EVM withdrawal tx
        // let evm_tx_hash = self.wait_for_evm_withdrawal(core_tx_id).await?;
        //
        // // 4. Get EVM balance to confirm
        // let evm_balance = self.get_evm_balance(evm_token, self.wallet.address()).await?;
        //
        // Ok(CoreToEvmReceipt { ... })
        // ```
    }

    async fn check_deposit_status(&self, evm_tx_hash: H256) -> Result<Option<String>, EvmCoreBridgeError> {
        log::debug!("Checking deposit status for EVM tx {:?}", evm_tx_hash);

        // TODO: Implement status check
        // Query Hyperliquid Core API to see if deposit from this EVM tx is confirmed

        Err(EvmCoreBridgeError::CoreApiFailed(
            "Deposit status check not yet implemented".to_string(),
        ))
    }

    async fn check_withdrawal_status(&self, core_tx_id: String) -> Result<Option<H256>, EvmCoreBridgeError> {
        log::debug!("Checking withdrawal status for Core tx {}", core_tx_id);

        // TODO: Implement status check
        // Monitor EVM for bridge contract withdrawal corresponding to this Core tx

        Err(EvmCoreBridgeError::ContractCallFailed(
            "Withdrawal status check not yet implemented".to_string(),
        ))
    }

    async fn get_core_balance(&self, core_token: String) -> Result<U256, EvmCoreBridgeError> {
        log::debug!("Getting Core balance for token {}", core_token);

        // TODO: Implement balance query via Hyperliquid Core API

        Err(EvmCoreBridgeError::CoreApiFailed(
            "Core balance query not yet implemented".to_string(),
        ))
    }

    async fn get_evm_balance(&self, evm_token: Address, address: Address) -> Result<U256, EvmCoreBridgeError> {
        log::debug!("Getting EVM balance for token {:?}, address {:?}", evm_token, address);

        // TODO: Implement ERC20 balanceOf query

        Err(EvmCoreBridgeError::ContractCallFailed(
            "EVM balance query not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bridge_creation() {
        // TODO: Add tests when implementation is complete
    }
}
