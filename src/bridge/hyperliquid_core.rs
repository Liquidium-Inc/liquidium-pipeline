use async_trait::async_trait;
use alloy::primitives::{Address, B256};
use serde::{Deserialize, Serialize};


// Receipt from depositing tokens from EVM to Hyperliquid Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreDepositReceipt {
    // Token address on EVM
    pub evm_token_address: Address,
    // Amount deposited from EVM
    pub evm_amount: u128,
    // Transaction hash on EVM for the deposit
    pub evm_tx_hash: B256,
    // Transaction hash/ID on Hyperliquid Core
    pub core_tx_hash: String,
    // Amount received on Core (after fees)
    pub amount_received: u128,
    // Timestamp of confirmation
    pub confirmed_at: u64,
}

// Receipt from withdrawing tokens from Hyperliquid Core to EVM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreWithdrawReceipt {
    // Token address/ID on Core
    pub core_token_address: String,
    // Amount withdrawn from Core
    pub core_amount: u128,
    // Transaction hash/ID on Hyperliquid Core
    pub core_tx_hash: String,
    // Transaction hash on EVM where tokens were received
    pub evm_tx_hash: B256,
    // Amount received on EVM (after fees)
    pub amount_received: u128,
    // Timestamp of confirmation
    pub confirmed_at: u64,
}

// Receipt from a transaction on Hyperliquid Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreTxReceipt {
    // Transaction ID on Core
    pub tx_hash: String,
    // Block number/height
    pub block_number: u64,
    // Status (success/failure)
    pub status: CoreTxStatus,
    // Gas or fee paid
    pub fee: u128,
    // Timestamp
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoreTxStatus {
    Success,
    Failed(String),
    Pending,
}

// Errors that can occur during Hyperliquid Core operations
#[derive(Debug, Clone)]
pub enum HyperliquidCoreError {
    // Deposit from EVM to Core failed
    DepositFailed(String),
    // Withdrawal from Core to EVM failed
    WithdrawFailed(String),
    // Insufficient balance on Core
    InsufficientBalance { required: u128, available: u128 },
    // Transaction not found
    TxNotFound(String),
    // API error
    ApiError(String),
    // Network error
    NetworkError(String),
    // Timeout
    Timeout(String),
}

impl std::fmt::Display for HyperliquidCoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HyperliquidCoreError::DepositFailed(e) => write!(f, "Deposit to Core failed: {}", e),
            HyperliquidCoreError::WithdrawFailed(e) => write!(f, "Withdrawal from Core failed: {}", e),
            HyperliquidCoreError::InsufficientBalance { required, available } => {
                write!(f, "Insufficient balance on Core: required {}, available {}", required, available)
            }
            HyperliquidCoreError::TxNotFound(tx) => write!(f, "Transaction not found: {}", tx),
            HyperliquidCoreError::ApiError(e) => write!(f, "API error: {}", e),
            HyperliquidCoreError::NetworkError(e) => write!(f, "Network error: {}", e),
            HyperliquidCoreError::Timeout(e) => write!(f, "Timeout: {}", e),
        }
    }
}

impl std::error::Error for HyperliquidCoreError {}

// Trait for interacting with Hyperliquid Core (L2)
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait HyperliquidCoreInterface: Send + Sync {
    // Deposit tokens from Hyperliquid EVM to Hyperliquid Core
    //
    // This transfers tokens from the EVM layer (L1) to the Core layer (L2)
    // where they can be used for trading on the DEX.
    async fn deposit_to_core(
        &self,
        evm_token: Address,
        amount: u128,
    ) -> Result<CoreDepositReceipt, HyperliquidCoreError>;

    // Withdraw tokens from Hyperliquid Core to Hyperliquid EVM
    //
    // This transfers tokens from the Core layer (L2) back to the EVM layer (L1)
    // where they can be bridged back to other chains.
    async fn withdraw_from_core(
        &self,
        core_token: String,
        amount: u128,
    ) -> Result<CoreWithdrawReceipt, HyperliquidCoreError>;

    // Get balance of a token on Hyperliquid Core
    async fn get_core_balance(&self, core_token: String) -> Result<u128, HyperliquidCoreError>;

    // Wait for a Core transaction to be confirmed
    async fn wait_for_core_tx(&self, tx_hash: String) -> Result<CoreTxReceipt, HyperliquidCoreError>;

    // Check if a deposit from EVM has been credited on Core
    async fn check_deposit_status(&self, evm_tx_hash: B256) -> Result<Option<String>, HyperliquidCoreError>;

    // Check if a withdrawal from Core has been received on EVM
    async fn check_withdrawal_status(&self, core_tx_hash: String) -> Result<Option<B256>, HyperliquidCoreError>;
}

// Implementation of Hyperliquid Core interface
pub struct HyperliquidCore {
    // Hyperliquid API URL (e.g., https://api.hyperliquid.xyz)
    api_url: String,
    // Wallet private key for signing Core transactions
    wallet_key: String,
    // Chain ID
    chain_id: u64,
}

impl HyperliquidCore {
    pub fn new(
        api_url: String,
        wallet_key: String,
        _evm_rpc_url: String,
        chain_id: u64,
    ) -> Result<Self, HyperliquidCoreError> {
        Ok(Self {
            api_url,
            wallet_key,
            chain_id,
        })
    }
}

#[async_trait]
impl HyperliquidCoreInterface for HyperliquidCore {
    async fn deposit_to_core(
        &self,
        evm_token: Address,
        amount: u128,
    ) -> Result<CoreDepositReceipt, HyperliquidCoreError> {
        log::info!("Depositing {} of token {:?} from EVM to Core", amount, evm_token);

        // TODO: Implement actual deposit logic
        // This would:
        // 1. Call the EVM bridge contract to initiate deposit
        // 2. Wait for EVM transaction confirmation
        // 3. Wait for Core to credit the deposit
        // 4. Return receipt with both transaction hashes

        Err(HyperliquidCoreError::ApiError(
            "Deposit to Core not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected flow:
        // 1. Approve EVM token for bridge contract
        // 2. Call bridge contract's deposit() function
        // 3. Wait for EVM tx confirmation
        // 4. Poll Hyperliquid Core API to check deposit status
        // 5. Return CoreDepositReceipt
    }

    async fn withdraw_from_core(
        &self,
        core_token: String,
        amount: u128,
    ) -> Result<CoreWithdrawReceipt, HyperliquidCoreError> {
        log::info!("Withdrawing {} of token {} from Core to EVM", amount, core_token);

        // TODO: Implement actual withdrawal logic
        // This would:
        // 1. Call Hyperliquid Core API to initiate withdrawal
        // 2. Wait for Core transaction confirmation
        // 3. Wait for tokens to appear on EVM
        // 4. Return receipt with both transaction hashes

        Err(HyperliquidCoreError::ApiError(
            "Withdrawal from Core not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected flow:
        // 1. Sign withdrawal request with wallet
        // 2. Submit to Hyperliquid Core API
        // 3. Wait for Core tx confirmation
        // 4. Monitor EVM for withdrawal completion
        // 5. Return CoreWithdrawReceipt
    }

    async fn get_core_balance(&self, core_token: String) -> Result<u128, HyperliquidCoreError> {
        log::info!("Getting Core balance for token {}", core_token);

        // TODO: Implement balance query
        // This would call Hyperliquid Core API to get balance

        Err(HyperliquidCoreError::ApiError(
            "Balance query not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected flow:
        // 1. Call Core API endpoint for balance
        // 2. Parse response
        // 3. Return balance as u128
    }

    async fn wait_for_core_tx(&self, tx_hash: String) -> Result<CoreTxReceipt, HyperliquidCoreError> {
        log::info!("Waiting for Core transaction {}", tx_hash);

        // TODO: Implement transaction waiting
        // This would poll Core API until tx is confirmed

        Err(HyperliquidCoreError::ApiError(
            "Transaction waiting not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected flow:
        // 1. Poll Core API for transaction status
        // 2. Wait for confirmation
        // 3. Parse receipt
        // 4. Return CoreTxReceipt
    }

    async fn check_deposit_status(&self, evm_tx_hash: B256) -> Result<Option<String>, HyperliquidCoreError> {
        log::info!("Checking deposit status for EVM tx {:?}", evm_tx_hash);

        // TODO: Implement deposit status check
        // Returns Some(core_tx_hash) if deposit is complete, None if pending

        Err(HyperliquidCoreError::ApiError(
            "Deposit status check not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))
    }

    async fn check_withdrawal_status(&self, core_tx_hash: String) -> Result<Option<B256>, HyperliquidCoreError> {
        log::info!("Checking withdrawal status for Core tx {}", core_tx_hash);

        // TODO: Implement withdrawal status check
        // Returns Some(evm_tx_hash) if withdrawal is complete, None if pending

        Err(HyperliquidCoreError::ApiError(
            "Withdrawal status check not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))
    }
}