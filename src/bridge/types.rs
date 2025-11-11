use crate::icrc_token::icrc_token::IcrcToken;
use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;
pub use alloy::primitives::{Address, B256};

// Request to burn ckTokens on IC and receive native tokens on Hyperliquid EVM
#[derive(Debug, Clone)]
pub struct BurnRequest {
    // The ckToken to burn (e.g., ckBTC)
    pub ck_token: IcrcToken,
    // Amount to burn
    pub amount: IcrcTokenAmount,
    // Destination address on Hyperliquid EVM
    pub destination_address: Address,
}

// Receipt from burning ckTokens
#[derive(Debug, Clone)]
pub struct BurnReceipt {
    // The burn request that was executed
    pub request: BurnRequest,
    // Block index of the burn transaction on IC
    pub ic_block_index: u64,
    // Transaction hash on Hyperliquid EVM (after unwrapping)
    pub evm_tx_hash: B256,
    // Amount actually received on Hyperliquid (after fees)
    pub received_amount: u128,
}

// Request to wrap tokens on Hyperliquid EVM and mint ckTokens on IC
#[derive(Debug, Clone)]
pub struct MintRequest {
    // The native token on Hyperliquid to wrap (e.g., USDT)
    pub native_token_address: Address,
    // Amount to wrap
    pub amount: u128,
    // The ckToken to mint on IC (e.g., ckUSDT)
    pub ck_token: IcrcToken,
    // Destination principal on IC
    pub destination_principal: candid::Principal,
}

// Receipt from minting ckTokens
#[derive(Debug, Clone)]
pub struct MintReceipt {
    // The mint request that was executed
    pub request: MintRequest,
    // Transaction hash on Hyperliquid EVM (wrapping transaction)
    pub evm_tx_hash: B256,
    // Block index of the mint transaction on IC
    pub ic_block_index: u64,
    // Amount actually minted on IC (after fees)
    pub minted_amount: IcrcTokenAmount,
}

// Represents the full cross-chain transaction flow
#[derive(Debug, Clone)]
pub enum CrossChainTxStatus {
    // Burn initiated on IC
    BurnInitiated,
    // Burn completed, tokens received on Hyperliquid
    BurnCompleted(BurnReceipt),
    // First swap completed (BTC -> USDC)
    FirstSwapCompleted { tx_hash: B256, usdc_amount: u128 },
    // Second swap completed (USDC -> USDT)
    SecondSwapCompleted { tx_hash: B256, usdt_amount: u128 },
    // Wrap initiated on Hyperliquid
    WrapInitiated { tx_hash: B256 },
    // Mint completed on IC
    MintCompleted(MintReceipt),
    // Transaction failed at some stage
    Failed { stage: String, error: String },
}

// Complete cross-chain transaction record
#[derive(Debug, Clone)]
pub struct CrossChainTransaction {
    // Unique identifier for this transaction
    pub id: String,
    // Current status
    pub status: CrossChainTxStatus,
    // Initial burn request
    pub burn_request: BurnRequest,
    // Target mint parameters
    pub mint_request: Option<MintRequest>,
    // Timestamp of initiation
    pub initiated_at: chrono::DateTime<chrono::Utc>,
    // Timestamp of last update
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// Errors that can occur during bridge operations
#[derive(Debug, Clone)]
pub enum BridgeError {
    // Failed to burn ckTokens on IC
    BurnFailed(String),
    // Failed to receive tokens on Hyperliquid
    UnwrapFailed(String),
    // Failed to wrap tokens on Hyperliquid
    WrapFailed(String),
    // Failed to mint ckTokens on IC
    MintFailed(String),
    // EVM transaction failed
    EvmTransactionFailed { tx_hash: B256, reason: String },
    // Insufficient balance
    InsufficientBalance { required: u128, available: u128 },
    // Configuration error
    ConfigError(String),
    // Network error
    NetworkError(String),
    // Timeout waiting for transaction
    Timeout(String),
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BridgeError::BurnFailed(e) => write!(f, "Burn failed: {}", e),
            BridgeError::UnwrapFailed(e) => write!(f, "Unwrap failed: {}", e),
            BridgeError::WrapFailed(e) => write!(f, "Wrap failed: {}", e),
            BridgeError::MintFailed(e) => write!(f, "Mint failed: {}", e),
            BridgeError::EvmTransactionFailed { tx_hash, reason } => {
                write!(f, "EVM transaction {} failed: {}", tx_hash, reason)
            }
            BridgeError::InsufficientBalance { required, available } => {
                write!(
                    f,
                    "Insufficient balance: required {}, available {}",
                    required, available
                )
            }
            BridgeError::ConfigError(e) => write!(f, "Configuration error: {}", e),
            BridgeError::NetworkError(e) => write!(f, "Network error: {}", e),
            BridgeError::Timeout(e) => write!(f, "Timeout: {}", e),
        }
    }
}

impl std::error::Error for BridgeError {}
