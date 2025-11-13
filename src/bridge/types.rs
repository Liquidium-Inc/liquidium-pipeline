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

// Minter canister types for fetching ERC20 addresses dynamically

// Supported ckERC20 token information from minter
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct SupportedCkErc20Token {
    pub erc20_contract_address: String,
    pub ledger_canister_id: candid::Principal,
    pub ckerc20_token_symbol: String,
}

// Gas fee estimate from minter
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct GasFeeEstimate {
    pub max_priority_fee_per_gas: candid::Nat,
    pub max_fee_per_gas: candid::Nat,
    pub timestamp: u64,
}

// ERC20 balance information
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct Erc20Balance {
    pub balance: candid::Nat,
    pub erc20_contract_address: String,
}

// Ethereum block height variant
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub enum EthereumBlockHeight {
    Safe,
    Finalized,
    Latest,
}

// Complete minter info response
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct MinterInfoResponse {
    pub deposit_with_subaccount_helper_contract_address: Option<String>,
    pub eth_balance: Option<candid::Nat>,
    pub eth_helper_contract_address: Option<String>,
    pub last_observed_block_number: Option<candid::Nat>,
    pub evm_rpc_id: Option<candid::Principal>,
    pub erc20_helper_contract_address: Option<String>,
    pub last_erc20_scraped_block_number: Option<candid::Nat>,
    pub supported_ckerc20_tokens: Option<Vec<SupportedCkErc20Token>>,
    pub last_gas_fee_estimate: Option<GasFeeEstimate>,
    pub cketh_ledger_id: Option<candid::Principal>,
    pub smart_contract_address: Option<String>,
    pub last_eth_scraped_block_number: Option<candid::Nat>,
    pub minimum_withdrawal_amount: Option<candid::Nat>,
    pub erc20_balances: Option<Vec<Erc20Balance>>,
    pub minter_address: Option<String>,
    pub last_deposit_with_subaccount_scraped_block_number: Option<candid::Nat>,
    pub ethereum_block_height: Option<EthereumBlockHeight>,
}

// Processed minter info with parsed addresses for fast lookup
#[derive(Debug, Clone)]
pub struct MinterInfo {
    // Raw response from minter
    pub raw: MinterInfoResponse,
    // Map of ledger canister ID to ERC20 contract address for O(1) lookup
    pub token_addresses: std::collections::HashMap<candid::Principal, Address>,
    // Reverse map: ERC20 contract address to token symbol for O(1) lookup
    pub address_to_symbol: std::collections::HashMap<Address, String>,
    // Bridge contract address (smart_contract_address from minter)
    pub bridge_address: Address,
}

impl MinterInfo {
    // Create MinterInfo from raw response, parsing addresses
    pub fn from_response(response: MinterInfoResponse) -> Result<Self, BridgeError> {
        let mut token_addresses = std::collections::HashMap::new();
        let mut address_to_symbol = std::collections::HashMap::new();

        // Parse supported tokens and build both forward and reverse mappings
        if let Some(tokens) = &response.supported_ckerc20_tokens {
            for token in tokens {
                let address: Address = token
                    .erc20_contract_address
                    .parse()
                    .map_err(|_| {
                        BridgeError::ConfigError(format!(
                            "Invalid ERC20 address for {}: {}",
                            token.ckerc20_token_symbol, token.erc20_contract_address
                        ))
                    })?;

                // Forward mapping: ledger ID -> ERC20 address
                token_addresses.insert(token.ledger_canister_id, address);

                // Reverse mapping: ERC20 address -> token symbol
                // Strip "ck" prefix to get Hyperliquid Core token symbol
                let core_symbol = token.ckerc20_token_symbol.strip_prefix("ck")
                    .unwrap_or(&token.ckerc20_token_symbol)
                    .to_string();
                address_to_symbol.insert(address, core_symbol);
            }
        }

        // Parse bridge address
        let bridge_address = response
            .smart_contract_address
            .as_ref()
            .ok_or_else(|| BridgeError::ConfigError("Missing smart_contract_address in minter info".to_string()))?
            .parse()
            .map_err(|_| {
                BridgeError::ConfigError(format!(
                    "Invalid smart_contract_address: {}",
                    response.smart_contract_address.as_ref().unwrap()
                ))
            })?;

        Ok(Self {
            raw: response,
            token_addresses,
            address_to_symbol,
            bridge_address,
        })
    }

    // Get ERC20 address for a given ledger canister ID
    pub fn get_erc20_address(&self, ledger_id: &candid::Principal) -> Option<Address> {
        self.token_addresses.get(ledger_id).copied()
    }

    // Get token symbol for a ledger canister ID
    pub fn get_token_symbol(&self, ledger_id: &candid::Principal) -> Option<String> {
        self.raw
            .supported_ckerc20_tokens
            .as_ref()?
            .iter()
            .find(|t| t.ledger_canister_id == *ledger_id)
            .map(|t| t.ckerc20_token_symbol.clone())
    }

    // Get Core token symbol from ERC20 address (reverse lookup)
    pub fn get_symbol_by_address(&self, address: &Address) -> Option<String> {
        self.address_to_symbol.get(address).cloned()
    }
}

// Minter withdrawal types

// Argument for getting EIP-1559 transaction price estimate
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct Eip1559TransactionPriceArg {
    pub ckerc20_ledger_id: candid::Principal,
}

// EIP-1559 transaction price response
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct Eip1559TransactionPrice {
    pub max_fee_per_gas: candid::Nat,
    pub max_priority_fee_per_gas: candid::Nat,
    pub max_transaction_fee: candid::Nat,
    pub timestamp: Option<u64>,
}

// Subaccount type for IC
pub type Subaccount = [u8; 32];

// Argument for withdrawing ERC20 tokens (burn)
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct WithdrawErc20Arg {
    pub amount: candid::Nat,
    pub ckerc20_ledger_id: candid::Principal,
    pub recipient: String,
    pub from_ckerc20_subaccount: Option<Subaccount>,
    pub from_cketh_subaccount: Option<Subaccount>,
}

// Successful withdrawal response
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub struct WithdrawErc20Success {
    pub cketh_block_index: candid::Nat,
    pub ckerc20_block_index: candid::Nat,
    pub tx_hash: String,
}

// Withdrawal error
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub enum WithdrawErc20Error {
    TokenNotSupported { supported_tokens: Vec<SupportedCkErc20Token> },
    CkEthLedgerError { error: String },
    CkErc20LedgerError { error: String },
    TemporarilyUnavailable(String),
}

// Withdrawal result
#[derive(Debug, Clone, candid::CandidType, candid::Deserialize, serde::Serialize)]
pub enum WithdrawErc20Result {
    Ok(WithdrawErc20Success),
    Err(WithdrawErc20Error),
}

// Fee calculation result
#[derive(Debug, Clone)]
pub struct BurnFees {
    // Amount to actually withdraw after fees
    pub net_withdrawal: candid::Nat,
    // Fee to charge the user (in ckERC20)
    pub erc_burn_fee: candid::Nat,
    // ETH gas fee estimate
    pub eth_gas_fee: candid::Nat,
}
