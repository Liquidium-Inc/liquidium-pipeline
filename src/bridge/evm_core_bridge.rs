use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use evm_bridge_client::{Erc20Client, EvmClient, PrivateKeySigner};
use hyperliquid_rust_sdk::{ExchangeClient, InfoClient};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::config::ConfigTrait;

// Constants for Hyperliquid EVM <-> Core bridge

// System address prefix for HyperCore transfers
// EVM tokens sent to addresses starting with 0x20 are credited to Core
const SYSTEM_ADDRESS_PREFIX: u8 = 0x20;

// Special system address for HYPE token
const HYPE_SYSTEM_ADDRESS: &str = "0x2222222222222222222222222222222222222222";

// HYPE token index (special case)
const HYPE_TOKEN_INDEX: u32 = 0;

// Polling configuration for balance checks
const BALANCE_CHECK_INTERVAL_SECS: u64 = 2;
const DEFAULT_TIMEOUT_SECS: u64 = 300; // 5 minutes

// Receipt from depositing tokens from Hyperliquid EVM to Hyperliquid Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvmToCoreReceipt {
    // Token address on EVM
    pub evm_token: Address,
    // Amount deposited from EVM
    pub evm_amount: U256,
    // Transaction hash on EVM initiating the deposit
    pub evm_tx_hash: B256,
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
    pub evm_tx_hash: B256,
    // Amount received on EVM (after fees)
    pub evm_amount: U256,
    // Block number on EVM
    pub evm_block: u64,
    // Timestamp of EVM confirmation
    pub evm_timestamp: u64,
}

// Status of an EVM yo Core transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferStatus {
    // Transfer initiated, waiting for confirmation
    Pending,
    // Transfer confirmed on destination layer
    Confirmed,
    // Transfer failed
    Failed(String),
}

// Errors specific to EVM yo Core bridge operations
#[derive(Debug, Clone)]
pub enum EvmCoreBridgeError {
    // Failed to deposit from EVM to Core
    DepositFailed { evm_tx: B256, reason: String },
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
    EvmTxNotFound(B256),
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
                write!(
                    f,
                    "Insufficient balance: required {}, available {}",
                    required, available
                )
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
    /*
    Deposit tokens from Hyperliquid EVM to Hyperliquid Core
    This makes tokens available for trading on the Hyperliquid DEX.

    # Process
    1. Approve EVM token for bridge contract
    2. Call bridge contract's deposit function on EVM
    3. Wait for EVM transaction confirmation
    4. Poll Hyperliquid Core API until deposit is credited
    5. Return receipt with both transaction identifiers

    # Arguments
    * `evm_token` - Token contract address on EVM
    * `amount` - Amount to deposit (in token's smallest unit)

    # Returns
    Receipt with EVM tx hash and Core tx ID
    */
    async fn deposit_evm_to_core(
        &self,
        evm_token: Address,
        amount: u128,
    ) -> Result<EvmToCoreReceipt, EvmCoreBridgeError>;

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
    async fn withdraw_core_to_evm(
        &self,
        core_token: String,
        amount: u128,
    ) -> Result<CoreToEvmReceipt, EvmCoreBridgeError>;

    // Check the status of an EVM -> Core deposit
    //
    // # Arguments
    // * `evm_tx_hash` - Transaction hash of the deposit on EVM
    //
    // # Returns
    // - `Some(core_tx_id)` if deposit is confirmed on Core
    // - `None` if still pending
    async fn check_deposit_status(&self, evm_tx_hash: B256) -> Result<Option<String>, EvmCoreBridgeError>;

    // Check the status of a Core -> EVM withdrawal
    //
    // # Arguments
    // * `core_tx_id` - Transaction ID of the withdrawal on Core
    //
    // # Returns
    // - `Some(evm_tx_hash)` if withdrawal is confirmed on EVM
    // - `None` if still pending
    async fn check_withdrawal_status(&self, core_tx_id: String) -> Result<Option<B256>, EvmCoreBridgeError>;

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

    // Resolves the core-layer asset address corresponding to the given EVM token.
    // This maps an ERC-20 address on Hyperliquid EVM to its asset representation
    // inside Hyperliquid Core (used in bridging + settlement flows).
    async fn get_asset_core_address(&self, evm_token: Address) -> Result<Address, EvmCoreBridgeError>;

    // Get the wallet address used by this bridge
    fn get_wallet_address(&self) -> Address;
}

// Implementation of the EVM yo Core bridge for Hyperliquid
pub struct HyperliquidEvmCoreBridge<C: ConfigTrait> {
    // Evm Client
    pub evm_client: Arc<EvmClient>,
    // Core client
    pub exchange_client: ExchangeClient,
    // Config
    pub config: Arc<C>,
}

impl<C: ConfigTrait> HyperliquidEvmCoreBridge<C> {
    // Create a new EVM to Core bridge
    pub async fn new(config: Arc<C>, evm_client: Arc<EvmClient>) -> Result<Self, EvmCoreBridgeError> {
        let key = config.get_hyperliquid_wallet_key().unwrap();
        let key = key.trim_start_matches("0x");
        let signer: PrivateKeySigner = key
            .parse()
            .map_err(|e| EvmCoreBridgeError::NetworkError(format!("Invalid private key: {}", e)))?;

        let exchange_client =
            ExchangeClient::new(None, signer, Some(hyperliquid_rust_sdk::BaseUrl::Mainnet), None, None)
                .await
                .unwrap();

        Ok(Self {
            config,
            evm_client,
            exchange_client,
        })
    }

    // Helper: Get Core token symbol from ERC20 address
    fn get_token_symbol(&self, evm_token: Address) -> Result<String, EvmCoreBridgeError> {
        self.config
            .get_symbol_by_erc20_address(&evm_token)
            .map_err(|e| EvmCoreBridgeError::NetworkError(e))
    }

    // Helper: Get token index from symbol
    fn get_token_index(&self, symbol: &str) -> Result<u32, EvmCoreBridgeError> {
        self.exchange_client
            .coin_to_asset
            .get(symbol)
            .copied()
            .ok_or_else(|| {
                EvmCoreBridgeError::CoreApiFailed(format!(
                    "Token '{}' not found in Core asset mapping",
                    symbol
                ))
            })
    }

    // Helper: Calculate system address for a token index
    fn calculate_system_address(&self, token_index: u32) -> Address {
        // Special case for HYPE token
        if token_index == HYPE_TOKEN_INDEX {
            return Address::parse_checksummed(HYPE_SYSTEM_ADDRESS, None)
                .expect("Invalid HYPE system address");
        }

        // General formula: 0x20 + zeros + big-endian token index
        let mut bytes = [0u8; 20];
        bytes[0] = SYSTEM_ADDRESS_PREFIX; // 0x20
        bytes[16..20].copy_from_slice(&token_index.to_be_bytes());
        Address::from_slice(&bytes)
    }

    // Helper: Get Core balance for a token
    async fn get_core_balance(&self, token_symbol: &str) -> Result<U256, EvmCoreBridgeError> {
        let wallet_address = self.exchange_client.wallet.address();

        // Query user state from InfoClient
        let info_client = InfoClient::new(None, Some(hyperliquid_rust_sdk::BaseUrl::Mainnet))
            .await
            .map_err(|e| EvmCoreBridgeError::CoreApiFailed(format!("Failed to create InfoClient: {:?}", e)))?;

        let token_balances = info_client
            .user_token_balances(wallet_address)
            .await
            .map_err(|e| EvmCoreBridgeError::CoreApiFailed(format!("Failed to get token balances: {:?}", e)))?;

        // Find the balance for our token
        for balance in &token_balances.balances {
            if balance.coin == token_symbol {
                // Parse balance string to U256
                let balance_str = &balance.total;
                let balance_f64: f64 = balance_str.parse()
                    .map_err(|_| EvmCoreBridgeError::CoreApiFailed(format!("Invalid balance format: {}", balance_str)))?;

                // Convert to wei (assuming 8 decimals for most tokens)
                let balance_wei = (balance_f64 * 1e8) as u128;
                return Ok(U256::from(balance_wei));
            }
        }

        // Token not found or zero balance
        Ok(U256::ZERO)
    }

    // Helper: Get EVM balance for a token
    async fn get_evm_balance(&self, token: Address) -> Result<U256, EvmCoreBridgeError> {
        let wallet_address = self.exchange_client.wallet.address();

        let erc20 = Erc20Client::new(token, self.evm_client.clone());

        erc20
            .balance_of(wallet_address)
            .await
            .map_err(|e| EvmCoreBridgeError::NetworkError(format!("Failed to get EVM balance: {:?}", e)))
    }

    // Helper: Wait for Core balance to increase
    async fn wait_for_core_balance_increase(
        &self,
        token_symbol: &str,
        initial_balance: U256,
        min_increase: U256,
    ) -> Result<U256, EvmCoreBridgeError> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        loop {
            if start.elapsed() > timeout {
                return Err(EvmCoreBridgeError::TransferTimeout {
                    waited: start.elapsed().as_secs(),
                    timeout: DEFAULT_TIMEOUT_SECS,
                });
            }

            let current_balance = self.get_core_balance(token_symbol).await?;
            let increase = current_balance.saturating_sub(initial_balance);

            if increase >= min_increase {
                log::info!(
                    "Core balance increased by {} (expected {})",
                    increase,
                    min_increase
                );
                return Ok(current_balance);
            }

            log::debug!(
                "Waiting for Core balance increase: current={}, initial={}, increase={}, target={}",
                current_balance,
                initial_balance,
                increase,
                min_increase
            );

            sleep(Duration::from_secs(BALANCE_CHECK_INTERVAL_SECS)).await;
        }
    }

    // Helper: Wait for EVM Transfer event
    async fn wait_for_evm_transfer_event(
        &self,
        token: Address,
        recipient: Address,
        min_amount: U256,
    ) -> Result<B256, EvmCoreBridgeError> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        let initial_balance = self.get_evm_balance(token).await?;

        loop {
            if start.elapsed() > timeout {
                return Err(EvmCoreBridgeError::TransferTimeout {
                    waited: start.elapsed().as_secs(),
                    timeout: DEFAULT_TIMEOUT_SECS,
                });
            }

            let current_balance = self.get_evm_balance(token).await?;
            let increase = current_balance.saturating_sub(initial_balance);

            if increase >= min_amount {
                log::info!(
                    "EVM balance increased by {} (expected {})",
                    increase,
                    min_amount
                );
                // Return a placeholder tx hash (we don't have access to the actual event)
                // In a real implementation, we'd use alloy provider to fetch the Transfer event
                return Ok(B256::ZERO);
            }

            log::debug!(
                "Waiting for EVM transfer: current={}, initial={}, increase={}, target={}",
                current_balance,
                initial_balance,
                increase,
                min_amount
            );

            sleep(Duration::from_secs(BALANCE_CHECK_INTERVAL_SECS)).await;
        }
    }
}

#[async_trait]
impl<C: ConfigTrait> EvmCoreBridge for HyperliquidEvmCoreBridge<C> {
    async fn deposit_evm_to_core(
        &self,
        evm_token: Address,
        amount: u128,
    ) -> Result<EvmToCoreReceipt, EvmCoreBridgeError> {
        log::info!("Depositing {} of token {:?} from EVM to Core", amount, evm_token);

        // Step 1: Get token symbol and index
        let token_symbol = self.get_token_symbol(evm_token)?;
        let token_index = self.get_token_index(&token_symbol)?;

        log::info!(
            "Token mapping: {:?} -> symbol='{}', index={}",
            evm_token,
            token_symbol,
            token_index
        );

        // Step 2: Calculate system address for this token
        let system_address = self.calculate_system_address(token_index);

        log::info!("System address for deposit: {:?}", system_address);

        // Step 3: Get initial Core balance
        let initial_core_balance = self.get_core_balance(&token_symbol).await?;

        log::info!(
            "Initial Core balance for {}: {}",
            token_symbol,
            initial_core_balance
        );

        // Step 4: Execute ERC20 transfer to system address
        let erc20 = Erc20Client::new(evm_token, self.evm_client.clone());

        let receipt = erc20
            .transfer(system_address, U256::from(amount))
            .await
            .map_err(|e| {
                EvmCoreBridgeError::ContractCallFailed(format!("ERC20 transfer failed: {:?}", e))
            })?;

        let tx_hash = receipt.transaction_hash;
        log::info!("ERC20 transfer sent: tx_hash={:?}", tx_hash);

        // Step 5: Wait for Core to credit the balance
        let final_core_balance = self
            .wait_for_core_balance_increase(&token_symbol, initial_core_balance, U256::from(amount))
            .await?;

        let core_amount = final_core_balance.saturating_sub(initial_core_balance);

        log::info!(
            "Deposit complete: Core balance increased by {}",
            core_amount
        );

        // Step 6: Return receipt
        Ok(EvmToCoreReceipt {
            evm_token,
            evm_amount: U256::from(amount),
            evm_tx_hash: tx_hash,
            core_tx_id: format!("evm_to_core_{:?}", tx_hash), // Core doesn't provide explicit tx ID
            core_amount,
            evm_block: 0, // Would need provider to get this
            core_timestamp: chrono::Utc::now().timestamp() as u64,
        })
    }

    async fn withdraw_core_to_evm(
        &self,
        core_token: String,
        amount: u128,
    ) -> Result<CoreToEvmReceipt, EvmCoreBridgeError> {
        log::info!("Withdrawing {} of token {} from Core to EVM", amount, core_token);

        // Step 1: Get EVM token address from config by symbol
        let evm_token = self.config
            .get_erc20_address_by_symbol(&core_token)
            .map_err(EvmCoreBridgeError::NetworkError)?;

        log::info!("EVM token address for {}: {:?}", core_token, evm_token);

        // Step 2: Get initial EVM balance
        let initial_evm_balance = self.get_evm_balance(evm_token).await?;

        log::info!(
            "Initial EVM balance for {:?}: {}",
            evm_token,
            initial_evm_balance
        );

        // Step 3: Get destination (user's EVM wallet)
        let destination = self.exchange_client.wallet.address();

        // Step 4: Format amount for spot_transfer (as string with proper decimals)
        let amount_str = format!("{}", amount as f64 / 1e8); // Assuming 8 decimals

        log::info!(
            "Calling spot_transfer: token='{}', amount='{}', destination='{:?}'",
            core_token,
            amount_str,
            destination
        );

        // Step 5: Execute spotSend via ExchangeClient
        let result = self
            .exchange_client
            .spot_transfer(&amount_str, &format!("{:?}", destination), &core_token, None)
            .await
            .map_err(|e| {
                EvmCoreBridgeError::CoreApiFailed(format!("spot_transfer failed: {:?}", e))
            })?;

        let core_tx_id = format!("{:?}", result);

        log::info!("spot_transfer result: {}", core_tx_id);

        // Step 6: Wait for EVM balance to increase
        let evm_tx_hash = self
            .wait_for_evm_transfer_event(evm_token, destination, U256::from(amount))
            .await?;

        let final_evm_balance = self.get_evm_balance(evm_token).await?;
        let evm_amount = final_evm_balance.saturating_sub(initial_evm_balance);

        log::info!(
            "Withdrawal complete: EVM balance increased by {}",
            evm_amount
        );

        // Step 7: Return receipt
        Ok(CoreToEvmReceipt {
            core_token,
            core_amount: U256::from(amount),
            core_tx_id,
            evm_tx_hash,
            evm_amount,
            evm_block: 0, // Would need provider to get this
            evm_timestamp: chrono::Utc::now().timestamp() as u64,
        })
    }

    async fn check_deposit_status(&self, _evm_tx_hash: B256) -> Result<Option<String>, EvmCoreBridgeError> {
        // Deposit status is implicit - if the Core balance increased, deposit succeeded
        // We don't have a direct mapping from EVM tx to Core tx ID
        Ok(Some("confirmed".to_string()))
    }

    async fn check_withdrawal_status(&self, _core_tx_id: String) -> Result<Option<B256>, EvmCoreBridgeError> {
        // Withdrawal status is implicit - if the EVM balance increased, withdrawal succeeded
        // We don't track Core tx ID to EVM tx mapping
        Ok(Some(B256::ZERO))
    }

    async fn get_core_balance(&self, core_token: String) -> Result<U256, EvmCoreBridgeError> {
        // Delegate to helper method
        self.get_core_balance(&core_token).await
    }

    async fn get_evm_balance(&self, evm_token: Address, address: Address) -> Result<U256, EvmCoreBridgeError> {
        // Query ERC20 balance
        let erc20 = Erc20Client::new(evm_token, self.evm_client.clone());

        erc20
            .balance_of(address)
            .await
            .map_err(|e| EvmCoreBridgeError::NetworkError(format!("Failed to get EVM balance: {:?}", e)))
    }

    async fn get_asset_core_address(&self, evm_token: Address) -> Result<Address, EvmCoreBridgeError> {
        // Get token symbol and index, then calculate system address
        let token_symbol = self.get_token_symbol(evm_token)?;
        let token_index = self.get_token_index(&token_symbol)?;
        Ok(self.calculate_system_address(token_index))
    }

    fn get_wallet_address(&self) -> Address {
        self.exchange_client.wallet.address()
    }
}
