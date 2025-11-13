use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use candid::{encode_args, decode_args, Nat, Principal};
use evm_bridge_client::{EvmClient, IcpHypeBridge};
use num_traits::ToPrimitive;
use std::sync::Arc;

use super::types::{
    BridgeError, BurnReceipt, BurnRequest, MintReceipt, MintRequest,
    Eip1559TransactionPriceArg, Eip1559TransactionPrice, WithdrawErc20Arg, WithdrawErc20Result,
};
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
            .map_err(|e| BridgeError::ConfigError(format!("Failed to get bridge address: {}", e)))?;

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
        self.config
            .get_erc20_address_by_ledger(ledger_id)
            .map_err(|e| BridgeError::ConfigError(e))
    }

    // Get the minter canister principal
    fn get_minter_principal(&self) -> Result<Principal, BridgeError> {
        self.config
            .get_hyperliquid_minter_principal()
            .map_err(|e| BridgeError::ConfigError(e))
    }

    // Get fee estimate from minter
    async fn get_fee_estimate(&self, ledger_id: Principal) -> Result<Eip1559TransactionPrice, BridgeError> {
        let minter_principal = self.get_minter_principal()?;

        let arg = Eip1559TransactionPriceArg {
            ckerc20_ledger_id: ledger_id,
        };

        let arg_bytes = encode_args((Some(arg),)).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to encode fee estimate args: {}", e))
        })?;

        let price: Eip1559TransactionPrice = self
            .ic_agent
            .call_query_tuple(&minter_principal, "eip_1559_transaction_price", arg_bytes)
            .await
            .map_err(|e| BridgeError::NetworkError(format!("Failed to call eip_1559_transaction_price: {}", e)))?;

        Ok(price)
    }

    // Calculate burn fees (simplified version - you may want to add more complex logic)
    fn calculate_burn_fees(&self, amount: &Nat, gas_fee: &Nat) -> Result<(Nat, Nat), BridgeError> {
        // Convert to u128 for calculations
        let amount_u128 = amount.0.to_u128()
            .ok_or_else(|| BridgeError::ConfigError("Amount too large".to_string()))?;
        let gas_fee_u128 = gas_fee.0.to_u128()
            .ok_or_else(|| BridgeError::ConfigError("Gas fee too large".to_string()))?;

        // Simple fee calculation: deduct gas fee from amount
        // In production, you'd want more sophisticated fee handling
        if amount_u128 <= gas_fee_u128 {
            return Err(BridgeError::InsufficientBalance {
                required: gas_fee_u128,
                available: amount_u128,
            });
        }

        let net_withdrawal = amount_u128 - gas_fee_u128;

        Ok((Nat::from(net_withdrawal), Nat::from(gas_fee_u128)))
    }

    // Approve ledger transfer to minter
    async fn approve_ledger_transfer(
        &self,
        ledger_id: Principal,
        spender: Principal,
        amount: Nat,
        from_subaccount: Option<[u8; 32]>,
    ) -> Result<Nat, BridgeError> {
        let arg_bytes = encode_args((spender, amount.clone(), from_subaccount)).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to encode approve args: {}", e))
        })?;

        let response_bytes = self
            .ic_agent
            .call_update_raw(&ledger_id, "icrc2_approve", arg_bytes)
            .await
            .map_err(|e| BridgeError::NetworkError(format!("Failed to call icrc2_approve: {}", e)))?;

        let (result,): (Result<Nat, String>,) = decode_args(&response_bytes).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to decode approve response: {}", e))
        })?;

        result.map_err(|e| BridgeError::BurnFailed(format!("Approval failed: {}", e)))
    }
}

#[async_trait]
impl<A: PipelineAgent, C: ConfigTrait> EvmBridge for HyperliquidToIcpBridge<A, C> {
    async fn burn_and_unwrap(&self, request: BurnRequest) -> Result<BurnReceipt, BridgeError> {
        log::info!(
            "Burning {} {} and unwrapping to {:?}",
            request.amount.formatted(),
            request.ck_token.symbol,
            request.destination_address
        );

        let minter_principal = self.get_minter_principal()?;
        let ledger_id = request.ck_token.ledger;

        // Step 1: Get fee estimate from minter
        log::info!("Getting fee estimate from minter for ledger {}", ledger_id);
        let fee_estimate = self.get_fee_estimate(ledger_id).await?;
        log::info!(
            "Fee estimate: max_fee_per_gas={}, max_transaction_fee={}",
            fee_estimate.max_fee_per_gas,
            fee_estimate.max_transaction_fee
        );

        // Step 2: Calculate fees
        let amount_nat = Nat::from(request.amount.value.0.to_u128().ok_or_else(|| {
            BridgeError::ConfigError("Amount too large".to_string())
        })?);

        let (net_withdrawal, _burn_fee) = self.calculate_burn_fees(&amount_nat, &fee_estimate.max_transaction_fee)?;

        log::info!(
            "Calculated fees: net_withdrawal={}, burn_fee={}",
            net_withdrawal,
            _burn_fee
        );

        // Step 3: Approve ckERC20 transfer to minter
        log::info!("Approving ckERC20 transfer to minter");
        self.approve_ledger_transfer(ledger_id, minter_principal, amount_nat.clone(), None)
            .await?;

        // Step 4: Call withdraw_erc_20 on minter
        log::info!("Calling withdraw_erc_20 on minter");
        let withdraw_arg = WithdrawErc20Arg {
            amount: net_withdrawal.clone(),
            ckerc20_ledger_id: ledger_id,
            recipient: format!("{:?}", request.destination_address),
            from_ckerc20_subaccount: None,
            from_cketh_subaccount: None, // TODO: Handle ckETH fee payment
        };

        let arg_bytes = encode_args((withdraw_arg,)).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to encode withdraw args: {}", e))
        })?;

        let response_bytes = self
            .ic_agent
            .call_update_raw(&minter_principal, "withdraw_erc20", arg_bytes)
            .await
            .map_err(|e| BridgeError::BurnFailed(format!("Failed to call withdraw_erc20: {}", e)))?;

        let (result,): (WithdrawErc20Result,) = decode_args(&response_bytes).map_err(|e| {
            BridgeError::NetworkError(format!("Failed to decode withdraw response: {}", e))
        })?;

        // Step 5: Handle response
        match result {
            WithdrawErc20Result::Ok(success) => {
                log::info!(
                    "Burn successful: cketh_block_index={}, ckerc20_block_index={}, tx_hash={}",
                    success.cketh_block_index,
                    success.ckerc20_block_index,
                    success.tx_hash
                );

                // Parse tx hash to B256
                let tx_hash: B256 = success.tx_hash.parse().map_err(|e| {
                    BridgeError::NetworkError(format!("Invalid tx hash: {}", e))
                })?;

                let received_amount = net_withdrawal.0.to_u128().ok_or_else(|| {
                    BridgeError::ConfigError("Net withdrawal too large".to_string())
                })?;

                Ok(BurnReceipt {
                    request,
                    ic_block_index: success.ckerc20_block_index.0.to_u64().unwrap_or(0),
                    evm_tx_hash: tx_hash,
                    received_amount,
                })
            }
            WithdrawErc20Result::Err(err) => {
                Err(BridgeError::BurnFailed(format!("Withdrawal failed: {:?}", err)))
            }
        }
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
