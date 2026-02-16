use alloy::{
    network::{AnyNetwork, NetworkWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{Provider, WalletProvider},
    rpc::types::TransactionRequest,
    sol,
};
use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::error::{AppError, error_codes};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmBackend: Send + Sync {
    // ERC20 balance for the pipeline wallet on a given chain.
    async fn erc20_balance(&self, chain: &str, token_address: &str) -> Result<Nat, AppError>;

    // Native coin balance (ETH, ARB, etc) for the pipeline wallet.
    async fn native_balance(&self, chain: &str) -> Result<Nat, AppError>;

    // ERC20 transfer from the pipeline wallet.
    async fn erc20_transfer(
        &self,
        chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, AppError>; // tx hash

    // Native transfer from the pipeline wallet.
    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: Nat) -> Result<String, AppError>;

    // Decimals
    async fn erc20_decimals(&self, chain: &str, token_address: &str) -> Result<u8, AppError>;
}

fn nat_to_u256(n: &Nat) -> Result<U256, AppError> {
    let bytes: Vec<u8> = n.0.to_bytes_be();
    if bytes.len() > 32 {
        return Err(AppError::from_def(error_codes::INVALID_INPUT).with_context("Nat too large for U256"));
    }
    Ok(U256::from_be_slice(&bytes))
}

fn u256_to_nat(v: U256) -> Nat {
    Nat::from(v.to::<u128>())
}

pub struct EvmBackendImpl<P> {
    pub provider: P,
}

impl<P: Provider<AnyNetwork>> EvmBackendImpl<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> EvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    async fn erc20_balance(&self, _chain: &str, token_address: &str) -> Result<Nat, AppError> {
        let owner = self.provider.wallet().default_signer_address();

        let token_addr: Address = token_address.parse().map_err(|e| {
            AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid token address: {e}"))
        })?;

        let contract = IERC20::new(token_addr, self.provider.clone());

        let res = contract.balanceOf(owner).call().await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED).with_context(format!("erc20 balanceOf failed: {e}"))
        })?;

        Ok(u256_to_nat(res))
    }

    async fn native_balance(&self, _chain: &str) -> Result<Nat, AppError> {
        let bal: U256 = self.provider.get_balance(self.wallet_address()).await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED)
                .with_context(format!("native get_balance failed: {e}"))
        })?;

        Ok(u256_to_nat(bal))
    }

    async fn erc20_transfer(
        &self,
        _chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, AppError> {
        let token_addr: Address = token_address.parse().map_err(|e| {
            AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid token address: {e}"))
        })?;
        let to_addr: Address = to.parse().map_err(|e| {
            AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid recipient address: {e}"))
        })?;

        let amount = nat_to_u256(&amount_wei)?;

        let contract = IERC20::new(token_addr, self.provider.clone());

        let tx = contract.transfer(to_addr, amount).send().await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED)
                .with_context(format!("erc20 transfer send failed: {e}"))
        })?;

        let tx_hash = format!("{:#x}", tx.tx_hash());
        Ok(tx_hash)
    }

    async fn native_transfer(&self, _chain: &str, to: &str, amount_wei: Nat) -> Result<String, AppError> {
        let to_addr: Address = to.parse().map_err(|e| {
            AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid recipient address: {e}"))
        })?;
        let amount = nat_to_u256(&amount_wei)?;

        let tx = TransactionRequest::default().with_to(to_addr).with_value(amount);

        let pending = self.provider.send_transaction(tx.into()).await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED)
                .with_context(format!("native transfer send failed: {e}"))
        })?;

        let tx_hash = pending.watch().await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED)
                .with_context(format!("native transfer watch failed: {e}"))
        })?;

        Ok(format!("{:#x}", tx_hash))
    }

    async fn erc20_decimals(&self, _chain: &str, token_address: &str) -> Result<u8, AppError> {
        let token_addr: Address = token_address.parse().map_err(|e| {
            AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid token address: {e}"))
        })?;

        let contract = IERC20::new(token_addr, self.provider.clone());

        let res = contract.decimals().call().await.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED).with_context(format!("erc20 decimals() failed: {e}"))
        })?;

        Ok(res)
    }
}

impl<P> EvmBackendImpl<P>
where
    P: WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    fn wallet_address(&self) -> Address {
        self.provider.default_signer_address()
    }
}
