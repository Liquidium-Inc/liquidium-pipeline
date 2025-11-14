use alloy::{
    network::AnyNetwork,
    providers::{Provider, RootProvider},
};
use async_trait::async_trait;
use candid::Nat;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmBackend: Send + Sync {
    /// ERC20 balance for the pipeline wallet on a given chain.
    async fn erc20_balance(&self, chain: &str, token_address: &str) -> Result<u128, String>;

    /// Native coin balance (ETH, ARB, etc) for the pipeline wallet.
    async fn native_balance(&self, chain: &str) -> Result<u128, String>;

    /// ERC20 transfer from the pipeline wallet.
    async fn erc20_transfer(
        &self,
        chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String>; // tx hash

    /// Native transfer from the pipeline wallet.
    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: Nat) -> Result<String, String>;

    /// Decimals
    async fn erc20_decimals(&self, chain: &str, token_address: &str) -> Result<u8, String>;
}

// P is "whatever ProviderBuilder produced"
pub struct EvmBackendImpl<P> {
    pub provider: P,
}

impl<P> EvmBackendImpl<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

// Example: impl EvmBackend using Alloy Provider<Ethereum>
#[async_trait]
impl<P> EvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + Send + Sync + Clone,
{
    async fn erc20_balance(&self, chain: &str, token_address: &str) -> Result<u128, String> {
        // TODO: call ERC20 balanceOf via self.provider
        unimplemented!()
    }

    async fn native_balance(&self, chain: &str) -> Result<u128, String> {
        // TODO: call eth_getBalance via self.provider
        unimplemented!()
    }

    async fn erc20_transfer(
        &self,
        chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String> {
        // TODO: send ERC20 transfer tx via self.provider
        unimplemented!()
    }

    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: Nat) -> Result<String, String> {
        // TODO: send native transfer via self.provider
        unimplemented!()
    }

    async fn erc20_decimals(&self, chain: &str, token_address: &str) -> Result<u8, String> {
        // TODO: call decimals() via self.provider
        unimplemented!()
    }
}
