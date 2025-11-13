use async_trait::async_trait;

#[async_trait]
pub trait EvmAccountActions: Send + Sync {
    async fn transfer(&self, chain: &str, token_address: &str, to: &str, amount_wei: u128) -> Result<String, String>; // tx hash
}

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
        amount_wei: u128,
    ) -> Result<String, String>; // tx hash

    /// Native transfer from the pipeline wallet.
    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: u128) -> Result<String, String>;
}
