// ERC-20 token client with automatic allowance management

use crate::{client::EvmClient, errors::EvmError, types::*};
use alloy::{providers::DynProvider, sol};

sol! {
    #[sol(rpc)]
    contract IERC20 {
        function decimals() external view returns (uint8);
        function symbol() external view returns (string memory);
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

#[derive(Clone)]
pub struct Erc20Client {
    pub token: Address,
    pub client: EvmClient,
}

impl Erc20Client {
    pub fn new(token: Address, client: EvmClient) -> Self {
        Self { token, client }
    }

    fn contract(&self) -> IERC20::IERC20Instance<DynProvider> {
        IERC20::new(self.token, self.client.provider.clone())
    }

    pub async fn decimals(&self) -> Result<u8, EvmError> {
        self.contract()
            .decimals()
            .call()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))
    }

    pub async fn symbol(&self) -> Result<String, EvmError> {
        self.contract()
            .symbol()
            .call()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))
    }

    pub async fn balance_of(&self, account: Address) -> Result<U256, EvmError> {
        self.contract()
            .balanceOf(account)
            .call()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))
    }

    pub async fn allowance(&self, owner: Address, spender: Address) -> Result<U256, EvmError> {
        self.contract()
            .allowance(owner, spender)
            .call()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))
    }

    pub async fn approve(&self, spender: Address, amount: U256) -> Result<TransactionReceipt, EvmError> {
        let res = self.contract().approve(spender, amount).send().await?;
        res.get_receipt().await.map_err(|e| EvmError::Other(e.to_string()))
    }

    pub async fn transfer(&self, to: Address, amount: U256) -> Result<TransactionReceipt, EvmError> {
        let res = self.contract().transfer(to, amount).send().await?;
        res.get_receipt().await.map_err(|e| EvmError::Other(e.to_string()))
    }

    // Ensure allowance is at least `min_amount`. Returns receipt if approval was needed.
    pub async fn ensure_allowance(
        &self,
        spender: Address,
        min_amount: U256,
    ) -> Result<Option<TransactionReceipt>, EvmError> {
        let current = self.allowance(self.client.from, spender).await?;

        if current >= min_amount {
            return Ok(None);
        }

        // Approve max to avoid future approvals
        let receipt = self.approve(spender, U256::MAX).await?;
        Ok(Some(receipt))
    }
}
