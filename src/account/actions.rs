use async_trait::async_trait;

use crate::account::model::{ChainBalance, ChainToken, TxRef};

#[async_trait]
pub trait AccountInfo: Send + Sync {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance>;
}

#[async_trait]
pub trait AccountActions: Send + Sync {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &str,
        amount_native: u128,
        from_subaccount: bool,
    ) -> Result<TxRef, String>;
}
