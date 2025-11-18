use crate::account::model::ChainBalance;
use crate::tokens::chain_token::ChainToken;
use async_trait::async_trait;

#[async_trait]
pub trait AccountInfo: Send + Sync {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance>;
}
