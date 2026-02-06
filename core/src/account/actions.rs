use crate::error::AccountError;
use crate::tokens::chain_token::ChainToken;
use crate::tokens::chain_token_amount::ChainTokenAmount;

use async_trait::async_trait;

#[mockall::automock]
#[async_trait]
pub trait AccountInfo: Send + Sync {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, AccountError>;
    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, AccountError>;
    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainTokenAmount>;
}
