use async_trait::async_trait;
use candid::Nat;

use crate::account::model::{ChainBalance, TxRef};
use crate::tokens::chain_token::ChainToken;

#[async_trait]
pub trait AccountInfo: Send + Sync {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait AccountActions: Send + Sync {
    async fn transfer(&self, token: &ChainToken, to: &str, amount: Nat, from_subaccount: bool)  -> Result<TxRef, String>;
}
