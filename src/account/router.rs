use std::sync::Arc;

use async_trait::async_trait;

use crate::account::actions::{AccountActions, AccountInfo};
use crate::account::model::{ChainBalance, ChainToken, TxRef};

pub struct MultiChainAccountInfoRouter {
    pub icp: Arc<dyn AccountInfo + Send + Sync>,
    pub evm: Arc<dyn AccountInfo + Send + Sync>,
}

impl MultiChainAccountInfoRouter {
    pub fn new(icp: Arc<dyn AccountInfo + Send + Sync>, evm: Arc<dyn AccountInfo + Send + Sync>) -> Self {
        Self { icp, evm }
    }
}

#[async_trait]
impl AccountInfo for MultiChainAccountInfoRouter {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        match token {
            ChainToken::Icp { .. } => self.icp.get_balance(token).await,
            ChainToken::Evm { .. } => self.evm.get_balance(token).await,
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        match token {
            ChainToken::Icp { .. } => self.icp.sync_balance(token).await,
            ChainToken::Evm { .. } => self.evm.sync_balance(token).await,
        }
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance> {
        match token {
            ChainToken::Icp { .. } => self.icp.get_cached_balance(token),
            ChainToken::Evm { .. } => self.evm.get_cached_balance(token),
        }
    }
}

pub struct MultiChainAccountActionsRouter {
    pub icp: Arc<dyn AccountActions + Send + Sync>,
    pub evm: Arc<dyn AccountActions + Send + Sync>,
}

impl MultiChainAccountActionsRouter {
    pub fn new(icp: Arc<dyn AccountActions + Send + Sync>, evm: Arc<dyn AccountActions + Send + Sync>) -> Self {
        Self { icp, evm }
    }
}

#[async_trait]
impl AccountActions for MultiChainAccountActionsRouter {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &str,
        amount_native: u128,
        from_subaccount: bool,
    ) -> Result<TxRef, String> {
        match token {
            ChainToken::Icp { .. } => self.icp.transfer(token, to, amount_native, from_subaccount).await,
            ChainToken::Evm { .. } => self.evm.transfer(token, to, amount_native, from_subaccount).await,
        }
    }
}
