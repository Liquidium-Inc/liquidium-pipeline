use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_core::error::AppResult;
use liquidium_pipeline_core::{
    account::actions::AccountInfo,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
};

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
    async fn get_balance(&self, token: &ChainToken) -> AppResult<ChainTokenAmount> {
        match token {
            ChainToken::Icp { .. } => self.icp.get_balance(token).await,
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => self.evm.get_balance(token).await,
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> AppResult<ChainTokenAmount> {
        match token {
            ChainToken::Icp { .. } => self.icp.sync_balance(token).await,
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => self.evm.sync_balance(token).await,
        }
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainTokenAmount> {
        match token {
            ChainToken::Icp { .. } => self.icp.get_cached_balance(token),
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => self.evm.get_cached_balance(token),
        }
    }
}
