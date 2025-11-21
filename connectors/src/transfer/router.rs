use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;

pub struct MultiChainTransferRouter<I, E> {
    pub icp: Arc<I>,
    pub evm: Arc<E>,
}

impl<I, E> MultiChainTransferRouter<I, E> {
    pub fn new(icp: Arc<I>, evm: Arc<E>) -> Self {
        Self { icp, evm }
    }
}

#[async_trait]
impl<I, E> TransferActions for MultiChainTransferRouter<I, E>
where
    I: TransferActions + Send + Sync,
    E: TransferActions + Send + Sync,
{
    async fn transfer(&self, token: &ChainToken, to: &ChainAccount, amount_native: Nat) -> Result<String, String> {
        // Validate that token and destination are on the same chain
        match (token, to) {
            (ChainToken::Icp { .. }, ChainAccount::Icp(_)) => self.icp.transfer(token, to, amount_native).await,
            (ChainToken::Icp { .. }, ChainAccount::IcpLedger(_)) => self.icp.transfer(token, to, amount_native).await,
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, ChainAccount::Evm(_)) => {
                self.evm.transfer(token, to, amount_native).await
            }
            _ => Err("invalid transfer configuration".to_string()),
        }
    }
}
