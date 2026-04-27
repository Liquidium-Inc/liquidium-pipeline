use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;
use tracing::instrument;

pub struct MultiChainTransferRouter<I, E, S> {
    pub icp: Arc<I>,
    pub evm: Arc<E>,
    pub sol: Arc<S>,
}

impl<I, E, S> MultiChainTransferRouter<I, E, S> {
    pub fn new(icp: Arc<I>, evm: Arc<E>, sol: Arc<S>) -> Self {
        Self { icp, evm, sol }
    }
}

#[async_trait]
impl<I, E, S> TransferActions for MultiChainTransferRouter<I, E, S>
where
    I: TransferActions + Send + Sync,
    E: TransferActions + Send + Sync,
    S: TransferActions + Send + Sync,
{
    #[instrument(name = "transfer_router.transfer", skip_all, err, fields(token = %token.symbol()))]
    async fn transfer(&self, token: &ChainToken, to: &ChainAccount, amount_native: Nat) -> Result<String, String> {
        // Validate that token and destination are on the same chain
        match (token, to) {
            (ChainToken::Icp { .. }, ChainAccount::Icp(_)) => self.icp.transfer(token, to, amount_native).await,
            (ChainToken::Icp { .. }, ChainAccount::IcpLedger(_)) => self.icp.transfer(token, to, amount_native).await,
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, ChainAccount::Evm(_)) => {
                self.evm.transfer(token, to, amount_native).await
            }
            (ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. }, ChainAccount::Solana(_)) => {
                self.sol.transfer(token, to, amount_native).await
            }
            _ => Err("invalid transfer configuration".to_string()),
        }
    }

    #[instrument(name = "transfer_router.approve", skip_all, err, fields(token = %token.symbol()))]
    async fn approve(&self, token: &ChainToken, spender: &ChainAccount, amount_native: Nat) -> Result<String, String> {
        match token {
            ChainToken::Icp { .. } => self.icp.approve(token, spender, amount_native).await,
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                self.evm.approve(token, spender, amount_native).await
            }
            ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => {
                self.sol.approve(token, spender, amount_native).await
            }
        }
    }
}
