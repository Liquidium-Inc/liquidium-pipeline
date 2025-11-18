use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;

use crate::backend::evm_backend::EvmBackend;

pub struct EvmTransferAdapter<B: EvmBackend> {
    backend: Arc<B>,
}

impl<B: EvmBackend> EvmTransferAdapter<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl<B: EvmBackend + Send + Sync> TransferActions for EvmTransferAdapter<B> {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
    ) -> Result<String, String> {
        match (token, to) {
            (ChainToken::EvmNative { chain, .. }, ChainAccount::Evm(to_address)) => {
                let amount = amount_native;
                let tx_hash = self
                    .backend
                    .native_transfer(chain, to_address, amount)
                    .await?;

                Ok(tx_hash)
            }
            (
                ChainToken::EvmErc20 {
                    chain,
                    token_address,
                    ..
                },
                ChainAccount::Evm(to_address),
            ) => {
                let amount = Nat::from(amount_native);
                let tx_hash = self
                    .backend
                    .erc20_transfer(chain, token_address, to_address, amount)
                    .await?;

                Ok(tx_hash)
            }
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, _) => {
                Err("EvmTransferAdapter: destination chain must be EVM".to_string())
            }
            _ => Err("EvmTransferAdapter only supports EvmNative and EvmErc20 tokens".to_string()),
        }
    }
}
