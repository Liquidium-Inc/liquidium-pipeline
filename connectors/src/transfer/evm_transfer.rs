use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::{TransferActions, TransferFailure};

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
    ) -> Result<String, TransferFailure> {
        match (token, to) {
            // A broadcast that fails is ambiguous: the transaction may still be
            // in a mempool and land later. This backend reports failures as
            // plain strings, so there is nothing finer to classify on.
            (ChainToken::EvmNative { chain, .. }, ChainAccount::Evm(to_address)) => {
                let amount = amount_native;
                let tx_hash = self
                    .backend
                    .native_transfer(chain, to_address, amount)
                    .await
                    .map_err(TransferFailure::Ambiguous)?;

                Ok(tx_hash)
            }
            (
                ChainToken::EvmErc20 {
                    chain, token_address, ..
                },
                ChainAccount::Evm(to_address),
            ) => {
                let amount = amount_native;
                let tx_hash = self
                    .backend
                    .erc20_transfer(chain, token_address, to_address, amount)
                    .await
                    .map_err(TransferFailure::Ambiguous)?;

                Ok(tx_hash)
            }
            // Routing refusals never reach a chain, so nothing moved.
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, _) => Err(TransferFailure::Rejected(
                "EvmTransferAdapter: destination chain must be EVM".to_string(),
            )),
            _ => Err(TransferFailure::Rejected(
                "EvmTransferAdapter only supports EvmNative and EvmErc20 tokens".to_string(),
            )),
        }
    }

    async fn approve(
        &self,
        _token: &ChainToken,
        _spender: &ChainAccount,
        _amount_native: Nat,
    ) -> Result<String, String> {
        Err("EvmTransferAdapter does not support approve".to_string())
    }
}
