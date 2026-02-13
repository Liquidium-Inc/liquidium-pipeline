use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::error::{AppError, AppResult, error_codes};
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
    async fn transfer(&self, token: &ChainToken, to: &ChainAccount, amount_native: Nat) -> AppResult<String> {
        match (token, to) {
            (ChainToken::EvmNative { chain, .. }, ChainAccount::Evm(to_address)) => {
                let amount = amount_native;
                let tx_hash = self.backend.native_transfer(chain, to_address, amount).await?;

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
                    .await?;

                Ok(tx_hash)
            }
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, _) => {
                Err(AppError::from_def(error_codes::INVALID_INPUT)
                    .with_context("EvmTransferAdapter: destination chain must be EVM"))
            }
            _ => Err(AppError::from_def(error_codes::UNSUPPORTED)
                .with_context("EvmTransferAdapter only supports EvmNative and EvmErc20 tokens")),
        }
    }

    async fn approve(&self, _token: &ChainToken, _spender: &ChainAccount, _amount_native: Nat) -> AppResult<String> {
        Err(AppError::from_def(error_codes::UNSUPPORTED).with_context("EvmTransferAdapter does not support approve"))
    }
}
