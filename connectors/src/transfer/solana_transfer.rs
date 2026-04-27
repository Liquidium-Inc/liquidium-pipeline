use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;

use crate::backend::solana_backend::SolanaBackend;

pub struct SolanaTransferAdapter<B: SolanaBackend> {
    backend: Arc<B>,
}

impl<B: SolanaBackend> SolanaTransferAdapter<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl<B: SolanaBackend + Send + Sync> TransferActions for SolanaTransferAdapter<B> {
    async fn transfer(&self, token: &ChainToken, to: &ChainAccount, amount_native: Nat) -> Result<String, String> {
        match (token, to) {
            (ChainToken::SolanaNative { .. }, ChainAccount::Solana(to_address)) => {
                self.backend.native_transfer(to_address, amount_native).await
            }
            (ChainToken::SolanaSpl { mint, .. }, ChainAccount::Solana(to_address)) => {
                self.backend.spl_transfer(mint, to_address, amount_native).await
            }
            (ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. }, _) => {
                Err("SolanaTransferAdapter: destination chain must be Solana".to_string())
            }
            _ => Err("SolanaTransferAdapter only supports Solana tokens".to_string()),
        }
    }

    async fn approve(
        &self,
        _token: &ChainToken,
        _spender: &ChainAccount,
        _amount_native: Nat,
    ) -> Result<String, String> {
        Err("SolanaTransferAdapter does not support approve".to_string())
    }
}
