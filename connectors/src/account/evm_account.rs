use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::Nat;

// All these come from core
use liquidium_pipeline_core::account::actions::{AccountActions, AccountInfo};
use liquidium_pipeline_core::account::model::{ChainBalance, TxRef};
use liquidium_pipeline_core::tokens::chain_token::ChainToken;

// This comes from connectors
use crate::backend::evm_backend::EvmBackend;

pub struct EvmAccountInfoAdapter<B: EvmBackend> {
    pub backend: Arc<B>,
    // Cache key: (chain, address_or_native) -> ChainBalance
    // For native assets, use "native" as the address
    cache: Mutex<HashMap<(String, String), ChainBalance>>,
}

impl<B: EvmBackend> EvmAccountInfoAdapter<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<B: EvmBackend> AccountInfo for EvmAccountInfoAdapter<B> {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        match token {
            ChainToken::EvmNative {
                chain,
                symbol,
                decimals,
            } => {
                let amount_native = self.backend.native_balance(chain).await?;

                Ok(ChainBalance {
                    chain: liquidium_pipeline_core::account::model::Chain::Evm { chain: chain.clone() },
                    symbol: symbol.clone(),
                    amount_native,
                    decimals: *decimals,
                })
            }
            ChainToken::EvmErc20 {
                chain,
                token_address,
                symbol,
                decimals,
            } => {
                let amount_native = self.backend.erc20_balance(chain, token_address).await?;

                Ok(ChainBalance {
                    chain: liquidium_pipeline_core::account::model::Chain::Evm { chain: chain.clone() },
                    symbol: symbol.clone(),
                    amount_native,
                    decimals: *decimals,
                })
            }
            _ => Err("EvmAccountInfoAdapter only supports EvmNative and EvmErc20 tokens".to_string()),
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        let bal = self.get_balance(token).await?;

        let cache_key = match token {
            ChainToken::EvmNative { chain, .. } => Some((chain.clone(), "native".to_string())),
            ChainToken::EvmErc20 { chain, token_address, .. } => Some((chain.clone(), token_address.clone())),
            _ => None,
        };

        if let Some((chain, addr)) = cache_key {
            let mut lock = self.cache.lock().unwrap();
            lock.insert((chain, addr), bal.clone());
        }

        Ok(bal)
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance> {
        let cache_key = match token {
            ChainToken::EvmNative { chain, .. } => Some((chain.clone(), "native".to_string())),
            ChainToken::EvmErc20 { chain, token_address, .. } => Some((chain.clone(), token_address.clone())),
            _ => None,
        };

        if let Some((chain, addr)) = cache_key {
            let lock = self.cache.lock().unwrap();
            return lock.get(&(chain, addr)).cloned();
        }
        None
    }
}

pub struct EvmAccountActionsAdapter<B: EvmBackend> {
    backend: Arc<B>,
}

impl<B: EvmBackend> EvmAccountActionsAdapter<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl<B: EvmBackend> AccountActions for EvmAccountActionsAdapter<B> {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &str,
        amount: Nat,
        _from_subaccount: bool,
    ) -> Result<TxRef, String> {
        match token {
            ChainToken::EvmNative { chain, .. } => {
                let tx_hash = self.backend.native_transfer(chain, to, amount).await?;
                Ok(TxRef::EvmTxHash(tx_hash))
            }
            ChainToken::EvmErc20 {
                chain, token_address, ..
            } => {
                let tx_hash = self.backend.erc20_transfer(chain, token_address, to, amount).await?;
                Ok(TxRef::EvmTxHash(tx_hash))
            }
            _ => Err("EvmAccountActionsAdapter only supports EvmNative and EvmErc20 tokens".to_string()),
        }
    }
}
