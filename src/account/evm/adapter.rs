use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::account::actions::{AccountActions, AccountInfo};
use crate::account::evm::model::EvmBackend;
use crate::account::model::{ChainBalance, ChainToken, TxRef};

pub struct EvmAccountInfoAdapter<B: EvmBackend> {
    backend: Arc<B>,
    // (chain, token_address) -> ChainBalance
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
            ChainToken::Evm {
                chain,
                token_address,
                symbol,
                decimals,
            } => {
                let amount_native = self.backend.erc20_balance(chain, token_address).await?;

                Ok(ChainBalance {
                    chain: chain.clone(),
                    symbol: symbol.clone(),
                    amount_native,
                    decimals: *decimals,
                })
            }
            _ => Err("EvmAccountInfoAdapter only supports Evm tokens".to_string()),
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        let bal = self.get_balance(token).await?;

        if let ChainToken::Evm {
            chain, token_address, ..
        } = token
        {
            let mut lock = self.cache.lock().unwrap();
            lock.insert((chain.clone(), token_address.clone()), bal.clone());
        }

        Ok(bal)
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance> {
        if let ChainToken::Evm {
            chain, token_address, ..
        } = token
        {
            let lock = self.cache.lock().unwrap();
            return lock.get(&(chain.clone(), token_address.clone())).cloned();
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
        amount_native: u128,
        _from_subaccount: bool,
    ) -> Result<TxRef, String> {
        match token {
            ChainToken::Evm {
                chain, token_address, ..
            } => {
                let tx_hash = self
                    .backend
                    .erc20_transfer(chain, token_address, to, amount_native)
                    .await?;

                Ok(TxRef::EvmTxHash(tx_hash))
            }
            _ => Err("EvmAccountActionsAdapter only supports Evm tokens".to_string()),
        }
    }
}
