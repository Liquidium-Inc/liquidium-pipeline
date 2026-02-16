use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use liquidium_pipeline_core::error::{AppError, error_codes};

// All these come from core
use liquidium_pipeline_core::account::actions::AccountInfo;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

// This comes from connectors
use crate::backend::evm_backend::EvmBackend;

const CACHE_TTL: Duration = Duration::from_secs(30);

pub struct EvmAccountInfoAdapter<B: EvmBackend> {
    pub backend: Arc<B>,
    // Cache key: (chain, address_or_native) -> ChainTokenAmount
    // For native assets, use "native" as the address
    cache: Mutex<HashMap<(String, String), (ChainTokenAmount, Instant)>>,
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
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, AppError> {
        match token {
            ChainToken::EvmNative { chain, .. } => {
                let amount_native = self.backend.native_balance(chain).await?;

                Ok(ChainTokenAmount {
                    token: token.clone(),
                    value: amount_native,
                })
            }
            ChainToken::EvmErc20 {
                chain, token_address, ..
            } => {
                let amount_native = self.backend.erc20_balance(chain, token_address).await?;

                Ok(ChainTokenAmount {
                    token: token.clone(),
                    value: amount_native,
                })
            }
            _ => Err(AppError::from_def(error_codes::UNSUPPORTED)
                .with_context("EvmAccountInfoAdapter only supports EvmNative and EvmErc20 tokens")),
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, AppError> {
        let bal = self.get_balance(token).await?;

        let cache_key = match token {
            ChainToken::EvmNative { chain, .. } => Some((chain.clone(), "native".to_string())),
            ChainToken::EvmErc20 {
                chain, token_address, ..
            } => Some((chain.clone(), token_address.clone())),
            _ => None,
        };

        if let Some((chain, addr)) = cache_key {
            let mut lock = self.cache.lock().map_err(|_| {
                AppError::from_def(error_codes::INTERNAL_ERROR).with_context("evm balance cache poisoned")
            })?;
            lock.insert((chain, addr), (bal.clone(), Instant::now()));
        }

        Ok(bal)
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainTokenAmount> {
        let cache_key = match token {
            ChainToken::EvmNative { chain, .. } => Some((chain.clone(), "native".to_string())),
            ChainToken::EvmErc20 {
                chain, token_address, ..
            } => Some((chain.clone(), token_address.clone())),
            _ => None,
        };

        let (chain, addr) = cache_key?;

        let mut is_stale = false;
        let mut result = None;

        {
            let lock = self.cache.lock().ok()?;
            if let Some((amount, ts)) = lock.get(&(chain.clone(), addr.clone())) {
                if ts.elapsed() <= CACHE_TTL {
                    result = Some(amount.clone());
                } else {
                    is_stale = true;
                }
            }
        }

        if is_stale && let Ok(mut lock) = self.cache.lock() {
            lock.remove(&(chain, addr));
        }

        result
    }
}
