use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use liquidium_pipeline_core::account::actions::AccountInfo;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::backend::solana_backend::SolanaBackend;

const CACHE_TTL: Duration = Duration::from_secs(30);

pub struct SolanaAccountInfoAdapter<B: SolanaBackend> {
    backend: Arc<B>,
    // Cache key: "native" or SPL mint address.
    cache: Mutex<HashMap<String, (ChainTokenAmount, Instant)>>,
}

impl<B: SolanaBackend> SolanaAccountInfoAdapter<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<B: SolanaBackend> AccountInfo for SolanaAccountInfoAdapter<B> {
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, String> {
        match token {
            ChainToken::SolanaNative { .. } => {
                let amount_native = self.backend.native_balance().await?;
                Ok(ChainTokenAmount {
                    token: token.clone(),
                    value: amount_native,
                })
            }
            ChainToken::SolanaSpl { mint, .. } => {
                let amount_native = self.backend.spl_balance(mint).await?;
                Ok(ChainTokenAmount {
                    token: token.clone(),
                    value: amount_native,
                })
            }
            _ => Err("SolanaAccountInfoAdapter only supports Solana tokens".to_string()),
        }
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, String> {
        let bal = self.get_balance(token).await?;
        let cache_key = match token {
            ChainToken::SolanaNative { .. } => Some("native".to_string()),
            ChainToken::SolanaSpl { mint, .. } => Some(mint.clone()),
            _ => None,
        };

        if let Some(key) = cache_key {
            let mut lock = self
                .cache
                .lock()
                .map_err(|_| "solana balance cache poisoned".to_string())?;
            lock.insert(key, (bal.clone(), Instant::now()));
        }

        Ok(bal)
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainTokenAmount> {
        let cache_key = match token {
            ChainToken::SolanaNative { .. } => Some("native".to_string()),
            ChainToken::SolanaSpl { mint, .. } => Some(mint.clone()),
            _ => None,
        }?;

        let mut is_stale = false;
        let mut result = None;

        {
            let lock = self.cache.lock().ok()?;
            if let Some((amount, ts)) = lock.get(&cache_key) {
                if ts.elapsed() <= CACHE_TTL {
                    result = Some(amount.clone());
                } else {
                    is_stale = true;
                }
            }
        }

        if is_stale && let Ok(mut lock) = self.cache.lock() {
            lock.remove(&cache_key);
        }

        result
    }
}
