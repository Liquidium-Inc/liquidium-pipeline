use std::sync::Arc;

use futures::future::join_all;

use crate::{
    account::actions::AccountInfo,
    tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait},
};

/// Service for querying balances for a given account across a token registry.
/// This is pure core logic: it only depends on AccountInfo and TokenRegistry,
/// and has no knowledge of concrete backends (ICP, EVM, etc).
pub struct BalanceService {
    registry: Arc<dyn TokenRegistryTrait>,
    accounts: Arc<dyn AccountInfo + Send + Sync>,
}

impl BalanceService {
    pub fn new(registry: Arc<dyn TokenRegistryTrait>, accounts: Arc<dyn AccountInfo + Send + Sync>) -> Self {
        Self { registry, accounts }
    }

    // Sync a custom list of assets for this account.
    pub async fn sync_assets(&self, assets: &[AssetId]) -> Vec<Result<(AssetId, ChainTokenAmount), String>> {
        let futs = assets.iter().map(|asset_id| {
            let accounts = self.accounts.clone();
            let token = match self.registry.get(asset_id) {
                Some(t) => t.clone(),
                None => {
                    return futures::future::Either::Left(async move {
                        Err::<_, String>(format!("unknown asset {}", asset_id))
                    })
                }
            };

            futures::future::Either::Right(async move {
                let bal = accounts
                    .sync_balance(&token)
                    .await
                    .map_err(|e| format!("sync_balance failed for {}: {}", asset_id, e))?;
                Ok::<_, String>((asset_id.clone(), bal))
            })
        });

        join_all(futs).await
    }

    // Sync all assets from the registry for this account.
    pub async fn sync_all(&self) -> Vec<Result<(AssetId, ChainTokenAmount), String>> {
        let asset_ids: Vec<AssetId> = self.registry.all().iter().map(|item| item.0.clone()).collect();
        self.sync_assets(&asset_ids).await
    }

    // Sync all assets and return only non-zero balances.
    pub async fn non_zero(&self) -> Vec<(AssetId, ChainTokenAmount)> {
        let results = self.sync_all().await;
        results
            .into_iter()
            .filter_map(|r| match r {
                Ok((id, bal)) if bal.value > 0u128 => Some((id, bal)),
                _ => None,
            })
            .collect()
    }

    // Get the balance for a single AssetId.
    pub async fn get_balance(&self, asset_id: &AssetId) -> Result<ChainTokenAmount, String> {
        let token = self
            .registry
            .get(asset_id)
            .ok_or_else(|| format!("unknown asset {}", asset_id))?
            .clone();

        self.accounts
            .sync_balance(&token)
            .await
            .map_err(|e| format!("sync_balance failed for {}: {}", asset_id, e))
    }

    // Expose the underlying registry if needed by callers.
    pub fn registry(&self) -> Arc<dyn TokenRegistryTrait> {
        self.registry.clone()
    }

    // Expose the underlying AccountInfo router if needed by callers.
    pub fn accounts(&self) -> Arc<dyn AccountInfo + Send + Sync> {
        self.accounts.clone()
    }
}
