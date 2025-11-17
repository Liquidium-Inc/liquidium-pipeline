use std::sync::Arc;

use futures::future::join_all;

use crate::{
    account::{
        actions::AccountInfo,
        model::ChainBalance,
    },
    tokens::{
        asset_id::AssetId,
        token_registry::TokenRegistry,
    },
};

/// Service for querying balances for a given account across a token registry.
/// This is pure core logic: it only depends on AccountInfo and TokenRegistry,
/// and has no knowledge of concrete backends (ICP, EVM, etc).
pub struct BalanceService {
    registry: Arc<TokenRegistry>,
    accounts: Arc<dyn AccountInfo + Send + Sync>,
}

impl BalanceService {
    pub fn new(registry: Arc<TokenRegistry>, accounts: Arc<dyn AccountInfo + Send + Sync>) -> Self {
        Self { registry, accounts }
    }

    /// Sync a custom list of assets for this account.
    pub async fn sync_assets(
        &self,
        assets: &[AssetId],
    ) -> Vec<Result<(AssetId, ChainBalance), String>> {
        let futs = assets.iter().cloned().map(|asset_id| {
            let accounts = self.accounts.clone();
            let token = self
                .registry
                .get(&asset_id)
                .expect("TokenRegistry out of sync with AssetId list")
                .clone();

            async move {
                let bal = accounts
                    .sync_balance(&token)
                    .await
                    .map_err(|e| format!("sync_balance failed for {}: {}", asset_id, e))?;
                Ok::<_, String>((asset_id, bal))
            }
        });

        join_all(futs).await
    }

    /// Sync all assets from the registry for this account.
    pub async fn sync_all(&self) -> Vec<Result<(AssetId, ChainBalance), String>> {
        let asset_ids: Vec<AssetId> = self.registry.tokens.keys().cloned().collect();
        self.sync_assets(&asset_ids).await
    }

    /// Sync all assets and return only non-zero balances.
    pub async fn non_zero(&self) -> Vec<(AssetId, ChainBalance)> {
        let results = self.sync_all().await;
        results
            .into_iter()
            .filter_map(|r| match r {
                Ok((id, bal)) if bal.amount_native > 0u128 => Some((id, bal)),
                _ => None,
            })
            .collect()
    }

    /// Get the balance for a single AssetId.
    pub async fn get_balance(
        &self,
        asset_id: &AssetId,
    ) -> Result<ChainBalance, String> {
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

    /// Expose the underlying registry if needed by callers.
    pub fn registry(&self) -> &TokenRegistry {
        &self.registry
    }

    /// Expose the underlying AccountInfo router if needed by callers.
    pub fn accounts(&self) -> Arc<dyn AccountInfo + Send + Sync> {
        self.accounts.clone()
    }
}