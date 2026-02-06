use std::sync::Arc;

use candid::Nat;

use crate::account::model::ChainAccount;
use crate::error::{CoreError, CoreResult, TokenRegistryError};
use crate::tokens::asset_id::AssetId;
use crate::tokens::token_registry::{TokenRegistry, TokenRegistryTrait};
use crate::transfer::actions::TransferActions;

// Service for transferring tokens across chains using a token registry.
// This is pure core logic: it only depends on TransferActions and TokenRegistry,
// and has no knowledge of concrete backends (ICP, EVM, etc).
pub struct TransferService {
    registry: Arc<dyn TokenRegistryTrait>,
    actions: Arc<dyn TransferActions + Send + Sync>,
}

impl TransferService {
    pub fn new(registry: Arc<TokenRegistry>, actions: Arc<dyn TransferActions + Send + Sync>) -> Self {
        Self { registry, actions }
    }

    // Transfer a token identified by AssetId to a ChainAccount.
    pub async fn transfer_by_asset_id(
        &self,
        asset_id: &AssetId,
        to: ChainAccount,
        amount_native: Nat,
    ) -> CoreResult<String> {
        let token = self
            .registry
            .get(asset_id)
            .ok_or_else(|| TokenRegistryError::UnknownAsset {
                asset_id: asset_id.clone(),
            })?;

        self.actions
            .transfer(&token, &to, amount_native)
            .await
            .map_err(CoreError::from)
    }

    // Expose the underlying registry if needed by callers.
    pub fn registry(&self) -> Arc<dyn TokenRegistryTrait> {
        self.registry.clone()
    }

    // Expose the underlying TransferActions router if needed by callers.
    pub fn actions(&self) -> Arc<dyn TransferActions + Send + Sync> {
        self.actions.clone()
    }
}
