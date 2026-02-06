use std::collections::{HashMap, HashSet};

use crate::error::TokenRegistryError;
use crate::tokens::{asset_id::AssetId, chain_token::ChainToken};

// In-memory registry of all supported tokens.
// Also tracks which assets are usable as collateral and/or debt.
#[derive(Clone, Debug)]
pub struct TokenRegistry {
    pub tokens: HashMap<AssetId, ChainToken>,
    collateral_ids: HashSet<AssetId>,
    debt_ids: HashSet<AssetId>,
}

// Registry trait for resolving ChainToken from a position
#[mockall::automock]
pub trait TokenRegistryTrait: Send + Sync {
    fn get(&self, id: &AssetId) -> Option<ChainToken>;
    fn all(&self) -> Vec<(AssetId, ChainToken)>;
    fn by_symbol(&self, symbol: &str) -> Vec<(AssetId, ChainToken)>;
    fn resolve(&self, id: &AssetId) -> Result<ChainToken, TokenRegistryError>;

    // Returns all assets that are marked as eligible collateral.
    fn collateral_assets(&self) -> Vec<(AssetId, ChainToken)>;
    // Returns all assets that are marked as eligible debt assets.
    fn debt_assets(&self) -> Vec<(AssetId, ChainToken)>;
}

impl TokenRegistry {
    pub fn new(tokens: HashMap<AssetId, ChainToken>) -> Self {
        Self {
            tokens,
            collateral_ids: HashSet::new(),
            debt_ids: HashSet::new(),
        }
    }

    // Construct a registry with explicit collateral/debt classification.
    pub fn with_roles(
        tokens: HashMap<AssetId, ChainToken>,
        collateral_ids: impl IntoIterator<Item = AssetId>,
        debt_ids: impl IntoIterator<Item = AssetId>,
    ) -> Self {
        let collateral_ids: HashSet<AssetId> = collateral_ids.into_iter().collect();
        let debt_ids: HashSet<AssetId> = debt_ids.into_iter().collect();
        Self {
            tokens,
            collateral_ids,
            debt_ids,
        }
    }

    // Mark a token as eligible collateral.
    pub fn mark_collateral(&mut self, id: AssetId) {
        self.collateral_ids.insert(id);
    }

    // Mark a token as eligible debt asset.
    pub fn mark_debt(&mut self, id: AssetId) {
        self.debt_ids.insert(id);
    }
}

impl TokenRegistryTrait for TokenRegistry {
    fn get(&self, id: &AssetId) -> Option<ChainToken> {
        self.tokens.get(id).cloned()
    }

    fn all(&self) -> Vec<(AssetId, ChainToken)> {
        self.tokens
            .iter()
            .map(|item| (item.0.clone(), item.1.clone()))
            .collect::<Vec<(AssetId, ChainToken)>>()
    }

    fn by_symbol(&self, symbol: &str) -> Vec<(AssetId, ChainToken)> {
        self.tokens
            .iter()
            .filter(move |(_, t)| t.symbol() == symbol)
            .map(|item| (item.0.clone(), item.1.clone()))
            .collect()
    }

    fn resolve(&self, id: &AssetId) -> Result<ChainToken, TokenRegistryError> {
        self.tokens
            .get(id)
            .cloned()
            .ok_or_else(|| TokenRegistryError::UnknownAsset { asset_id: id.clone() })
    }

    fn collateral_assets(&self) -> Vec<(AssetId, ChainToken)> {
        self.tokens
            .iter()
            .filter(|(id, _)| self.collateral_ids.contains(*id))
            .map(|(id, tok)| (id.clone(), tok.clone()))
            .collect()
    }

    fn debt_assets(&self) -> Vec<(AssetId, ChainToken)> {
        self.tokens
            .iter()
            .filter(|(id, _)| self.debt_ids.contains(*id))
            .map(|(id, tok)| (id.clone(), tok.clone()))
            .collect()
    }
}
