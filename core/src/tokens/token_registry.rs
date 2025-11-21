use std::collections::HashMap;

use crate::tokens::{asset_id::AssetId, chain_token::ChainToken};

// In-memory registry of all supported tokens.
#[derive(Clone, Debug)]
pub struct TokenRegistry {
    pub tokens: HashMap<AssetId, ChainToken>,
}

// Registry trait for resolving ChainToken from a position
#[mockall::automock]
pub trait TokenRegistryTrait: Send + Sync {
    fn get(&self, id: &AssetId) -> Option<ChainToken>;
    fn all(&self) -> Vec<(AssetId, ChainToken)>;
    fn by_symbol(&self, symbol: &str) -> Vec<(AssetId, ChainToken)>;
    fn resolve(&self, id: &AssetId) -> Result<ChainToken, String>;
}

impl TokenRegistry {
    pub fn new(tokens: HashMap<AssetId, ChainToken>) -> Self {
        Self { tokens }
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

    fn resolve(&self, id: &AssetId) -> Result<ChainToken, String> {
        self.tokens
            .get(id)
            .ok_or_else(|| format!("unknown asset id: {}", id))
            .cloned()
    }
}
