use std::collections::HashMap;

use crate::tokens::{asset_id::AssetId, chain_token::ChainToken};

/// In-memory registry of all supported tokens.
#[derive(Clone, Debug)]
pub struct TokenRegistry {
    pub tokens: HashMap<AssetId, ChainToken>,
}

impl TokenRegistry {
    pub fn new(tokens: HashMap<AssetId, ChainToken>) -> Self {
        Self { tokens }
    }

    pub fn get(&self, id: &AssetId) -> Option<&ChainToken> {
        self.tokens.get(id)
    }

    pub fn all(&self) -> impl Iterator<Item = (&AssetId, &ChainToken)> {
        self.tokens.iter()
    }

    pub fn by_symbol(&self, symbol: &str) -> impl Iterator<Item = (&AssetId, &ChainToken)> {
        self.tokens.iter().filter(move |(_, t)| t.symbol() == symbol)
    }

    pub fn resolve(&self, id: &AssetId) -> Result<&ChainToken, String> {
        self.tokens.get(id).ok_or_else(|| format!("unknown asset id: {}", id))
    }
}
