use core::fmt;
use std::str::FromStr;

use candid::CandidType;
use serde::{Deserialize, Serialize};

/// Canonical key for a token across chains.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, CandidType)]
pub struct AssetId {
    pub chain: String,   // "icp", "evm-arb", etc
    pub address: String, // ICP ledger principal or EVM address
    pub symbol: String,  // "ckBTC", "USDC", "ICP"
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.chain, self.address, self.symbol)
    }
}

impl FromStr for AssetId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return Err(format!("invalid AssetId `{}` (expected chain:address:symbol)", s));
        }

        Ok(AssetId {
            chain: parts[0].to_string(),
            address: parts[1].to_string(),
            symbol: parts[2].to_string(),
        })
    }
}
