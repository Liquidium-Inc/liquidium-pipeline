use core::fmt;

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
