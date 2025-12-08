use candid::{Nat, Principal};
use serde::{Deserialize, Serialize};

use crate::types::protocol_types::{Asset, Assets};

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcrcToken {
    // The ledger id
    pub ledger: Principal,
    // icrc1:decimals
    pub decimals: u8,
    // icrc1:name
    pub name: String,
    // icrc1:symbol
    pub symbol: String,
    // icrc1:fee (in the smallest units)
    pub fee: Nat,
}

impl From<(Assets, Principal)> for IcrcToken {
    fn from(value: (Assets, Principal)) -> Self {
        IcrcToken {
            ledger: value.1,
            decimals: value.0.decimals() as u8,
            name: value.0.symbol(),
            symbol: value.0.symbol(),
            fee: Nat::from(0u8), // TODO: Set this to the real value
        }
    }
}
