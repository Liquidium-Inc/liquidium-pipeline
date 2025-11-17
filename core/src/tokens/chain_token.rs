use std::{fmt, str::FromStr};

use candid::{CandidType, Principal};
use serde::{Deserialize, Serialize};

use crate::{account::model::Chain, tokens::{asset_id::AssetId, icrc::icrc_token::IcrcToken}};

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq)]
pub enum ChainToken {
    Icp {
        ledger: Principal,
        symbol: String,
        decimals: u8,
    },
    Evm {
        chain: Chain,
        token_address: String,
        symbol: String,
        decimals: u8,
    },
}

impl ChainToken {
    pub fn chain(&self) -> String {
        match self {
            ChainToken::Icp { .. } => "ICP".to_string(),
            ChainToken::Evm { chain, .. } => chain.to_string(),
        }
    }

    pub fn symbol(&self) -> String {
        match self {
            ChainToken::Icp { symbol, .. } => symbol.clone(),
            ChainToken::Evm { symbol, .. } => symbol.clone(),
        }
    }

    pub fn decimals(&self) -> u8 {
        match self {
            ChainToken::Icp { decimals, .. } => decimals.clone(),
            ChainToken::Evm { decimals, .. } => decimals.clone(),
        }
    }

    pub fn asset_id(&self) -> AssetId {
        match self {
            ChainToken::Icp { ledger, symbol, .. } => AssetId {
                chain: "icp".to_string(),
                address: ledger.to_text(),
                symbol: symbol.clone(),
            },

            ChainToken::Evm {
                chain,
                token_address,
                symbol,
                ..
            } => AssetId {
                chain: chain.to_string(),
                address: token_address.clone(),
                symbol: symbol.clone(),
            },
        }
    }
}

impl fmt::Display for ChainToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainToken::Icp { symbol, .. } => {
                write!(f, "ICP:{}", symbol)
            }
            ChainToken::Evm {
                chain,
                token_address,
                symbol,
                ..
            } => {
                // format: EVM[chain]:symbol@address
                write!(f, "EVM[{}]:{}@{}", chain, symbol, token_address)
            }
        }
    }
}


impl From<IcrcToken> for ChainToken {
    fn from(value: IcrcToken) -> Self {
        ChainToken::Icp {
            ledger: value.ledger,
            symbol: value.symbol,
            decimals: value.decimals,
        }
    }
}

impl From<&IcrcToken> for ChainToken {
    fn from(value: &IcrcToken) -> Self {
        value.clone().into()
    }
}
