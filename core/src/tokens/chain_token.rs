use std::fmt;

use candid::{CandidType, Principal};
use serde::{Deserialize, Serialize};

use crate::tokens::{asset_id::AssetId, icrc::icrc_token::IcrcToken};

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq)]
pub enum ChainToken {
    Icp {
        ledger: Principal,
        symbol: String,
        decimals: u8,
    },
    EvmNative {
        chain: String,     // "eth", "arb", "base"
        symbol: String,    // "ETH"
        decimals: u8,      // usually 18
    },
    EvmErc20 {
        chain: String,          // "eth", "arb"
        token_address: String,  // "0x..."
        symbol: String,
        decimals: u8,
    },
}

impl ChainToken {
    pub fn chain(&self) -> String {
        match self {
            ChainToken::Icp { .. } => "ICP".to_string(),
            ChainToken::EvmNative { chain, .. } => format!("evm-{}", chain),
            ChainToken::EvmErc20 { chain, .. } => format!("evm-{}", chain),
        }
    }

    pub fn symbol(&self) -> String {
        match self {
            ChainToken::Icp { symbol, .. } => symbol.clone(),
            ChainToken::EvmNative { symbol, .. } => symbol.clone(),
            ChainToken::EvmErc20 { symbol, .. } => symbol.clone(),
        }
    }

    pub fn decimals(&self) -> u8 {
        match self {
            ChainToken::Icp { decimals, .. } => decimals.clone(),
            ChainToken::EvmNative { decimals, .. } => decimals.clone(),
            ChainToken::EvmErc20 { decimals, .. } => decimals.clone(),
        }
    }

    pub fn asset_id(&self) -> AssetId {
        match self {
            ChainToken::Icp { ledger, symbol, .. } => AssetId {
                chain: "icp".to_string(),
                address: ledger.to_text(),
                symbol: symbol.clone(),
            },

            ChainToken::EvmNative { chain, symbol, .. } => AssetId {
                chain: format!("evm-{}", chain),
                address: "native".to_string(),
                symbol: symbol.clone(),
            },

            ChainToken::EvmErc20 {
                chain,
                token_address,
                symbol,
                ..
            } => AssetId {
                chain: format!("evm-{}", chain),
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
            ChainToken::EvmNative { chain, symbol, .. } => {
                write!(f, "EVM[{}]:{}@native", chain, symbol)
            }
            ChainToken::EvmErc20 {
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
