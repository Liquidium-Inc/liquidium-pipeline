use std::fmt;

use candid::{CandidType, Nat, Principal};
use serde::{Deserialize, Serialize};

use crate::tokens::{asset_id::AssetId, icrc::icrc_token::IcrcToken};

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq, Hash)]
pub enum ChainToken {
    Icp {
        ledger: Principal,
        symbol: String,
        decimals: u8,
        fee: Nat,
    },
    EvmNative {
        chain: String,  // "eth", "arb", "base"
        symbol: String, // "ETH"
        decimals: u8,   // usually 18
        fee: Nat,
    },
    EvmErc20 {
        chain: String,         // "eth", "arb"
        token_address: String, // "0x..."
        symbol: String,
        decimals: u8,
        fee: Nat,
    },
    SolanaNative {
        symbol: String, // "SOL"
        decimals: u8,   // 9
        fee: Nat,
    },
    SolanaSpl {
        mint: String, // base58 token mint address
        symbol: String,
        decimals: u8,
        fee: Nat,
    },
}

impl ChainToken {
    pub fn chain(&self) -> String {
        match self {
            ChainToken::Icp { .. } => "ICP".to_string(),
            ChainToken::EvmNative { chain, .. } => format!("evm-{}", chain),
            ChainToken::EvmErc20 { chain, .. } => format!("evm-{}", chain),
            ChainToken::SolanaNative { .. } => "sol".to_string(),
            ChainToken::SolanaSpl { .. } => "sol".to_string(),
        }
    }

    pub fn symbol(&self) -> String {
        match self {
            ChainToken::Icp { symbol, .. } => symbol.clone(),
            ChainToken::EvmNative { symbol, .. } => symbol.clone(),
            ChainToken::EvmErc20 { symbol, .. } => symbol.clone(),
            ChainToken::SolanaNative { symbol, .. } => symbol.clone(),
            ChainToken::SolanaSpl { symbol, .. } => symbol.clone(),
        }
    }

    pub fn decimals(&self) -> u8 {
        match self {
            ChainToken::Icp { decimals, .. } => *decimals,
            ChainToken::EvmNative { decimals, .. } => *decimals,
            ChainToken::EvmErc20 { decimals, .. } => *decimals,
            ChainToken::SolanaNative { decimals, .. } => *decimals,
            ChainToken::SolanaSpl { decimals, .. } => *decimals,
        }
    }

    pub fn fee(&self) -> Nat {
        match self {
            ChainToken::Icp { fee, .. } => fee.clone(),
            ChainToken::EvmNative { fee, .. } => fee.clone(),
            ChainToken::EvmErc20 { fee, .. } => fee.clone(),
            ChainToken::SolanaNative { fee, .. } => fee.clone(),
            ChainToken::SolanaSpl { fee, .. } => fee.clone(),
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
                chain: chain.to_string(),
                address: "native".to_string(),
                symbol: symbol.clone(),
            },

            ChainToken::EvmErc20 {
                chain,
                token_address,
                symbol,
                ..
            } => AssetId {
                chain: chain.to_string(),
                address: token_address.clone(),
                symbol: symbol.clone(),
            },

            ChainToken::SolanaNative { symbol, .. } => AssetId {
                chain: "sol".to_string(),
                address: "native".to_string(),
                symbol: symbol.clone(),
            },

            ChainToken::SolanaSpl { mint, symbol, .. } => AssetId {
                chain: "sol".to_string(),
                address: mint.clone(),
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
            ChainToken::SolanaNative { symbol, .. } => {
                write!(f, "SOL:{}@native", symbol)
            }
            ChainToken::SolanaSpl { mint, symbol, .. } => {
                write!(f, "SOL:{}@{}", symbol, mint)
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
            fee: value.fee,
        }
    }
}

impl From<&IcrcToken> for ChainToken {
    fn from(value: &IcrcToken) -> Self {
        value.clone().into()
    }
}
