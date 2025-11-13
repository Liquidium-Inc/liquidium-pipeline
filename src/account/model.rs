use std::fmt;

use async_trait::async_trait;
use candid::{CandidType, Nat, Principal};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub enum Chain {
    Icp,
    Evm { chain: String }, // "ETH", "ARB", ...
}

#[derive(Clone, Debug, Serialize, Deserialize, CandidType)]
pub enum ChainToken {
    Icp {
        ledger: Principal,
        symbol: String,
        decimals: u8,
    },
    Evm {
        chain: String,
        token_address: String,
        symbol: String,
        decimals: u8,
    },
}

impl ChainToken {
    pub fn chain(&self) -> String {
        match self {
            ChainToken::Icp { .. } => "ICP".to_string(),
            ChainToken::Evm { chain, .. } => chain.clone(),
        }
    }

    pub fn symbol(&self) -> String {
        match self {
            ChainToken::Icp { symbol, .. } => symbol.clone(),
            ChainToken::Evm { symbol, .. } => symbol.clone(),
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

#[derive(Clone, Debug)]
pub struct ChainBalance {
    pub chain: String,
    pub symbol: String,
    pub amount_native: u128,
    pub decimals: u8,
}

#[derive(Clone, Debug)]
pub enum TxRef {
    IcpBlockIndex(String),
    EvmTxHash(String),
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait AccountActions: Send + Sync {
    async fn transfer(&self, token: ChainToken, to: &str, amount: Nat, from_subaccount: bool) -> Result<TxRef, String>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait AccountInfo: Send + Sync {
    async fn get_wallet_balance(&self, token: &ChainToken) -> Result<ChainBalance, String>;
}
