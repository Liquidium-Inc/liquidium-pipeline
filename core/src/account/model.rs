use candid::CandidType;
use icrc_ledger_types::icrc1::account::Account;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, error_codes};

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq)]
pub enum Chain {
    Icp,
    Evm { chain: String }, // "ETH", "ARB", ...
}

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq)]
pub enum ChainAccount {
    Icp(Account),
    IcpLedger(String),
    Evm(String), // EVM address as string
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Chain::Icp => write!(f, "icp"),
            Chain::Evm { chain } => write!(f, "evm:{chain}"),
        }
    }
}

impl std::str::FromStr for Chain {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();

        if lower == "icp" {
            return Ok(Chain::Icp);
        }

        if let Some(rest) = lower.strip_prefix("evm:") {
            if rest.is_empty() {
                return Err(
                    AppError::from_def(error_codes::INVALID_INPUT).with_context("invalid chain: empty evm chain name")
                );
            }
            return Ok(Chain::Evm {
                chain: rest.to_string(),
            });
        }

        Err(AppError::from_def(error_codes::INVALID_INPUT).with_context(format!("invalid chain: {s}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, CandidType, PartialEq, Eq)]
pub enum TxRef {
    IcpBlockIndex(String),
    EvmTxHash(String),
}
