use std::fmt;

use candid::{CandidType, Nat};
use serde::{Deserialize, Serialize};

use crate::account::model::ChainToken;

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxRef {
    IcBlockIndex { ledger: String, block_index: Nat },
    TxHash { chain: String, hash: String },
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetId {
    pub chain: String,  // "IC", "MEXC-SPOT", "SOLANA", etc
    pub symbol: String, // "ckBTC", "USDT", "BTC"
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Simple, predictable, stable formatting
        write!(f, "{}:{}", self.chain, self.symbol)
    }
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct SwapRequest {
    pub pay_asset: ChainToken,
    pub pay_amount: Nat,
    pub pay_tx_ref: Option<TxRef>,

    pub receive_asset: ChainToken,
    pub receive_address: Option<String>,

    pub max_slippage_bps: Option<u32>, // 100 = 1%
    pub referred_by: Option<String>,

    pub venue_hint: Option<String>, // "kong", "mexc", etc (optional)
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct SwapQuoteLeg {
    pub venue: String,    // "kong", "mexc", "raydium"
    pub route_id: String, // pool id, market symbol, etc

    pub pay_chain: String,
    pub pay_symbol: String,
    pub pay_amount: Nat,

    pub receive_chain: String,
    pub receive_symbol: String,
    pub receive_amount: Nat,

    pub price: f64,
    pub lp_fee: Nat,
    pub gas_fee: Nat,
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct SwapQuote {
    pub pay_asset: AssetId,
    pub pay_amount: Nat,
    pub receive_asset: AssetId,
    pub receive_amount: Nat,
    pub mid_price: f64,
    pub exec_price: f64,
    pub slippage: f64,

    pub legs: Vec<SwapQuoteLeg>,
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct TransferRecord {
    pub asset: AssetId,
    pub is_send: bool,
    pub amount: Nat,
    pub tx_ref: TxRef,
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct SwapExecution {
    pub swap_id: u64,
    pub request_id: u64,
    pub status: String,

    pub pay_asset: AssetId,
    pub pay_amount: Nat,
    pub receive_asset: AssetId,
    pub receive_amount: Nat,

    pub mid_price: f64,
    pub exec_price: f64,
    pub slippage: f64,

    pub legs: Vec<SwapQuoteLeg>,
    pub transfers: Vec<TransferRecord>,
    pub claim_ids: Vec<u64>,

    pub ts: u64,
}
