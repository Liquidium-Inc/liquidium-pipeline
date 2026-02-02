
use candid::{CandidType, Nat};
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};
use serde::{Deserialize, Serialize};

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxRef {
    IcBlockIndex { ledger: String, block_index: Nat },
    TxHash { chain: String, hash: String },
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct SwapRequest {
    pub pay_asset: AssetId,
    pub pay_amount: ChainTokenAmount,
    pub receive_asset: AssetId,
    pub receive_address: Option<String>,
    pub max_slippage_bps: Option<u32>, // 100 = 1%
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
    #[serde(default)]
    pub approval_count: Option<u32>,
    pub ts: u64,
}
