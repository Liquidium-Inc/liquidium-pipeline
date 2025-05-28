use candid::{CandidType, Nat};
use ic_agent::export::Principal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, CandidType)]
pub struct LiquidationOpportunity {
    pub debt_pool_id: Principal, // Pool containing the debt to be repaid
    pub debt_amount: Nat,        // The debt amount
    pub debt_asset: String,      // The debt asset
    pub borrower: Principal,     // The borrower's principal
}

#[derive(Debug, Clone)]
pub struct ExecutionReceipt {
    pub seized_collateral: u128,
    pub tx_id: String,
}

#[derive(Debug, Clone)]
pub struct SwapResult {
    pub received_asset: String,
    pub received_amount: u128,
}

#[derive(CandidType, Debug, Deserialize, Serialize)]
pub struct SwapAmountsReply {
    pub pay_chain: String,
    pub pay_symbol: String,
    pub pay_address: String,
    pub pay_amount: Nat,
    pub receive_chain: String,
    pub receive_symbol: String,
    pub receive_address: String,
    pub receive_amount: Nat,
    pub mid_price: f64,
    pub price: f64,
    pub slippage: f64,
    pub txs: Vec<SwapAmountsTxReply>,
}

#[derive(CandidType, Debug, Deserialize, Serialize)]
pub struct SwapAmountsTxReply {
    pub pool_symbol: String,
    pub pay_chain: String,
    pub pay_symbol: String,
    pub pay_address: String,
    pub pay_amount: Nat,
    pub receive_chain: String,
    pub receive_symbol: String,
    pub receive_address: String,
    pub receive_amount: Nat,
    pub price: f64,
    pub lp_fee: Nat,
    pub gas_fee: Nat,
}
