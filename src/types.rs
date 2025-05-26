use candid::{CandidType, Nat};
use ic_agent::export::Principal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, CandidType)]
pub struct LiquidationOpportunity {
    pub debt_pool_id: Principal, // Pool containing the debt to be repaid
    pub debt_amount: Nat,        // The debt amount
    pub debt_asset: String,      // The debt asset
    pub borrower: Principal      // The borrower's principal
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
