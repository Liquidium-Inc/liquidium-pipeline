use std::sync::Arc;

use crate::stage::PipelineStage;
use crate::types::*;
use async_trait::async_trait;
use candid::{CandidType, Decode, Encode, Nat, Principal};
use ic_agent::Agent;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

pub struct LiquidationExecutor {
    pub agent: Arc<Agent>,
    pub canister_id: Principal,
    pub liquidator_principal: Principal,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone)]
pub struct LiquidationRequest {
    pub borrower: Principal,           // Portfolio to be liquidated
    pub debt_pool_id: Principal,       // Pool containing the debt to be repaid
    pub collateral_pool_id: Principal, // Pool containing collateral to be sized
    pub debt_amount: Option<Nat>,      // Amount of debt to repay
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone)]
pub struct LiquidationResult {
    pub collateral_received: Nat, // Amount of collateral received by liquidator
    pub debt_repaid: Nat,         // Amount of debt repaid
    pub bonus_earned: Nat,        // Value of bonus earned (in debt asset)
}

impl LiquidationExecutor {
    pub fn new(agent: Arc<Agent>, canister_id: Principal, liquidator_principal: Principal) -> Self {
        Self {
            agent,
            canister_id,
            liquidator_principal,
        }
    }
}

#[async_trait]
impl PipelineStage<LiquidationOpportunity, ExecutionReceipt> for LiquidationExecutor {
    async fn process(&self, opp: LiquidationOpportunity) -> Result<ExecutionReceipt, String> {
        // Construct the liquidation request
        let req = LiquidationRequest {
            borrower: opp.borrower,
            debt_pool_id: opp.debt_pool_id,
            collateral_pool_id: opp.debt_pool_id,
            debt_amount: None,
        };

        // Encode candid args: (liquidator: Principal, params: LiquidationRequest)
        let args = Encode!(&self.liquidator_principal, &req).map_err(|e| e.to_string())?;

        // Make the update call to the canister
        let response = self
            .agent
            .update(&self.canister_id, "liquidate")
            .with_arg(args)
            .call_and_wait()
            .await
            .map_err(|e| format!("Agent update error: {e}"))?;

        // Decode the candid response
        let result: Result<LiquidationResult, String> =
            Decode!(&response, Result<LiquidationResult, String>)
                .map_err(|e| format!("Candid decode error: {e}"))?;

        match result {
            Ok(liq_res) => Ok(ExecutionReceipt {
                seized_collateral: liq_res.collateral_received.0.to_u128().unwrap_or(0),
                tx_id: "".to_string(), // Or the actual transaction id, if available
            }),
            Err(msg) => Err(msg),
        }
    }
}
