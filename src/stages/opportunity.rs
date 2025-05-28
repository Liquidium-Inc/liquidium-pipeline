use std::sync::Arc;

use crate::stage::PipelineStage;
use crate::types::*;
use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use ic_agent::Agent;

pub struct OpportunityFinder {
    pub agent: Arc<Agent>,
    pub canister_id: Principal,
}

impl OpportunityFinder {
    pub fn new(agent: Arc<Agent>, canister_id: Principal) -> Self {
        Self { agent, canister_id }
    }
}

#[async_trait]
impl PipelineStage<(), Vec<LiquidationOpportunity>> for OpportunityFinder {
    async fn process(&self, _: ()) -> Result<Vec<LiquidationOpportunity>, String> {
        // Configure pagination
        let offset: u64 = 0;
        let limit: u64 = 100;

        // Encode candid args
        let args = Encode!(&offset, &limit).map_err(|e| e.to_string())?;

        // Query canister
        let response = self
            .agent
            .query(&self.canister_id, "get_at_risk_positions")
            .with_arg(args)
            .call()
            .await
            .map_err(|e| format!("Agent query error: {e}"))?;

        // Decode candid response
        // Adjust the type below to match your candid output!
        let decoded: Vec<(Principal, Vec<LiquidationOpportunity>, Nat)> = Decode!(
            &response,
            Vec<(Principal, Vec<LiquidationOpportunity>, Nat)>
        )
        .map_err(|e| format!("Candid decode error: {e}"))?;

        // Flatten Vec<Vec<LiquidationOpportunity>>
        let opportunities = decoded
            .into_iter()
            .flat_map(|(_principal, ops, _nat)| ops)
            .collect();

        Ok(opportunities)
    }
}
