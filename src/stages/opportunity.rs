use std::sync::Arc;

use crate::stage::PipelineStage;
use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use ic_agent::Agent;
use lending::liquidation::liquidation::LiquidateblePosition;
use lending_utils::types::pool::AssetType;

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
impl PipelineStage<Vec<String>, Vec<LiquidateblePosition>> for OpportunityFinder {
    async fn process(&self, supported_assets: Vec<String>) -> Result<Vec<LiquidateblePosition>, String> {
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
        // Vec<(Borrower, Opportunities, Health)>
        let decoded: Vec<(Principal, Vec<LiquidateblePosition>, Nat)> =
            Decode!(&response, Vec<(Principal, Vec<LiquidateblePosition>, Nat)>)
                .map_err(|e| format!("Candid decode error: {e}"))?;

        // Flatten Vec<Vec<LiquidateblePosition>>
        let opportunities = decoded
            .into_iter()
            .flat_map(|(_principal, ops, _nat)| ops)
            .filter(|item| matches!(item.asset_type, AssetType::CkAsset(_))) // Only liquidated ck asset collaterals
            .filter(|item| {
                supported_assets.contains(&item.asset.to_string()) // Filter out any unsupported assets
            })
            .collect();

        Ok(opportunities)
    }
}
