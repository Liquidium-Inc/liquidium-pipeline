use std::sync::Arc;

use crate::stage::PipelineStage;
use crate::types::protocol_types::AssetType;
use crate::{pipeline_agent::PipelineAgent, types::protocol_types::LiquidatebleUser};
use async_trait::async_trait;
use candid::{Encode, Principal};

pub struct OpportunityFinder<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub canister_id: Principal,
}

impl<A: PipelineAgent> OpportunityFinder<A> {
    pub fn new(agent: Arc<A>, canister_id: Principal) -> Self {
        Self { agent, canister_id }
    }
}

#[async_trait]
impl<'a, A> PipelineStage<'a, Vec<String>, Vec<LiquidatebleUser>> for OpportunityFinder<A>
where
    A: PipelineAgent,
{
    async fn process(&self, supported_assets: &'a Vec<String>) -> Result<Vec<LiquidatebleUser>, String> {
        // Configure pagination
        let offset: u64 = 0;
        let limit: u64 = 100;

        // Encode candid args
        let args = Encode!(&offset, &limit).map_err(|e| e.to_string())?;

        // Query canister
        let mut opportunities = self
            .agent
            .call_query::<Vec<LiquidatebleUser>>(&self.canister_id, "get_at_risk_positions", args)
            .await
            .map_err(|e| format!("Agent query error: {e}"))?;

        opportunities.iter_mut().for_each(|user| {
            user.positions = user
                .positions
                .iter()
                .filter(|item| matches!(item.asset_type, AssetType::CkAsset(_))) // Only liquidated ck asset collaterals
                .filter(|item| {
                    let AssetType::CkAsset(asset_principal) = item.asset_type else {
                        return false;
                    };

                    supported_assets.contains(&asset_principal.to_string()) // Filter out any unsupported assets
                })
                .cloned()
                .collect();
        });

        let opportunities: Vec<LiquidatebleUser> = opportunities
            .iter()
            .filter(|item| !item.positions.is_empty())
            .cloned()
            .collect();

        Ok(opportunities)
    }
}

