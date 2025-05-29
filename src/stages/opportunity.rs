use std::sync::Arc;

use crate::pipeline_agent::PipelineAgent;
use crate::stage::PipelineStage;
use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};

use lending::liquidation::liquidation::LiquidateblePosition;
use lending_utils::types::pool::AssetType;
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
impl<A> PipelineStage<Vec<String>, Vec<LiquidateblePosition>> for OpportunityFinder<A>
where
    A: PipelineAgent,
{
    async fn process(
        &self,
        supported_assets: Vec<String>,
    ) -> Result<Vec<LiquidateblePosition>, String> {
        // Configure pagination
        let offset: u64 = 0;
        let limit: u64 = 100;

        // Encode candid args
        let args = Encode!(&offset, &limit).map_err(|e| e.to_string())?;

        // Query canister
        let response = self
            .agent
            .call_query(&self.canister_id, "get_at_risk_positions", args)
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
            .flat_map(|(_principal, ops, health_factor)| (ops))
            .filter(|item| matches!(item.asset_type, AssetType::CkAsset(_))) // Only liquidated ck asset collaterals
            .filter(|item| {
                supported_assets.contains(&item.asset.to_string()) // Filter out any unsupported assets
            })
            .collect();

        Ok(opportunities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_agent::MockPipelineAgent;
    use crate::stage::PipelineStage;
    use candid::{Encode, Nat, Principal};
    use lending_utils::types::{assets::Assets, pool::AssetType};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_process_filters_supported_ck_assets() {
        let mut mock = MockPipelineAgent::new();
        let canister_id = Principal::anonymous();

        let pos_ckbtc = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1000u64),
            collateral_amount: Nat::from(2000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            account: Principal::anonymous(),
        };

        let pos_ckusdc = LiquidateblePosition {
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            ..pos_ckbtc.clone()
        };

        let pos_sol = LiquidateblePosition {
            asset: Assets::SOL,
            asset_type: AssetType::Unknown,
            ..pos_ckbtc.clone()
        };

        let positions = vec![
            (
                Principal::anonymous(),
                vec![pos_ckbtc.clone()],
                Nat::from(1u64),
            ),
            (
                Principal::anonymous(),
                vec![pos_ckusdc.clone()],
                Nat::from(2u64),
            ),
            (
                Principal::anonymous(),
                vec![pos_sol.clone()],
                Nat::from(3u64),
            ),
        ];

        let encoded_response = Encode!(&positions).unwrap();

        mock.expect_call_query()
            .returning(move |_, _, _| Ok(encoded_response.clone()));

        let finder = OpportunityFinder::new(Arc::new(mock), canister_id);
        let result = finder
            .process(vec!["BTC".to_string(), "USDC".to_string()])
            .await
            .unwrap();

        // SOL is excluded (unsupported + Unknown type)
        assert_eq!(result.len(), 2);
        assert!(result.contains(&pos_ckbtc));
        assert!(result.contains(&pos_ckusdc));
        assert!(!result.contains(&pos_sol));
    }
}
