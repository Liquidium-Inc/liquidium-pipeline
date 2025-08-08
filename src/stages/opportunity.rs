use std::sync::Arc;

use crate::types::protocol_types::AssetType;
use crate::{pipeline_agent::PipelineAgent, types::protocol_types::LiquidatebleUser};
use crate::stage::PipelineStage;
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
            .filter(|item| item.positions.len() > 0)
            .cloned()
            .collect();

        Ok(opportunities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::protocol_types::Assets;
    use crate::{pipeline_agent::MockPipelineAgent, types::protocol_types::LiquidateblePosition};
    use crate::stage::PipelineStage;
    use candid::{Nat, Principal};
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
            liquidation_bonus: Nat::from(60u8),
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

        let users = vec![
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                total_debt: Nat::from(1000u64),
                positions: vec![pos_ckbtc.clone()],
            },
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                total_debt: Nat::from(1000u64),
                positions: vec![pos_ckusdc.clone()],
            },
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                total_debt: Nat::from(1000u64),
                positions: vec![pos_sol.clone()],
            },
        ];

        mock.expect_call_query().returning(move |_, _, _| Ok(users.clone()));

        let finder = OpportunityFinder::new(Arc::new(mock), canister_id);
        let result = finder
            .process(&vec!["BTC".to_string(), "USDC".to_string()])
            .await
            .unwrap();

        // Only BTC and USDC should remain
        assert_eq!(result.len(), 2);

        let all_positions: Vec<_> = result.iter().flat_map(|u| u.positions.iter()).collect();
        assert!(all_positions.contains(&&pos_ckbtc));
        assert!(all_positions.contains(&&pos_ckusdc));
        assert!(!all_positions.iter().any(|p| p.asset == Assets::SOL));
    }
}
