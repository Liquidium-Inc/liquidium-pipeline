use std::sync::Arc;

use async_trait::async_trait;
use candid::{Encode, Principal};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{AssetType, LiquidatebleUser, ScanResult};

use crate::stage::PipelineStage;

pub struct OpportunityFinder<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub canister_id: Principal,
    account_filter: Vec<Principal>,
}

impl<A: PipelineAgent> OpportunityFinder<A> {
    pub fn new(agent: Arc<A>, canister_id: Principal, account_filter: Vec<Principal>) -> Self {
        Self {
            agent,
            canister_id,
            account_filter,
        }
    }
}

#[async_trait]
impl<'a, A> PipelineStage<'a, Vec<String>, Vec<LiquidatebleUser>> for OpportunityFinder<A>
where
    A: PipelineAgent,
{
    async fn process(&self, supported_assets: &'a Vec<String>) -> Result<Vec<LiquidatebleUser>, String> {
        let max_results: u64 = 500; // stop once we find this many risky users
        let scan_limit: u64 = 100; // how many accounts to scan per call

        let mut cursor: Option<Principal> = None;
        let mut opportunities: Vec<LiquidatebleUser> = Vec::new();

        loop {
            let args = Encode!(&cursor, &scan_limit, &max_results).map_err(|e| e.to_string())?;

            let ScanResult {
                users: mut page_users,
                next_cursor,
                scanned,
                ..
            } = self
                .agent
                .call_query::<ScanResult>(&self.canister_id, "scan_at_risk_positions", args)
                .await
                .map_err(|e| format!("Agent query error: {e}"))?;

            opportunities.append(&mut page_users);

            if opportunities.len() as u64 >= max_results {
                break;
            }

            // End of keyspace or scan made no progress.
            if next_cursor.is_none() || scanned == 0 || next_cursor == cursor {
                break;
            }
            cursor = next_cursor;
        }

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
            .filter(|item| self.account_filter.is_empty() || self.account_filter.contains(&item.account))
            .cloned()
            .collect();

        Ok(opportunities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, Assets, LiquidateblePosition, LiquidatebleUser, ScanResult,
    };

    #[tokio::test]
    async fn opportunity_finder_filters_supported_ck_assets_by_principal() {
        let canister_id = Principal::anonymous();

        // Two supported ck assets, one unsupported native/unknown asset.
        let ckbtc_principal = Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap();
        let ckusdc_principal = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap();

        let pos_ckbtc = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(2_000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(ckbtc_principal),
            account: Principal::anonymous(),
            liquidation_bonus: 60,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        };

        let pos_ckusdc = LiquidateblePosition {
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(ckusdc_principal),
            ..pos_ckbtc.clone()
        };

        let pos_sol_unknown = LiquidateblePosition {
            asset: Assets::SOL,
            asset_type: AssetType::Unknown,
            ..pos_ckbtc.clone()
        };

        let users = vec![
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![pos_ckbtc.clone()],
            },
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![pos_ckusdc.clone()],
            },
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![pos_sol_unknown.clone()],
            },
        ];

        let mut agent = MockPipelineAgent::new();

        let users_len = users.len() as u64;
        let users_for_scan = users.clone();

        agent.expect_call_query::<ScanResult>().returning(move |_, _, _| {
            Ok(ScanResult {
                users: users_for_scan.clone(),
                next_cursor: None,
                scanned: users_len,
            })
        });

        let finder = OpportunityFinder::new(Arc::new(agent), canister_id, vec![]);
        let supported_assets = vec![ckbtc_principal.to_string(), ckusdc_principal.to_string()];

        // when
        let result = finder.process(&supported_assets).await.unwrap();

        // Only BTC and USDC users should remain; SOL should be dropped
        assert_eq!(result.len(), 2);

        let all_positions: Vec<_> = result.iter().flat_map(|u| u.positions.iter()).collect();
        assert!(all_positions.iter().any(|p| p.asset == Assets::BTC));
        assert!(all_positions.iter().any(|p| p.asset == Assets::USDC));
        assert!(!all_positions.iter().any(|p| p.asset == Assets::SOL));
    }

    #[tokio::test]
    async fn opportunity_finder_drops_users_with_no_supported_positions() {
        let canister_id = Principal::anonymous();

        let ckbtc_principal = Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap();
        let unsupported_principal = Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap(); // e.g. ICP

        let supported_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(2_000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(ckbtc_principal),
            account: Principal::anonymous(),
            liquidation_bonus: 60,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        };

        let unsupported_pos = LiquidateblePosition {
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(unsupported_principal),
            ..supported_pos.clone()
        };

        let users = vec![
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![supported_pos.clone()],
            },
            LiquidatebleUser {
                account: Principal::anonymous(),
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![unsupported_pos.clone()],
            },
        ];

        let mut agent = MockPipelineAgent::new();

        let users_len = users.len() as u64;
        let users_for_scan = users.clone();

        agent.expect_call_query::<ScanResult>().returning(move |_, _, _| {
            Ok(ScanResult {
                users: users_for_scan.clone(),
                next_cursor: None,
                scanned: users_len,
            })
        });

        let finder = OpportunityFinder::new(Arc::new(agent), canister_id, vec![]);
        let supported_assets = vec![ckbtc_principal.to_string()];

        // when
        let result = finder.process(&supported_assets).await.unwrap();

        // Only the user with a supported ck asset principal should remain
        assert_eq!(result.len(), 1);
        let positions = &result[0].positions;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].asset_type, AssetType::CkAsset(ckbtc_principal));
    }

    #[tokio::test]
    async fn opportunity_finder_filters_by_configured_account() {
        let canister_id = Principal::anonymous();
        let target_account = Principal::from_slice(&[1; 29]);
        let other_account = Principal::from_slice(&[2; 29]);

        let position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(2_000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            account: Principal::anonymous(),
            liquidation_bonus: 60,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        };

        let users = vec![
            LiquidatebleUser {
                account: target_account,
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![position.clone()],
            },
            LiquidatebleUser {
                account: other_account,
                health_factor: Nat::from(1u64),
                weighted_liquidation_threshold: Nat::from(8500u64),
                total_debt: Nat::from(1_000u64),
                positions: vec![position.clone()],
            },
        ];

        let mut agent = MockPipelineAgent::new();

        let users_len = users.len() as u64;
        let users_for_scan = users.clone();

        agent.expect_call_query::<ScanResult>().returning(move |_, _, _| {
            Ok(ScanResult {
                users: users_for_scan.clone(),
                next_cursor: None,
                scanned: users_len,
            })
        });

        let finder = OpportunityFinder::new(Arc::new(agent), canister_id, vec![target_account]);
        let supported_assets = vec![Principal::anonymous().to_string()];

        // when
        let result = finder.process(&supported_assets).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].account, target_account);
    }
}
