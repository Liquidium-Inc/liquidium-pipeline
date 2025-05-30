use std::collections::HashMap;
use std::sync::Arc;

use crate::account::account::IcrcAccountInfo;
use crate::config::ConfigTrait;
use crate::executors::executor::{ExecutorRequest, IcrcSwapExecutor};
use crate::executors::kong_swap::types::SwapArgs;
use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;
use crate::liquidation::collateral_service::CollateralServiceTrait;
use crate::stage::PipelineStage;
use async_trait::async_trait;

use candid::{Int, Nat, Principal};
use lending::interface::liquidation::LiquidationRequest;
use lending::liquidation::liquidation::LiquidatebleUser;
use lending_utils::types::pool::AssetType;

pub struct IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapExecutor + Send + Sync,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    pub config: Arc<C>,
    pub executor: Arc<T>,
    pub collateral_service: Arc<U>,
    pub account_service: Arc<W>,
}

impl<T, C, U, W> IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapExecutor,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    pub fn new(config: Arc<C>, executor: Arc<T>, collateral_service: Arc<U>, account_service: Arc<W>) -> Self {
        Self {
            config,
            executor,
            collateral_service,
            account_service,
        }
    }
}

#[async_trait]
impl<T, C, U, W> PipelineStage<Vec<LiquidatebleUser>, Vec<ExecutorRequest>> for IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapExecutor,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    async fn process(&self, users: Vec<LiquidatebleUser>) -> Result<Vec<ExecutorRequest>, String> {
        let mut result: Vec<ExecutorRequest> = vec![];
        let mut balances: HashMap<Principal, Nat> = HashMap::new();

        // We need to keep track of available balances when preparing our liquidations
        for asset in self.config.get_debt_assets().keys() {
            let balance = self
                .account_service
                .get_cached_balance(*asset, self.config.get_liquidator_principal())
                .ok_or("Could not get balance")?;

            balances.insert(*asset, balance);
        }

        for user in users {
            // Find the largest debt position
            let debt_position = user.positions.iter().max_by(|a, b| a.debt_amount.cmp(&b.debt_amount)).unwrap();

            // Find the largest collateral position
            let collateral_position = user
                .positions
                .iter()
                .max_by(|a, b| a.collateral_amount.cmp(&b.collateral_amount))
                .unwrap();

            let debt_asset_principal = match debt_position.asset_type {
                AssetType::CkAsset(principal) => Ok(principal),
                _ => Err("invalid asset type"),
            }
            .unwrap();

            let collateral_asset_principal = match debt_position.asset_type {
                AssetType::CkAsset(principal) => Ok(principal),
                _ => Err("invalid asset type"),
            }
            .unwrap();

            let available_balance = if let Some(b) = balances.get_mut(&debt_asset_principal) {
                b
            } else {
                continue;
            };

            let collateral_assets = self.config.get_collateral_assets();
            let collateral_token = collateral_assets.get(&collateral_asset_principal).ok_or("invalid asset principal")?;

            let debt_assets = self.config.get_debt_assets();
            let repayment_token = debt_assets.get(&debt_asset_principal).ok_or("invalid asset principal")?;

            let max_balance = available_balance.clone() - repayment_token.fee.clone();

            let estimation = self
                .collateral_service
                .calculate_liquidation_amounts(max_balance, debt_position, collateral_position, &user)
                .await?;

            let amount_in = IcrcTokenAmount {
                token: collateral_token.clone(),
                value: estimation.received_collateral,
            };

            let swap_info = self
                .executor
                .get_swap_info(collateral_token, repayment_token, &amount_in)
                .await
                .expect("could not get swap info");

            let amount_received = swap_info.receive_amount.clone();

            // Calculate profit as:
            // profit = received_from_swap
            //        - debt_repaid_in_tokens
            //        - 2 * token_fee
            //
            // Explanation:
            // - We repay debt to trigger liquidation (cost 1x token fee)
            // - We perform a swap to get profit in another asset (cost 1x token fee)
            // - So total cost is 2x token_fee plus the debt amount repaid
            let profit = Int::from(amount_received)
                - Int::from(estimation.repaid_debt.clone())
                - Int::from(repayment_token.fee.clone()) * 2
                - Int::from(collateral_token.fee.clone()) * 2;

            if profit < 0 {
                // No profit, move on
                continue;
            }

            // We have profit update the available balance
            *available_balance = estimation.repaid_debt.clone() - repayment_token.fee.clone() * 2u64;

            result.push(ExecutorRequest {
                liquidation: LiquidationRequest {
                    borrower: debt_position.account,
                    debt_pool_id: debt_position.pool_id,
                    collateral_pool_id: collateral_position.pool_id,
                    debt_amount: Some(estimation.repaid_debt.clone()),
                },
                swap_args: SwapArgs {
                    pay_token: collateral_token.symbol.clone(),
                    pay_amount: amount_in.value,
                    pay_tx_id: None,
                    receive_token: swap_info.receive_symbol,
                    receive_amount: Some(swap_info.receive_amount),
                    receive_address: None,
                    max_slippage: Some(swap_info.slippage),
                    referred_by: None,
                },
            });
        }

        Ok(result)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{
//         config::MockConfigTrait,
//         executors::{executor::MockIcrcSwapExecutor, kong_swap::types::SwapAmountsReply},
//         icrc_token::icrc_token::IcrcToken,
//         liquidation::collateral_service::MockCollateralServiceTrait,
//     };
//     use arbitrary::Unstructured;
//     use candid::{Nat, Principal};
//     use lending::liquidation::liquidation::LiquidateblePosition;
//     use lending_utils::types::{assets::Assets, pool::AssetType};
//     use std::{collections::HashMap, sync::Arc};

//     #[tokio::test]
//     async fn test_icrc_liquidation_strategy_process() {
//         let principal_a = Principal::anonymous();
//         let principal_b = Principal::anonymous();
//         let token_in = IcrcToken {
//             ledger: principal_a,
//             decimals: 6,
//             name: "ckUSDC".to_string(),
//             symbol: "ckUSDC".to_string(),
//             fee: Nat::from(10u64),
//         };

//         let token_out = IcrcToken {
//             ledger: principal_b,
//             decimals: 8,
//             name: "Bitcoin".to_string(),
//             symbol: "ckBTC".to_string(),
//             fee: Nat::from(5u64),
//         };

//         // Mock the config
//         let mut mock_config = MockConfigTrait::new();
//         mock_config.expect_get_collateral_assets().return_const(
//             vec![(principal_a, token_in.clone()), (principal_b, token_out.clone())]
//                 .into_iter()
//                 .collect::<HashMap<_, _>>(),
//         );

//         mock_config.expect_get_debt_assets().return_const(
//             vec![(principal_a, token_in.clone()), (principal_b, token_out.clone())]
//                 .into_iter()
//                 .collect::<HashMap<_, _>>(),
//         );

//         let config = Arc::new(mock_config);

//         let mut executor = MockIcrcSwapExecutor::new();
//         executor.expect_get_swap_info().returning(move |_, _, _| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "ICP".to_string(),
//                 pay_symbol: "ckBTC".to_string(),
//                 pay_address: "pay-addr".to_string(),
//                 pay_amount: Nat::from(500_000u64),
//                 receive_chain: "ICP".to_string(),
//                 receive_symbol: "ckUSDC".to_string(),
//                 receive_address: "recv-addr".to_string(),
//                 receive_amount: Nat::from(100_000u64),
//                 mid_price: 0.0002,
//                 price: 0.00019,
//                 slippage: 0.005,
//                 txs: vec![],
//             })
//         });

//         let mut liquidation_utils_mock = MockCollateralServiceTrait::new();

//         liquidation_utils_mock
//             .expect_calculate_received_collateral()
//             .return_const(Nat::from(4000u64));

//         let strategy = IcrcLiquidationStrategy::new(config.clone(), Arc::new(executor), Arc::new(liquidation_utils_mock));

//         let pos = LiquidateblePosition {
//             pool_id: Principal::anonymous(),
//             debt_amount: Nat::from(1000u64),
//             collateral_amount: Nat::from(1500u64),
//             asset: Assets::BTC,
//             asset_type: AssetType::CkAsset(principal_a),
//             account: Principal::anonymous(),
//         };

//         let result = strategy.process(vec![pos.clone()]).await.unwrap();

//         assert_eq!(result.liquidation.borrower, pos.account);
//         assert_eq!(result.swap_args.pay_token, "ckBTC");
//         assert_eq!(result.swap_args.receive_token, "ckUSDC");
//     }

//     #[tokio::test]
//     async fn test_icrc_liquidation_strategy_selects_highest_debt_and_collateral() {
//         let principal_a = Principal::anonymous();
//         let principal_b = Principal::management_canister();

//         let token_in = IcrcToken {
//             ledger: principal_a,
//             decimals: 6,
//             name: "ckUSDC".to_string(),
//             symbol: "ckUSDC".to_string(),
//             fee: Nat::from(10u64),
//         };

//         let token_out = IcrcToken {
//             ledger: principal_b,
//             decimals: 8,
//             name: "Bitcoin".to_string(),
//             symbol: "ckBTC".to_string(),
//             fee: Nat::from(5u64),
//         };

//         // Mock the config
//         let mut mock_config = MockConfigTrait::new();
//         mock_config.expect_get_collateral_assets().return_const(
//             vec![(principal_a, token_in.clone()), (principal_b, token_out.clone())]
//                 .into_iter()
//                 .collect::<HashMap<_, _>>(),
//         );
//         mock_config.expect_get_debt_assets().return_const(
//             vec![(principal_a, token_in.clone()), (principal_b, token_out.clone())]
//                 .into_iter()
//                 .collect::<HashMap<_, _>>(),
//         );
//         let config = Arc::new(mock_config);

//         // Mock executor
//         let mut mock_executor = MockIcrcSwapExecutor::new();
//         mock_executor.expect_get_swap_info().returning(move |_, _, _| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "ICP".to_string(),
//                 pay_symbol: "ckUSDC".to_string(),
//                 pay_address: "pay-addr".to_string(),
//                 pay_amount: Nat::from(500_000u64),
//                 receive_chain: "ICP".to_string(),
//                 receive_symbol: "ckBTC".to_string(),
//                 receive_address: "recv-addr".to_string(),
//                 receive_amount: Nat::from(123_000u64),
//                 mid_price: 0.0002,
//                 price: 0.00019,
//                 slippage: 0.005,
//                 txs: vec![],
//             })
//         });

//         // Mock collateral service
//         let mut mock_collateral = MockCollateralServiceTrait::new();
//         mock_collateral.expect_calculate_received_collateral().return_const(Nat::from(4000u64));

//         let strategy = IcrcLiquidationStrategy::new(config, Arc::new(mock_executor), Arc::new(mock_collateral));
//         use arbitrary::Arbitrary;
//         let raw = (0..32).collect::<Vec<u8>>();
//         let mut u = Unstructured::new(&raw);
//         // Create multiple positions to trigger max selection
//         let pos_low = LiquidateblePosition {
//             pool_id: Principal::arbitrary(&mut u).unwrap(),
//             debt_amount: Nat::from(100u64),
//             collateral_amount: Nat::from(200u64),
//             asset: Assets::BTC,
//             asset_type: AssetType::CkAsset(principal_a),
//             account: Principal::arbitrary(&mut u).unwrap(),
//         };

//         let pos_high = LiquidateblePosition {
//             pool_id: Principal::arbitrary(&mut u).unwrap(),
//             debt_amount: Nat::from(5000u64),
//             collateral_amount: Nat::from(8000u64),
//             asset: Assets::BTC,
//             asset_type: AssetType::CkAsset(principal_a),
//             account: Principal::arbitrary(&mut u).unwrap(),
//         };

//         let result = strategy.process(vec![pos_low.clone(), pos_high.clone()]).await.unwrap();

//         // Assert that the highest values were selected
//         assert_eq!(result.liquidation.borrower, pos_high.account);
//         assert_eq!(result.liquidation.debt_pool_id, pos_high.pool_id);
//         assert_eq!(result.liquidation.collateral_pool_id, pos_high.pool_id); // same in this setup
//         assert_eq!(result.swap_args.pay_token, token_in.symbol);
//         assert_eq!(result.swap_args.receive_token, "ckBTC");
//     }

//     #[tokio::test]
//     async fn test_process_fails_on_missing_asset_principal() {
//         let principal_in = Principal::anonymous();

//         // config is missing `principal_in` on purpose
//         let mut mock_config = MockConfigTrait::new();
//         mock_config.expect_get_collateral_assets().return_const(HashMap::new());
//         mock_config.expect_get_debt_assets().return_const(HashMap::new());

//         let mut mock_executor = MockIcrcSwapExecutor::new();
//         // shouldn't be called, but stub to avoid panic
//         mock_executor.expect_get_swap_info().returning(|_, _, _| panic!("should not be called"));

//         let mut mock_collateral_service = MockCollateralServiceTrait::new();
//         mock_collateral_service
//             .expect_calculate_received_collateral()
//             .return_const(Nat::from(1000u64));

//         let strategy = IcrcLiquidationStrategy::new(Arc::new(mock_config), Arc::new(mock_executor), Arc::new(mock_collateral_service));

//         let position = LiquidateblePosition {
//             pool_id: Principal::from_text("aaaaa-aa").unwrap(),
//             debt_amount: Nat::from(1000u64),
//             collateral_amount: Nat::from(2000u64),
//             asset: Assets::USDC,
//             asset_type: AssetType::CkAsset(principal_in),
//             account: Principal::from_text("aaaaa-aa").unwrap(),
//         };

//         let result = strategy.process(vec![position]).await;

//         assert!(result.is_err());

//         assert!(result.unwrap_err().contains("invalid asset principal"), "Expected asset principal error");
//     }
// }
