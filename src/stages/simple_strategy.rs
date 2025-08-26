use std::collections::HashMap;
use std::sync::Arc;

use crate::account::account::IcrcAccountInfo;
use crate::config::ConfigTrait;
use crate::executors::executor::{ExecutorRequest, IcrcSwapExecutor};
use crate::executors::kong_swap::types::SwapArgs;
use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;
use crate::liquidation::collateral_service::CollateralServiceTrait;
use crate::stage::PipelineStage;
use crate::types::protocol_types::{AssetType, LiquidatebleUser, LiquidationRequest};
use async_trait::async_trait;
use itertools::Itertools;

use candid::{Int, Nat, Principal};
use log::{debug, info};
use num_traits::ToPrimitive;
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
impl<'a, T, C, U, W> PipelineStage<'a, Vec<LiquidatebleUser>, Vec<ExecutorRequest>>
    for IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapExecutor,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    async fn process(&self, users: &'a Vec<LiquidatebleUser>) -> Result<Vec<ExecutorRequest>, String> {
        let mut result: Vec<ExecutorRequest> = vec![];
        let mut balances: HashMap<Principal, Nat> = HashMap::new();

        // We need to keep track of available balances when preparing our liquidations
        for asset in self.config.get_debt_assets().keys() {
            let asset = Principal::from_text(asset).unwrap();
            let balance = self
                .account_service
                .get_cached_balance(asset, self.config.get_liquidator_principal())
                .ok_or("Could not get balance")?;

            balances.insert(asset, balance.value);
        }

        // Take smallest hf first
        let users: Vec<LiquidatebleUser> = users
            .iter()
            .sorted_by(|a, b| a.health_factor.cmp(&b.health_factor))
            .cloned()
            .collect();

        for user in users {
            // Find the largest debt position
            let debt_position = user
                .positions
                .iter()
                .max_by(|a, b| a.debt_amount.cmp(&b.debt_amount))
                .unwrap();

            // Find the largest collateral position
            let collateral_position = user
                .positions
                .iter()
                .max_by(|a, b| a.collateral_amount.cmp(&b.collateral_amount))
                .unwrap();

            let debt_asset_principal = match debt_position.asset_type {
                AssetType::CkAsset(principal) => principal,
                _ => return Err("invalid asset type".to_string()),
            };

            let collateral_asset_principal = match collateral_position.asset_type {
                AssetType::CkAsset(principal) => principal,
                _ => return Err("invalid asset type".to_string()),
            };

            let available_balance = if let Some(b) = balances.get_mut(&debt_asset_principal) {
                b
            } else {
                println!("Asset balance not found {:?}", debt_asset_principal.to_string());
                continue;
            };

            let collateral_assets = self.config.get_collateral_assets();
            let collateral_token = collateral_assets
                .get(&collateral_asset_principal.to_text())
                .ok_or("invalid collateral asset principal")?;

            let debt_assets = self.config.get_debt_assets();
            let repayment_token = debt_assets
                .get(&debt_asset_principal.to_text())
                .ok_or("invalid debt asset principal")?;

            if available_balance.clone() < repayment_token.fee.clone() * 2u64 {
                return Err("Insufficient funds to execute liquidation".to_string());
            }

            let max_balance = available_balance.clone() - repayment_token.fee.clone() * 2u64;

            debug!(
                "available_balance: {:?} repayment_token_fee {:?} max_balance: {:?}",
                available_balance, repayment_token.fee, max_balance
            );
            let mut estimation = self
                .collateral_service
                .calculate_liquidation_amounts(max_balance, debt_position, collateral_position, &user)
                .await?;

            estimation.received_collateral = if estimation.received_collateral < collateral_token.fee {
                0u64.into()
            } else {
                estimation.received_collateral - collateral_token.fee.clone()
            };

            if !self.config.should_buy_bad_debt() && estimation.received_collateral == 0u32 {
                // Not enough collateral to cover fees, skip
                info!(
                    "Not enough collateral {} to cover fees {}, skipping liquidation",
                    estimation.received_collateral, collateral_token.fee
                );
                continue;
            }

            let amount_in = IcrcTokenAmount {
                token: collateral_token.clone(),
                value: estimation.received_collateral.clone(),
            };

            let (swap_args, amount_received, price) =
                if estimation.received_collateral == 0u32 || collateral_asset_principal == debt_asset_principal {
                    (None, amount_in.value, 1f64)
                } else {
                    let swap_info = self
                        .executor
                        .get_swap_info(collateral_token, repayment_token, &amount_in)
                        .await
                        .expect("could not get swap info");
                    (
                        Some(SwapArgs {
                            pay_token: collateral_token.symbol.clone(),
                            pay_amount: amount_in.value,
                            pay_tx_id: None,
                            receive_token: swap_info.receive_symbol,
                            receive_amount: Some(swap_info.receive_amount.clone()),
                            receive_address: None,
                            max_slippage: Some(swap_info.slippage),
                            referred_by: None,
                        }),
                        swap_info.receive_amount,
                        swap_info.price,
                    )
                };

            info!(
                "repaid_debt={} ({}),  amount_received={} ({}), price={}",
                estimation.repaid_debt.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals as u32) as f64,
                repayment_token.symbol,
                amount_received.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals as u32) as f64,
                repayment_token.symbol,
                price
            );

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
                - Int::from(repayment_token.fee.clone()) * 2u128
                - Int::from(collateral_token.fee.clone()) * 2u128;

            info!(
                "Expected Profit {} {}... Executing...",
                profit.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals as u32) as f64,
                repayment_token.symbol
            );

            if profit <= 0 && !self.config.should_buy_bad_debt() {
                // No profit, move on
                continue;
            }

            // We have profit update the available balance
            debug!(
                "Updating available balance: {:?} {} {}",
                available_balance, estimation.repaid_debt, repayment_token.fee
            );

            *available_balance =
                available_balance.clone() - estimation.repaid_debt.clone() - repayment_token.fee.clone() * 2u64;

            result.push(ExecutorRequest {
                debt_asset: repayment_token.clone(),
                collateral_asset: collateral_token.clone(),
                liquidation: LiquidationRequest {
                    borrower: debt_position.account,
                    debt_pool_id: debt_position.pool_id,
                    collateral_pool_id: collateral_position.pool_id,
                    debt_amount: Some(estimation.repaid_debt.clone()),
                    min_collateral_amount: Some(estimation.received_collateral.clone()),
                },
                swap_args,
                expected_profit: profit.0.to_i128().unwrap(),
            });

            if profit <= 0 && self.config.should_buy_bad_debt() {
                info!(
                    "Buying bad debt {}",
                    estimation.repaid_debt.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals as u32) as f64
                );
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        account::account::MockIcrcAccountInfo,
        config::MockConfigTrait,
        executors::{executor::MockIcrcSwapExecutor, kong_swap::types::SwapAmountsReply},
        icrc_token::icrc_token::IcrcToken,
        liquidation::collateral_service::{LiquidationEstimation, MockCollateralServiceTrait},
        types::protocol_types::{Assets, LiquidateblePosition},
    };
    use rand::random;

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_process() {
        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(10u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        // Mocks
        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(collateral_token_principal.clone(), collateral_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );

        config.expect_get_debt_assets().return_const(
            vec![(debt_token_principal.clone(), debt_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );

        config
            .expect_get_liquidator_principal()
            .return_const(Principal::anonymous());

        let mut executor = MockIcrcSwapExecutor::new();
        executor.expect_get_swap_info().returning(move |_, _, _| {
            Ok(SwapAmountsReply {
                pay_chain: "ICP".to_string(),
                pay_symbol: collateral_token.symbol.clone(),
                pay_address: "pay-addr".to_string(),
                pay_amount: Nat::from(4000u64),
                receive_chain: "ICP".to_string(),
                receive_symbol: debt_token.symbol.clone(),
                receive_address: "recv-addr".to_string(),
                receive_amount: Nat::from(6000u64),
                mid_price: 1.0,
                price: 1.0,
                slippage: 0.005,
                txs: vec![],
            })
        });

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4000u64),
                    repaid_debt: Nat::from(1000u64),
                })
            });

        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 8,
                    name: "Dummy Token".to_string(),
                    symbol: "DUM".to_string(),
                    fee: Nat::from(0u8), // example fee in smallest units
                },
                value: Nat::from(10_000u64),
            })
        });

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(1500u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(collateral_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let second_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1000u64),
            collateral_amount: Nat::from(0u8),
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(debt_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![pos.clone(), second_pos.clone()],
            total_debt: Nat::from(100u8),
            health_factor: Nat::from(950u64),
        };

        let result = strategy.process(&vec![user]).await.unwrap();
        let req = &result[0];

        assert_eq!(req.liquidation.borrower, pos.account);
        assert_eq!(req.swap_args.as_ref().unwrap().pay_token, "ckBTC");
        assert_eq!(req.swap_args.as_ref().unwrap().receive_token, "ckUSDC");
    }

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_skips_if_no_profit() {
        // Setup debt asset (ckUSDC) and collateral asset (ckBTC)
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();
        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(10u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        // Mock config with correct principal-to-token mappings
        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(collateral_token_principal.clone(), collateral_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config.expect_get_debt_assets().return_const(
            vec![(debt_token_principal.clone(), debt_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config
            .expect_get_liquidator_principal()
            .return_const(Principal::anonymous());

        // Mock swap returns receive_amount = repaid_debt → no profit
        let mut executor = MockIcrcSwapExecutor::new();
        executor.expect_get_swap_info().returning(move |_, _, _| {
            Ok(SwapAmountsReply {
                pay_chain: "ICP".to_string(),
                pay_symbol: collateral_token.symbol.clone(),
                pay_address: "pay-addr".to_string(),
                pay_amount: Nat::from(4000u64),
                receive_chain: "ICP".to_string(),
                receive_symbol: debt_token.symbol.clone(),
                receive_address: "recv-addr".to_string(),
                receive_amount: Nat::from(1000u64), // equal to repaid_debt
                mid_price: 1.0,
                price: 1.0,
                slippage: 0.005,
                txs: vec![],
            })
        });

        // Mock liquidation estimation with equal repaid_debt
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4000u64),
                    repaid_debt: Nat::from(1000u64),
                })
            });

        // Mock available balance as sufficient
        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 8,
                    name: "Dummy Token".to_string(),
                    symbol: "DUM".to_string(),
                    fee: Nat::from(0u8), // example fee in smallest units
                },
                value: Nat::from(10_000u64),
            })
        });

        // Build strategy
        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        // Define positions: collateral = ckBTC, debt = ckUSDC
        let btc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(1500u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(collateral_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let usdc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1000u64),
            collateral_amount: Nat::from(0u8),
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(debt_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        // One user with a liquidation opportunity
        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![btc_pos, usdc_pos],
            total_debt: Nat::from(100u8),
            health_factor: Nat::from(900u16),
        };

        // Execute strategy
        let result = strategy.process(&vec![user]).await.unwrap();

        // Expect no result due to negative profit
        assert!(
            result.is_empty(),
            "Expected no liquidation request due to negative profit"
        );
    }

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_fails_on_missing_balance() {
        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(10u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        // Mock config with valid token principals
        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(collateral_token_principal.clone(), collateral_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config.expect_get_debt_assets().return_const(
            vec![(debt_token_principal.clone(), debt_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );

        config
            .expect_get_liquidator_principal()
            .return_const(Principal::anonymous());

        // We shouldn't reach these
        let mut executor = MockIcrcSwapExecutor::new();
        executor
            .expect_get_swap_info()
            .returning(|_, _, _| panic!("should not be called"));

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("should not be called"));

        // Simulate missing balance
        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| None); // balance not found

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let btc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(2000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(collateral_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let usdc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1000u64),
            collateral_amount: Nat::from(0u8),
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(debt_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![btc_pos, usdc_pos],
            total_debt: Nat::from(100u8),
            health_factor: Nat::from(900u16),
        };

        let result = strategy.process(&vec![user]).await;
        // We expect an early failure due to missing balance
        assert!(result.is_err(), "Expected error due to missing cached balance");
        assert_eq!(result.unwrap_err(), "Could not get balance");
    }

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_fails_on_unsupported_asset_type() {
        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(10u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        // Mock config
        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(collateral_token_principal.clone(), collateral_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config.expect_get_debt_assets().return_const(
            vec![(debt_token_principal.clone(), debt_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config
            .expect_get_liquidator_principal()
            .return_const(Principal::anonymous());

        // We shouldn't reach these
        let mut executor = MockIcrcSwapExecutor::new();
        executor
            .expect_get_swap_info()
            .returning(|_, _, _| panic!("should not be called"));

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("should not be called"));

        let mut account = MockIcrcAccountInfo::new();

        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 8,
                    name: "Dummy Token".to_string(),
                    symbol: "DUM".to_string(),
                    fee: Nat::from(0u8), // example fee in smallest units
                },
                value: Nat::from(10_000u64),
            })
        });

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        // This asset type is invalid for the strategy
        let pos_native = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1000u64),
            collateral_amount: Nat::from(1000u64),
            asset: Assets::USDC,
            asset_type: AssetType::Unknown, // <-- not supported
            account: Principal::anonymous(),
            liquidation_bonus: Nat::from(1000u64),
        };

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![pos_native.clone()],
            total_debt: Nat::from(100u8),
            health_factor: Nat::from(900u16),
        };

        let result = strategy.process(&vec![user]).await;

        // Should fail due to invalid asset type
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid asset type");
    }

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_mixed_users_only_one_executes() {
        let principal_debt = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap(); // ckUSDC
        let principal_collateral = Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap(); // ckBTC

        let debt_token = IcrcToken {
            ledger: principal_debt,
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(10u64),
        };

        let collateral_token = IcrcToken {
            ledger: principal_collateral,
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(principal_collateral.to_text(), collateral_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config.expect_get_debt_assets().return_const(
            vec![(principal_debt.to_text(), debt_token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config
            .expect_get_liquidator_principal()
            .return_const(Principal::from_text("aaaaa-aa").unwrap());

        let mut executor = MockIcrcSwapExecutor::new();
        let mut calls = 0;
        executor.expect_get_swap_info().returning(move |_, _, _| {
            Ok(SwapAmountsReply {
                pay_chain: "ICP".to_string(),
                pay_symbol: "ckBTC".to_string(),
                pay_address: "pay-addr".to_string(),
                pay_amount: Nat::from(4000u64),
                receive_chain: "ICP".to_string(),
                receive_symbol: "ckUSDC".to_string(),
                receive_address: "recv-addr".to_string(),
                receive_amount: if calls == 0 {
                    calls += 1;
                    Nat::from(2000u64) // profitable
                } else {
                    calls += 1;
                    Nat::from(500u64)
                },
                mid_price: 1.0,
                price: 1.0,
                slippage: 0.005,
                txs: vec![],
            })
        });

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4000u64),
                    repaid_debt: Nat::from(1000u64),
                })
            });

        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 8,
                    name: "Dummy Token".to_string(),
                    symbol: "DUM".to_string(),
                    fee: Nat::from(0u8), // example fee in smallest units
                },
                value: Nat::from(10_000u64),
            })
        });

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let valid_user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(1000u64),
                    collateral_amount: Nat::from(2000u64),
                    asset: Assets::USDC,
                    asset_type: AssetType::CkAsset(principal_debt),
                    account: Principal::from_text("user-valid").unwrap_or_else(|_| Principal::management_canister()),
                    liquidation_bonus: Nat::from(1000u64),
                },
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(0u64),
                    collateral_amount: Nat::from(2000u64),
                    asset: Assets::BTC,
                    asset_type: AssetType::CkAsset(principal_collateral),
                    account: Principal::from_text("user-valid").unwrap_or_else(|_| Principal::management_canister()),
                    liquidation_bonus: Nat::from(1000u64),
                },
            ],
            total_debt: Nat::from(2000u64),
            health_factor: Nat::from(900u64),
        };

        let unprofitable_user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(1000u64),
                    collateral_amount: Nat::from(0u64),
                    asset: Assets::USDC,
                    asset_type: AssetType::CkAsset(principal_debt),
                    account: Principal::anonymous(),
                    liquidation_bonus: Nat::from(1000u64),
                },
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(0u64),
                    collateral_amount: Nat::from(2000u64),
                    asset: Assets::BTC,
                    asset_type: AssetType::CkAsset(principal_collateral),
                    account: Principal::anonymous(),
                    liquidation_bonus: Nat::from(1000u64),
                },
            ],
            total_debt: Nat::from(1000u64),
            health_factor: Nat::from(910u64),
        };

        let result = strategy.process(&vec![valid_user, unprofitable_user]).await.unwrap();

        // Only the valid user should yield one ExecutorRequest
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].swap_args.as_ref().unwrap().pay_token, "ckBTC");
        assert_eq!(result[0].swap_args.as_ref().unwrap().receive_token, "ckUSDC");
    }

    #[tokio::test]
    async fn test_icrc_liquidation_strategy_with_leveraged_position() {
        let principal = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap(); // ckUSDC
        let token = IcrcToken {
            ledger: principal,
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(10u64),
        };

        let mut config = MockConfigTrait::new();
        config.expect_get_collateral_assets().return_const(
            vec![(principal.to_text(), token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config.expect_get_debt_assets().return_const(
            vec![(principal.to_text(), token.clone())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        config
            .expect_get_liquidator_principal()
            .return_const(Principal::from_text("aaaaa-aa").unwrap());

        let mut executor = MockIcrcSwapExecutor::new();

        executor
            .expect_get_swap_info()
            .returning(move |_, _, _| panic!("We should not get here"));

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(11e8 as u32),
                    repaid_debt: Nat::from(10e8 as u32),
                })
            });

        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 8,
                    name: "Dummy Token".to_string(),
                    symbol: "DUM".to_string(),
                    fee: Nat::from(0u8), // example fee in smallest units
                },
                value: Nat::from(10_000e8 as u64),
            })
        });

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let valid_user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![LiquidateblePosition {
                pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                debt_amount: Nat::from(80e8 as u64),
                collateral_amount: Nat::from(100e8 as u64),
                asset: Assets::BTC,
                asset_type: AssetType::CkAsset(principal),
                account: Principal::self_authenticating(random::<[u8; 32]>()),
                liquidation_bonus: Nat::from(1000u64),
            }],
            total_debt: Nat::from(2000u64),
            health_factor: Nat::from(900u64),
        };

        let result = strategy.process(&vec![valid_user]).await.unwrap();

        // Only the valid user should yield one ExecutorRequest
        assert_eq!(result.len(), 1);
        assert!(result[0].swap_args.is_none());
    }
}
