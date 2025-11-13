use std::collections::HashMap;
use std::sync::Arc;

use crate::account::account::IcrcAccountInfo;
use crate::config::ConfigTrait;
use crate::executors::executor::ExecutorRequest;

use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;
use crate::liquidation::collateral_service::CollateralServiceTrait;
use crate::stage::PipelineStage;

use crate::swappers::kong_types::SwapArgs;
use crate::swappers::swap_interface::IcrcSwapInterface;
use crate::types::protocol_types::{AssetType, LiquidateblePosition, LiquidatebleUser, LiquidationRequest};

use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use async_trait::async_trait;
use itertools::Itertools;

use candid::{Int, Nat, Principal};
use log::{debug, info};
use num_traits::ToPrimitive;
pub struct IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapInterface + Send + Sync,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    pub config: Arc<C>,
    pub swapper: Arc<T>,
    pub collateral_service: Arc<U>,
    pub account_service: Arc<W>,
    pub watchdog: Arc<dyn Watchdog>,
}

impl<T, C, U, W> IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapInterface,
    C: ConfigTrait,
    U: CollateralServiceTrait,
    W: IcrcAccountInfo,
{
    pub fn new(config: Arc<C>, executor: Arc<T>, collateral_service: Arc<U>, account_service: Arc<W>) -> Self {
        Self {
            config,
            swapper: executor,
            collateral_service,
            account_service,
            watchdog: noop_watchdog(),
        }
    }

    pub fn with_watchdog(mut self, wd: Arc<dyn Watchdog>) -> Self {
        self.watchdog = wd;
        self
    }
}

#[async_trait]
impl<'a, T, C, U, W> PipelineStage<'a, Vec<LiquidatebleUser>, Vec<ExecutorRequest>>
    for IcrcLiquidationStrategy<T, C, U, W>
where
    T: IcrcSwapInterface,
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
                .get_cached_balance(asset, self.config.get_liquidator_principal().into())
                .ok_or("Could not get balance")?;

            balances.insert(asset, balance.value);
        }

        // Take smallest hf first
        let users: Vec<LiquidatebleUser> = users
            .iter()
            .sorted_by(|a, b| a.health_factor.cmp(&b.health_factor))
            .cloned()
            .collect();
        // Working copy of users for in-loop mutation
        let mut work_users: Vec<LiquidatebleUser> = users.clone();

        // Build all candidate (user_idx, debt_position, collateral_position) combinations
        let mut combos: Vec<(usize, LiquidateblePosition, LiquidateblePosition)> = vec![];
        for (idx, user) in work_users.iter().enumerate() {
            let debts = user
                .positions
                .iter()
                .filter(|p| p.debt_amount > 0u8)
                .cloned()
                .collect::<Vec<_>>();
            let colls = user
                .positions
                .iter()
                .filter(|p| p.collateral_amount > 0u8)
                .cloned()
                .collect::<Vec<_>>();

            for d in &debts {
                for c in &colls {
                    combos.push((idx, d.clone(), c.clone()));
                }
            }
        }

        // Sort by most urgent first: lowest health factor, then largest debt, then largest collateral
        combos.sort_by(|(i1, d1, c1), (i2, d2, c2)| {
            work_users[*i1]
                .health_factor
                .cmp(&work_users[*i2].health_factor)
                .then(d2.debt_amount.cmp(&d1.debt_amount))
                .then(c2.collateral_amount.cmp(&c1.collateral_amount))
        });

        for (user_idx, debt_position, collateral_position) in combos {
            if work_users[user_idx].health_factor >= 1000u32 {
                continue;
            }

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
                debug!("Asset balance not found {:?}", debt_asset_principal.to_string());
                self.watchdog
                    .notify(WatchdogEvent::BalanceMissing {
                        asset: &debt_position.asset.to_string(),
                    })
                    .await;
                continue;
            };

            let collateral_assets = self.config.get_collateral_assets();
            let collateral_token = if let Some(tok) = collateral_assets.get(&collateral_asset_principal.to_text()) {
                tok
            } else {
                debug!(
                    "Skipping combo due to unknown collateral principal {}",
                    collateral_asset_principal.to_text()
                );
                continue;
            };

            let debt_assets = self.config.get_debt_assets();
            let repayment_token = if let Some(tok) = debt_assets.get(&debt_asset_principal.to_text()) {
                tok
            } else {
                debug!(
                    "Skipping combo due to unknown debt principal {}",
                    debt_asset_principal.to_text()
                );
                continue;
            };

            if available_balance.clone() < repayment_token.fee.clone() * 2u64 {
                self.watchdog
                    .notify(WatchdogEvent::InsufficientFunds {
                        asset: &debt_position.asset.to_string(),
                        available: available_balance.to_string(),
                    })
                    .await;
                debug!(
                    "Skipping combo due to insufficient funds: asset={}, available={} < min_required={}",
                    debt_position.asset,
                    available_balance,
                    repayment_token.fee.clone() * 2u64
                );
                continue;
            }

            let max_balance = available_balance.clone() - repayment_token.fee.clone() * 2u64;

            debug!(
                "available_balance: {:?} repayment_token_fee {:?} max_balance: {:?}",
                available_balance, repayment_token.fee, max_balance
            );

            let mut estimation = self
                .collateral_service
                .calculate_liquidation_amounts(
                    max_balance,
                    &debt_position,
                    &collateral_position,
                    &mut work_users[user_idx],
                )
                .await?;

            estimation.received_collateral = if estimation.received_collateral < collateral_token.fee {
                0u64.into()
            } else {
                estimation.received_collateral - collateral_token.fee.clone()
            };

            if !self.config.should_buy_bad_debt() && estimation.received_collateral == 0u32 {
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
                        .swapper
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
                            receive_address: Some(self.config.get_liquidator_principal().to_string()),
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

            let profit = Int::from(amount_received)
                - Int::from(estimation.repaid_debt.clone())
                - Int::from(repayment_token.fee.clone()) * 2u128
                - Int::from(collateral_token.fee.clone()) * 2u128;

            info!(
                "dx Profit {} {}...",
                profit.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals as u32) as f64,
                repayment_token.symbol
            );

            if profit <= 0 && !self.config.should_buy_bad_debt() {
                continue;
            }

            debug!(
                "Updating available balance: {:?} {} {}",
                available_balance, estimation.repaid_debt, repayment_token.fee
            );

            if available_balance.clone() >= estimation.repaid_debt.clone() + repayment_token.fee.clone() * 2u64 {
                *available_balance =
                    available_balance.clone() - estimation.repaid_debt.clone() - repayment_token.fee.clone() * 2u64;
            } else {
                debug!(
                    "Available balance would underflow when updating. available={}, repay={}, fees={}",
                    available_balance,
                    estimation.repaid_debt,
                    repayment_token.fee.clone() * 2u64
                );
                continue;
            }

            result.push(ExecutorRequest {
                debt_asset: repayment_token.clone(),
                collateral_asset: collateral_token.clone(),
                liquidation: LiquidationRequest {
                    borrower: debt_position.account,
                    debt_pool_id: debt_position.pool_id,
                    collateral_pool_id: collateral_position.pool_id,
                    debt_amount: estimation.repaid_debt.clone(),
                    receiver_address: self.config.get_trader_principal(),
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
        icrc_token::icrc_token::IcrcToken,
        liquidation::collateral_service::{LiquidationEstimation, MockCollateralServiceTrait},
        stages::simple_strategy::tests::test_helpers::{
            mk_account_missing_balance, mk_account_with_balance, mk_collateral_panic, mk_collateral_reply,
            mk_config_with_maps, mk_executor_panic, mk_executor_reply, mk_token, mk_user, pos_collateral_btc,
            pos_debt_usdc,
        },
        swappers::{kong_types::SwapAmountsReply, swap_interface::MockIcrcSwapInterface},
        types::protocol_types::{Assets, LiquidateblePosition},
    };
    use rand::random;

    mod test_helpers {
        use super::*;
        use crate::icrc_token::icrc_token::IcrcToken;

        use crate::swappers::kong_types::SwapAmountsReply;
        use crate::swappers::swap_interface::MockIcrcSwapInterface;
        use crate::types::protocol_types::Assets;

        use std::collections::HashMap;

        pub fn p(text: &str) -> Principal {
            Principal::from_text(text).unwrap_or_else(|_| Principal::management_canister())
        }

        pub fn mk_token(principal_text: &str, sym: &str, name: &str, decimals: u8, fee: u64) -> IcrcToken {
            IcrcToken {
                ledger: p(principal_text),
                decimals,
                name: name.to_string(),
                symbol: sym.to_string(),
                fee: Nat::from(fee),
            }
        }

        pub fn mk_config_with_maps(
            debt: &[(String, IcrcToken)],
            coll: &[(String, IcrcToken)],
            liquidator: Principal,
            buy_bad_debt: bool,
        ) -> MockConfigTrait {
            let mut cfg = MockConfigTrait::new();
            let debt_map: HashMap<_, _> = debt.iter().cloned().collect();
            let coll_map: HashMap<_, _> = coll.iter().cloned().collect();
            cfg.expect_get_debt_assets().return_const(debt_map);
            cfg.expect_get_collateral_assets().return_const(coll_map);
            cfg.expect_get_liquidator_principal().return_const(liquidator);
            cfg.expect_get_trader_principal().return_const(liquidator);
            cfg.expect_should_buy_bad_debt().return_const(buy_bad_debt);
            cfg
        }

        pub fn mk_account_with_balance(decimals: u8, fee: u64, value: u128) -> MockIcrcAccountInfo {
            let mut acc = MockIcrcAccountInfo::new();
            acc.expect_get_cached_balance().returning(move |_, _| {
                Some(IcrcTokenAmount {
                    token: IcrcToken {
                        ledger: Principal::anonymous(),
                        decimals,
                        name: "Dummy Token".to_string(),
                        symbol: "DUM".to_string(),
                        fee: Nat::from(fee),
                    },
                    value: Nat::from(value),
                })
            });
            acc
        }

        pub fn mk_account_missing_balance() -> MockIcrcAccountInfo {
            let mut acc = MockIcrcAccountInfo::new();
            acc.expect_get_cached_balance().returning(|_, _| None);
            acc
        }

        pub fn mk_executor_panic() -> MockIcrcSwapInterface {
            let mut ex = MockIcrcSwapInterface::new();
            ex.expect_get_swap_info()
                .returning(|_, _, _| panic!("executor should not be called"));
            ex
        }

        pub fn mk_executor_reply(pay_sym: String, recv_sym: String, recv_amount: u64) -> MockIcrcSwapInterface {
            let mut ex = MockIcrcSwapInterface::new();
            ex.expect_get_swap_info().returning(move |_, _, _| {
                Ok(SwapAmountsReply {
                    pay_chain: "ICP".to_string(),
                    pay_symbol: pay_sym.clone(),
                    pay_address: "pay-addr".to_string(),
                    pay_amount: Nat::from(0u64),
                    receive_chain: "ICP".to_string(),
                    receive_symbol: recv_sym.clone(),
                    receive_address: "recv-addr".to_string(),
                    receive_amount: Nat::from(recv_amount),
                    mid_price: 1.0,
                    price: 1.0,
                    slippage: 0.005,
                    txs: vec![],
                })
            });
            ex
        }

        pub fn mk_collateral_panic() -> MockCollateralServiceTrait {
            let mut c = MockCollateralServiceTrait::new();
            c.expect_calculate_liquidation_amounts()
                .returning(|_, _, _, _| panic!("collateral should not be called"));
            c
        }

        pub fn mk_collateral_reply(received_collateral: u64, repaid_debt: u64) -> MockCollateralServiceTrait {
            let mut c = MockCollateralServiceTrait::new();
            c.expect_calculate_liquidation_amounts().returning(move |_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(received_collateral),
                    repaid_debt: Nat::from(repaid_debt),
                })
            });
            c
        }

        pub fn pos_collateral_btc(pool: Principal, principal: Principal, amount: u64) -> LiquidateblePosition {
            LiquidateblePosition {
                pool_id: pool,
                debt_amount: Nat::from(0u64),
                collateral_amount: Nat::from(amount),
                asset: Assets::BTC,
                asset_type: AssetType::CkAsset(principal),
                account: Principal::anonymous(),
                liquidation_bonus: 1000,
                protocol_fee: 200,
            }
        }

        pub fn pos_debt_usdc(pool: Principal, principal: Principal, amount: u64) -> LiquidateblePosition {
            LiquidateblePosition {
                pool_id: pool,
                debt_amount: Nat::from(amount),
                collateral_amount: Nat::from(0u64),
                asset: Assets::USDC,
                asset_type: AssetType::CkAsset(principal),
                account: Principal::anonymous(),
                liquidation_bonus: 1000,
                protocol_fee: 200,
            }
        }

        pub fn mk_user(positions: Vec<LiquidateblePosition>, total_debt: u64, hf: u64) -> LiquidatebleUser {
            LiquidatebleUser {
                account: Principal::anonymous(),
                positions,
                total_debt: Nat::from(total_debt),
                health_factor: Nat::from(hf),
            }
        }
    }

    // Happy path: builds a valid combo and produces one ExecutorRequest with a BTC->USDC swap.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_process() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );
        let executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 6000);
        let collateral = mk_collateral_reply(4000, 1000);
        let account = mk_account_with_balance(8, 0, 10_000);
        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );
        let pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 1500);
        let second_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1000);
        let user = mk_user(vec![pos.clone(), second_pos.clone()], 100, 950);
        let result = strategy.process(&vec![user]).await.unwrap();
        let req = &result[0];
        assert_eq!(req.liquidation.borrower, pos.account);
        assert_eq!(req.swap_args.as_ref().unwrap().pay_token, "ckBTC");
        assert_eq!(req.swap_args.as_ref().unwrap().receive_token, "ckUSDC");
    }

    // Skips when profit is not positive: receive_amount equals repaid_debt so no request is emitted.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_skips_if_no_profit() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );
        let mut executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 1000);
        // Override to ensure receive_amount = repaid_debt
        executor.expect_get_swap_info().returning(move |_, _, _| {
            Ok(SwapAmountsReply {
                pay_chain: "ICP".to_string(),
                pay_symbol: "ckBTC".to_string(),
                pay_address: "pay-addr".to_string(),
                pay_amount: Nat::from(4000u64),
                receive_chain: "ICP".to_string(),
                receive_symbol: "ckUSDC".to_string(),
                receive_address: "recv-addr".to_string(),
                receive_amount: Nat::from(1000u64),
                mid_price: 1.0,
                price: 1.0,
                slippage: 0.005,
                txs: vec![],
            })
        });
        let collateral = mk_collateral_reply(4000, 1000);
        let account = mk_account_with_balance(8, 0, 10_000);
        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );
        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 1500);
        let usdc_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1000);
        let user = mk_user(vec![btc_pos, usdc_pos], 100, 900);
        let result = strategy.process(&vec![user]).await.unwrap();
        assert!(
            result.is_empty(),
            "Expected no liquidation request due to negative profit"
        );
    }

    // Fails fast when cached balance is missing: returns an error and does not call executor or collateral service.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_fails_on_missing_balance() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );
        let executor = mk_executor_panic();
        let collateral = mk_collateral_panic();
        let account = mk_account_missing_balance();
        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );
        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2000);
        let usdc_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1000);
        let user = mk_user(vec![btc_pos, usdc_pos], 100, 900);
        let result = strategy.process(&vec![user]).await;
        assert!(result.is_err(), "Expected error due to missing cached balance");
        assert_eq!(result.unwrap_err(), "Could not get balance");
    }

    // Errors on unsupported asset type: using a non CkAsset position returns "invalid asset type".
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
        config.expect_should_buy_bad_debt().return_const(false);

        // We shouldn't reach these
        let mut executor = MockIcrcSwapInterface::new();
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
            liquidation_bonus: 1000u64,
            protocol_fee: 200u64,
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

    // Mixed input: only the profitable and valid user yields a single request; the unprofitable one is skipped.
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
        config
            .expect_get_trader_principal()
            .return_const(Principal::from_text("aaaaa-aa").unwrap());
        config.expect_should_buy_bad_debt().return_const(false);

        let mut executor = MockIcrcSwapInterface::new();
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
                    liquidation_bonus: 1000u64,
                    protocol_fee: 200u64,
                },
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(0u64),
                    collateral_amount: Nat::from(2000u64),
                    asset: Assets::BTC,
                    asset_type: AssetType::CkAsset(principal_collateral),
                    account: Principal::from_text("user-valid").unwrap_or_else(|_| Principal::management_canister()),
                    liquidation_bonus: 1000u64,
                    protocol_fee: 200u64,
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
                    liquidation_bonus: 1000u64,
                    protocol_fee: 200u64,
                },
                LiquidateblePosition {
                    pool_id: Principal::self_authenticating(random::<[u8; 32]>()),
                    debt_amount: Nat::from(0u64),
                    collateral_amount: Nat::from(2000u64),
                    asset: Assets::BTC,
                    asset_type: AssetType::CkAsset(principal_collateral),
                    account: Principal::anonymous(),
                    liquidation_bonus: 1000u64,
                    protocol_fee: 200u64,
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

    // Same asset repay and collateral: no swap is needed; ensures swap_args is None.
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
        config
            .expect_get_trader_principal()
            .return_const(Principal::from_text("aaaaa-aa").unwrap());
        config.expect_should_buy_bad_debt().return_const(false);

        let mut executor = MockIcrcSwapInterface::new();

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
                liquidation_bonus: 1000u64,
                protocol_fee: 200u64,
            }],
            total_debt: Nat::from(2000u64),
            health_factor: Nat::from(900u64),
        };

        let result = strategy.process(&vec![valid_user]).await.unwrap();

        // Only the valid user should yield one ExecutorRequest
        assert_eq!(result.len(), 1);
        assert!(result[0].swap_args.is_none());
    }

    // Skips when available balance is below fee threshold: no request is produced and services are not called.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_skips_on_insufficient_funds() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );
        let executor = mk_executor_panic();
        let collateral = mk_collateral_panic();
        // Available balance is less than fee*2 (10*2=20 > 10) so we skip
        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10),
                value: Nat::from(10u64), // available < fee*2
            })
        });
        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );
        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 1500);
        let usdc_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1000);
        let user = mk_user(vec![btc_pos, usdc_pos], 100, 900);
        let res = strategy.process(&vec![user]).await.unwrap();
        assert!(res.is_empty(), "expected no requests when funds are insufficient");
    }

    // Skips when HF is at or above threshold: no liquidation attempts are made.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_skips_when_hf_at_threshold() {
        use crate::icrc_token::icrc_token::IcrcToken;
        use crate::types::protocol_types::Assets;

        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(0u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(0u64),
        };

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
        config
            .expect_get_trader_principal()
            .return_const(Principal::anonymous());
        config.expect_should_buy_bad_debt().return_const(false);

        // Executor and collateral service should not be called since HF >= 1000 skips combos
        let mut executor = MockIcrcSwapInterface::new();
        executor
            .expect_get_swap_info()
            .returning(|_, _, _| panic!("executor should not be called when HF >= 1000"));

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("collateral service should not be called when HF >= 1000"));

        let debt_token_for_balance = debt_token.clone();
        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(move |_, _| {
            Some(IcrcTokenAmount {
                token: debt_token_for_balance.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let btc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(2_000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(collateral_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: 1000u64,
            protocol_fee: 200u64,
        };
        let usdc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(0u8),
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(debt_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: 1000u64,
            protocol_fee: 200u64,
        };

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![btc_pos, usdc_pos],
            total_debt: Nat::from(1_000u64),
            health_factor: Nat::from(1000u64),
        };

        let res = strategy.process(&vec![user]).await.unwrap();
        assert!(res.is_empty(), "expected no requests when HF >= 1000");
    }

    // When should_buy_bad_debt is true: allow negative profit and zero received collateral, still produce a request with no swap.
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_should_buy_bad_debt_allows_negative_profit_and_zero_collateral() {
        use crate::icrc_token::icrc_token::IcrcToken;
        use crate::types::protocol_types::Assets;

        let debt_token_principal = "xevnm-gaaaa-aaaar-qafnq-cai".to_string();
        let collateral_token_principal = "mxzaz-hqaaa-aaaar-qaada-cai".to_string();

        let debt_token = IcrcToken {
            ledger: Principal::from_text(&debt_token_principal).unwrap(),
            decimals: 6,
            name: "ckUSDC".to_string(),
            symbol: "ckUSDC".to_string(),
            fee: Nat::from(5u64),
        };

        let collateral_token = IcrcToken {
            ledger: Principal::from_text(&collateral_token_principal).unwrap(),
            decimals: 8,
            name: "ckBTC".to_string(),
            symbol: "ckBTC".to_string(),
            fee: Nat::from(1000u64),
        };

        let mut config = MockConfigTrait::new();
        config
            .expect_get_trader_principal()
            .return_const(Principal::anonymous());
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
        // allow negative profit and zero-collateral cases to proceed
        config.expect_should_buy_bad_debt().return_const(true);

        // Executor should not be called because we will have zero received collateral after fee
        let mut executor = MockIcrcSwapInterface::new();
        executor
            .expect_get_swap_info()
            .returning(|_, _, _| panic!("executor should not be called when received collateral is zero"));

        // Liquidation estimation where received_collateral equals the fee, so net becomes zero
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1000u64), // equals fee
                    repaid_debt: Nat::from(500u64),
                })
            });

        let mut account = MockIcrcAccountInfo::new();
        account.expect_get_cached_balance().returning(|_, _| {
            Some(IcrcTokenAmount {
                token: IcrcToken {
                    ledger: Principal::anonymous(),
                    decimals: 6,
                    name: "ckUSDC".to_string(),
                    symbol: "ckUSDC".to_string(),
                    fee: Nat::from(5u64),
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

        let btc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(2_000u64),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(collateral_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: 1000u64,
            protocol_fee: 200u64,
        };
        let usdc_pos = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(0u8),
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(debt_token.ledger),
            account: Principal::anonymous(),
            liquidation_bonus: 1000u64,
            protocol_fee: 200u64,
        };

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![btc_pos, usdc_pos],
            total_debt: Nat::from(1_000u64),
            health_factor: Nat::from(900u64),
        };

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(
            res.len(),
            1,
            "expected a request even with zero collateral and negative profit when should_buy_bad_debt=true"
        );
        assert!(
            res[0].swap_args.is_none(),
            "no swap expected when received collateral is zero"
        );
        assert!(res[0].expected_profit < 0, "profit should be negative in this setup");
    }

    // All users healthy: no liquidation attempts and no service calls
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_all_users_healthy_noops() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );

        // Both executor and collateral service would panic if called
        let executor = mk_executor_panic();
        let collateral = mk_collateral_panic();

        // Plenty of balance so the only gate is HF
        let account = mk_account_with_balance(6, 0, 1_000_000);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        // Two users, both healthy (HF >= 1000)
        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2_000);
        let usdc_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1_000);
        let u1 = mk_user(vec![btc_pos.clone(), usdc_pos.clone()], 1_000, 1000);
        let u2 = mk_user(vec![btc_pos, usdc_pos], 2_000, 1100);

        let res = strategy.process(&vec![u1, u2]).await.unwrap();
        assert!(res.is_empty(), "expected no requests when all users are healthy");
    }

    // After first liquidation, HF bumps to threshold and remaining combos for that user are skipped
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_hf_bump_stops_followup_combos_for_same_user() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );

        let mut calls = 0u32;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_, _, _, user| {
                calls += 1;
                // First call repays something and bumps HF to 1000; later calls should be skipped
                if calls == 1 {
                    user.health_factor = Nat::from(1000u64);
                    Ok(LiquidationEstimation {
                        received_collateral: Nat::from(4000u64),
                        repaid_debt: Nat::from(1000u64),
                    })
                } else {
                    // If the strategy still calls us again for the same user, fail the test
                    panic!("collateral service should not be called after HF bump");
                }
            });

        // Executor returns a profitable swap so the first combo produces a request
        let executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 2000);
        let account = mk_account_with_balance(6, 0, 1_000_000);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        // One user with two debt positions and one collateral position to form two combos
        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 3_000);
        let usdc_pos_1 = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1_000);
        let usdc_pos_2 = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 500);
        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            positions: vec![btc_pos, usdc_pos_1, usdc_pos_2],
            total_debt: Nat::from(1_500u64),
            health_factor: Nat::from(900u64),
        };

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(
            res.len(),
            1,
            "expected only one request; followup combos were skipped after HF bump"
        );
    }

    // Balance budgeting across multiple combos uses one wallet and skips when funds fall below fee threshold
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_balance_budgeting_skips_followup_combo() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );

        // Collateral service returns a fixed repay to control budgeting
        let collateral = mk_collateral_reply(4000, 1000);
        // Executor profitable so one request is produced
        let executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 2000);

        // Initial available balance A = R + 4F - 1 = 1000 + 40 - 1 = 1039
        // After the first request, new balance = A - R - 2F = 19 < 2F, so next combo is skipped
        let account = mk_account_with_balance(6, 10, 1039);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        // One user with one debt and two collateral positions produces two combos against same repay token
        let btc_pos1 = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 3_000);
        let btc_pos2 = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2_500);
        let usdc_debt = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 2_000);
        let user = mk_user(vec![btc_pos1, btc_pos2, usdc_debt], 2_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1, "expected only one request due to balance budgeting");
    }

    // Zero net collateral is skipped when should_buy_bad_debt is false
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_zero_net_collateral_skips_when_flag_false() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 5);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 1000);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );

        // Received collateral equals fee, strategy will net it to zero and skip when flag is false
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1000u64),
                    repaid_debt: Nat::from(500u64),
                })
            });

        // Executor must not be called
        let executor = mk_executor_panic();
        let account = mk_account_with_balance(6, 5, 10_000);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let btc_pos = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2_000);
        let usdc_pos = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1_000);
        let user = mk_user(vec![btc_pos, usdc_pos], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert!(
            res.is_empty(),
            "expected skip when net collateral is zero and flag is false"
        );
    }

    // Ordering: lower HF is processed first
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_orders_by_health_factor() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );

        // First call should be for the lower HF user; assert inside the mock then return a valid estimation
        let mut first_called_for_low_hf = true;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_, _, _coll_pos, user| {
                if first_called_for_low_hf {
                    // Assert the first processed user has HF 900
                    assert_eq!(
                        user.health_factor,
                        Nat::from(900u64),
                        "expected lower HF to be processed first"
                    );
                    first_called_for_low_hf = false;
                }
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4000u64),
                    repaid_debt: Nat::from(1000u64),
                })
            });

        let executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 2000);
        let account = mk_account_with_balance(6, 0, 1_000_000);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let btc = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2_000);
        let debt = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1_000);
        let low_hf = mk_user(vec![btc.clone(), debt.clone()], 1_000, 900);
        let high_hf = mk_user(vec![btc, debt], 1_000, 950);

        let _ = strategy.process(&vec![low_hf, high_hf]).await.unwrap();
    }

    // Ordering tie: for equal HF, larger debt is processed first
    #[tokio::test]
    async fn test_icrc_liquidation_strategy_orders_by_debt_when_hf_equal() {
        let debt_token = mk_token("xevnm-gaaaa-aaaar-qafnq-cai", "ckUSDC", "ckUSDC", 6, 10);
        let collateral_token = mk_token("mxzaz-hqaaa-aaaar-qaada-cai", "ckBTC", "ckBTC", 8, 10);
        let mut config = mk_config_with_maps(
            &[("xevnm-gaaaa-aaaar-qafnq-cai".to_string(), debt_token.clone())],
            &[("mxzaz-hqaaa-aaaar-qaada-cai".to_string(), collateral_token.clone())],
            Principal::anonymous(),
            false,
        );
        config
            .expect_get_trader_principal()
            .return_const(Principal::from_text("aaaaa-aa").unwrap());

        // Assert the first debt seen is the larger one when HF ties
        let mut first_checked = true;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_, debt_pos, _coll_pos, _user| {
                if first_checked {
                    assert_eq!(
                        debt_pos.debt_amount,
                        Nat::from(2_000u64),
                        "expected larger debt first on HF tie"
                    );
                    first_checked = false;
                }
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4000u64),
                    repaid_debt: Nat::from(1000u64),
                })
            });

        let executor = mk_executor_reply("ckBTC".to_string(), "ckUSDC".to_string(), 2000);
        let account = mk_account_with_balance(6, 0, 1_000_000);

        let strategy = IcrcLiquidationStrategy::new(
            Arc::new(config),
            Arc::new(executor),
            Arc::new(collateral),
            Arc::new(account),
        );

        let btc = pos_collateral_btc(Principal::anonymous(), collateral_token.ledger, 2_000);
        let debt_small = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 1_000);
        let debt_large = pos_debt_usdc(Principal::anonymous(), debt_token.ledger, 2_000);

        // Two users with equal HF; the larger debt combo should be handled first
        let u_small = mk_user(vec![btc.clone(), debt_small], 1_000, 900);
        let u_large = mk_user(vec![btc, debt_large], 2_000, 900);

        let _ = strategy.process(&vec![u_small, u_large]).await.unwrap();
    }
}
