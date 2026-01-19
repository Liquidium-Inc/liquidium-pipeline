use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::config::ConfigTrait;
use crate::executors::executor::ExecutorRequest;

use crate::liquidation::collateral_service::CollateralServiceTrait;
use crate::stage::PipelineStage;

use crate::swappers::swap_interface::SwapInterface;

use candid::{Int, Nat};
use futures::TryFutureExt;
use liquidium_pipeline_core::{
    balance_service::BalanceService,
    tokens::{
        asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
        token_registry::TokenRegistryTrait,
    },
    types::protocol_types::{Asset, AssetType, LiquidateblePosition, LiquidatebleUser, LiquidationRequest},
};
use log::{debug, info};

use num_traits::ToPrimitive;

use crate::swappers::model::SwapRequest;
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use async_trait::async_trait;

use itertools::Itertools;

fn resolve_token_for_position(registry: &dyn TokenRegistryTrait, pos: &LiquidateblePosition) -> Option<ChainToken> {
    match pos.asset_type {
        AssetType::CkAsset(principal) => {
            // Build the AssetId the same way you did when populating the registry.
            // Adjust this to your actual AssetId shape / constructors.
            let id = AssetId {
                chain: "icp".to_string(),
                address: principal.to_text(),
                symbol: pos.asset.symbol(),
            };
            registry.get(&id)
        }
        _ => None,
    }
}

pub struct SimpleLiquidationStrategy<T, C, R, U>
where
    T: SwapInterface + Send + Sync,
    C: ConfigTrait,
    R: TokenRegistryTrait,
    U: CollateralServiceTrait,
{
    pub config: Arc<C>,
    pub registry: Arc<R>,
    pub swapper: Arc<T>,
    pub collateral_service: Arc<U>,
    pub account_service: Arc<BalanceService>,
    pub watchdog: Arc<dyn Watchdog>,
}

impl<T, C, R, U> SimpleLiquidationStrategy<T, C, R, U>
where
    T: SwapInterface,
    C: ConfigTrait,
    R: TokenRegistryTrait,
    U: CollateralServiceTrait,
{
    pub fn new(
        config: Arc<C>,
        registry: Arc<R>,
        swapper: Arc<T>,
        collateral_service: Arc<U>,
        balance_service: Arc<BalanceService>,
    ) -> Self {
        Self {
            config,
            registry,
            swapper,
            collateral_service,
            account_service: balance_service,
            watchdog: noop_watchdog(),
        }
    }

    pub fn with_watchdog(mut self, wd: Arc<dyn Watchdog>) -> Self {
        self.watchdog = wd;
        self
    }

    // Helper: Prefetch balances for all debt assets we might need
    async fn prefetch_balances_for_users(&self, users: &[LiquidatebleUser]) -> Result<HashMap<String, Nat>, String> {
        let mut debt_assets: HashSet<ChainToken> = HashSet::new();
        for user in users.iter() {
            for pos in user.positions.iter() {
                if pos.debt_amount > 0u8
                    && let Some(token) = resolve_token_for_position(self.registry.as_ref(), pos)
                {
                    debt_assets.insert(token);
                }
            }
        }

        let mut balances: HashMap<String, Nat> = HashMap::new();
        for asset in debt_assets {
            let balance = self
                .account_service
                .get_balance(&asset.asset_id())
                .map_err(|_| "Could not get balance".to_string())
                .await?
                .value;

            balances.insert(asset.asset_id().address, balance);
        }

        Ok(balances)
    }

    fn sort_users_by_health(&self, users: &[LiquidatebleUser]) -> Vec<LiquidatebleUser> {
        users
            .iter()
            .sorted_by(|a, b| a.health_factor.cmp(&b.health_factor))
            .cloned()
            .collect()
    }

    #[allow(clippy::type_complexity)]
    fn build_combos(
        &self,
        work_users: &[LiquidatebleUser],
    ) -> (
        Vec<(usize, LiquidateblePosition, LiquidateblePosition)>,
        Vec<(usize, LiquidateblePosition)>,
    ) {
        let mut combos: Vec<(usize, LiquidateblePosition, LiquidateblePosition)> = Vec::new();
        let mut bad_debts: Vec<(usize, LiquidateblePosition)> = Vec::new();

        for (idx, user) in work_users.iter().enumerate() {
            let debts: Vec<_> = user.positions.iter().filter(|p| p.debt_amount > 0u8).cloned().collect();

            let colls: Vec<_> = user
                .positions
                .iter()
                .filter(|p| p.collateral_amount > 0u8)
                .cloned()
                .collect();

            if debts.is_empty() {
                continue;
            }

            if colls.is_empty() {
                info!(
                    "User {:?} has debt but no collateral. Treating as bad debt and creating bad-debt entries.",
                    user.account
                );
                for d in debts {
                    bad_debts.push((idx, d));
                }
                continue;
            }

            for d in &debts {
                for c in &colls {
                    combos.push((idx, d.clone(), c.clone()));
                }
            }
        }

        (combos, bad_debts)
    }

    async fn handle_bad_debt_positions(
        &self,
        bad_debts: Vec<(usize, LiquidateblePosition)>,
        balances: &mut HashMap<String, Nat>,
        work_users: &mut [LiquidatebleUser],
        result: &mut Vec<ExecutorRequest>,
    ) -> Result<(), String> {
        if bad_debts.is_empty() {
            return Ok(());
        }

        if !self.config.should_buy_bad_debt() {
            // Config says we should not buy bad debt; just log and skip.
            for (idx, pos) in bad_debts {
                info!(
                    "Skipping pure bad debt for user {:?} pool {:?} amount {} because should_buy_bad_debt=false",
                    work_users[idx].account, pos.pool_id, pos.debt_amount,
                );
            }
            return Ok(());
        }

        for (user_idx, debt_position) in bad_debts {
            if work_users[user_idx].health_factor >= 1000u32 {
                continue;
            }

            if !matches!(debt_position.asset_type, AssetType::CkAsset(_)) {
                return Err("invalid asset type".to_string());
            }

            let repayment_token = if let Some(tok) = resolve_token_for_position(self.registry.as_ref(), &debt_position)
            {
                tok
            } else {
                debug!(
                    "Skipping bad-debt position due to unknown debt asset {:?}",
                    debt_position.asset
                );
                continue;
            };

            let balance_key = repayment_token.asset_id().address.clone();
            let Some(available_balance) = balances.get_mut(&balance_key) else {
                debug!("Asset balance not found for bad-debt position: {:?}", balance_key);
                self.watchdog
                    .notify(WatchdogEvent::BalanceMissing {
                        asset: &debt_position.asset.to_string(),
                    })
                    .await;
                continue;
            };

            if available_balance.clone() < repayment_token.fee() * 2u64 {
                self.watchdog
                    .notify(WatchdogEvent::InsufficientFunds {
                        asset: &debt_position.asset.to_string(),
                        available: available_balance.to_string(),
                    })
                    .await;
                debug!(
                    "Skipping bad-debt position due to insufficient funds: asset={}, available={} < min_required={}",
                    debt_position.asset,
                    available_balance,
                    repayment_token.fee() * 2u64
                );
                continue;
            }

            let max_balance = available_balance.clone() - repayment_token.fee() * 2u64;

            // We buy as much bad debt as we can, capped by wallet balance and position size.
            let repay_amount = if max_balance.clone() < debt_position.debt_amount {
                max_balance
            } else {
                debt_position.debt_amount.clone()
            };

            if repay_amount == 0u64 {
                continue;
            }

            // No collateral to seize, so amount_received is zero and profit is strictly negative.
            let amount_received: Nat = Nat::from(0u64);
            let profit = Int::from(amount_received.clone())
                - Int::from(repay_amount.clone())
                - Int::from(repayment_token.fee()) * 2u128;

            info!(
                "Buying pure bad debt: repay={} {} profit={} {}",
                repay_amount.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64,
                repayment_token.symbol(),
                profit.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64,
                repayment_token.symbol()
            );

            if profit <= 0 && !self.config.should_buy_bad_debt() {
                continue;
            }

            if available_balance.clone() >= repay_amount.clone() + repayment_token.fee() * 2u64 {
                *available_balance = available_balance.clone() - repay_amount.clone() - repayment_token.fee() * 2u64;
            } else {
                debug!(
                    "Available balance would underflow when updating bad-debt balance. available={}, repay={}, fees={}",
                    available_balance,
                    repay_amount,
                    repayment_token.fee() * 2u64
                );
                continue;
            }

            result.push(ExecutorRequest {
                debt_asset: repayment_token.clone(),
                collateral_asset: repayment_token.clone(),
                liquidation: LiquidationRequest {
                    borrower: debt_position.account,
                    debt_pool_id: debt_position.pool_id,
                    collateral_pool_id: debt_position.pool_id,
                    debt_amount: repay_amount.clone(),
                    receiver_address: self.config.get_trader_principal(),
                    buy_bad_debt: true,
                },
                ref_price: 0u32.into(),
                swap_args: None,
                expected_profit: profit.0.to_i128().unwrap(),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, T, C, R, U> PipelineStage<'a, Vec<LiquidatebleUser>, Vec<ExecutorRequest>>
    for SimpleLiquidationStrategy<T, C, R, U>
where
    T: SwapInterface,
    C: ConfigTrait,
    R: TokenRegistryTrait + 'static,
    U: CollateralServiceTrait,
{
    async fn process(&self, users: &'a Vec<LiquidatebleUser>) -> Result<Vec<ExecutorRequest>, String> {
        let mut result: Vec<ExecutorRequest> = Vec::new();

        // Prefetch balances for all debt assets we might need
        let mut balances = self.prefetch_balances_for_users(users).await?;

        // Take smallest hf first
        let users_sorted: Vec<LiquidatebleUser> = self.sort_users_by_health(users);

        // Working copy of users for in-loop mutation
        let mut work_users: Vec<LiquidatebleUser> = users_sorted.clone();

        // Build all candidate (user_idx, debt_position, collateral_position) combinations,
        // and collect pure bad-debt positions.
        let (mut combos, bad_debts) = self.build_combos(&work_users);

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

            if !matches!(debt_position.asset_type, AssetType::CkAsset(_))
                || !matches!(collateral_position.asset_type, AssetType::CkAsset(_))
            {
                return Err("invalid asset type".to_string());
            }

            let repayment_token = if let Some(tok) = resolve_token_for_position(self.registry.as_ref(), &debt_position)
            {
                tok
            } else {
                debug!("Skipping combo due to unknown debt asset {:?}", debt_position.asset);
                continue;
            };

            let collateral_token =
                if let Some(tok) = resolve_token_for_position(self.registry.as_ref(), &collateral_position) {
                    tok
                } else {
                    debug!(
                        "Skipping combo due to unknown collateral asset {:?}",
                        collateral_position.asset
                    );
                    continue;
                };

            let balance_key = repayment_token.asset_id().address.clone();
            let Some(available_balance) = balances.get_mut(&balance_key) else {
                debug!("Asset balance not found {:?}", balance_key);
                self.watchdog
                    .notify(WatchdogEvent::BalanceMissing {
                        asset: &debt_position.asset.to_string(),
                    })
                    .await;
                continue;
            };

            if available_balance.clone() < repayment_token.fee() * 2u64 {
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
                    repayment_token.fee() * 2u64
                );
                continue;
            }

            let max_balance = available_balance.clone() - repayment_token.fee() * 2u64;

            debug!(
                "available_balance: {:?} repayment_token_fee {:?} max_balance: {:?}",
                available_balance,
                repayment_token.fee(),
                max_balance
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

            estimation.received_collateral = if estimation.received_collateral < collateral_token.fee() {
                0u64.into()
            } else {
                estimation.received_collateral - collateral_token.fee()
            };

            if !self.config.should_buy_bad_debt() && estimation.received_collateral == 0u32 {
                info!(
                    "Not enough collateral {} to cover fees {}, skipping liquidation",
                    estimation.received_collateral,
                    collateral_token.fee()
                );
                continue;
            }

            let amount_in = ChainTokenAmount {
                token: collateral_token.clone(),
                value: estimation.received_collateral.clone(),
            };

            let same_asset = collateral_token.asset_id().address == repayment_token.asset_id().address;
            let (swap_args, amount_received, price) = if estimation.received_collateral == 0u32 || same_asset {
                (None, amount_in.value.clone(), 1f64)
            } else {
                debug!(" ======== Amount in {:?}", amount_in);
                let swap_request = SwapRequest {
                    pay_asset: collateral_token.asset_id(),
                    pay_amount: amount_in,
                    receive_asset: repayment_token.asset_id(),
                    receive_address: Some(self.config.get_liquidator_principal().to_string()),
                    max_slippage_bps: Some(2000),
                    venue_hint: None,
                };
                let swap_info = self
                    .swapper
                    .quote(&swap_request)
                    .await
                    .expect("could not get swap info");

                (Some(swap_request), swap_info.receive_amount, swap_info.mid_price)
            };

            info!(
                "repaid_debt={} ({}),  amount_received={} ({}), price={}",
                estimation.repaid_debt.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64,
                repayment_token.symbol(),
                amount_received.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64,
                repayment_token.symbol(),
                price
            );

            let profit = Int::from(amount_received)
                - Int::from(estimation.repaid_debt.clone())
                - Int::from(repayment_token.fee()) * 2u128
                - Int::from(collateral_token.fee()) * 2u128;

            info!(
                "dx Profit {} {}...",
                profit.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64,
                repayment_token.symbol()
            );

            if profit <= 0 && !self.config.should_buy_bad_debt() {
                continue;
            }

            debug!(
                "Updating available balance: {:?} {} {}",
                available_balance,
                estimation.repaid_debt,
                repayment_token.fee()
            );

            if available_balance.clone() >= estimation.repaid_debt.clone() + repayment_token.fee() * 2u64 {
                *available_balance =
                    available_balance.clone() - estimation.repaid_debt.clone() - repayment_token.fee() * 2u64;
            } else {
                debug!(
                    "Available balance would underflow when updating. available={}, repay={}, fees={}",
                    available_balance,
                    estimation.repaid_debt,
                    repayment_token.fee() * 2u64
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
                    buy_bad_debt: false,
                },
                ref_price: estimation.ref_price,
                swap_args,
                expected_profit: profit.0.to_i128().unwrap(),
            });

            if profit <= 0 && self.config.should_buy_bad_debt() {
                info!(
                    "Buying bad debt {}",
                    estimation.repaid_debt.0.to_f64().unwrap() / 10u32.pow(repayment_token.decimals() as u32) as f64
                );
            }
        }

        // After handling normal collateral-backed combos, handle pure bad-debt positions.
        self.handle_bad_debt_positions(bad_debts, &mut balances, &mut work_users, &mut result)
            .await?;

        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MockConfigTrait;
    use crate::liquidation::collateral_service::{LiquidationEstimation, MockCollateralServiceTrait};
    use crate::swappers::swap_interface::MockSwapInterface;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::account::actions::MockAccountInfo;
    use liquidium_pipeline_core::tokens::token_registry::MockTokenRegistryTrait;
    use liquidium_pipeline_core::types::protocol_types::Assets;

    use std::sync::Arc;

    fn p(text: &str) -> Principal {
        Principal::from_text(text).unwrap_or_else(|_| Principal::management_canister())
    }

    fn mk_icp_token(symbol: &str, decimals: u8) -> ChainToken {
        // Fee comes from ChainToken impl, not from tests.
        ChainToken::Icp {
            ledger: p("mxzaz-hqaaa-aaaar-qaada-cai"),
            symbol: symbol.to_string(),
            decimals,
            fee: 100u8.into(),
        }
    }

    fn mk_position(
        pool: Principal,
        account: Principal,
        ledger: Principal,
        debt: u64,
        coll: u64,
        asset: Assets,
    ) -> LiquidateblePosition {
        LiquidateblePosition {
            pool_id: pool,
            debt_amount: Nat::from(debt),
            collateral_amount: Nat::from(coll),
            asset,
            asset_type: AssetType::CkAsset(ledger),
            account,
            liquidation_bonus: 1000,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        }
    }

    fn mk_user(positions: Vec<LiquidateblePosition>, total_debt: u64, hf: u64) -> LiquidatebleUser {
        LiquidatebleUser {
            account: Principal::anonymous(),
            positions,
            total_debt: Nat::from(total_debt),
            health_factor: Nat::from(hf),
            weighted_liquidation_threshold: Nat::from(8500u64),
        }
    }

    // Happy path: same asset for debt and collateral, no swap needed, one ExecutorRequest emitted.
    #[tokio::test]
    async fn simple_strategy_happy_path_same_asset_no_swap() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        // Registry always resolves to the same token for this test.
        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        // Config
        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        // Collateral service: repay 1_000, receive 2_000 collateral
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(2_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                })
            });

        // Account service: plenty of balance
        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        // Swapper should not be called because asset ids match (no swap path).
        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called in same-asset happy path"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-valid");
        let pos = mk_position(pool, borrower, ledger, 1_000, 2_000, Assets::USDC);
        let user = mk_user(vec![pos.clone()], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1);
        let req = &res[0];
        assert_eq!(req.liquidation.borrower, pos.account);
        assert!(req.swap_args.is_none(), "no swap expected when assets match");
    }

    // Fails fast when balance fetch errors.
    #[tokio::test]
    async fn simple_strategy_fails_on_missing_balance() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("collateral should not be called when balance fetch fails"));

        let mut account = MockAccountInfo::new();
        account
            .expect_sync_balance()
            .returning(move |_t: &ChainToken| Err("boom".to_string()));
        account
            .expect_get_balance()
            .returning(|_t: &ChainToken| Err("boom".to_string()));

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when balance fetch fails"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-bad-balance");
        let pos = mk_position(pool, borrower, ledger, 1_000, 2_000, Assets::USDC);
        let user = mk_user(vec![pos], 1_000, 900);

        let res = strategy.process(&vec![user]).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "Could not get balance");
    }

    // Errors on unsupported asset type.
    #[tokio::test]
    async fn simple_strategy_fails_on_unsupported_asset_type() {
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("collateral should not be called for invalid asset type"));

        let mut account = MockAccountInfo::new();
        account
            .expect_sync_balance()
            .returning(|_t: &ChainToken| panic!("sync_balance should not be called for invalid asset type"));
        account
            .expect_get_balance()
            .returning(|_t: &ChainToken| panic!("balance should not be fetched for invalid asset type"));

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called for invalid asset type"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-invalid-asset");
        let pos = LiquidateblePosition {
            pool_id: pool,
            debt_amount: Nat::from(1_000u64),
            collateral_amount: Nat::from(1_000u64),
            asset: Assets::USDC,
            asset_type: AssetType::Unknown,
            account: borrower,
            liquidation_bonus: 1000,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        };

        let user = mk_user(vec![pos], 1_000, 900);
        let res = strategy.process(&vec![user]).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "invalid asset type");
    }

    // HF at or above 1000: no liquidation attempts.
    #[tokio::test]
    async fn simple_strategy_skips_when_hf_at_threshold() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("collateral should not be called when HF >= 1000"));

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when HF >= 1000"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-healthy");
        let pos = mk_position(pool, borrower, ledger, 1_000, 2_000, Assets::USDC);
        let user = mk_user(vec![pos], 1_000, 1_000); // HF == 1000

        let res = strategy.process(&vec![user]).await.unwrap();
        assert!(res.is_empty(), "expected no requests when HF >= 1000");
    }

    // When should_buy_bad_debt is true: allow negative profit and zero collateral, still produce a request.
    #[tokio::test]
    async fn simple_strategy_should_buy_bad_debt_allows_negative_profit_and_zero_collateral() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = ChainToken::Icp {
            ledger,
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(true);

        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1_000u64), // equals fee, nets to zero
                    repaid_debt: Nat::from(500u64),
                    ref_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when assets are equal"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-bad-debt");
        let pos = mk_position(pool, borrower, ledger, 1_000, 2_000, Assets::USDC);
        let user = mk_user(vec![pos], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(
            res.len(),
            1,
            "expected a request even with zero net collateral and negative profit"
        );
        assert!(res[0].swap_args.is_none(), "no swap expected when assets match");
        assert!(res[0].expected_profit < 0, "profit should be negative in this setup");
    }

    // Balance budgeting across multiple combos uses one wallet and skips when funds fall below fee threshold.
    #[tokio::test]
    async fn simple_strategy_balance_budgeting_skips_followup_combo() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6); // fee = 100

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        // Collateral service returns fixed repay so we can control budgeting
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                })
            });

        // Initial available balance B = R + 4F - 1 = 1000 + 400 - 1 = 1399
        // After first request, new balance = B - R - 2F = 199 < 2F, so next combo is skipped.
        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_399u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_399u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when assets match"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        // One user with one debt and two collateral positions produces two combos against same repay token
        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-budget");
        let debt_pos = mk_position(pool, borrower, ledger, 1_000, 0, Assets::USDC);
        let coll_pos1 = mk_position(pool, borrower, ledger, 0, 3_000, Assets::USDC);
        let coll_pos2 = mk_position(pool, borrower, ledger, 0, 2_500, Assets::USDC);
        let user = mk_user(vec![debt_pos, coll_pos1, coll_pos2], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1, "expected only one request due to balance budgeting");
    }

    // After first liquidation, HF bumps to threshold and remaining combos for that user are skipped.
    #[tokio::test]
    async fn simple_strategy_hf_bump_stops_followup_combos_for_same_user() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut calls = 0u32;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral.expect_calculate_liquidation_amounts().returning(
            move |_max_balance, _debt_pos, _coll_pos, user: &mut LiquidatebleUser| {
                calls += 1;
                if calls == 1 {
                    user.health_factor = Nat::from(1_000u64);
                    Ok(LiquidationEstimation {
                        received_collateral: Nat::from(4_000u64),
                        repaid_debt: Nat::from(1_000u64),
                        ref_price: Nat::from(0u8),
                    })
                } else {
                    panic!("collateral service should not be called after HF bump");
                }
            },
        );

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when assets match"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-hf-bump");
        let coll_pos = mk_position(pool, borrower, ledger, 0, 3_000, Assets::USDC);
        let debt_pos1 = mk_position(pool, borrower, ledger, 1_000, 0, Assets::USDC);
        let debt_pos2 = mk_position(pool, borrower, ledger, 500, 0, Assets::USDC);
        let user = mk_user(vec![coll_pos, debt_pos1, debt_pos2], 1_500, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(
            res.len(),
            1,
            "expected only one request; followup combos were skipped after HF bump"
        );
    }

    // Ordering: lower HF is processed first.
    #[tokio::test]
    async fn simple_strategy_orders_by_health_factor() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut first_called_for_low_hf = true;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral.expect_calculate_liquidation_amounts().returning(
            move |_max_balance, _debt_pos, _coll_pos, user: &mut LiquidatebleUser| {
                if first_called_for_low_hf {
                    assert_eq!(
                        user.health_factor,
                        Nat::from(900u64),
                        "expected lower HF to be processed first"
                    );
                    first_called_for_low_hf = false;
                }
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                })
            },
        );

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when assets match"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower_low = p("user-low-hf");
        let borrower_high = p("user-high-hf");

        let debt_pos = mk_position(pool, borrower_low, ledger, 1_000, 0, Assets::USDC);
        let coll_pos = mk_position(pool, borrower_low, ledger, 0, 2_000, Assets::USDC);
        let low_hf_user = mk_user(vec![debt_pos.clone(), coll_pos.clone()], 1_000, 900);

        let debt_pos2 = mk_position(pool, borrower_high, ledger, 1_000, 0, Assets::USDC);
        let coll_pos2 = mk_position(pool, borrower_high, ledger, 0, 2_000, Assets::USDC);
        let high_hf_user = mk_user(vec![debt_pos2, coll_pos2], 1_000, 950);

        let _ = strategy.process(&vec![high_hf_user, low_hf_user]).await.unwrap();
    }

    // Ordering tie: for equal HF, larger debt is processed first.
    #[tokio::test]
    async fn simple_strategy_orders_by_debt_when_hf_equal() {
        let ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let token = mk_icp_token("ckUSDC", 6);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);

        let mut first_checked = true;
        let mut collateral = MockCollateralServiceTrait::new();
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_max_balance, debt_pos, _coll_pos, _user| {
                if first_checked {
                    assert_eq!(
                        debt_pos.debt_amount,
                        Nat::from(2_000u64),
                        "expected larger debt first on HF tie"
                    );
                    first_checked = false;
                }
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000u64),
            })
        });

        let mut swapper = MockSwapInterface::new();
        swapper
            .expect_quote()
            .returning(|_req| panic!("quote should not be called when assets match"));

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(swapper),
            Arc::new(collateral),
            balance_service,
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower1 = p("user-small-debt");
        let borrower2 = p("user-large-debt");

        let coll_pos = mk_position(pool, borrower1, ledger, 0, 2_000, Assets::USDC);
        let debt_small = mk_position(pool, borrower1, ledger, 1_000, 0, Assets::USDC);
        let debt_large = mk_position(pool, borrower2, ledger, 2_000, 0, Assets::USDC);

        let user_small = mk_user(vec![coll_pos.clone(), debt_small], 1_000, 900);
        let user_large = mk_user(vec![coll_pos, debt_large], 2_000, 900);

        let _ = strategy.process(&vec![user_small, user_large]).await.unwrap();
    }
}
