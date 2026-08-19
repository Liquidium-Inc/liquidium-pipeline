use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::config::ConfigTrait;
use crate::constants::repay_buffer_native_units;
use crate::executors::executor::ExecutorRequest;

use crate::approval_state::ApprovalState;
use crate::liquidation::collateral_service::CollateralServiceTrait;
use crate::liquidation::settlement_speed::{SettlementSpeed, deposit_speed, repayment_speed};
use crate::stage::PipelineStage;

use candid::{Int, Nat};
use futures::TryFutureExt;
use liquidium_pipeline_core::{
    balance_service::BalanceService,
    tokens::{
        asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
        token_registry::TokenRegistryTrait,
    },
    types::protocol_types::{Asset, AssetType, Assets, LiquidateblePosition, LiquidatebleUser, LiquidationRequest},
};
use log::{debug, info, warn};

use num_traits::ToPrimitive;

use crate::liquidation::liquidation_math::oracle_implied_output;
use crate::swappers::model::{BASIS_POINTS_DENOMINATOR, SwapRequest, amount_after_bps_haircut};
use crate::utils::{ICP_LEDGER_PRINCIPAL, max_for_ledger, now_ts};
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use async_trait::async_trait;

use itertools::Itertools;

#[cfg(test)]
const WIPEOUT_THRESHOLD: u32 = 975;

fn resolve_token_for_position(registry: &dyn TokenRegistryTrait, pos: &LiquidateblePosition) -> Option<ChainToken> {
    if pos.asset.symbol().eq_ignore_ascii_case("ICP") {
        let id = AssetId {
            chain: "icp".to_string(),
            address: ICP_LEDGER_PRINCIPAL.to_string(),
            symbol: "ICP".to_string(),
        };
        return registry.get(&id);
    }

    match pos.asset_type {
        AssetType::CkAsset(principal) => {
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

fn is_supported_position_asset_type(pos: &LiquidateblePosition) -> bool {
    pos.asset.symbol().eq_ignore_ascii_case("ICP") || matches!(pos.asset_type, AssetType::CkAsset(_))
}

/// One (debt, collateral) candidate together with everything ranking orders it
/// by, so the comparator reads as the policy instead of as tuple indices.
struct RankedCombo {
    user_idx: usize,
    debt_position: LiquidateblePosition,
    collateral_position: LiquidateblePosition,
    debt_value: Nat,
    collateral_value: Nat,
    /// The outbound leg: how the seized collateral reaches the venue.
    settlement: SettlementSpeed,
    /// The return leg: how the proceeds come back as the repayment asset.
    repayment_settlement: SettlementSpeed,
    /// `debt_value` after the return-leg haircut, which is what ranking orders
    /// debts by. See [`RETURN_LEG_HAIRCUT_BPS`].
    ranked_debt_value: Nat,
    /// Whether this collateral can back a full repayment of this debt.
    covers_debt: bool,
}

/// How much of a debt's ranking weight a bridged return leg gives up.
///
/// Unlike the outbound leg, which collateral to seize is nearly a free choice
/// -- two positions that both cover the debt seize the same value, so only time
/// differs. Which debt to repay is not: a bigger debt earns a bigger bonus, so
/// preferring a faster one trades real profit for capital velocity.
///
/// A haircut rather than a tier expresses that trade-off directly and, unlike a
/// "within 25% of each other" rule, stays a total order -- proximity is not
/// transitive, and `sort_by` panics on a comparator that is not. At 2500 bps a
/// debt whose proceeds bridge must be a third larger to outrank one whose
/// proceeds come straight home, which keeps a trivially small fast debt from
/// displacing a materially bigger slow one. That matters because liquidations
/// are contested: taking the small one first can raise the borrower's health
/// factor enough to lose the large one to somebody else entirely.
const RETURN_LEG_HAIRCUT_BPS: u32 = 2_500;

/// Whether collateral worth `collateral_value` can back a repayment worth
/// `debt_value`, both in RAY.
///
/// Mirrors the cap `max_repay_from_collateral` applies: repaying value V
/// seizes `V * denom_bps / 10_000` of collateral, where the denominator
/// carries the liquidation bonus net of the protocol's cut of it. Inverted and
/// cross-multiplied so the comparison stays in integers.
///
/// The bps are clamped rather than rejected. Out-of-range values make the
/// estimator fail this combo a moment later anyway, and ranking must not panic
/// on a `Nat` subtraction to find that out.
fn collateral_covers_debt(
    debt_value: &Nat,
    collateral_value: &Nat,
    collateral_position: &LiquidateblePosition,
) -> bool {
    let one_bps = Nat::from(BASIS_POINTS_DENOMINATOR);
    let bonus = Nat::from(collateral_position.liquidation_bonus);
    let fee = Nat::from(
        collateral_position
            .protocol_fee
            .min(u64::from(BASIS_POINTS_DENOMINATOR)),
    );

    let bonus_fee = (bonus.clone() * fee) / one_bps.clone();
    let denom_bps = one_bps.clone() + bonus - bonus_fee;

    collateral_value.clone() * one_bps >= debt_value.clone() * denom_bps
}

pub struct SimpleLiquidationStrategy<C, R, U>
where
    C: ConfigTrait,
    R: TokenRegistryTrait,
    U: CollateralServiceTrait,
{
    pub config: Arc<C>,
    pub registry: Arc<R>,
    pub collateral_service: Arc<U>,
    pub account_service: Arc<BalanceService>,
    pub approval_state: Arc<ApprovalState>,
    pub watchdog: Arc<dyn Watchdog>,
    /// ICP-native symbols the enabled CEX venues list themselves, used to rank
    /// collateral that recycles in seconds ahead of collateral that has to
    /// bridge. Empty makes every collateral count as delayed, which leaves
    /// ranking on collateral value exactly as it was before.
    venue_native_symbols: Vec<String>,
}

impl<C, R, U> SimpleLiquidationStrategy<C, R, U>
where
    C: ConfigTrait,
    R: TokenRegistryTrait,
    U: CollateralServiceTrait,
{
    pub fn new(
        config: Arc<C>,
        registry: Arc<R>,
        collateral_service: Arc<U>,
        balance_service: Arc<BalanceService>,
        approval_state: Arc<ApprovalState>,
    ) -> Self {
        Self {
            config,
            registry,
            collateral_service,
            account_service: balance_service,
            approval_state,
            watchdog: noop_watchdog(),
            venue_native_symbols: Vec::new(),
        }
    }

    pub fn with_watchdog(mut self, wd: Arc<dyn Watchdog>) -> Self {
        self.watchdog = wd;
        self
    }

    /// Declares which collateral symbols reach a CEX without bridging, so
    /// ranking can prefer the ones that free capital again in seconds.
    pub fn with_venue_native_symbols(mut self, symbols: Vec<String>) -> Self {
        self.venue_native_symbols = symbols;
        self
    }

    fn debt_approval_needed(&self, token: &ChainToken) -> bool {
        let ChainToken::Icp { ledger, .. } = token else {
            return false;
        };

        let threshold = max_for_ledger(ledger) / Nat::from(2u8);
        self.approval_state
            .needs_approval(*ledger, self.config.get_lending_canister(), &threshold)
    }

    /// Ranking weight for one leg of a combo.
    ///
    /// Ranking now consults the oracle for combos the main loop may never
    /// evaluate -- an unsupported asset, a position with no balance to fund it.
    /// A price we cannot fetch must therefore never sink the cycle, or one
    /// borrower holding an unknown asset would block every liquidation in it.
    /// The combo sorts last instead, and the loop's own gates handle it exactly
    /// as they did before ranking consulted prices at all.
    async fn ranking_value(&self, asset: &Assets, native_amount: &Nat, prices: &mut HashMap<String, Nat>) -> Nat {
        match self.quote_value_ray(asset, native_amount, prices).await {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    "Could not price {} for ranking; sorting this position last: {}",
                    asset.symbol(),
                    err
                );
                Nat::from(0u8)
            }
        }
    }

    /// Both legs of this combo's round trip: how the seized collateral reaches
    /// a venue, and how the proceeds come back as the repayment asset.
    ///
    /// Either side resolving to no known token counts as delayed. The loop
    /// skips such a combo a moment later anyway, and calling it fast would let
    /// an asset we cannot even name displace one that really does recycle in
    /// seconds.
    fn combo_settlement_speed(
        &self,
        debt_position: &LiquidateblePosition,
        collateral_position: &LiquidateblePosition,
    ) -> (SettlementSpeed, SettlementSpeed) {
        let (Some(collateral), Some(repayment)) = (
            resolve_token_for_position(self.registry.as_ref(), collateral_position),
            resolve_token_for_position(self.registry.as_ref(), debt_position),
        ) else {
            return (SettlementSpeed::Delayed, SettlementSpeed::Delayed);
        };

        (
            deposit_speed(&collateral, &repayment, &self.venue_native_symbols),
            repayment_speed(&collateral, &repayment, &self.venue_native_symbols),
        )
    }

    /// Worth of `native_amount` of `asset`, in RAY, so positions denominated in
    /// different tokens can be compared against each other.
    ///
    /// Prices are cached for the caller's cycle because the same handful of
    /// assets recurs across every combo and each miss is a canister query.
    async fn quote_value_ray(
        &self,
        asset: &Assets,
        native_amount: &Nat,
        prices: &mut HashMap<String, Nat>,
    ) -> Result<Nat, String> {
        let symbol = asset.symbol();
        let price_ray = match prices.get(&symbol) {
            Some(price) => price.clone(),
            None => {
                let price = self.collateral_service.price_ray(asset).await?;
                prices.insert(symbol, price.clone());
                price
            }
        };

        // Dividing a RAY price by the token scale leaves the result in RAY,
        // matching the convention `calculate_liquidation_amounts` uses.
        Ok((price_ray * native_amount.clone()) / Nat::from(10u128.pow(asset.decimals())))
    }

    fn min_collateral_for_bad_debt(gross_collateral: Nat, slippage_bps: u32) -> Nat {
        amount_after_bps_haircut(&gross_collateral, slippage_bps.min(BASIS_POINTS_DENOMINATOR))
            .unwrap_or_else(|_| Nat::from(0u8))
    }

    /// Renders a RAY-denominated value as whole units for logging.
    fn ray_to_units(value: &Nat) -> f64 {
        value.0.to_f64().unwrap_or(f64::MAX) / 1e27
    }

    fn native_to_units(amount: &Nat, decimals: u8) -> f64 {
        let scale = 10f64.powi(decimals as i32);
        if scale > 0.0 {
            amount.0.to_f64().unwrap_or(f64::MAX) / scale
        } else {
            0.0
        }
    }

    fn signed_native_to_units(amount: &Int, decimals: u8) -> f64 {
        let scale = 10f64.powi(decimals as i32);
        if scale > 0.0 {
            amount.0.to_f64().unwrap_or(0.0) / scale
        } else {
            0.0
        }
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
                debug!("Bad debt detected: user has no collateral; queueing debt positions.");
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
        cleared_debts: &HashSet<String>,
    ) -> Result<(), String> {
        if bad_debts.is_empty() {
            return Ok(());
        }

        if !self.config.should_buy_bad_debt() {
            // Config says we should not buy bad debt; just log and skip.
            for (_idx, pos) in bad_debts {
                debug!(
                    "Skip bad debt (disabled): pool={:?} debt={}",
                    pos.pool_id, pos.debt_amount,
                );
            }
            return Ok(());
        }

        for (user_idx, debt_position) in bad_debts {
            let debt_key = format!("{}:{}", debt_position.account, debt_position.pool_id);
            if cleared_debts.contains(&debt_key) {
                continue;
            }

            if work_users[user_idx].health_factor >= 1000u32 {
                continue;
            }

            if !is_supported_position_asset_type(&debt_position) {
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

            let debt_approval_needed = self.debt_approval_needed(&repayment_token);
            let mut debt_fee_total = repayment_token.fee();
            if debt_approval_needed {
                debt_fee_total += repayment_token.fee();
            }

            if available_balance.clone() < debt_fee_total.clone() {
                self.watchdog
                    .notify(WatchdogEvent::InsufficientFunds {
                        asset: &debt_position.asset.to_string(),
                        available: available_balance.to_string(),
                    })
                    .await;
                debug!(
                    "Skipping bad-debt position due to insufficient funds: asset={}, available={} < min_required={}",
                    debt_position.asset, available_balance, debt_fee_total
                );
                continue;
            }

            let max_balance = available_balance.clone() - debt_fee_total.clone();

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
                - Int::from(debt_fee_total.clone());

            info!(
                "🧯 Bad debt buy: repay={} {} | profit={} {}",
                Self::native_to_units(&repay_amount, repayment_token.decimals()),
                repayment_token.symbol(),
                Self::signed_native_to_units(&profit, repayment_token.decimals()),
                repayment_token.symbol()
            );

            if profit <= 0 && !self.config.should_buy_bad_debt() {
                continue;
            }

            if available_balance.clone() >= repay_amount.clone() + debt_fee_total.clone() {
                *available_balance = available_balance.clone() - repay_amount.clone() - debt_fee_total.clone();
            } else {
                debug!(
                    "Available balance would underflow when updating bad-debt balance. available={}, repay={}, fees={}",
                    available_balance, repay_amount, debt_fee_total
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
                debt_ref_price: 0u32.into(),
                ref_price_at: now_ts(),
                swap_args: None,
                expected_profit: profit.0.to_i128().unwrap_or(i128::MAX),
                debt_approval_needed,
                min_collateral_amount: Nat::from(0u8),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, C, R, U> PipelineStage<'a, Vec<LiquidatebleUser>, Vec<ExecutorRequest>> for SimpleLiquidationStrategy<C, R, U>
where
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
        let (combos, bad_debts) = self.build_combos(&work_users);
        let mut cleared_debts: HashSet<String> = HashSet::new();

        // Price every combo before ranking. Comparing `debt_amount` directly
        // compares raw ledger integers across tokens with different decimals,
        // so an 18-decimal dust position outranks an 8-decimal position worth
        // twenty times more purely on scale -- which is how a $2.50 ckETH
        // position got liquidated ahead of a $50 ICP one on the same borrower.
        let mut prices: HashMap<String, Nat> = HashMap::new();
        let mut priced = Vec::with_capacity(combos.len());
        for (user_idx, debt_position, collateral_position) in combos {
            let debt_value = self
                .ranking_value(&debt_position.asset, &debt_position.debt_amount, &mut prices)
                .await;
            let collateral_value = self
                .ranking_value(
                    &collateral_position.asset,
                    &collateral_position.collateral_amount,
                    &mut prices,
                )
                .await;
            let (settlement, repayment_settlement) = self.combo_settlement_speed(&debt_position, &collateral_position);
            let covers_debt = collateral_covers_debt(&debt_value, &collateral_value, &collateral_position);
            // A debt whose proceeds bridge home is worth less to rank on than
            // the same debt repaid in an asset the venue settles on the IC.
            let ranked_debt_value = match repayment_settlement {
                SettlementSpeed::Direct => debt_value.clone(),
                SettlementSpeed::Delayed => {
                    amount_after_bps_haircut(&debt_value, RETURN_LEG_HAIRCUT_BPS).unwrap_or_else(|_| Nat::from(0u8))
                }
            };
            priced.push(RankedCombo {
                user_idx,
                debt_position,
                collateral_position,
                debt_value,
                collateral_value,
                settlement,
                repayment_settlement,
                ranked_debt_value,
                covers_debt,
            });
        }

        // Most urgent first: lowest health factor, then largest debt by value
        // after the return-leg haircut. That haircut is how the debt side of
        // the round trip enters ranking at all: repaying ckUSDT comes home in
        // seconds because MEXC settles it on the IC, while repaying ckUSDC is
        // withdrawn on Ethereum and bridged back, and ranking on raw size could
        // not tell those apart.
        //
        // Which collateral backs that repayment is decided in two further
        // steps, kept separate from the debt haircut on purpose: folding the
        // outbound leg into the same figure would let a dust position that
        // settles fast outweigh a large one that covers the debt. First,
        // collateral that can cover the repayment outranks collateral that
        // cannot: a position too small to back the debt caps the repay, and the
        // buffer override downstream still charges the full request against the
        // cycle's balance, so letting a dust position win the pairing spends the
        // budget of a liquidation it cannot perform.
        //
        // Only among collateral that can do the job does speed decide, and it
        // decides before size. Either position seizes the same value for the
        // same repayment, so the choice sets nothing except when the capital is
        // spendable again: ckBTC reaches MEXC in one ICRC-1 transfer, while
        // ckETH has to burn through the ckETH minter and wait 15-20 minutes on
        // Ethereum before the venue sees a deposit. Ranking purely by size
        // paired the largest debt with the largest collateral, which is how a
        // USDT debt took the slow ckETH collateral and left the ckBTC that
        // would have recycled the capital in seconds for a second liquidation
        // the balance could no longer fund.
        priced.sort_by(|a, b| {
            work_users[a.user_idx]
                .health_factor
                .cmp(&work_users[b.user_idx].health_factor)
                .then(b.ranked_debt_value.cmp(&a.ranked_debt_value))
                .then(b.covers_debt.cmp(&a.covers_debt))
                .then(a.settlement.cmp(&b.settlement))
                .then(b.collateral_value.cmp(&a.collateral_value))
        });

        // The order the loop is about to walk. Without this a live run leaves
        // the ranking to be inferred from the order quotes appear in, which
        // cannot tell a coverage decision from a speed one. Debug because a
        // full cycle ranks every combo of every borrower; enable it with
        // `RUST_LOG=liquidator::stages::simple_strategy=debug`.
        for (rank, combo) in priced.iter().enumerate() {
            debug!(
                "Ranked #{}: borrower={} hf={} | debt={} (${:.2} -> ranked ${:.2}) | collateral={} (${:.2}) | covers={} out={:?} back={:?}",
                rank,
                combo.debt_position.account,
                work_users[combo.user_idx].health_factor,
                combo.debt_position.asset.symbol(),
                Self::ray_to_units(&combo.debt_value),
                Self::ray_to_units(&combo.ranked_debt_value),
                combo.collateral_position.asset.symbol(),
                Self::ray_to_units(&combo.collateral_value),
                combo.covers_debt,
                combo.settlement,
                combo.repayment_settlement,
            );
        }

        for RankedCombo {
            user_idx,
            debt_position,
            collateral_position,
            settlement,
            repayment_settlement,
            covers_debt,
            ..
        } in priced
        {
            let debt_key = format!("{}:{}", debt_position.account, debt_position.pool_id);
            if cleared_debts.contains(&debt_key) {
                continue;
            }

            if work_users[user_idx].health_factor >= 1000u32 {
                continue;
            }

            if !is_supported_position_asset_type(&debt_position)
                || !is_supported_position_asset_type(&collateral_position)
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

            let debt_approval_needed = self.debt_approval_needed(&repayment_token);
            let mut debt_fee_total = repayment_token.fee();
            if debt_approval_needed {
                debt_fee_total += repayment_token.fee();
            }

            if available_balance.clone() < debt_fee_total.clone() {
                self.watchdog
                    .notify(WatchdogEvent::InsufficientFunds {
                        asset: &debt_position.asset.to_string(),
                        available: available_balance.to_string(),
                    })
                    .await;
                debug!(
                    "Skipping combo due to insufficient funds: asset={}, available={} < min_required={}",
                    debt_position.asset, available_balance, debt_fee_total
                );
                continue;
            }

            let max_balance = available_balance.clone() - debt_fee_total.clone();

            debug!(
                "available_balance: {:?} repayment_token_fee {:?} max_balance: {:?}",
                available_balance,
                repayment_token.fee(),
                max_balance
            );

            let mut estimation = self
                .collateral_service
                .calculate_liquidation_amounts(
                    max_balance.clone(),
                    &debt_position,
                    &collateral_position,
                    &mut work_users[user_idx],
                )
                .await?;

            let gross_estimated_collateral = estimation.received_collateral.clone();
            estimation.received_collateral = if estimation.received_collateral < collateral_token.fee() {
                0u64.into()
            } else {
                estimation.received_collateral - collateral_token.fee()
            };

            if !self.config.should_buy_bad_debt() && estimation.received_collateral == 0u32 {
                info!(
                    "⛔️ Skip liquidation: net collateral {} < fee {}",
                    estimation.received_collateral,
                    collateral_token.fee()
                );
                continue;
            }

            // The canister settles at most the liquidation-ratio cap and returns
            // the remainder as change, so the request below is deliberately
            // oversized. `received_collateral` is paired with the repay figure
            // the estimator actually chose -- `compute_liquidation_amounts`
            // derives the seize from the capped repay -- so the profit basis has
            // to stay on that figure. Charging the oversized request against
            // collateral from a capped repayment understates profit by the
            // change we get back, which mislabels profitable liquidations as bad
            // debt.
            let expected_repaid_debt = estimation.repaid_debt.clone();

            // Add only explicitly configured repay buffers; unspecified assets get no bump.
            let repay_buffer = repay_buffer_native_units(&repayment_token.symbol())
                .map(Nat::from)
                .unwrap_or_else(|| Nat::from(0u8));
            // Target = full debt plus the buffer, but never more than what we can actually spend.
            let desired_repay = (debt_position.debt_amount.clone() + repay_buffer).min(max_balance.clone());
            // Only bump up the repay amount; never reduce what the estimator already chose.
            if estimation.repaid_debt < desired_repay {
                debug!("Repay buffer override: {} -> {}", estimation.repaid_debt, desired_repay,);
                estimation.repaid_debt = desired_repay;
            }

            let amount_in = ChainTokenAmount {
                token: collateral_token.clone(),
                value: estimation.received_collateral.clone(),
            };

            let same_asset = collateral_token.asset_id().address == repayment_token.asset_id().address;
            let price_coll_ray = estimation.ref_price.clone();
            let price_debt_ray = estimation.debt_price.clone();
            let max_slippage_bps = self.config.get_max_allowed_dex_slippage();
            let swap_needed = estimation.received_collateral != 0u32 && !same_asset;
            let (swap_args, amount_received, price) = if !swap_needed {
                (None, amount_in.value.clone(), 1f64)
            } else {
                let swap_request = SwapRequest {
                    pay_asset: collateral_token.asset_id(),
                    pay_amount: amount_in.clone(),
                    receive_asset: repayment_token.asset_id(),
                    receive_address: Some(self.config.get_liquidator_principal().to_string()),
                    max_slippage_bps: Some(max_slippage_bps),
                    venue_hint: None,
                };
                let price = if price_coll_ray == 0u8 || price_debt_ray == 0u8 {
                    0.0
                } else {
                    let coll_f = price_coll_ray.0.to_f64().unwrap_or(0.0);
                    let debt_f = price_debt_ray.0.to_f64().unwrap_or(0.0);
                    if debt_f > 0.0 { coll_f / debt_f } else { 0.0 }
                };

                // The multi-venue quote guard compares against this same
                // conversion, so both use one implementation.
                let amount_received = oracle_implied_output(
                    &amount_in.value,
                    &price_coll_ray,
                    &price_debt_ray,
                    u64::from(collateral_token.decimals()),
                    u64::from(repayment_token.decimals()),
                )
                .unwrap_or_else(|_| Nat::from(0u8));

                (Some(swap_request), amount_received, price)
            };

            let inverse_price = if price > 0.0 { 1.0 / price } else { 0.0 };
            info!(
                "💱 Quote: repay_debt={} {} | seized_collateral={} {} | estimated_swap_out={} {} | price={} inverse_price={} | swap={} -> {} | ranked: covers={} out={:?} back={:?}",
                Self::native_to_units(&expected_repaid_debt, repayment_token.decimals()),
                repayment_token.symbol(),
                Self::native_to_units(&estimation.received_collateral, collateral_token.decimals()),
                collateral_token.symbol(),
                Self::native_to_units(&amount_received, repayment_token.decimals()),
                repayment_token.symbol(),
                price,
                inverse_price,
                collateral_token.symbol(),
                repayment_token.symbol(),
                covers_debt,
                settlement,
                repayment_settlement,
            );

            let profit = Int::from(amount_received)
                - Int::from(expected_repaid_debt.clone())
                - Int::from(debt_fee_total.clone());
            let is_bad_debt = profit <= 0;
            let buy_bad_debt = is_bad_debt;

            let min_collateral_amount = if buy_bad_debt {
                Self::min_collateral_for_bad_debt(
                    gross_estimated_collateral,
                    self.config.get_bad_debt_collateral_slippage_bps(),
                )
            } else {
                Nat::from(0u8)
            };

            info!(
                "📊 Profit: {} {}",
                Self::signed_native_to_units(&profit, repayment_token.decimals()),
                repayment_token.symbol()
            );

            if is_bad_debt && !self.config.should_buy_bad_debt() {
                continue;
            }

            debug!(
                "Updating available balance: {:?} {} {}",
                available_balance,
                estimation.repaid_debt,
                repayment_token.fee()
            );

            if available_balance.clone() >= estimation.repaid_debt.clone() + debt_fee_total.clone() {
                *available_balance =
                    available_balance.clone() - estimation.repaid_debt.clone() - debt_fee_total.clone();
            } else {
                debug!(
                    "Available balance would underflow when updating. available={}, repay={}, fees={}",
                    available_balance, estimation.repaid_debt, debt_fee_total
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
                    buy_bad_debt,
                },
                ref_price: estimation.ref_price,
                debt_ref_price: estimation.debt_price,
                // Our own clock, not the canister's: the finalizer uses this to
                // decide whether these prices are still fresh.
                ref_price_at: now_ts(),
                swap_args,
                expected_profit: profit.0.to_i128().unwrap_or(i128::MAX),
                debt_approval_needed,
                min_collateral_amount,
            });

            // The request above is padded on purpose and the canister returns
            // the difference as change, so it is not evidence the debt closed.
            // `expected_repaid_debt` is what the estimator says will actually
            // settle -- the same figure the profit basis uses. Reading the
            // padded request here let a collateral-capped partial repayment
            // mark the debt cleared and skip the combo that could have closed
            // it with the borrower's other collateral.
            if expected_repaid_debt >= debt_position.debt_amount {
                cleared_debts.insert(debt_key);
            }

            if is_bad_debt && self.config.should_buy_bad_debt() {
                // The request is oversized on purpose; the canister caps the
                // settlement and returns the difference as change. Reporting the
                // request alone reads as if we spent twice what we did.
                info!(
                    "🧯 Buying bad debt: expected_repaid={} {} | requested={} {}",
                    Self::native_to_units(&expected_repaid_debt, repayment_token.decimals()),
                    repayment_token.symbol(),
                    Self::native_to_units(&estimation.repaid_debt, repayment_token.decimals()),
                    repayment_token.symbol()
                );
            }
        }

        // After handling normal collateral-backed combos, handle pure bad-debt positions.
        self.handle_bad_debt_positions(bad_debts, &mut balances, &mut work_users, &mut result, &cleared_debts)
            .await?;

        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval_state::ApprovalState;
    use crate::config::MockConfigTrait;
    use crate::liquidation::collateral_service::{LiquidationEstimation, MockCollateralServiceTrait};
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
        //
        // The ledger is derived from the symbol because real ck-assets each
        // have their own. A shared principal made every token compare equal by
        // `asset_id`, which silently reads as "collateral is already the
        // repayment asset" and collapses settlement ranking to one class.
        ChainToken::Icp {
            ledger: Principal::from_slice(symbol.as_bytes()),
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

    fn mk_unknown_native_icp_position(
        pool: Principal,
        account: Principal,
        debt: u64,
        coll: u64,
    ) -> LiquidateblePosition {
        LiquidateblePosition {
            pool_id: pool,
            debt_amount: Nat::from(debt),
            collateral_amount: Nat::from(coll),
            asset: Assets::ICP,
            asset_type: AssetType::Unknown,
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        // Collateral service: repay 1_000, receive 4_000 collateral
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-valid");
        let pos = mk_position(pool, borrower, ledger, 1_000, 4_000, Assets::USDC);
        let user = mk_user(vec![pos.clone()], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1);
        let req = &res[0];
        assert_eq!(req.liquidation.borrower, pos.account);
        assert_eq!(req.liquidation.debt_amount, Nat::from(2_000u64));
        assert!(req.swap_args.is_none(), "no swap expected when assets match");
        assert_eq!(req.min_collateral_amount, Nat::from(0u8));
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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

    #[tokio::test]
    async fn simple_strategy_allows_native_icp_unknown_collateral() {
        let ckusdc_ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let icp_ledger = p(ICP_LEDGER_PRINCIPAL);
        let icp_pool = p("en2mt-fyaaa-aaaae-qkefq-cai");
        let ckusdc_token = ChainToken::Icp {
            ledger: ckusdc_ledger,
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        };
        let icp_token = ChainToken::Icp {
            ledger: icp_ledger,
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        };

        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning({
            let ckusdc_token = ckusdc_token.clone();
            let icp_token = icp_token.clone();
            move |id: &AssetId| {
                if id.address == ckusdc_ledger.to_text() {
                    Some(ckusdc_token.clone())
                } else if id.address == ICP_LEDGER_PRINCIPAL {
                    Some(icp_token.clone())
                } else {
                    None
                }
            }
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, coll_pos, _user| {
                assert_eq!(coll_pos.asset, Assets::ICP);
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(200_000_000u64),
                    repaid_debt: Nat::from(1_000_000u64),
                    ref_price: Nat::from(10_000_000_000_000_000_000_000_000_000u128),
                    debt_price: Nat::from(1_000_000_000_000_000_000_000_000_000u128),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let debt_pool = p("rw6tq-vyaaa-aaaae-qjx5a-cai");
        let borrower = p("aaaaa-aa");
        let debt_pos = mk_position(debt_pool, borrower, ckusdc_ledger, 1_000_000, 0, Assets::USDC);
        let collateral_pos = mk_unknown_native_icp_position(icp_pool, borrower, 0, 200_000_000);
        let user = mk_user(vec![debt_pos, collateral_pos], 1_000_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(res.len(), 1);
        let req = &res[0];
        assert_eq!(req.debt_asset.symbol(), "ckUSDC");
        assert_eq!(req.collateral_asset.symbol(), "ICP");
        assert_eq!(req.liquidation.collateral_pool_id, icp_pool);
        assert!(!req.liquidation.buy_bad_debt);
        assert!(req.swap_args.is_some());
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(500u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1_000u64), // equals fee, nets to zero
                    repaid_debt: Nat::from(500u64),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
        assert!(
            res[0].liquidation.buy_bad_debt,
            "negative-profit liquidation should be marked as bad debt"
        );
        assert_eq!(
            res[0].min_collateral_amount,
            Nat::from(950u64),
            "bad debt min collateral should use gross estimate haircut"
        );
    }

    #[tokio::test]
    async fn simple_strategy_wipeout_full_close_adds_18_decimal_repay_buffer() {
        let ledger = p("ss2fx-dyaaa-aaaar-qacoq-cai");
        let token = ChainToken::Icp {
            ledger,
            symbol: "ckETH".to_string(),
            decimals: 18,
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let scanned_debt = Nat::from(5_000_000_000u64);
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral.expect_calculate_liquidation_amounts().returning(
            move |_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(400_000_000_000u64),
                    repaid_debt: scanned_debt.clone(),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
                })
            },
        );

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(1_000_000_000_000u64),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("jrgdg-siaaa-aaaae-qkfmq-cai");
        let borrower = p("user-wipeout-buffer");
        let pos = mk_position(pool, borrower, ledger, 5_000_000_000, 400_000_000_000, Assets::ETH);
        let user = mk_user(vec![pos], 5_000_000_000, WIPEOUT_THRESHOLD as u64);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0].liquidation.debt_amount,
            Nat::from(305_000_000_000u64),
            "wipeout full-close offer should add a 300 gwei buffer for 18-decimal debt"
        );
    }

    /// The canister settles at most the liquidation-ratio cap and returns the
    /// rest as change, so the request is deliberately oversized. Profit must
    /// still be charged against the repay the estimator paired the collateral
    /// with -- billing the oversized request instead turned profitable
    /// liquidations into bad debt and dropped them.
    #[tokio::test]
    async fn profit_basis_uses_the_expected_settlement_not_the_oversized_request() {
        let ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let token = mk_icp_token("ckBTC", 8);

        let mut registry = MockTokenRegistryTrait::new();
        registry
            .expect_get()
            .returning(move |_id: &AssetId| Some(token.clone()));

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        // Bad-debt buying is off, so a mislabelled liquidation is dropped
        // outright -- which is exactly what used to happen here.
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));
        // Only reached if this gets misclassified as bad debt; stubbed so the
        // regression surfaces as a failed assertion rather than a mock panic.
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(100u32);

        // The estimator caps the repay and sizes the seized collateral to that
        // capped figure -- the pairing the profit basis has to respect.
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1_100_000u64),
                    repaid_debt: Nat::from(1_000_000u64),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-capped-repay");
        // Position debt is double what the estimator will settle, so the repay
        // buffer override doubles the request.
        let pos = mk_position(pool, borrower, ledger, 2_000_000, 4_000_000, Assets::BTC);
        let user = mk_user(vec![pos], 2_000_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            res.len(),
            1,
            "a profitable liquidation must survive even though the request is oversized"
        );
        assert_eq!(
            res[0].liquidation.debt_amount,
            Nat::from(2_000_010u64),
            "the request stays oversized on purpose; the canister returns the change"
        );
        assert!(
            !res[0].liquidation.buy_bad_debt,
            "profitable at the settled repay, so it is not bad debt"
        );
        // collateral 1_100_000 - 100 fee, less the 1_000_000 settled repay and
        // two 100-unit debt fees (transfer + approval).
        assert_eq!(
            res[0].expected_profit, 99_700i128,
            "profit is charged against the settled repay, not the oversized request"
        );
    }

    #[tokio::test]
    async fn simple_strategy_leaves_cketh_bridge_viability_to_the_venue_planner() {
        let ckusdc_ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let cketh_ledger = p("ss2fx-dyaaa-aaaar-qacoq-cai");
        let ckusdc_token = ChainToken::Icp {
            ledger: ckusdc_ledger,
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };
        let cketh_token = ChainToken::Icp {
            ledger: cketh_ledger,
            symbol: "ckETH".to_string(),
            decimals: 18,
            fee: Nat::from(1_000u64),
        };

        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning({
            let ckusdc_token = ckusdc_token.clone();
            let cketh_token = cketh_token.clone();
            move |id: &AssetId| {
                if id.address == ckusdc_ledger.to_text() {
                    Some(ckusdc_token.clone())
                } else if id.address == cketh_ledger.to_text() {
                    Some(cketh_token.clone())
                } else {
                    None
                }
            }
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_900_000_000_000_000u64),
                    repaid_debt: Nat::from(1_000_000u64),
                    ref_price: Nat::from(2_000_000_000_000_000_000_000_000_000_000u128),
                    debt_price: Nat::from(1_000_000_000_000_000_000_000_000_000u128),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));
        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-sub-bridge");
        let debt_pos = mk_position(pool, borrower, ckusdc_ledger, 1_000_000, 0, Assets::USDC);
        let collateral_pos = mk_position(pool, borrower, cketh_ledger, 0, 4_900_000_000_000_000, Assets::ETH);
        let user = mk_user(vec![debt_pos, collateral_pos], 1_000_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(
            res.len(),
            1,
            "venue-neutral strategy should not apply a MEXC bridge floor"
        );
        assert!(!res[0].liquidation.buy_bad_debt);
    }

    #[tokio::test]
    async fn profitable_cketh_is_not_marked_bad_debt_by_venue_specific_costs() {
        let ckusdc_ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let cketh_ledger = p("ss2fx-dyaaa-aaaar-qacoq-cai");
        let ckusdc_token = ChainToken::Icp {
            ledger: ckusdc_ledger,
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };
        let cketh_token = ChainToken::Icp {
            ledger: cketh_ledger,
            symbol: "ckETH".to_string(),
            decimals: 18,
            fee: Nat::from(1_000u64),
        };

        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning({
            let ckusdc_token = ckusdc_token.clone();
            let cketh_token = cketh_token.clone();
            move |id: &AssetId| {
                if id.address == ckusdc_ledger.to_text() {
                    Some(ckusdc_token.clone())
                } else if id.address == cketh_ledger.to_text() {
                    Some(cketh_token.clone())
                } else {
                    None
                }
            }
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(true);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(500u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_900_000_000_000_000u64),
                    repaid_debt: Nat::from(1_000_000u64),
                    ref_price: Nat::from(2_000_000_000_000_000_000_000_000_000_000u128),
                    debt_price: Nat::from(1_000_000_000_000_000_000_000_000_000u128),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(10_000_000u64),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));
        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-sub-bridge");
        let debt_pos = mk_position(pool, borrower, ckusdc_ledger, 1_000_000, 0, Assets::USDC);
        let collateral_pos = mk_position(pool, borrower, cketh_ledger, 0, 4_900_000_000_000_000, Assets::ETH);
        let user = mk_user(vec![debt_pos, collateral_pos], 1_000_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].liquidation.buy_bad_debt);
        assert!(
            res[0].expected_profit > 0,
            "paper profit should still be positive in this setup"
        );
        assert_eq!(res[0].min_collateral_amount, Nat::from(0u8));
    }

    #[tokio::test]
    async fn simple_strategy_pure_bad_debt_keeps_min_collateral_zero() {
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
        cfg.expect_should_buy_bad_debt().return_const(true);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_, _, _, _| panic!("collateral estimation should not run for pure bad debt"));

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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let borrower = p("user-pure-bad-debt");
        let debt_pos = mk_position(pool, borrower, ledger, 1_000, 0, Assets::USDC);
        let user = mk_user(vec![debt_pos], 1_000, 900);

        let res = strategy.process(&vec![user]).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].liquidation.buy_bad_debt);
        assert_eq!(res[0].min_collateral_amount, Nat::from(0u8));
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        // Collateral service returns fixed repay so we can control budgeting
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut calls = 0u32;
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
        collateral.expect_calculate_liquidation_amounts().returning(
            move |_max_balance, _debt_pos, _coll_pos, user: &mut LiquidatebleUser| {
                calls += 1;
                if calls == 1 {
                    user.health_factor = Nat::from(1_000u64);
                    Ok(LiquidationEstimation {
                        received_collateral: Nat::from(4_000u64),
                        repaid_debt: Nat::from(1_000u64),
                        ref_price: Nat::from(0u8),
                        debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut first_called_for_low_hf = true;
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
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
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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

    /// Reproduces liquidations 1559-1561: one borrower with a dust ckETH debt
    /// (18 decimals) and a far more valuable ICP debt (8 decimals). Comparing
    /// `debt_amount` as raw ledger integers made 878_385_217_534_923 outrank
    /// 1_095_934_098 by ~800_000x, so ~$2.50 of ckETH was liquidated ahead of
    /// ~$50 of ICP -- and the ckETH collateral was too small to sell.
    #[tokio::test]
    async fn ranks_debt_positions_by_value_not_by_raw_ledger_units() {
        let cketh_ledger = p("ss2fx-dyaaa-aaaar-qacoq-cai");
        let icp_ledger = p(ICP_LEDGER_PRINCIPAL);
        let ckbtc_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");

        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning(move |id: &AssetId| {
            Some(match id.symbol.as_str() {
                "ETH" | "ckETH" => mk_icp_token("ckETH", 18),
                "ICP" => mk_icp_token("ICP", 8),
                _ => mk_icp_token("ckBTC", 8),
            })
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(100u32);

        // Records which debt asset the estimator is asked about first.
        let first_priced = Arc::new(std::sync::Mutex::new(None::<String>));
        let sink = first_priced.clone();

        let mut collateral = MockCollateralServiceTrait::new();
        // ETH ~$3000, ICP ~$4.60, BTC ~$100k, all in RAY.
        collateral.expect_price_ray().returning(|asset| {
            Ok(match asset {
                Assets::ETH => Nat::from(3_000_000_000_000_000_000_000_000_000_000u128),
                Assets::ICP => Nat::from(4_600_000_000_000_000_000_000_000_000u128),
                _ => Nat::from(100_000_000_000_000_000_000_000_000_000_000u128),
            })
        });
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_max_balance, debt_pos, _coll_pos, _user| {
                sink.lock().unwrap().get_or_insert_with(|| debt_pos.asset.symbol());
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(47_058u64),
                    repaid_debt: debt_pos.debt_amount.clone(),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(100_000_000_000_000_000_000u128),
            })
        });
        account.expect_get_balance().returning(move |_t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: _t.clone(),
                value: Nat::from(100_000_000_000_000_000_000u128),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        // 0.000878 ckETH -- a huge integer, a trivial amount of money.
        let cketh_debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            cketh_ledger,
            878_385_217_534_923,
            0,
            Assets::ETH,
        );
        // 10.96 ICP -- a far smaller integer, twenty times the value.
        let icp_debt = mk_position(
            p("en2mt-fyaaa-aaaae-qkefq-cai"),
            borrower,
            icp_ledger,
            1_095_934_098,
            0,
            Assets::ICP,
        );
        let ckbtc_collateral = mk_position(
            p("42svn-2yaaa-aaaae-qfcsq-cai"),
            borrower,
            ckbtc_ledger,
            0,
            47_058,
            Assets::BTC,
        );
        let user = mk_user(vec![cketh_debt, icp_debt, ckbtc_collateral], 1_095_934_098, 900);

        let _ = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            first_priced.lock().unwrap().as_deref(),
            Some("ICP"),
            "the $50 ICP position must be evaluated before the $2.50 ckETH one"
        );
    }

    /// Reproduces the pairing on a borrower holding USDT and USDC debt against
    /// ckETH and ckBTC collateral. Ranking collateral purely by value handed
    /// the larger USDT debt the more valuable ckETH, which cannot reach MEXC
    /// without burning through the ckETH minter and waiting 15-20 minutes on
    /// Ethereum -- while the ckBTC that MEXC accepts in a single ICRC-1
    /// transfer went to the smaller debt. Same profit, capital back a quarter
    /// of an hour later, and one fewer liquidation funded in between.
    #[tokio::test]
    async fn pairs_the_largest_debt_with_the_collateral_that_recycles_fastest() {
        let ckusdt_ledger = p("cngnf-vqaaa-aaaar-qag4q-cai");
        let ckusdc_ledger = p("xevnm-gaaaa-aaaar-qafnq-cai");
        let cketh_ledger = p("ss2fx-dyaaa-aaaar-qacoq-cai");
        let ckbtc_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");

        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning(move |id: &AssetId| {
            Some(match id.symbol.as_str() {
                "USDT" => mk_icp_token("ckUSDT", 6),
                "USDC" => mk_icp_token("ckUSDC", 6),
                "ETH" => mk_icp_token("ckETH", 18),
                _ => mk_icp_token("ckBTC", 8),
            })
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(100u32);

        // The (debt, collateral) pair the loop evaluates first.
        let first_pair = Arc::new(std::sync::Mutex::new(None::<(String, String)>));
        let sink = first_pair.clone();

        let mut collateral = MockCollateralServiceTrait::new();
        // BTC ~$100k, ETH ~$3k, both stables $1, all in RAY.
        collateral.expect_price_ray().returning(|asset| {
            Ok(match asset {
                Assets::BTC => Nat::from(100_000_000_000_000_000_000_000_000_000_000u128),
                Assets::ETH => Nat::from(3_000_000_000_000_000_000_000_000_000_000u128),
                _ => Nat::from(1_000_000_000_000_000_000_000_000_000u128),
            })
        });
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_max_balance, debt_pos, coll_pos, _user| {
                sink.lock()
                    .unwrap()
                    .get_or_insert_with(|| (debt_pos.asset.symbol(), coll_pos.asset.symbol()));
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(1_000u64),
                    repaid_debt: debt_pos.debt_amount.clone(),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: t.clone(),
                value: Nat::from(100_000_000_000u128),
            })
        });
        account.expect_get_balance().returning(move |t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: t.clone(),
                value: Nat::from(100_000_000_000u128),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        )
        // What MEXC lists on the ICP network. ckETH is absent on purpose.
        .with_venue_native_symbols(vec!["ckUSDT".to_string(), "ckBTC".to_string(), "ICP".to_string()]);

        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        // 600 USDT, the larger debt, so it is paired first either way.
        let usdt_debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            ckusdt_ledger,
            600_000_000,
            0,
            Assets::USDT,
        );
        // 500 USDC.
        let usdc_debt = mk_position(
            p("en2mt-fyaaa-aaaae-qkefq-cai"),
            borrower,
            ckusdc_ledger,
            500_000_000,
            0,
            Assets::USDC,
        );
        // 1 ckETH -- $3000, the more valuable collateral, but it has to bridge.
        let cketh_collateral = mk_position(
            p("42svn-2yaaa-aaaae-qfcsq-cai"),
            borrower,
            cketh_ledger,
            0,
            1_000_000_000_000_000_000,
            Assets::ETH,
        );
        // 0.02 ckBTC -- $2000, worth less, but tradeable seconds after the seize.
        let ckbtc_collateral = mk_position(
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            borrower,
            ckbtc_ledger,
            0,
            2_000_000,
            Assets::BTC,
        );

        let user = mk_user(
            vec![usdt_debt, usdc_debt, cketh_collateral, ckbtc_collateral],
            1_100_000_000,
            900,
        );

        let _ = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            first_pair.lock().unwrap().clone(),
            Some(("USDT".to_string(), "BTC".to_string())),
            "the most urgent debt must take the collateral that recycles in seconds, not the largest one"
        );
    }

    /// Builds a strategy whose estimator records every (debt, collateral) pair
    /// the loop commits to, repaying `repaid_fraction_bps` of each debt.
    ///
    /// Shared by the ranking tests because they differ only in the positions
    /// they hand it and the order they expect back.
    fn ranking_probe(
        repaid_fraction_bps: u64,
        received_collateral: u64,
    ) -> (
        SimpleLiquidationStrategy<MockConfigTrait, MockTokenRegistryTrait, MockCollateralServiceTrait>,
        Arc<std::sync::Mutex<Vec<(String, String)>>>,
    ) {
        let mut registry = MockTokenRegistryTrait::new();
        registry.expect_get().returning(move |id: &AssetId| {
            Some(match id.symbol.as_str() {
                "USDT" => mk_icp_token("ckUSDT", 6),
                "USDC" => mk_icp_token("ckUSDC", 6),
                "ETH" => mk_icp_token("ckETH", 18),
                "ICP" => mk_icp_token("ICP", 8),
                _ => mk_icp_token("ckBTC", 8),
            })
        });

        let mut cfg = MockConfigTrait::new();
        let trader = p("aaaaa-aa");
        cfg.expect_get_trader_principal().return_const(trader);
        cfg.expect_get_liquidator_principal().return_const(trader);
        cfg.expect_should_buy_bad_debt().return_const(false);
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));
        cfg.expect_get_bad_debt_collateral_slippage_bps().return_const(100u32);

        let pairs = Arc::new(std::sync::Mutex::new(Vec::<(String, String)>::new()));
        let sink = pairs.clone();

        let mut collateral = MockCollateralServiceTrait::new();
        // BTC ~$100k, ETH ~$3k, both stables $1, all in RAY.
        collateral.expect_price_ray().returning(|asset| {
            Ok(match asset {
                Assets::BTC => Nat::from(100_000_000_000_000_000_000_000_000_000_000u128),
                Assets::ETH => Nat::from(3_000_000_000_000_000_000_000_000_000_000u128),
                Assets::ICP => Nat::from(5_000_000_000_000_000_000_000_000_000u128),
                _ => Nat::from(1_000_000_000_000_000_000_000_000_000u128),
            })
        });
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(move |_max_balance, debt_pos, coll_pos, _user| {
                sink.lock()
                    .unwrap()
                    .push((debt_pos.asset.symbol(), coll_pos.asset.symbol()));
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(received_collateral),
                    repaid_debt: (debt_pos.debt_amount.clone() * Nat::from(repaid_fraction_bps))
                        / Nat::from(BASIS_POINTS_DENOMINATOR),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
                })
            });

        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: t.clone(),
                value: Nat::from(1_000_000_000_000u128),
            })
        });
        account.expect_get_balance().returning(move |t: &ChainToken| {
            Ok(ChainTokenAmount {
                token: t.clone(),
                value: Nat::from(1_000_000_000_000u128),
            })
        });

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry,
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        )
        // What MEXC lists on the ICP network. ckETH is absent on purpose.
        .with_venue_native_symbols(vec!["ckUSDT".to_string(), "ckBTC".to_string(), "ICP".to_string()]);

        (strategy, pairs)
    }

    /// A dust position that settles fast must not outrank collateral that can
    /// actually close the debt.
    ///
    /// Preferring speed alone sent the $5,000 ckUSDT debt to $20 of ckBTC. The
    /// estimator caps that repayment at what $20 of collateral supports, but
    /// the repay-buffer override then raises the request back to the full debt
    /// -- which charges $5,000 against the cycle's balance for a $20
    /// liquidation and, before the `expected_repaid_debt` fix below, marked the
    /// debt cleared so the $9,000 ckETH position was never tried.
    #[tokio::test]
    async fn a_dust_position_that_settles_fast_does_not_outrank_collateral_that_covers_the_debt() {
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);

        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        // 5,000 ckUSDT.
        let debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            p("cngnf-vqaaa-aaaar-qag4q-cai"),
            5_000_000_000,
            0,
            Assets::USDT,
        );
        // 3 ckETH -- $9,000, slow, and the only position that can close the debt.
        let cketh_collateral = mk_position(
            p("42svn-2yaaa-aaaae-qfcsq-cai"),
            borrower,
            p("ss2fx-dyaaa-aaaar-qacoq-cai"),
            0,
            3_000_000_000_000_000_000,
            Assets::ETH,
        );
        // 0.0002 ckBTC -- $20, fast, and nowhere near enough.
        let ckbtc_collateral = mk_position(
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            borrower,
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            0,
            20_000,
            Assets::BTC,
        );

        let user = mk_user(vec![debt, cketh_collateral, ckbtc_collateral], 5_000_000_000, 900);
        let _ = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            pairs.lock().unwrap().first().cloned(),
            Some(("USDT".to_string(), "ETH".to_string())),
            "the collateral that can close the debt must be paired first, however slowly it settles"
        );
    }

    /// Collateral that is already the repayment asset never reaches a venue at
    /// all, so it must not be ranked behind one that does.
    ///
    /// Classifying on the venue list alone made ckUSDC collateral against
    /// ckUSDC debt `Delayed`, losing the pairing to a smaller ckBTC position
    /// and buying a MEXC deposit, trade and withdrawal that nothing needed.
    #[tokio::test]
    async fn collateral_that_is_already_the_repayment_asset_wins_the_pairing() {
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);

        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        // 1,000 ckUSDC.
        let debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            p("xevnm-gaaaa-aaaar-qafnq-cai"),
            1_000_000_000,
            0,
            Assets::USDC,
        );
        // 5,000 ckUSDC -- needs no swap whatsoever.
        let ckusdc_collateral = mk_position(
            p("en2mt-fyaaa-aaaae-qkefq-cai"),
            borrower,
            p("xevnm-gaaaa-aaaar-qafnq-cai"),
            0,
            5_000_000_000,
            Assets::USDC,
        );
        // 0.02 ckBTC -- $2,000, fast, but still a full venue round trip.
        let ckbtc_collateral = mk_position(
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            borrower,
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            0,
            2_000_000,
            Assets::BTC,
        );

        let user = mk_user(vec![debt, ckusdc_collateral, ckbtc_collateral], 1_000_000_000, 900);
        let _ = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            pairs.lock().unwrap().first().cloned(),
            Some(("USDC".to_string(), "USDC".to_string())),
            "same-asset collateral skips the swap entirely and must rank ahead of a venue round trip"
        );
    }

    /// A repayment the collateral capped below the full debt must leave that
    /// debt open for the borrower's other collateral.
    ///
    /// The request sent to the canister is padded on purpose and the change
    /// comes back, so reading it as proof the debt closed retired the debt key
    /// after a partial settlement and skipped every remaining combo for it.
    /// The first pairing here is same-asset so it needs no swap and clears the
    /// profit gate, which is what carries it as far as the decision.
    #[tokio::test]
    async fn a_partially_repaid_debt_stays_open_for_the_next_collateral() {
        // The estimator settles half of whatever it is asked to repay, and
        // seizes enough for the combo to be profitable.
        let (strategy, pairs) = ranking_probe(5_000, 2_000_000_000);

        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        // 1,000 ckUSDT.
        let debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            p("cngnf-vqaaa-aaaar-qag4q-cai"),
            1_000_000_000,
            0,
            Assets::USDT,
        );
        // 5,000 ckUSDT -- same asset, so no swap and a clean profit.
        let ckusdt_collateral = mk_position(
            p("en2mt-fyaaa-aaaae-qkefq-cai"),
            borrower,
            p("cngnf-vqaaa-aaaar-qag4q-cai"),
            0,
            5_000_000_000,
            Assets::USDT,
        );
        // 3 ckETH -- $9,000, the collateral that should still get its turn.
        let cketh_collateral = mk_position(
            p("42svn-2yaaa-aaaae-qfcsq-cai"),
            borrower,
            p("ss2fx-dyaaa-aaaar-qacoq-cai"),
            0,
            3_000_000_000_000_000_000,
            Assets::ETH,
        );

        let user = mk_user(vec![debt, ckusdt_collateral, cketh_collateral], 1_000_000_000, 900);
        let _ = strategy.process(&vec![user]).await.unwrap();

        assert_eq!(
            pairs.lock().unwrap().clone(),
            vec![
                ("USDT".to_string(), "USDT".to_string()),
                ("USDT".to_string(), "ETH".to_string()),
            ],
            "a half-settled debt must still be offered the borrower's other collateral"
        );
    }

    /// Builds the borrower from the first live run: ckUSDC and ckUSDT debt
    /// against ckBTC and ckETH collateral, with the ckUSDC debt worth
    /// `ckusdc_debt` and the ckUSDT debt $40.
    fn mixed_debt_borrower(ckusdc_debt: u64) -> LiquidatebleUser {
        let borrower = p("4awtt-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-cvai");
        let usdc_debt = mk_position(
            p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            borrower,
            p("xevnm-gaaaa-aaaar-qafnq-cai"),
            ckusdc_debt,
            0,
            Assets::USDC,
        );
        // 40 ckUSDT.
        let usdt_debt = mk_position(
            p("en2mt-fyaaa-aaaae-qkefq-cai"),
            borrower,
            p("cngnf-vqaaa-aaaar-qag4q-cai"),
            40_000_000,
            0,
            Assets::USDT,
        );
        // 0.02 ckBTC -- $2,000, covers either debt.
        let ckbtc_collateral = mk_position(
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            borrower,
            p("mxzaz-hqaaa-aaaar-qaada-cai"),
            0,
            2_000_000,
            Assets::BTC,
        );
        // 1 ckETH -- $3,000, also covers either debt.
        let cketh_collateral = mk_position(
            p("42svn-2yaaa-aaaae-qfcsq-cai"),
            borrower,
            p("ss2fx-dyaaa-aaaar-qacoq-cai"),
            0,
            1_000_000_000_000_000_000,
            Assets::ETH,
        );

        mk_user(
            vec![usdc_debt, usdt_debt, ckbtc_collateral, cketh_collateral],
            ckusdc_debt + 40_000_000,
            900,
        )
    }

    /// MEXC settles ckUSDT on the ICP network but not ckUSDC, so repaying
    /// ckUSDC withdraws USDC on Ethereum and bridges home -- 15-20 minutes the
    /// ckUSDT debt does not cost. Ranking debts on raw size alone could not see
    /// that, and sent a $51 ckUSDC debt ahead of a $40 ckUSDT one.
    #[tokio::test]
    async fn a_debt_whose_proceeds_come_straight_home_beats_a_slightly_larger_bridged_one() {
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);

        // $51.35 ckUSDC against $40 ckUSDT: bigger, but its proceeds bridge.
        let _ = strategy.process(&vec![mixed_debt_borrower(51_350_000)]).await.unwrap();

        assert_eq!(
            pairs.lock().unwrap().first().cloned(),
            Some(("USDT".to_string(), "BTC".to_string())),
            "the debt that comes home in seconds must be repaid first when the two are comparable"
        );
    }

    /// The haircut must not hand a materially bigger liquidation to a
    /// competitor for a latency win. Liquidations are contested, and taking the
    /// small one first can lift the borrower's health factor out of range.
    #[tokio::test]
    async fn a_materially_larger_bridged_debt_still_outranks_a_fast_small_one() {
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);

        // $500 ckUSDC against $40 ckUSDT: far past the 25% haircut.
        let _ = strategy.process(&vec![mixed_debt_borrower(500_000_000)]).await.unwrap();

        assert_eq!(
            pairs.lock().unwrap().first().cloned(),
            Some(("USDC".to_string(), "BTC".to_string())),
            "a debt worth an order more must win despite its slower return leg"
        );
    }

    /// The haircut is exactly 25%, so the boundary is where a bridged debt is
    /// one third larger than a direct one.
    #[tokio::test]
    async fn the_return_leg_haircut_turns_over_at_a_third_larger() {
        // $53 ckUSDC vs $40 ckUSDT: 53 * 0.75 = 39.75, still short of 40.
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);
        let _ = strategy.process(&vec![mixed_debt_borrower(53_000_000)]).await.unwrap();
        assert_eq!(
            pairs.lock().unwrap().first().cloned().map(|pair| pair.0),
            Some("USDT".to_string()),
            "just under a third larger, the fast debt still wins"
        );

        // $54 ckUSDC vs $40 ckUSDT: 54 * 0.75 = 40.5, now ahead.
        let (strategy, pairs) = ranking_probe(BASIS_POINTS_DENOMINATOR as u64, 1_000);
        let _ = strategy.process(&vec![mixed_debt_borrower(54_000_000)]).await.unwrap();
        assert_eq!(
            pairs.lock().unwrap().first().cloned().map(|pair| pair.0),
            Some("USDC".to_string()),
            "just over a third larger, the bigger debt wins despite bridging"
        );
    }

    /// Ranking asks the oracle about combos the main loop may never reach, so a
    /// single unpriceable asset must not cost us every other liquidation in the
    /// cycle.
    #[tokio::test]
    async fn an_unpriceable_asset_does_not_block_the_rest_of_the_cycle() {
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut collateral = MockCollateralServiceTrait::new();
        // The oracle has no feed for SOL; USDC prices fine.
        collateral.expect_price_ray().returning(|asset| match asset {
            Assets::SOL => Err("no price feed for SOL".to_string()),
            _ => Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)),
        });
        collateral
            .expect_calculate_liquidation_amounts()
            .returning(|_max_balance, _debt_pos, _coll_pos, _user| {
                Ok(LiquidationEstimation {
                    received_collateral: Nat::from(4_000u64),
                    repaid_debt: Nat::from(1_000u64),
                    ref_price: Nat::from(0u8),
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
        );

        let pool = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let unpriceable_borrower = p("user-with-sol");
        let healthy_borrower = p("user-with-usdc");

        let sol_debt = mk_position(pool, unpriceable_borrower, ledger, 1_000, 0, Assets::SOL);
        let sol_collateral = mk_position(pool, unpriceable_borrower, ledger, 0, 4_000, Assets::SOL);
        let usdc_debt = mk_position(pool, healthy_borrower, ledger, 1_000, 0, Assets::USDC);
        let usdc_collateral = mk_position(pool, healthy_borrower, ledger, 0, 4_000, Assets::USDC);

        let sol_user = mk_user(vec![sol_debt, sol_collateral], 1_000, 900);
        let usdc_user = mk_user(vec![usdc_debt, usdc_collateral], 1_000, 900);

        let res = strategy
            .process(&vec![sol_user, usdc_user])
            .await
            .expect("an unpriceable asset must not fail the whole cycle");

        assert!(!res.is_empty(), "the priceable liquidation must still be produced");
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
        cfg.expect_get_max_allowed_dex_slippage().return_const(2000u32);
        cfg.expect_get_lending_canister()
            .return_const(p("mxzaz-hqaaa-aaaar-qaada-cai"));

        let mut first_checked = true;
        let mut collateral = MockCollateralServiceTrait::new();
        // Flat RAY price: these tests compare same-asset positions, so the
        // value ranking reduces to the raw amounts they already assert on.
        collateral
            .expect_price_ray()
            .returning(|_| Ok(Nat::from(1_000_000_000_000_000_000_000_000_000u128)));
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
                    debt_price: Nat::from(0u8),
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

        let registry = Arc::new(registry);
        let account = Arc::new(account);
        let balance_service = Arc::new(BalanceService::new(registry.clone(), account.clone()));

        let strategy = SimpleLiquidationStrategy::new(
            Arc::new(cfg),
            registry.clone(),
            Arc::new(collateral),
            balance_service,
            Arc::new(ApprovalState::new()),
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
