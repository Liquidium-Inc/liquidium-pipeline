use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use log::debug;

use crate::{
    liquidation::liquidation_math::{compute_liquidation_amounts, max_repay_from_collateral},
    price_oracle::price_oracle::PriceOracle,
};

use liquidium_pipeline_core::types::protocol_types::{
    Asset, Assets, LiquidateblePosition, LiquidatebleUser, MAX_LIQUIDATION_RATIO,
};

pub(crate) const USD_QUOTE_CURRENCY: &str = "USDT";

#[derive(Debug)]
pub struct LiquidationEstimation {
    pub repaid_debt: Nat,
    pub received_collateral: Nat,
    pub ref_price: Nat,
    pub debt_price: Nat,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CollateralServiceTrait: Send + Sync {
    async fn calculate_liquidation_amounts(
        &self,
        max_repay_amount: Nat,
        debt_position: &LiquidateblePosition,
        collateral_position: &LiquidateblePosition,
        user: &mut LiquidatebleUser,
    ) -> Result<LiquidationEstimation, String>;

    /// Oracle price of one whole unit of `asset`, in RAY.
    ///
    /// Exposed separately from the estimation pass so callers can rank
    /// positions by what they are worth before committing to the much heavier
    /// per-combo estimation.
    async fn price_ray(&self, asset: &Assets) -> Result<Nat, String>;
}

pub struct CollateralService<P: PriceOracle> {
    pub price_oracle: Arc<P>,
}

impl<P: PriceOracle> CollateralService<P> {
    pub fn new(price_oracle: Arc<P>) -> Self {
        Self { price_oracle }
    }
}
#[async_trait]
impl<P: PriceOracle> CollateralServiceTrait for CollateralService<P> {
    async fn price_ray(&self, asset: &Assets) -> Result<Nat, String> {
        let symbol = asset.symbol();
        self.price_oracle
            .get_price(&symbol, USD_QUOTE_CURRENCY)
            .await
            .map(|(price_ray, _)| price_ray)
            .map_err(|e| format!("Could not get price for {}: {}", symbol, e))
    }

    async fn calculate_liquidation_amounts(
        &self,
        max_repay_amount: Nat,
        debt_position: &LiquidateblePosition,
        collateral_position: &LiquidateblePosition,
        user: &mut LiquidatebleUser,
    ) -> Result<LiquidationEstimation, String> {
        let debt_symbol = debt_position.asset.symbol();
        let collateral_symbol = collateral_position.asset.symbol();

        // Fetch oracle prices: (raw, decimals)
        let (price_debt_ray, _) = self
            .price_oracle
            .get_price(&debt_symbol, USD_QUOTE_CURRENCY)
            .await
            .map_err(|e| format!("Could not get debt price: {}", e))?;

        let (price_coll_ray, _) = self
            .price_oracle
            .get_price(&collateral_symbol, USD_QUOTE_CURRENCY)
            .await
            .map_err(|e| format!("Could not get collateral price: {}", e))?;

        // Decimals for tokens
        let debt_decimals = debt_position.asset.decimals();
        let coll_decimals = collateral_position.asset.decimals();

        debug!(
            "[debug] debt_position.debt_amount={} max_repay_amount={} ",
            debt_position.debt_amount, max_repay_amount
        );

        // Start with the requested repay (bounded by remaining debt and external max)
        let mut repay_native = debt_position.debt_amount.clone().min(max_repay_amount.clone());

        // Mirror the pool's close factor (`compute_repay_cap`): one call repays at
        // most half of the selected debt position, or all of it at HF <= 975.
        const WIPEOUT_THRESHOLD: u128 = 975u128;
        let liquidation_ratio = if user.health_factor <= WIPEOUT_THRESHOLD {
            1000
        } else {
            MAX_LIQUIDATION_RATIO
        };
        let close_cap = (debt_position.debt_amount.clone() * Nat::from(liquidation_ratio)) / Nat::from(1000u32);
        repay_native = repay_native.min(close_cap);

        debug!("[debug] repay_native={} ", repay_native);

        // Cap by available collateral
        let bonus_bps = collateral_position.liquidation_bonus;
        let fee_bps = collateral_position.protocol_fee;

        let repay_cap_by_coll = max_repay_from_collateral(
            &collateral_position.collateral_amount,
            &price_coll_ray,
            &price_debt_ray,
            debt_decimals as u64,
            coll_decimals as u64,
            bonus_bps,
            fee_bps,
        )
        .map_err(|e| format!("max_repay_from_collateral: {}", e))?;

        debug!("[debug] repay_cap_by_coll={} ", repay_cap_by_coll);

        repay_native = repay_native.min(repay_cap_by_coll);

        // Compute seized collateral (net to liquidator) & protocol fee
        let (seized_native, fee_native) = compute_liquidation_amounts(
            &repay_native,
            &price_debt_ray,
            &price_coll_ray,
            debt_decimals as u64,
            coll_decimals as u64,
            bonus_bps,
            fee_bps,
        )
        .map_err(|e| format!("compute_liquidation_amounts: {}", e))?;

        debug!("[debug] seized_native={} fee_native={}", seized_native, fee_native);

        // --- Apply liquidation effects to user's positions BEFORE recomputing HF ---
        // Reduce collateral on the targeted collateral position by seized + protocol fee (saturating at zero)
        if let Some(pos) = user.positions.iter_mut().find(|p| {
            p.pool_id == collateral_position.pool_id
                && p.asset == collateral_position.asset
                && p.account == collateral_position.account
        }) {
            let total_taken = seized_native.clone() + fee_native.clone();
            pos.collateral_amount = if pos.collateral_amount > total_taken {
                pos.collateral_amount.clone() - total_taken
            } else {
                Nat::from(0u8)
            };
        }

        // Reduce debt on the targeted debt position by the repayment amount (saturating at zero)
        if let Some(pos) = user.positions.iter_mut().find(|p| {
            p.pool_id == debt_position.pool_id && p.asset == debt_position.asset && p.account == debt_position.account
        }) {
            pos.debt_amount = if pos.debt_amount > repay_native {
                pos.debt_amount.clone() - repay_native.clone()
            } else {
                Nat::from(0u8)
            };
        }

        // Recompute total collateral and weighted liquidation threshold after applying the seize on the chosen collateral position
        let mut total_coll_quote_new = Nat::from(0u8);
        let mut weighted_sum_new = Nat::from(0u8);

        for pos in &user.positions {
            let sym = pos.asset.symbol();
            let (p_ray, _) = self
                .price_oracle
                .get_price(&sym, USD_QUOTE_CURRENCY)
                .await
                .map_err(|e| format!("Could not get price for {}: {}", sym, e))?;
            let dec = pos.asset.decimals();
            let scale = Nat::from(10u128.pow(dec));

            // Positions are already mutated above; use current collateral amount directly
            // p_ray is RAY; dividing only by scale keeps quote in RAY
            let coll_quote = (p_ray.clone() * pos.collateral_amount.clone()) / scale.clone();
            total_coll_quote_new += coll_quote.clone();

            // Mirror the canister's per-pool weighting:
            // weighted_liquidation_threshold = sum(coll_value * pool.liq_threshold_bps) / total_collateral
            let liq_threshold_bps = Nat::from(pos.liquidation_threshold);
            weighted_sum_new += coll_quote * liq_threshold_bps;
        }

        // New total debt after repay (recomputed from updated positions)
        let mut new_debt_quote = Nat::from(0u8);
        for pos in &user.positions {
            let sym = pos.asset.symbol();
            let (p_ray, _) = self
                .price_oracle
                .get_price(&sym, USD_QUOTE_CURRENCY)
                .await
                .map_err(|e| format!("Could not get price for {}: {}", sym, e))?;
            let dec = pos.asset.decimals();
            let scale = Nat::from(10u128.pow(dec));
            // p_ray is RAY; dividing only by scale keeps debt quote in RAY
            let d_q = (p_ray.clone() * pos.debt_amount.clone()) / scale.clone();
            new_debt_quote += d_q;
        }

        // Compute weighted_liquidation_threshold_new (bps)
        let weighted_liq_threshold_new = if total_coll_quote_new == 0u8 {
            Nat::from(0u8)
        } else {
            weighted_sum_new.clone() / total_coll_quote_new.clone()
        };

        // The caller reuses this user across combos, so every derived field has
        // to move together. Leaving the threshold at its pre-liquidation value
        // beside a post-liquidation health factor that was computed from the new
        // one would hand the next reader two numbers that disagree.
        user.weighted_liquidation_threshold = weighted_liq_threshold_new.clone();

        // HF = (coll * 1000 * WLT_bps) / (debt * 10_000)
        let projected_hf = if new_debt_quote == 0u8 {
            Nat::from(u128::MAX)
        } else if total_coll_quote_new == 0u8 {
            Nat::from(0u8)
        } else {
            (total_coll_quote_new * Nat::from(1000u32) * weighted_liq_threshold_new)
                / (new_debt_quote.clone() * Nat::from(10_000u32))
        };

        user.total_debt = new_debt_quote.clone();
        user.health_factor = projected_hf.clone();
        debug!(
            "[debug] post-liq totals: new_debt_quote(RAY)={} projected_hf(permille)={}",
            new_debt_quote, projected_hf
        );

        Ok(LiquidationEstimation {
            received_collateral: seized_native,
            repaid_debt: repay_native,
            ref_price: price_coll_ray,
            debt_price: price_debt_ray,
        })
    }
}

#[cfg(test)]
mod test {
    use candid::Principal;
    use ctor::ctor;
    use mockall::predicate::eq;
    use proptest::prelude::*;

    use crate::price_oracle::price_oracle::MockPriceOracle;
    use liquidium_pipeline_core::types::protocol_types::{AssetType, Assets};

    use super::*;
    use std::sync::Arc;

    #[ctor]
    fn init() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug) // or Info/Trace
            .format_module_path(false)
            .format_file(false)
            .format_source_path(false)
            .format_target(false)
            .format_line_number(true)
            .try_init();
    }

    #[tokio::test]
    async fn sol_debt_uses_lamports_when_seizing_usdt_collateral() {
        // given: 4 SOL at $230 against 1,000 USDT.
        let mut oracle = MockPriceOracle::new();
        oracle.expect_get_price().returning(|asset, _| {
            let price = match asset {
                "SOL" => 230u128,
                "USDT" => 1u128,
                _ => panic!("unexpected asset"),
            };
            Ok((Nat::from(price * 10u128.pow(27)), 27))
        });
        let debt = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::SOL,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(4_000_000_000u64),
            collateral_amount: Nat::from(0u8),
            liquidation_bonus: 500,
            liquidation_threshold: 8500,
            protocol_fee: 200,
        };
        let collateral = LiquidateblePosition {
            asset: Assets::USDT,
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(1_000_000_000u64),
            ..debt.clone()
        };
        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u16),
            weighted_liquidation_threshold: Nat::from(8500u16),
            total_debt: Nat::from(920u128 * 10u128.pow(27)),
            positions: vec![debt.clone(), collateral.clone()],
        };

        // when
        let result = CollateralService::new(Arc::new(oracle))
            .calculate_liquidation_amounts(debt.debt_amount.clone(), &debt, &collateral, &mut user)
            .await
            .unwrap();

        // then: $920 plus 5% bonus, less 2% of that bonus.
        assert_eq!(result.repaid_debt, Nat::from(4_000_000_000u64));
        assert_eq!(result.received_collateral, Nat::from(965_080_000u64));
    }

    #[tokio::test]
    async fn test_full_liquidation_btc_debt_usdt_collateral() {
        let mut mock_oracle = MockPriceOracle::new();

        // BTC -> USDT = $80,000 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        // USDT -> USDT = $1.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(8500u64),
            positions: vec![],
            // total_debt in RAY-scaled quote units (1e27)
            total_debt: Nat::from(81_789_600_000u128) * Nat::from(10u128.pow(21)), // scaled to RAY (1e27) from 6-dec quote
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(102_237u64), // ≈ $95.489
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 2000u64, // 20% (bps)
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64, // 2% (bps)
        };

        user.positions.push(debt_position.clone());

        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(99_940_000u64), // 99.94 USDT
            liquidation_bonus: 2000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        user.positions.push(collateral_position.clone());

        let result = service
            .calculate_liquidation_amounts(
                Nat::from(95_489u64), // max repay = 95_489 sats
                &debt_position,
                &collateral_position,
                &mut user,
            )
            .await
            .expect("Expected liquidation to succeed");

        assert_eq!(result.repaid_debt, Nat::from(95_489u64)); // repay in sats
        assert_eq!(result.received_collateral, Nat::from(91_363_875u64)); // 76,391,200 + 20% - 2% of bonus
    }

    #[tokio::test]
    async fn test_full_liquidation_usdt_debt_btc_collateral() {
        let mut mock_oracle = MockPriceOracle::new();

        // USDT -> USDT = $1.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq(USD_QUOTE_CURRENCY))
            .return_once(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        // BTC -> USDT = $80,000.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq(USD_QUOTE_CURRENCY))
            .return_once(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(8500u64),
            positions: vec![],
            // total_debt in RAY-scaled quote units (1e27)
            total_debt: Nat::from(120u128 * 10u128.pow(27)),
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64), // $100.00 in 6 decimals
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 1000u64, // 10% (bps)
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(1_400_000u64), // 0.014 BTC = ~$1120
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        let mut user = user;
        let result = service
            .calculate_liquidation_amounts(
                Nat::from(100_000_000u64), // $100 in 6 decimals
                &debt_position,
                &collateral_position,
                &mut user,
            )
            .await
            .expect("Expected liquidation to succeed");

        // Expected: full repay of $100, collateral needed ~0.0013725 BTC (includes 10% bonus minus 2% fee on bonus)
        assert_eq!(result.repaid_debt, Nat::from(100_000_000u64));
        assert_eq!(result.received_collateral, Nat::from(137_250u64)); // 125_000 + 10% bonus (12_500) - 2% fee on bonus (250)
    }

    #[tokio::test]
    async fn test_partial_liquidation_usdt_debt_btc_collateral() {
        use mockall::predicate::eq;
        use std::sync::Arc;

        // Mock the oracle
        let mut mock_oracle = MockPriceOracle::new();

        // USDT -> USDT = $1.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq(USD_QUOTE_CURRENCY))
            .return_once(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        // BTC -> USDT = $80,000.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq(USD_QUOTE_CURRENCY))
            .return_once(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        // Build user and positions
        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(8500u64),
            positions: vec![],
            // total_debt in RAY-scaled quote units (1e27)
            total_debt: Nat::from(120u128 * 10u128.pow(27)),
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64), // $100.00
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 1000u64, // 10% (bps)
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        // Only 0.001 BTC available -> 100 000 sats
        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(100_000u64), // 0.001 BTC
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        // Execute
        let mut user = user;
        let result = service
            .calculate_liquidation_amounts(
                Nat::from(100_000_000u64), // try to repay $100
                &debt_position,
                &collateral_position,
                &mut user,
            )
            .await
            .expect("should partial liquidate");

        // All available collateral should be seized (allow <=1 unit rounding diff)
        assert!(result.received_collateral <= 100_000u64);
        let diff = Nat::from(100_000u64) - result.received_collateral.clone();
        assert!(diff <= 1u32, "unexpected rounding diff: {}", diff);

        // Compute expected repay using the same RAY-domain math:
        // prices in RAY
        let price_btc_ray = Nat::from(80_000u128) * Nat::from(10u128.pow(27));
        let price_usdt_ray = Nat::from(10u128.pow(27));
        let tenk = Nat::from(10_000u32);
        let bonus = Nat::from(1_000u32); // 10%
        let fee = Nat::from(200u32); // 2% of bonus
        let fee_on_bonus = (bonus.clone() * fee.clone()) / tenk.clone();
        let denom_bps = tenk.clone() + bonus - fee_on_bonus; // 10_980
        let coll_scale = Nat::from(10u128.pow(8)); // BTC sats
        let debt_scale = Nat::from(10u128.pow(6)); // USDT 6-dec

        // coll_cap_quote_ray = price_btc_ray * 100_000 sats / 1e8
        let coll_cap_quote_ray = (price_btc_ray.clone() * Nat::from(100_000u64)) / coll_scale;
        let r_max_quote_ray = (coll_cap_quote_ray * tenk) / denom_bps;
        let expected_repaid = (r_max_quote_ray * debt_scale) / price_usdt_ray;

        assert_eq!(result.repaid_debt, expected_repaid);
    }

    #[tokio::test]
    async fn test_service_mutates_user_positions_and_hf() {
        use mockall::predicate::eq;

        // Oracle: USDT=1, BTC=80_000 in RAY
        let mut mock_oracle = MockPriceOracle::new();
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        // User starts slightly undercollateralized
        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(8500u64),
            positions: vec![],
            total_debt: Nat::from(120u128 * 10u128.pow(27)),
        };

        // Debt: 100 USDT
        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: user.account,
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64),
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        // Collateral: 0.002 BTC (200_000 sats)
        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: user.account,
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(200_000u64),
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        user.positions.push(debt_position.clone());
        user.positions.push(collateral_position.clone());

        let before_total_debt = user.total_debt.clone();
        let before_hf = user.health_factor.clone();
        let before_coll = collateral_position.collateral_amount.clone();
        let before_debt_native = debt_position.debt_amount.clone();

        let res = service
            .calculate_liquidation_amounts(
                Nat::from(100_000_000u64),
                &debt_position,
                &collateral_position,
                &mut user,
            )
            .await
            .expect("liquidation should succeed");

        // Sanity: some debt was repaid and collateral was seized
        assert!(res.repaid_debt > 0u8);
        assert!(res.received_collateral > 0u8);

        // User state should be updated
        assert!(user.total_debt < before_total_debt);
        assert!(user.health_factor >= before_hf);

        // Positions reflect mutation (debt down, collateral down by seized+fee)
        let post_coll = user
            .positions
            .iter()
            .find(|p| p.asset == Assets::BTC)
            .unwrap()
            .collateral_amount
            .clone();
        let post_debt_native = user
            .positions
            .iter()
            .find(|p| p.asset == Assets::USDT)
            .unwrap()
            .debt_amount
            .clone();

        assert!(post_coll < before_coll);
        assert!(post_debt_native < before_debt_native);
    }

    #[tokio::test]
    async fn projected_hf_uses_each_positions_liquidation_threshold() {
        let mut mock_oracle = MockPriceOracle::new();
        mock_oracle.expect_get_price().returning(|base, _| match base {
            "BTC" => Ok(((80_000u128 * 10u128.pow(27)).into(), 27)),
            "USDC" | "USDT" => Ok(((10u128.pow(27)).into(), 27)),
            _ => Err(format!("unsupported asset: {base}")),
        });

        let service = CollateralService::new(Arc::new(mock_oracle));
        let account = Principal::anonymous();
        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account,
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64), // $100
            collateral_amount: Nat::from(0u8),
            liquidation_bonus: 0,
            liquidation_threshold: 0,
            protocol_fee: 0,
        };
        let btc_collateral = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account,
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(125_000u64), // $100
            liquidation_bonus: 0,
            liquidation_threshold: 7_000,
            protocol_fee: 0,
        };
        let usdc_collateral = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account,
            asset: Assets::USDC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(100_000_000u64), // $100
            liquidation_bonus: 0,
            liquidation_threshold: 9_000,
            protocol_fee: 0,
        };
        let mut user = LiquidatebleUser {
            account,
            // Deliberately not the value the positions imply, so the assertion
            // below cannot pass on the input being echoed back.
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(5_000u64),
            positions: vec![debt_position.clone(), btc_collateral, usdc_collateral.clone()],
            total_debt: Nat::from(100u128 * 10u128.pow(27)),
        };

        service
            .calculate_liquidation_amounts(Nat::from(0u8), &debt_position, &usdc_collateral, &mut user)
            .await
            .expect("health-factor projection should succeed");

        // Equal-value collateral at 70% and 90% has an 80% weighted threshold:
        // ($200 * 0.8 / $100) * 1000 = 1600 permille.
        assert_eq!(user.health_factor, Nat::from(1_600u64));
        // The threshold the projection was derived from is carried on the user
        // alongside it, not left at the stale input.
        assert_eq!(user.weighted_liquidation_threshold, Nat::from(8_000u64));
    }

    #[tokio::test]
    async fn test_zero_collateral_results_in_zero_repay_and_seize() {
        use mockall::predicate::eq;

        let mut mock_oracle = MockPriceOracle::new();
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq(USD_QUOTE_CURRENCY))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            weighted_liquidation_threshold: Nat::from(8500u64),
            positions: vec![],
            total_debt: Nat::from(100u128 * 10u128.pow(27)),
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: user.account,
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(50_000_000u64),
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: user.account,
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: 1000u64,
            liquidation_threshold: 8500u64,
            protocol_fee: 200u64,
        };

        user.positions.push(debt_position.clone());
        user.positions.push(collateral_position.clone());

        let res = service
            .calculate_liquidation_amounts(
                Nat::from(50_000_000u64),
                &debt_position,
                &collateral_position,
                &mut user,
            )
            .await
            .expect("should succeed with zero collateral");

        assert_eq!(res.repaid_debt, Nat::from(0u8));
        assert_eq!(res.received_collateral, Nat::from(0u8));
    }

    /// The pool's `compute_repay_cap` caps one call at 50% of the selected debt
    /// position, or 100% at HF <= 975; the rest of the portfolio does not count.
    #[tokio::test]
    async fn the_close_factor_applies_to_the_selected_debt_position() {
        let mut oracle = MockPriceOracle::new();
        oracle.expect_get_price().returning(|asset, _| {
            let price = match asset {
                "USDT" | "USDC" => 1u128,
                "BTC" => 100_000u128,
                _ => panic!("unexpected asset {asset}"),
            };
            Ok((Nat::from(price * 10u128.pow(27)), 27))
        });
        let service = CollateralService::new(Arc::new(oracle));
        let usdt_debt = LiquidateblePosition {
            pool_id: Principal::from_text("cngnf-vqaaa-aaaar-qag4q-cai").unwrap(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64),
            collateral_amount: Nat::from(0u8),
            liquidation_bonus: 1000,
            liquidation_threshold: 8500,
            protocol_fee: 0,
        };
        let usdc_debt = LiquidateblePosition {
            pool_id: Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap(),
            asset: Assets::USDC,
            ..usdt_debt.clone()
        };
        let btc_collateral = LiquidateblePosition {
            pool_id: Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap(),
            asset: Assets::BTC,
            debt_amount: Nat::from(0u8),
            collateral_amount: Nat::from(1_000_000u64),
            ..usdt_debt.clone()
        };
        let repaid = |positions: Vec<LiquidateblePosition>, hf: u64| {
            let service = &service;
            let (usdt_debt, btc_collateral) = (usdt_debt.clone(), btc_collateral.clone());
            async move {
                let total = positions.iter().filter(|p| p.debt_amount > 0u8).count() as u128;
                let mut user = LiquidatebleUser {
                    account: Principal::anonymous(),
                    health_factor: Nat::from(hf),
                    weighted_liquidation_threshold: Nat::from(8500u64),
                    total_debt: Nat::from(total * 100 * 10u128.pow(27)),
                    positions,
                };
                service
                    .calculate_liquidation_amounts(Nat::from(u64::MAX), &usdt_debt, &btc_collateral, &mut user)
                    .await
                    .unwrap()
                    .repaid_debt
            }
        };
        let two_debts = vec![usdt_debt.clone(), usdc_debt.clone(), btc_collateral.clone()];
        let one_debt = vec![usdt_debt.clone(), btc_collateral.clone()];

        assert_eq!(
            repaid(two_debts.clone(), 980).await,
            Nat::from(50_000_000u64),
            "half of $100, not of $200"
        );
        assert_eq!(repaid(two_debts.clone(), 976).await, Nat::from(50_000_000u64));
        assert_eq!(
            repaid(two_debts, 975).await,
            Nat::from(100_000_000u64),
            "all of the position at HF <= 975"
        );
        assert_eq!(repaid(one_debt, 980).await, Nat::from(50_000_000u64));
    }

    mod fuzz {
        use super::*;
        proptest! {
            #[test]
            fn prop_service_respects_caps_and_mutates_consistently(
                // debt in USDT native (6 decimals)
                debt_native in 1u64..=1_000_000_000u64,   // up to $1,000
                // available BTC collateral in sats
                coll_native in 0u64..=5_000_000u64,       // up to 0.05 BTC
                // external max repay cap in USDT native
                max_repay_native in 1u64..=1_000_000_000u64,
                // user total debt in USD (whole), then scaled to RAY
                user_total_usd in 1u64..=2_000u64,        // up to $2,000
                // health factor around the threshold to exercise ratio cap
                hf_permille in 800u64..=1100u64,
                // liquidation params (reasonable ranges)
                bonus_bps in 0u16..=3000u16,
                fee_bps in 0u16..=10_000u16,
            ) {
                use liquidium_pipeline_core::types::protocol_types::{Assets, AssetType};

                // Mock oracle that always returns RAY prices for USDT and BTC
                let mut mock_oracle = MockPriceOracle::new();
                mock_oracle
                    .expect_get_price()
                    .returning(|base, _quote| {
                        if base == "USDT" { Ok(((10u128.pow(27)).into(), 27)) }
                        else if base == "BTC" { Ok(((80_000u128 * 10u128.pow(27)).into(), 27)) }
                        else { Err("unsupported".into()) }
                    });
                let service = CollateralService::new(Arc::new(mock_oracle));

                // Build user and positions
                let mut user = LiquidatebleUser {
                    account: Principal::anonymous(),
                    health_factor: Nat::from(hf_permille),
                    weighted_liquidation_threshold: Nat::from(8500u64),
                    positions: vec![],
                    total_debt: Nat::from(user_total_usd as u128) * Nat::from(10u128.pow(27)),
                };

                let debt_position = LiquidateblePosition {
                    pool_id: Principal::anonymous(),
                    account: user.account,
                    asset: Assets::USDT,
                    asset_type: AssetType::CkAsset(Principal::anonymous()),
                    debt_amount: Nat::from(debt_native),
                    collateral_amount: Nat::from(0u64),
                    liquidation_bonus: bonus_bps as u64,
                    liquidation_threshold: 8500u64,
                    protocol_fee: fee_bps as u64,
                };

                let collateral_position = LiquidateblePosition {
                    pool_id: Principal::anonymous(),
                    account: user.account,
                    asset: Assets::BTC,
                    asset_type: AssetType::CkAsset(Principal::anonymous()),
                    debt_amount: Nat::from(0u64),
                    collateral_amount: Nat::from(coll_native),
                    liquidation_bonus: bonus_bps as u64,
                    liquidation_threshold: 8500u64,
                    protocol_fee: fee_bps as u64,
                };

                user.positions.push(debt_position.clone());
                user.positions.push(collateral_position.clone());

                // Compute independent caps for invariants
                let (price_debt_ray, price_coll_ray) = ((10u128.pow(27)).into(), (80_000u128 * 10u128.pow(27)).into());
                let repay_cap_by_coll = max_repay_from_collateral(
                    &Nat::from(coll_native),
                    &price_coll_ray,
                    &price_debt_ray,
                    6, // USDT decimals
                    8, // BTC sats
                    bonus_bps as u64,
                    fee_bps as u64,
                ).unwrap_or_else(|_| Nat::from(0u8));

                // The pool's close factor applies to the selected debt position.
                let liquidation_ratio = if hf_permille <= 975 { 1000 } else { MAX_LIQUIDATION_RATIO };
                let close_cap = (Nat::from(debt_native) * Nat::from(liquidation_ratio)) / Nat::from(1000u32);
                let debt_scale = Nat::from(10u128.pow(6));
                let price_debt_ray_nat: Nat = price_debt_ray;

                // Compute pre-call debt quote using price_debt_ray_nat and debt_scale
                let pre_debt_quote = (price_debt_ray_nat.clone() * Nat::from(debt_native)) / debt_scale.clone();

                // Run service under a synchronous runtime to keep proptest happy
                let rt = tokio::runtime::Runtime::new().unwrap();
                let res = rt.block_on(async {
                    service
                        .calculate_liquidation_amounts(
                            Nat::from(max_repay_native),
                            &debt_position,
                            &collateral_position,
                            &mut user,
                        )
                        .await
                });
                if res.is_err() { prop_assume!(false); }
                let res = res.unwrap();

                // Invariants
                // Repay can’t exceed any of its caps
                prop_assert!(res.repaid_debt <= debt_native);
                prop_assert!(res.repaid_debt <= max_repay_native);
                prop_assert!(res.repaid_debt <= repay_cap_by_coll);

                // Seized collateral cannot exceed available collateral
                prop_assert!(res.received_collateral <= coll_native);

                // Close factor enforced on the selected position, in native units
                prop_assert!(res.repaid_debt <= close_cap);

                // Post-call total debt should not exceed the pre-call computed debt quote
                prop_assert!(user.total_debt <= pre_debt_quote);
                // If nothing was repaid, total debt should match the pre-call computed quote exactly
                if res.repaid_debt == 0u8 { prop_assert_eq!(user.total_debt, pre_debt_quote); }
            }
        }
    }
}
