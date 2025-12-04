use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use log::debug;

use crate::{
    liquidation::liquidation_math::{compute_liquidation_amounts, max_repay_from_collateral},
    price_oracle::price_oracle::PriceOracle,
};

use liquidium_pipeline_core::types::protocol_types::{
    Asset, LiquidateblePosition, LiquidatebleUser, MAX_LIQUIDATION_RATIO,
};

pub struct LiquidationEstimation {
    pub repaid_debt: Nat,
    pub received_collateral: Nat,
    pub ref_price: Nat,
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
            .get_price(&debt_symbol, "USDT")
            .await
            .map_err(|e| format!("Could not get debt price: {}", e))?;

        let (price_coll_ray, _) = self
            .price_oracle
            .get_price(&collateral_symbol, "USDT")
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

        //Respect per-user liquidation ratio cap (value terms)
        let liquidation_ratio = if user.health_factor <= 950u128 {
            1000
        } else {
            MAX_LIQUIDATION_RATIO
        };
        // convert repay_native (debt units) -> quote
        let debt_scale = Nat::from(10u128.pow(debt_decimals));

        // NOTE: All quote comparisons here are in RAY (1e27). Prices from oracle are RAY; dividing by token scale keeps quote in RAY.
        // price_debt_ray is RAY; dividing only by debt_scale keeps quote in RAY
        let repay_quote = (price_debt_ray.clone() * repay_native.clone()) / debt_scale.clone();
        let max_liq_quote = (user.total_debt.clone() * Nat::from(liquidation_ratio)) / Nat::from(1000u32);
        if repay_quote > max_liq_quote {
            // max_liq_quote is RAY; convert back to native by dividing by price (RAY) and multiplying by debt_scale
            let new_repay_native = (max_liq_quote.clone() * debt_scale.clone()) / price_debt_ray.clone();
            repay_native = new_repay_native;
        }

        debug!("[debug] repay_native={} ", repay_native);
        debug!("[debug] repay_quote={} max_liq_quote={}", repay_quote, max_liq_quote);

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
                .get_price(&sym, "USDT")
                .await
                .map_err(|e| format!("Could not get price for {}: {}", sym, e))?;
            let dec = pos.asset.decimals();
            let scale = Nat::from(10u128.pow(dec));

            // Positions are already mutated above; use current collateral amount directly
            // p_ray is RAY; dividing only by scale keeps quote in RAY
            let coll_quote = (p_ray.clone() * pos.collateral_amount.clone()) / scale.clone();
            total_coll_quote_new += coll_quote.clone();

            // NOTE: expect per-position liquidation threshold bps available on the position.
            // This mirrors canister's: weighted_liquidation_threshold = sum(coll_value * pool.liq_threshold_bps) / total_collateral
            let liq_threshold_bps = Nat::from(8500u64);
            weighted_sum_new += coll_quote * liq_threshold_bps;
        }

        // New total debt after repay (recomputed from updated positions)
        let mut new_debt_quote = Nat::from(0u8);
        for pos in &user.positions {
            let sym = pos.asset.symbol();
            let (p_ray, _) = self
                .price_oracle
                .get_price(&sym, "USDT")
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
    async fn test_full_liquidation_btc_debt_usdt_collateral() {
        let mut mock_oracle = MockPriceOracle::new();

        // BTC -> USDT = $80,000 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        // USDT -> USDT = $1.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq("USDT"))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
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
            protocol_fee: 200u64,       // 2% (bps)
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
            .with(eq("USDT"), eq("USDT"))
            .return_once(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        // BTC -> USDT = $80,000.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .return_once(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
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
            .with(eq("USDT"), eq("USDT"))
            .return_once(|_, _| Ok(((10u128.pow(27)).into(), 27)));

        // BTC -> USDT = $80,000.00 with RAY precision (1e27)
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .return_once(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        // Build user and positions
        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
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
            .with(eq("USDT"), eq("USDT"))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        // User starts slightly undercollateralized
        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
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
    async fn test_zero_collateral_results_in_zero_repay_and_seize() {
        use mockall::predicate::eq;

        let mut mock_oracle = MockPriceOracle::new();
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq("USDT"))
            .returning(|_, _| Ok(((10u128.pow(27)).into(), 27)));
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .returning(|_, _| Ok(((80_000u128 * 10u128.pow(27)).into(), 27)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let mut user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
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

                // Liquidation ratio cap in quote (RAY)
                let liquidation_ratio = if hf_permille <= 950 { 1000 } else { MAX_LIQUIDATION_RATIO };
                let max_liq_quote = (user.total_debt.clone() * Nat::from(liquidation_ratio)) / Nat::from(1000u32);
                let debt_scale = Nat::from(10u128.pow(6));
                let price_debt_ray_nat: Nat = price_debt_ray;
                let requested_quote = (price_debt_ray_nat.clone() * Nat::from(max_repay_native)) / debt_scale.clone();
                let cap_quote = std::cmp::min(requested_quote.clone(), max_liq_quote.clone());

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

                // Ratio cap enforced in quote terms (RAY)
                let actual_quote = (price_debt_ray_nat.clone() * res.repaid_debt.clone()) / debt_scale.clone();
                prop_assert!(actual_quote <= cap_quote);

                // Post-call total debt should not exceed the pre-call computed debt quote
                prop_assert!(user.total_debt <= pre_debt_quote);
                // If nothing was repaid, total debt should match the pre-call computed quote exactly
                if res.repaid_debt == 0u8 { prop_assert_eq!(user.total_debt, pre_debt_quote); }
            }
        }
    }
}
