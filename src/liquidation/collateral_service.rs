use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use lending::liquidation::liquidation::{LiquidateblePosition, LiquidatebleUser};
use lending_utils::{constants::MAX_LIQUIDATION_RATIO, types::assets::Asset};
use log::debug;

use crate::{liquidation::liquidation_utils::estimate_liquidation, price_oracle::price_oracle::PriceOracle};

pub struct LiquidationEstimation {
    pub repaid_debt: Nat,
    pub received_collateral: Nat,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CollateralServiceTrait: Send + Sync {
    async fn calculate_liquidation_amounts(
        &self,
        max_repay_amount: Nat,
        debt_position: &LiquidateblePosition,
        collateral_position: &LiquidateblePosition,
        user: &LiquidatebleUser,
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
        user: &LiquidatebleUser,
    ) -> Result<LiquidationEstimation, String> {
        let debt_symbol = debt_position.asset.symbol();
        let collateral_symbol = collateral_position.asset.symbol();

        let debt_price = self
            .price_oracle
            .get_price(&debt_symbol, "USDT")
            .await
            .map_err(|e| format!("Could not get debt price: {}", e))?;

        let collateral_price = self
            .price_oracle
            .get_price(&collateral_symbol, "USDT")
            .await
            .map_err(|e| format!("Could not get collateral price: {}", e))?;

        println!("Debt Price [{}]: {:?}", debt_symbol, debt_price);
        println!("Collateral Price [{}]: {:?}", collateral_symbol, collateral_price);

        let liquidation_ratio = if user.health_factor <= 950u128 {
            1000
        } else {
            MAX_LIQUIDATION_RATIO
        };

        let debt_decimals = debt_position.asset.decimals();
        let collateral_decimals = collateral_position.asset.decimals();

        let debt_amount = debt_position.debt_amount.clone().min(max_repay_amount.clone());

        let debt_value = (debt_amount.clone() * debt_price.0) / 10u128.pow(debt_decimals);
        let max_liquidation = (user.total_debt.clone() * liquidation_ratio) / 1000u128;

        debug!("Debt amount: {}", debt_amount);
        debug!("Max repay amount: {}", max_repay_amount);
        debug!("Debt value (USD): {}", debt_value);
        debug!("Max liquidation (USD): {}", max_liquidation);

        if debt_value > max_liquidation {
            return Err("Liquidation amount exceeds maximum allowed".to_string());
        }

        let bonus_multiplier = Nat::from(1000u128 + debt_position.liquidation_bonus.clone());

        debug!(
            "Estimating liquidation: debt_value={}, bonus_multiplier={}, available_collateral={}, decimals=(debt: {}, collateral: {})",
            debt_value, bonus_multiplier, collateral_position.collateral_amount, debt_decimals, collateral_decimals
        );

        let (received_collateral, repaid_debt) = estimate_liquidation(
            debt_value,
            bonus_multiplier,
            collateral_price,
            debt_price,
            collateral_position.collateral_amount.clone(),
            debt_decimals,
            collateral_decimals,
        );

        Ok(LiquidationEstimation {
            received_collateral,
            repaid_debt,
        })
    }
}

#[cfg(test)]
mod test {
    use candid::Principal;
    use ctor::ctor;
    use lending_utils::types::{assets::Assets, pool::AssetType};
    use mockall::predicate::eq;

    use crate::price_oracle::price_oracle::MockPriceOracle;

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

        // BTC -> USDT = $1.00 with 9 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .return_once(|_, _| Ok((80000000000000u64, 9)));

        // USDT -> USDT = $1.00 with 6 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq("USDT"))
            .return_once(|_, _| Ok((1_000_000_000, 9)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            positions: vec![],
            total_debt: Nat::from(81_789_600_000u128), // $81$
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(102_237u64), // ≈ $95.489
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: Nat::from(200u64), // 20%
        };

        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(99_940_000u64), // 99.94 USDT
            liquidation_bonus: Nat::from(2000u64),
        };

        let result = service
            .calculate_liquidation_amounts(
                Nat::from(95_489u64), // max repay = 95_489 sats
                &debt_position,
                &collateral_position,
                &user,
            )
            .await
            .expect("Expected liquidation to succeed");

        assert_eq!(result.repaid_debt, Nat::from(95_489u64)); // repay in sats
        assert_eq!(result.received_collateral, Nat::from(91_669_440u64)); // 91.669440 USDT
    }

    #[tokio::test]
    async fn test_full_liquidation_usdt_debt_btc_collateral() {
        let mut mock_oracle = MockPriceOracle::new();

        // USDT -> USDT = $1.00 with 6 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq("USDT"))
            .return_once(|_, _| Ok((1_000_000, 6)));

        // BTC -> USDT = $80,000.00 with 9 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .return_once(|_, _| Ok((80_000_000_000, 9)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            positions: vec![],
            total_debt: Nat::from(120_000_000u64), // $120.00 in 6 decimals
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64), // $100.00 in 6 decimals
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: Nat::from(100u64), // 10%
        };

        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(1_400_000u64), // 0.014 BTC = ~$1120
            liquidation_bonus: Nat::from(100u64),
        };

        let result = service
            .calculate_liquidation_amounts(
                Nat::from(100_000_000u64), // $100 in 6 decimals
                &debt_position,
                &collateral_position,
                &user,
            )
            .await
            .expect("Expected liquidation to succeed");

        // Expected: full repay of $100, collateral needed ~0.001375 BTC (includes 10% bonus)
        assert_eq!(result.repaid_debt, Nat::from(100_000_000u64));
        assert_eq!(result.received_collateral, Nat::from(137_500u64)); // 0.01375 BTC
    }

    #[tokio::test]
    async fn test_partial_liquidation_usdt_debt_btc_collateral() {
        use mockall::predicate::eq;
        use std::sync::Arc;

        // 1) Mock the oracle
        let mut mock_oracle = MockPriceOracle::new();

        // USDT -> USDT = $1.00 with 6 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("USDT"), eq("USDT"))
            .return_once(|_, _| Ok((1_000_000, 6)));

        // BTC -> USDT = $80,000.00 with 9 decimals
        mock_oracle
            .expect_get_price()
            .with(eq("BTC"), eq("USDT"))
            .return_once(|_, _| Ok((80_000_000_000, 9)));

        let service = CollateralService::new(Arc::new(mock_oracle));

        // 2) Build user, positions
        let user = LiquidatebleUser {
            account: Principal::anonymous(),
            health_factor: Nat::from(900u64),
            positions: vec![],
            total_debt: Nat::from(120_000_000u64), // $120.00 in USDT's 6 decimals
        };

        let debt_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::USDT,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(100_000_000u64), // $100.00
            collateral_amount: Nat::from(0u64),
            liquidation_bonus: Nat::from(100u64), // 10%
        };

        // Only 0.001 BTC available -> 100 000 sats
        let collateral_position = LiquidateblePosition {
            pool_id: Principal::anonymous(),
            account: Principal::anonymous(),
            asset: Assets::BTC,
            asset_type: AssetType::CkAsset(Principal::anonymous()),
            debt_amount: Nat::from(0u64),
            collateral_amount: Nat::from(100_000u64), // 0.001 BTC
            liquidation_bonus: Nat::from(100u64),
        };

        // 3) Execute
        let result = service
            .calculate_liquidation_amounts(
                Nat::from(100_000_000u64), // try to repay $100
                &debt_position,
                &collateral_position,
                &user,
            )
            .await
            .expect("should partial liquidate");

        // 4) We expect all collateral seized
        assert_eq!(result.received_collateral, Nat::from(100_000u64));

        // Repay only collateral_value * 1000/1100:
        // collateral_value = 0.001 * 80_000 = $80.00 -> raw = 80_000_000
        // after bonus: 80_000_000 * 1000 / 1100 ≈ 72_727_272
        let expected_repaid = Nat::from(72_727_272u64);
        assert_eq!(
            result.repaid_debt, expected_repaid,
            "expected partial repay of ~72.727272 USDT"
        );
    }
}
