use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use lending::liquidation::liquidation::{LiquidateblePosition, LiquidatebleUser};
use lending_utils::{constants::MAX_LIQUIDATION_RATIO, types::assets::Asset};
use log::debug;

use crate::price_oracle::price_oracle::PriceOracle;

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
        let debt_price = self
            .price_oracle
            .get_price(&debt_position.asset.symbol(), "USDT")
            .await
            .map_err(|_| "Could not get debt price")?;

        let collateral_price = self
            .price_oracle
            .get_price(&collateral_position.asset.symbol(), "USDT")
            .await
            .map_err(|_| "Could not get collateral price")?;

        debug!(" *** Collateral Price {} {:?}", collateral_position.asset.symbol(), collateral_price);

        let liquidation_ratio = if user.health_factor <= 950u64 { 1000u64 } else { MAX_LIQUIDATION_RATIO };

        let collateral_decimals = collateral_position.asset.decimals();
        let debt_amount = debt_position.debt_amount.clone().min(max_repay_amount);

        let debt_value = (debt_amount.clone() * debt_price.0) / 10u32.pow(debt_price.1);
        let max_liquidation = (user.total_debt.clone() * liquidation_ratio) / 1000u32;

        debug!(" *** Max Allowed {max_liquidation} ({debt_value})");

        if debt_value > max_liquidation {
            return Err("Liquidation amount exceeds maximum allowed".to_string());
        }

        let bonus_multiplier = Nat::from(1000u64 + debt_position.liquidation_bonus.clone());

        let (final_collateral, final_repaid_debt) = estimate_partial_liquidation(
            debt_value,
            bonus_multiplier,
            collateral_price,
            debt_price,
            collateral_position.collateral_amount.clone(),
            collateral_decimals,
        );

        Ok(LiquidationEstimation {
            received_collateral: final_collateral,
            repaid_debt: final_repaid_debt,
        })
    }
}

/// Computes the actual collateral that can be seized and the corresponding repayable debt.
/// Handles both full and partial liquidation scenarios.
fn estimate_partial_liquidation(
    debt_value: Nat,
    bonus_multiplier: Nat,
    collateral_price: (u64, u32),
    debt_price: (u64, u32),
    available_collateral: Nat,
    collateral_decimals: u32,
) -> (Nat, Nat) {
    // Ideal collateral needed
    let collateral_needed = (debt_value.clone() * bonus_multiplier.clone() * 10u32.pow(collateral_decimals)) / (collateral_price.0 * 1000u64);
    if collateral_needed <= available_collateral {
        return (collateral_needed, debt_value * 10u32.pow(debt_price.1) / debt_price.0);
    }

    // Partial liquidation fallback
    let adjusted_collateral = available_collateral;

    let adjusted_debt_value =
        (adjusted_collateral.clone() * collateral_price.0 * 1000u64) / (bonus_multiplier.clone() * 10u32.pow(collateral_decimals));

    let adjusted_debt_amount = (adjusted_debt_value.clone() * 10u32.pow(debt_price.1)) / debt_price.0;

    println!("Adjusted collateral{:?}", adjusted_collateral);
    (adjusted_collateral, adjusted_debt_amount)
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Nat;

    fn nat<T: Into<u64>>(v: T) -> Nat {
        Nat::from(v.into())
    }

    #[test]
    fn test_full_liquidation_allowed() {
        let debt_value = nat(1000e6 as u64);
        let bonus_multiplier = nat(1050u64); // 5% bonus
        let collateral_price = (2_000_000u64, 6); // $2 with 6 decimals
        let debt_price = (1_000_000u64, 6); // $1 with 6 decimals
        let available_collateral = nat(600_000e6 as u64);
        let collateral_decimals = 6;

        let (collateral, debt) = estimate_partial_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            collateral_decimals,
        );

        assert_eq!(collateral <= available_collateral, true);
        assert_eq!(debt, nat(1000e6 as u64));
    }

    #[test]
    fn test_partial_liquidation_due_to_insufficient_collateral() {
        // The debt we are targeting to repay (in dollars, scaled as Nat)
        let debt_value = nat(2000e6 as u64); // $2000

        // The liquidation bonus is 10%, so we use a multiplier of 1100 (i.e., 1.1x)
        let bonus_multiplier = nat(1100u64); // 1000 = no bonus, 1100 = 10% bonus

        // Collateral price is $2.00, represented as (2_000_000, 6 decimals)
        let collateral_price = (2_000_000u64, 6);

        // Debt asset price is $1.00, also with 6 decimals
        let debt_price = (1_000_000u64, 6);

        // The user only has 0.5 units of the collateral asset available
        // With 6 decimals, this is represented as 500_000 (0.5 * 10^6)
        // At $2 per unit, the total value = $1,000 worth of collateral
        let available_collateral = nat(500_000u64);

        // The collateral asset has 6 decimal places (like USDC, WBTC, etc.)
        let collateral_decimals = 6;

        // Now run the partial liquidation estimation function
        // It will try to repay as much debt as possible with the given collateral
        let (collateral, debt) = estimate_partial_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            collateral_decimals,
        );

        // We expect the function to return the max available collateral (i.e. cap was hit)
        assert_eq!(collateral, available_collateral);

        // And since we couldn’t seize enough to cover the full $2000 + bonus,
        // the debt we’re repaying must be less than the original requested amount
        assert!(debt < debt_value);
    }
    #[test]
    fn test_zero_collateral_returns_zero() {
        // We simulate a case where the user has zero collateral available.
        // Even if they want to repay $1000 worth of debt, nothing can be seized.

        // Target debt value of $1000, scaled to match 6 decimal precision (i.e., 1000 * 10^6)
        let debt_value = nat(1_000_000_000u64); // $1000.000000

        // Bonus multiplier of 1050 (i.e., 5% bonus over the base of 1000)
        let bonus_multiplier = nat(1050u64);

        // Collateral price: $2.000000 (scaled as 2_000_000 with 6 decimals)
        let collateral_price = (2_000_000u64, 6);

        // Debt price: $1.000000 (scaled as 1_000_000 with 6 decimals)
        let debt_price = (1_000_000u64, 6);

        // Zero available collateral — user has no funds to seize
        let available_collateral = nat(0u64);

        // Collateral asset uses 6 decimal places
        let collateral_decimals = 6;

        // Run the estimator
        let (collateral, debt) = estimate_partial_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            collateral_decimals,
        );

        // Both returned values should be exactly zero
        assert_eq!(collateral, nat(0u8), "Expected zero collateral seized");
        assert_eq!(debt, nat(0u8), "Expected zero debt repaid");
    }
}
