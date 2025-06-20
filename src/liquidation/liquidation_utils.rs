

use candid::Nat;
use log::debug;

use crate::ray_math::WadRayMath;

/// Computes the actual collateral that can be seized and the corresponding repayable debt.
/// Handles both full and partial liquidation scenarios.
pub fn estimate_liquidation(
    debt_value: Nat,
    bonus_multiplier: Nat,
    collateral_price: (u64, u32),
    debt_price: (u64, u32),
    available_collateral: Nat,
    debt_decimals: u32,
    collateral_decimals: u32,
) -> (Nat, Nat) {
    let coll_price_nat = Nat::from(collateral_price.0 as u128);
    let coll_price_decimals = Nat::from(10u128.pow(collateral_price.1));
    let debt_price_nat = Nat::from(debt_price.0 as u128);
    let debt_price_decimals = Nat::from(10u128.pow(debt_price.1));

    debug!(
        "[estimate] debt_value: {}, bonus: {}, coll_price: {} (decimals {}), debt_price: {} (decimals {})",
        debt_value,
        bonus_multiplier,
        coll_price_nat,
        collateral_price.1,
        debt_price_nat,
        debt_price.1
    );

    let adjusted_debt_value = debt_value
        .to_ray()
        .ray_mul(&bonus_multiplier)
        .ray_div(&Nat::from(1000u128));

    let collateral_needed = adjusted_debt_value
        .ray_mul(&coll_price_decimals)
        .ray_div(&coll_price_nat)
        .ray_div(&debt_price_decimals)
        .ray_mul(&Nat::from(10u128.pow(collateral_decimals)));

    debug!(
        "[estimate] adjusted_debt_value: {}, collateral_needed: {}, available: {}",
        adjusted_debt_value.from_ray(),
        collateral_needed.from_ray(),
        available_collateral
    );

    if collateral_needed.from_ray() <= available_collateral {
        let repayable_debt = debt_value
            .to_ray()
            .ray_div(&debt_price_nat)
            .ray_mul(&Nat::from(10u128.pow(debt_decimals)));

        debug!(
            "[estimate] full liquidation → repayable_debt: {}, collateral_used: {}",
            repayable_debt.from_ray(),
            collateral_needed.from_ray()
        );

        return (collateral_needed.from_ray(), repayable_debt.from_ray());
    }

    debug!("[estimate] executing partial liquidation");

    let collateral_value = available_collateral
        .to_ray()
        .ray_mul(&coll_price_nat)
        .ray_div(&coll_price_decimals);

    let adjusted_debt_value = collateral_value
        .ray_mul(&Nat::from(1000u128))
        .ray_div(&bonus_multiplier);

    let repayable_debt = adjusted_debt_value
        .ray_div(&Nat::from(10u128.pow(collateral_decimals)))
        .ray_mul(&debt_price_decimals)
        .ray_div(&debt_price_nat)
        .ray_mul(&Nat::from(10u128.pow(debt_decimals)))
        .from_ray();

    debug!(
        "[estimate] partial result → collateral_used: {}, repayable_debt: {}",
        available_collateral,
        repayable_debt
    );

    (available_collateral, repayable_debt)
}


#[cfg(test)]
mod tests {
    use super::*;
    use candid::Nat;

    fn nat<T: Into<u128>>(v: T) -> Nat {
        Nat::from(v.into())
    }

    #[test]
    fn test_full_liquidation_allowed() {
        let debt_value = nat(1000e6 as u64);
        let bonus_multiplier = nat(1050u64); // 5% bonus
        let collateral_price = (2_000_000u64, 6); // $2 with 6 decimals
        let debt_price = (1_000_000u64, 6); // $1 with 6 decimals
        let available_collateral = nat(600_000e6 as u64);

        let (collateral, debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            6u32,
            6u32
        );

        assert_eq!(collateral <= available_collateral, true);
        assert_eq!(debt, nat(1000e6 as u64));
    }


    #[test]
    fn test_full_liquidation_repayable_debt_calculation() {
        // Targeting to repay $1000 (scaled as 9 decimals)
        let debt_value = nat(1_000_000_000_000u64); // 1000 * 1e9

        // 10% bonus -> multiplier = 1100 (basis points)
        let bonus_multiplier = nat(1100u64);

        // Collateral price: $2.00 -> (price, decimals)
        let collateral_price = (2_000_000_000u64, 9);

        // Debt price: $1.00 with 9 decimals
        let debt_price = (1_000_000_000u64, 9);

        let available_collateral = nat(600_000_000u64); // enough for full liquidation

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            9u32,
            6u32
        );

        // ---------- Assertions ----------
        // We expect to repay full debt
        assert_eq!(repaid_debt, debt_value, "Expected full debt repayment");

        // Collateral seized should match what's needed for full liquidation
        // Expected collateral seized = adjusted_debt_value / price
        let adjusted = debt_value.clone() * bonus_multiplier.clone() / nat(1000u64); // in 9 decimals
        let expected_collateral = adjusted.clone()
            * nat(10u64.pow(6)) // scale to collateral decimals
            / nat(collateral_price.0 as u128); // price already in 6 decimals

        assert_eq!(
            collateral, expected_collateral,
            "Expected collateral seized to match adjusted debt value"
        );

        // Debug info
        println!("Debt value: {}", debt_value);
        println!("Adjusted: {}", adjusted);
        println!("Expected collateral: {}", expected_collateral);
        println!("Actual collateral seized: {}", collateral);
        println!("Repaid debt: {}", repaid_debt);
    }

    #[test]
    fn test_partial_liquidation_due_to_insufficient_collateral() {
        // Targeting to repay $2000 (scaled as 9 decimals)
        let debt_value = nat(2_000_000_000_000u64); // 2000 * 1e9

        // 10% bonus -> multiplier = 1100 (basis points)
        let bonus_multiplier = nat(1100u64);

        // Collateral price: $2.00 -> (price, decimals)
        let collateral_price = (2_000_000u64, 6);

        // Debt price: $1.00 with 9 decimals
        let debt_price = (1_000_000_000u64, 9);

        // Available collateral = 0.5 units -> 0.5 * 1e6 = 500_000
        let available_collateral = nat(500_000u64);

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            6u32,
            6u32
        );

        // ---------- Assertions ----------

        // Should seize all available collateral (since it's insufficient to cover full liquidation)
        assert_eq!(collateral, available_collateral, "Expected full collateral usage");

        // Repaid debt should be less than full requested value
        assert!(
            repaid_debt < debt_value,
            "Expected partial debt repayment, got full: {} < {}",
            repaid_debt,
            debt_value
        );

        // For debug purposes: print results in case of failure
        println!("Input debt value: {}", debt_value);
        println!("Actual repaid debt: {}", repaid_debt);
        println!("Used collateral: {}", collateral);
        println!("Available collateral: {}", available_collateral);

      
        let collateral_value =  available_collateral *  
                                Nat::from(collateral_price.0 as u128) // collateral USD value 
                                / Nat::from(10u128.pow(collateral_price.1));

        let expected_repaid_debt = collateral_value * Nat::from(1000u64) / bonus_multiplier.clone();

        let expected_repaid_debt =
            expected_repaid_debt * Nat::from(10u128.pow(debt_price.1)) / Nat::from(debt_price.0 as u128);
      
        assert!(
            expected_repaid_debt == repaid_debt,
            "Expected repaid debt ≈ {}, got {}",
            expected_repaid_debt,
            repaid_debt,
        );
    }

    #[test]
    fn test_full_liquidation_sam_case() {

        // Targeting to repay 76.3912$
        let debt_value = nat(76_391_200_000u128); // 76.3912$

        // 20% bonus -> multiplier = 1200 (basis points)
        let bonus_multiplier = nat(1200u64);

        // Collateral price: $1.00 -> (price, decimals)
        let collateral_price = (1000000000, 9);

        // Debt price: $80,000.00 with 9 decimals
        let debt_price = (80000000000000, 9);

        // Available collateral = 99.94 units -> * 1e6 = 99_940_000u
        let available_collateral = nat(99_940_000u128);

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
            8u32,
            6u32
        );

        // ---------- Assertions ----------
        assert_eq!(collateral, nat(91_669_440u128), "Invalid collateral usage");


        // For debug purposes: print results in case of failure
        println!("Input debt value: {}", debt_value);
        println!("Actual repaid debt: {}", repaid_debt);
        println!("Used collateral: {}", collateral);
        println!("Available collateral: {}", available_collateral);


        assert!(
            95_489u128 == repaid_debt,
            "Expected repaid debt ≈ 95_489, got {}",
            repaid_debt,
        );
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

        // Run the estimator
        let (collateral, debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price,
            debt_price,
            available_collateral.clone(),
              6u32,
            6u32
        );

        // Both returned values should be exactly zero
        assert_eq!(collateral, nat(0u8), "Expected zero collateral seized");
        assert_eq!(debt, nat(0u8), "Expected zero debt repaid");
    }
}
