

use candid::Nat;
use lending_utils::constants::LIQUIDATION_BONUS_SCALE;
use log::{debug, info};

use crate::ray_math::WadRayMath;

pub fn estimate_liquidation(
    debt_value: Nat,              // USD in raw units
    bonus_multiplier: Nat,        // e.g. 11_000 = 10% bonus (raw)
    collateral_price: (Nat, u32), // (price, decimals)
    debt_price: (Nat, u32),       // (price, decimals)
    available_collateral: Nat,    // in raw collateral units
    debt_decimals: u32,
    collateral_decimals: u32,
) -> (Nat, Nat) {
    // Ray‐scale all the building blocks exactly once:
let bonus_scale_ray                = Nat::from(LIQUIDATION_BONUS_SCALE).to_ray();
    let bonus_ray                  = bonus_multiplier.to_ray();
    let debt_value                 = debt_value.to_ray();
    let collateral_price_ray       = collateral_price.0.to_ray();
    let debt_price_ray             = debt_price.0.to_ray();
    let collateral_price_scale_ray = Nat::from(10u128.pow(collateral_price.1)).to_ray();
    let collateral_scale           = Nat::from(10u128.pow(collateral_decimals)).to_ray();
    let debt_scale                 = Nat::from(10u128.pow(debt_decimals)).to_ray();

    debug!(
        "[estimate] \n\n debt_val={} (raw)\n bonus={}\n price_coll={} (dec{}) \n price_debt={} (dec{}) \n avail_coll={}\n",
        debt_value.from_ray(),
        bonus_multiplier,
        collateral_price.0, 
        collateral_price.1,
        debt_price.0,      
        debt_price.1,
        available_collateral,
    );
    // 1) Apply liquidation bonus
    let adj_debt_value_ray = debt_value
        .ray_mul(&bonus_ray)
        .ray_div(&bonus_scale_ray);

    debug!(
        "[estimate] \nadj_debt={} USD",
        adj_debt_value_ray.from_ray()
    );

    // 2) Compute collateral needed in RAY: adj_debt_ray * (10^token_dec) / (10^price_dec) / price
    let collateral_needed_ray = adj_debt_value_ray 
        .ray_mul(&collateral_price_scale_ray)
        .ray_div(&collateral_price_ray)            
        .ray_mul(&collateral_scale)
        .ray_div(&collateral_price_scale_ray);
    
    let collateral_needed = collateral_needed_ray.from_ray();

    debug!(
        "[estimate] collateral_needed={}, available={}",
        collateral_needed, available_collateral
    );

    // 3) Full vs Partial
    if collateral_needed <= available_collateral {
        // full liquidation: repay entire debt
        let repay_ray = debt_value
            .ray_div(&debt_price_ray)
            .ray_mul(&debt_scale);
        let repay = repay_ray.from_ray();

        debug!(
            "[estimate] USING PARTIAL COLLATERAL -> use_coll={}, repay={}",
            collateral_needed, repay
        );
        return (collateral_needed, repay);
    }

    // Handle buying of bad debt
    if available_collateral <= Nat::from(1u8) {
        // full liquidation: repay entire debt
        let repay_ray = debt_value
            .ray_div(&debt_price_ray)
            .ray_mul(&debt_scale);
        let repay = repay_ray.from_ray();

        debug!(
            "[estimate] REPAYING BAD REBT -> use_coll={}, repay={}",
            available_collateral, repay
        );
        return (available_collateral, repay);
    }

    debug!("[estimate] FULL -> using all collateral");

    // value of collateral in USD ray:
    let collateral_value_ray = available_collateral
        .to_ray()
        .ray_mul(&collateral_price_ray)
        .ray_div(&collateral_scale); // get rid of collateral scaling

    debug!(
        "[estimate] collateral_value_ray={} (raw = {}), collateral_scale = {} ",
        collateral_value_ray, collateral_value_ray.from_ray(), collateral_scale.from_ray()
    );

    // reverse bonus: USD ray debt = coll_value_ray * 1000 / bonus
    let partial_debt_ray = collateral_value_ray
        .ray_mul(&bonus_scale_ray)
        .ray_div(&bonus_ray);

      debug!(
        "[estimate] partial_debt_ray= {}, debt_scale = {} ",
        partial_debt_ray.from_ray(), debt_scale.from_ray()
    );

    // convert USD ray -> debt tokens
    let repay_ray = partial_debt_ray
        .ray_div(&debt_price_ray) // USD scale is here
        .ray_mul(&debt_scale);

    let repay = repay_ray.from_ray();

    info!(
        "[estimate] PARTIAL -> use_coll={}, repay={}",
        available_collateral, repay
    );

    (available_collateral, repay)
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
        let collateral_price = (nat(2_000_000u64), 6); // $2 with 6 decimals
        let debt_price = (nat(1_000_000u64), 6); // $1 with 6 decimals
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
        let collateral_price = (nat(2_000_000_000u64), 9);

        // Debt price: $1.00 with 9 decimals
        let debt_price = (nat(1_000_000_000u64), 9);

        let available_collateral = nat(600_000_000u64); // enough for full liquidation

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price.clone(),
            debt_price.clone(),
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
            / collateral_price.0 ; // price already in 6 decimals

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
        let bonus_multiplier = nat(1200u64);

        // Collateral price: $2.00 -> (price, decimals)
        let collateral_price = (nat(2_000_00000u64), 8);

        // Debt price: $1.00 with 9 decimals
        let debt_price = (nat(1_000_000_000u64), 9);

        // Available collateral = 0.5 units -> 0.5 * 1e6 = 500_000
        let available_collateral = nat(500_000u64);

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price.clone(),
            debt_price.clone(),
            available_collateral.clone(),
            12u32,
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
                                collateral_price.0.clone()  // collateral USD value 
                                / Nat::from(10u128.pow(6));

        let expected_repaid_debt_value = collateral_value * Nat::from(1000u64) / bonus_multiplier.clone();

        println!("expected_repaid_debt: {}", expected_repaid_debt_value);

        let expected_repaid_debt = expected_repaid_debt_value.to_ray()
                                .ray_div(&debt_price.0.to_ray()) 
                                .ray_mul(&Nat::from(10u128.pow(12)).to_ray())
                                .from_ray();
      
        assert!(
             repaid_debt.clone() - expected_repaid_debt.clone()  < 1000u32,
            "Expected repaid debt ≈ {}, got {}, diff {}",
            expected_repaid_debt,
            repaid_debt,
            repaid_debt.clone() -  expected_repaid_debt.clone()
        );
    }

    #[test]
    fn test_full_liquidation_sam_case() {

        // Targeting to repay 76.3912$
        let debt_value = nat(76_391_200_000u128); // 76.3912$

        // 20% bonus -> multiplier = 1200 (basis points)
        let bonus_multiplier = nat(1200u64);

        // Collateral price: $1.00 -> (price, decimals)
        let collateral_price = (nat(1000000000u128), 9);

        // Debt price: $80,000.00 with 9 decimals
        let debt_price = (nat(80000000000000u128), 9);

        // Available collateral = 99.94 units -> * 1e6 = 99_940_000u
        let available_collateral = nat(99_940_000u128);

        // ---------- Execute ----------
        let (collateral, repaid_debt) = estimate_liquidation(
            debt_value.clone(),
            bonus_multiplier.clone(),
            collateral_price.clone(),
            debt_price.clone(),
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
        let collateral_price = (nat(2_000_000u64), 6);

        // Debt price: $1.000000 (scaled as 1_000_000 with 6 decimals)
        let debt_price = (nat(1_000_000u64), 6);

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
