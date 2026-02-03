use candid::Nat;

pub const BPS_ONE: u64 = 10_000; // 100% in basis points

#[inline]
fn pow10(decimals: u64) -> Nat {
    Nat::from(10u128.pow(decimals as u32))
}

/*
 Compute collateral seize and protocol fee in **collateral native units**
 from a given debt repayment amount and per-pool liquidation parameters.

 Inputs:
 - `repay_native`: native units of debt asset that will be repaid
 - `price_debt_ray`: RAY-scaled price per 1 debt-native (e.g., $2 * 1e27)
 - `price_coll_ray`: RAY-scaled price per 1 collateral-native (must be > 0)
 - `debt_decimals`: decimals of the debt token
 - `coll_decimals`: decimals of the collateral token
 - `bonus_bps`: liquidation bonus in basis points [0, 10_000]
 - `fee_bps`: protocol fee in basis points [0, 10_000]

 Returns:
 - `(seized_native, fee_native)` both in **collateral native** units

 Note:
 - All returned values (`seized_native`, `fee_native`, and values from `max_repay_from_collateral`) are in the *native unit scale* of their respective assets.
 - RAY (1e27) and token decimals are only used internally for conversion between quote and native.
 - The final results are truncated to the underlying asset precision (e.g., sats, wei, or 6‑dec stablecoin units).
*/
pub fn compute_liquidation_amounts(
    repay_native: &Nat,
    price_debt_ray: &Nat,
    price_coll_ray: &Nat,
    debt_decimals: u64,
    coll_decimals: u64,
    bonus_bps: u64,
    fee_bps: u64,
) -> Result<(Nat, Nat), String> {
    // Guards
    if *price_coll_ray == 0u8 {
        return Err("Zero collateral price".into());
    }
    if *price_debt_ray == 0u8 {
        return Err("Zero debt price".into());
    }
    if bonus_bps > BPS_ONE || fee_bps > BPS_ONE {
        return Err("bps out of range".into());
    }
    if debt_decimals > 38 || coll_decimals > 38 {
        return Err("decimals too large".into());
    }

    let debt_scale = pow10(debt_decimals);
    let coll_scale = pow10(coll_decimals);

    // Keep quote in RAY domain (no division by RAY here):
    // repay_quote_ray = price_debt_ray * repay_native / debt_scale
    let repay_quote_ray = (price_debt_ray.clone() * repay_native.clone()) / debt_scale.clone();

    // Bonus and fee in quote (RAY)
    let one_bps = Nat::from(BPS_ONE);
    let bonus_quote_ray = (repay_quote_ray.clone() * Nat::from(bonus_bps)) / one_bps.clone();
    let fee_quote_ray = (bonus_quote_ray.clone() * Nat::from(fee_bps)) / one_bps.clone();
    let net_quote_ray = repay_quote_ray + (bonus_quote_ray.clone() - fee_quote_ray.clone());

    // Convert quote (RAY) -> collateral native (single truncation)
    let seized_native = (net_quote_ray * coll_scale.clone()) / price_coll_ray.clone();
    let fee_native = (fee_quote_ray * coll_scale) / price_coll_ray.clone();

    Ok((seized_native, fee_native))
}

/*
 Intuition (everything below is in the same quote unit, e.g. USD):

 We want to know the **largest repay** we can accept so that the
 **collateral seized** (after bonus and protocol fee-on-bonus) does not
 exceed the borrower's **available collateral**.

 Definitions:
 - `bonus_bps` = liquidation bonus in basis points (1 bps = 1/100 of 1%)
 - `fee_bps`   = protocol fee, applied **only to the bonus**
 - Let `b = bonus_bps / 10_000`, `f = fee_bps / 10_000`.

 For a given repay value `r` (in quote):
   net_to_liquidator = r + (bonus_value - fee_on_bonus)   protocol takes its cut *only* from the bonus

   Using b = bonus_bps / 10_000 and f = fee_bps / 10_000:
     bonus_value   = r * b
     fee_on_bonus  = bonus_value * f = r * b * f
     => net_to_liquidator = r + (r*b - r*b*f)
                         = r * [ 1 + b * (1 - f) ]

   Quick example: r = $100, b = 10% (1000 bps), f = 5% (500 bps)
     bonus_value   = 100 * 0.10 = $10
     fee_on_bonus  = 10 * 0.05  = $0.50
     net_to_liquidator = 100 + (10 - 0.50) = $109.50

Call the bracket term `denom = 1 + b * (1 - f)`.
   We had: denom = 1 + b * (1 - f)
   with b = bonus_bps/10_000 and f = fee_bps/10_000.
   Multiply everything by 10_000 to avoid floats:
     denom_bps = 10_000 * denom
               = 10_000 * [1 + (bonus_bps/10_000) * (1 - fee_bps/10_000)]
               = 10_000 + bonus_bps - (bonus_bps * fee_bps / 10_000)


 Now cap the repay by available collateral:
    coll_cap_quote = (max_coll_native * price_coll_ray) / (RAY * 10^coll_decimals)
    r_max_quote    = coll_cap_quote / denom

    Using bps (integers):
      r_max_quote = coll_cap_quote * 10_000 / denom_bps

    Convert back to debt-native units:
      repay_native_max = (r_max_quote * RAY * 10^debt_decimals) / price_debt_ray
*/
pub fn max_repay_from_collateral(
    max_coll_native: &Nat,
    price_coll_ray: &Nat,
    price_debt_ray: &Nat,
    debt_decimals: u64,
    coll_decimals: u64,
    bonus_bps: u64,
    fee_bps: u64,
) -> Result<Nat, String> {
    if *price_debt_ray == 0u8 {
        return Err("Zero debt price".into());
    }
    if *price_coll_ray == 0u8 {
        return Err("Zero collateral price".into());
    }
    if bonus_bps > BPS_ONE || fee_bps > BPS_ONE {
        return Err("bps out of range".into());
    }
    if debt_decimals > 38 || coll_decimals > 38 {
        return Err("decimals too large".into());
    }

    let debt_scale = pow10(debt_decimals);
    let coll_scale = pow10(coll_decimals);

    // Collateral cap in quote (RAY):
    // coll_cap_quote_ray = price_coll_ray * max_coll_native / coll_scale
    let coll_cap_quote_ray = (price_coll_ray.clone() * max_coll_native.clone()) / coll_scale.clone();

    // denom_bps = 10_000 + bonus_bps - floor(bonus_bps * fee_bps / 10_000)
    let one_bps = Nat::from(BPS_ONE);
    let bonus = Nat::from(bonus_bps);
    let fee = Nat::from(fee_bps);
    let bonus_fee = (bonus.clone() * fee.clone()) / one_bps.clone();
    let denom_bps = one_bps.clone() + bonus - bonus_fee; // >= 10_000
    if denom_bps == 0u8 {
        return Err("Invalid liquidation params".into());
    }

    // r_max_quote_ray = coll_cap_quote_ray * 10_000 / denom_bps   (still RAY)
    let r_max_quote_ray = (coll_cap_quote_ray * one_bps.clone()) / denom_bps;

    // repay_native_max = (r_max_quote_ray * debt_scale) / price_debt_ray
    let repay_native = (r_max_quote_ray * debt_scale) / price_debt_ray.clone();
    Ok(repay_native)
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Nat;

    #[test]
    fn compute_errors_on_zero_prices() {
        let repay = Nat::from(1_000u64);
        // Zero collateral price
        assert!(compute_liquidation_amounts(&repay, &(10u128.pow(27)).into(), &Nat::from(0u8), 6, 8, 0, 0,).is_err());
        // Zero debt price
        assert!(compute_liquidation_amounts(&repay, &Nat::from(0u8), &(10u128.pow(27)).into(), 6, 8, 0, 0,).is_err());
    }

    #[test]
    fn max_repay_errors_on_zero_prices() {
        let max_coll = Nat::from(100_000u64);
        // Zero collateral price
        assert!(max_repay_from_collateral(&max_coll, &Nat::from(0u8), &(10u128.pow(27)).into(), 6, 8, 0, 0,).is_err());
        // Zero debt price
        assert!(max_repay_from_collateral(&max_coll, &(10u128.pow(27)).into(), &Nat::from(0u8), 6, 8, 0, 0,).is_err());
    }

    #[test]
    fn no_bonus_no_fee_identity_in_native() {
        // Prices in RAY
        let p_btc = Nat::from(80_000u128) * Nat::from(10u128.pow(27));
        let p_usdt = Nat::from(10u128.pow(27));

        // Repay $100 in USDT native
        let repay_native = Nat::from(100_000_000u64);
        let (seized_native, fee_native) =
            compute_liquidation_amounts(&repay_native, &p_usdt, &p_btc, 6, 8, 0, 0).unwrap();

        // Collateral needed for $100 at $80k/BTC = 0.00125 BTC = 125_000 sats
        assert_eq!(fee_native, Nat::from(0u8));
        assert_eq!(seized_native, Nat::from(125_000u64));

        // The max repay from exactly that collateral should be $100
        let rmax = max_repay_from_collateral(&seized_native, &p_btc, &p_usdt, 6, 8, 0, 0).unwrap();
        assert_eq!(rmax, repay_native);
    }

    #[test]
    fn fee_applies_only_on_bonus() {
        // Prices in RAY
        let p_btc = Nat::from(80_000u128) * Nat::from(10u128.pow(27));
        let p_usdt = Nat::from(10u128.pow(27));

        // Repay $100, 10% bonus, 100% fee-on-bonus => net bonus 0
        let repay_native = Nat::from(100_000_000u64);
        let (seized_native, fee_native) =
            compute_liquidation_amounts(&repay_native, &p_usdt, &p_btc, 6, 8, 1000, 10_000).unwrap();

        // Base collateral for $100 at $80k is 125_000 sats
        assert_eq!(seized_native, Nat::from(125_000u64));
        // Gross bonus native = floor(125_000 * 1000 / 10_000) = 12_500; fee should equal gross bonus
        assert_eq!(fee_native, Nat::from(12_500u64));
    }

    #[test]
    fn varying_decimals_round_trip() {
        // Debt: 18 decimals (WETH), Collateral: 6 decimals (USDC)
        let p_eth = Nat::from(2_000u128) * Nat::from(10u128.pow(27));
        let p_usdc = Nat::from(10u128.pow(27));
        let repay_eth_native = Nat::from(5_000_000_000_000_000u128); // 0.005 ETH

        let (seized_usdc_native, _fee) =
            compute_liquidation_amounts(&repay_eth_native, &p_eth, &p_usdc, 18, 6, 500, 100).unwrap();

        // Take that seized amount as the collateral cap, compute max repay, then recompute seized
        let rmax = max_repay_from_collateral(&seized_usdc_native, &p_usdc, &p_eth, 18, 6, 500, 100).unwrap();
        let (seized_again, _fee2) = compute_liquidation_amounts(&rmax, &p_eth, &p_usdc, 18, 6, 500, 100).unwrap();

        assert!(seized_again <= seized_usdc_native);
        let diff = if seized_usdc_native > seized_again {
            seized_usdc_native.clone() - seized_again
        } else {
            Nat::from(0u8)
        };
        // Tolerance: one smallest native unit of collateral (USDC has 6 decimals here)
        let tol = pow10(6);
        assert!(diff <= tol, "round-trip loss {} exceeds collateral unit {}", diff, tol);
    }

    #[test]
    fn seized_is_monotonic_in_repay() {
        // Prices in RAY
        let p_btc = Nat::from(80_000u128) * Nat::from(10u128.pow(27));
        let p_usdt = Nat::from(10u128.pow(27));

        let mut last = Nat::from(0u8);
        for repay in [10_000_000u64, 20_000_000, 30_000_000, 50_000_000, 100_000_000] {
            let (seized, _fee) =
                compute_liquidation_amounts(&Nat::from(repay), &p_usdt, &p_btc, 6, 8, 1000, 200).unwrap();
            assert!(seized >= last);
            last = seized;
        }
    }
}
