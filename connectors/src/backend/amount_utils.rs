use alloy::primitives::U256;
use candid::Nat;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

fn synthetic_token_with_decimals(decimals: u8) -> ChainToken {
    ChainToken::EvmNative {
        chain: "synthetic".to_string(),
        symbol: "SYN".to_string(),
        decimals,
        fee: Nat::from(0u8),
    }
}

/// Converts a human amount into scaled integer units with strict validation.
///
/// Rules:
/// - amount must be finite and strictly positive
/// - conversion uses floor semantics
/// - result must be at least 1 base unit
/// - result must fit in `u128`
pub fn amount_to_scaled_u128_strict(amount: f64, decimals: u8) -> Result<u128, String> {
    if !amount.is_finite() || amount <= 0.0 {
        return Err("amount must be positive and finite".to_string());
    }
    let scale = 10f64.powi(decimals as i32);
    let raw = (amount * scale).floor();
    if !raw.is_finite() || raw <= 0.0 {
        return Err("amount rounds to zero base units".to_string());
    }
    if raw > (u128::MAX as f64) {
        return Err("amount too large".to_string());
    }
    Ok(raw as u128)
}

/// Strictly converts a human amount into ICP-native `Nat` units.
pub fn amount_to_nat_units_strict(amount: f64, decimals: u8) -> Result<Nat, String> {
    Ok(Nat::from(amount_to_scaled_u128_strict(amount, decimals)?))
}

/// Strictly converts a human amount into EVM `U256` base units.
pub fn amount_to_base_units_strict(amount: f64, decimals: u8) -> Result<U256, String> {
    Ok(U256::from(amount_to_scaled_u128_strict(amount, decimals)?))
}

/// Converts ICRC native units into human amount using `core::ChainTokenAmount`.
///
/// Rejects values that cannot be represented by the current `core::ChainTokenAmount`
/// conversion path instead of silently returning `0.0`.
pub fn nat_units_to_amount_via_core(amount_native: &Nat, decimals: u8) -> Result<f64, String> {
    if amount_native > &Nat::from(u128::MAX) {
        return Err(format!(
            "amount out of range: native amount {} exceeds u128::MAX",
            amount_native
        ));
    }

    let token = synthetic_token_with_decimals(decimals);
    let amount = ChainTokenAmount::from_raw(token, amount_native.clone());
    let human = amount.to_f64();

    if !human.is_finite() {
        return Err(format!(
            "amount out of range: native amount {} is not representable as finite f64",
            amount_native
        ));
    }

    if amount_native != &Nat::from(0u8) && human == 0.0 {
        return Err(format!(
            "amount out of range: non-zero native amount {} cannot be represented as f64",
            amount_native
        ));
    }

    Ok(human)
}

/// Converts EVM base units into human amount using `core::ChainTokenAmount`.
pub fn base_units_to_amount_via_core(base_units: U256, decimals: u8) -> Result<f64, String> {
    let raw_nat: Nat = base_units
        .to_string()
        .parse()
        .map_err(|e| format!("failed to convert base units to Nat: {e}"))?;
    nat_units_to_amount_via_core(&raw_nat, decimals)
}

#[cfg(test)]
mod tests {
    use super::{
        amount_to_base_units_strict, amount_to_nat_units_strict, amount_to_scaled_u128_strict,
        base_units_to_amount_via_core, nat_units_to_amount_via_core,
    };
    use alloy::primitives::U256;
    use candid::Nat;

    #[test]
    fn strict_converter_rejects_non_finite_amount() {
        assert!(amount_to_scaled_u128_strict(f64::NAN, 6).is_err());
        assert!(amount_to_scaled_u128_strict(f64::INFINITY, 6).is_err());
    }

    #[test]
    fn strict_converter_rejects_non_positive_amount() {
        assert!(amount_to_scaled_u128_strict(0.0, 6).is_err());
        assert!(amount_to_scaled_u128_strict(-1.0, 6).is_err());
    }

    #[test]
    fn strict_converter_preserves_floor_semantics() {
        let value = amount_to_scaled_u128_strict(1.239, 2).expect("must convert");
        assert_eq!(value, 123);
    }

    #[test]
    fn strict_converter_rejects_round_to_zero() {
        assert!(amount_to_scaled_u128_strict(0.0001, 2).is_err());
    }

    #[test]
    fn strict_converter_rejects_overflow() {
        assert!(amount_to_scaled_u128_strict(f64::MAX, 0).is_err());
    }

    #[test]
    fn strict_nat_and_base_unit_converters_match_expected_values() {
        let nat = amount_to_nat_units_strict(1.23, 2).expect("nat convert");
        assert_eq!(nat, Nat::from(123u128));

        let base = amount_to_base_units_strict(1.23, 2).expect("base convert");
        assert_eq!(base, U256::from(123u128));
    }

    #[test]
    fn core_nat_converter_scales_with_decimals() {
        let amount = Nat::from(12345u128);
        let human = nat_units_to_amount_via_core(&amount, 2).expect("convert");
        assert_eq!(human, 123.45);
    }

    #[test]
    fn core_base_converter_scales_with_decimals() {
        let human = base_units_to_amount_via_core(U256::from(12345u128), 3).expect("convert");
        assert_eq!(human, 12.345);
    }

    #[test]
    fn core_read_converter_rejects_values_above_u128_limit() {
        let too_large = Nat::from(u128::MAX) + Nat::from(1u8);
        let err = nat_units_to_amount_via_core(&too_large, 0).expect_err("should fail");
        assert!(err.contains("amount out of range"));
    }

    #[test]
    fn core_base_converter_rejects_values_above_u128_limit() {
        let too_large = U256::from(u128::MAX) + U256::from(1u8);
        let err = base_units_to_amount_via_core(too_large, 0).expect_err("should fail");
        assert!(err.contains("amount out of range"));
    }
}
