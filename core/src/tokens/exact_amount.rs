use std::str::FromStr;

use candid::Nat;

/// Parses an unsigned human decimal into ledger-native units without using
/// floating point arithmetic.
pub fn parse_decimal_units(value: &str, decimals: u8) -> Result<Nat, String> {
    let value = value.trim();
    if value.is_empty() || value.starts_with('-') || value.starts_with('+') || value.contains(['e', 'E']) {
        return Err(format!("invalid decimal amount '{value}'"));
    }
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole.is_empty() && fraction.is_empty())
        || !whole.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return Err(format!("invalid decimal amount '{value}'"));
    }
    if fraction.len() > usize::from(decimals) {
        return Err(format!("amount '{value}' has more than {decimals} decimal places"));
    }
    let whole = if whole.is_empty() { "0" } else { whole };
    let mut raw = whole.to_string();
    raw.push_str(fraction);
    raw.extend(std::iter::repeat_n('0', usize::from(decimals) - fraction.len()));
    Nat::from_str(raw.trim_start_matches('0'))
        .or_else(|_| Nat::from_str("0"))
        .map_err(|error| format!("amount '{value}' is too large: {error}"))
}

/// Formats ledger-native units as an exact human decimal without separators
/// or insignificant trailing zeroes.
pub fn format_units(value: &Nat, decimals: u8) -> String {
    let mut digits = value.to_string().replace('_', "");
    if decimals == 0 {
        return digits;
    }
    let decimals = usize::from(decimals);
    if digits.len() <= decimals {
        digits.insert_str(0, &"0".repeat(decimals + 1 - digits.len()));
    }
    let split = digits.len() - decimals;
    digits.insert(split, '.');
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.ends_with('.') {
        digits.pop();
    }
    digits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_decimal_round_trip() {
        let amount = parse_decimal_units("1.00000001", 8).unwrap();
        assert_eq!(amount, Nat::from(100_000_001u64));
        assert_eq!(format_units(&amount, 8), "1.00000001");
    }

    #[test]
    fn rejects_signs_exponents_and_excess_precision() {
        for invalid in ["-1", "+1", "1e2", "1.000000001"] {
            assert!(parse_decimal_units(invalid, 8).is_err(), "accepted {invalid}");
        }
    }
}
