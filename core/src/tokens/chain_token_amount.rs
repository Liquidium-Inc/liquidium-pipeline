use candid::CandidType;
use candid::Nat;

use num_format::{Locale, ToFormattedString};

use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;

use crate::tokens::chain_token::ChainToken;

#[derive(Debug, Clone, CandidType, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainTokenAmount {
    pub token: ChainToken,
    pub value: Nat, // native units (10^decimals)
}

impl ChainTokenAmount {
    pub fn from_raw(token: ChainToken, raw: Nat) -> Self {
        Self { token, value: raw }
    }

    pub fn from_formatted(token: ChainToken, formatted_value: f64) -> Self {
        let decimals = token.decimals() as u32;
        let scale = 10_f64.powi(decimals as i32);
        let rounded = (formatted_value * scale).round();

        let value = if rounded.is_nan() || rounded.is_infinite() || rounded < 0.0 {
            Nat::from(0u8)
        } else if rounded > u128::MAX as f64 {
            Nat::from(u128::MAX)
        } else {
            Nat::from(rounded as u128)
        };

        Self { token, value }
    }

    /// Parses operator text such as "1.25" into exact native units. More
    /// fraction digits than the token has are refused, never rounded.
    pub fn from_decimal_str(token: ChainToken, text: &str) -> Result<Self, String> {
        let decimals = token.decimals();
        let units = decimal_to_units(text, decimals).ok_or_else(|| {
            format!(
                "invalid amount `{}`: expected a plain decimal with at most {decimals} decimals",
                text.trim()
            )
        })?;
        if units == 0 {
            return Err("amount must be above zero".to_string());
        }
        Ok(Self::from_raw(token, Nat::from(units)))
    }

    pub fn formatted(&self) -> String {
        let raw_value = self.value.clone().0.to_u128().unwrap_or(0);
        let decimals = self.token.decimals() as u32;

        let int_part = raw_value / 10u128.pow(decimals);
        let frac_part = raw_value % 10u128.pow(decimals);

        if decimals > 0 {
            let suffix = format!("{:0>width$}", frac_part, width = decimals as usize);
            format!(
                "{}: {}.{}",
                self.token.symbol(),
                int_part.to_formatted_string(&Locale::en),
                suffix
            )
        } else {
            format!("{}: {}", self.token.symbol(), int_part.to_formatted_string(&Locale::en),)
        }
    }

    pub fn to_f64(&self) -> f64 {
        let raw = self.value.clone().0.to_u128().unwrap_or(0);
        let decimals = self.token.decimals() as u32;

        let scale = 10u128.pow(decimals);
        (raw as f64) / (scale as f64)
    }
}

/// Exact native units for a plain decimal such as "1.25". `None` when the text
/// is not one, has more than `decimals` fraction digits, or exceeds `u128`.
pub fn decimal_to_units(dec_str: &str, decimals: u8) -> Option<u128> {
    let dec_str = dec_str.trim();
    if dec_str.is_empty() {
        return None;
    }
    let mut parts = dec_str.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return None;
    }
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut frac_norm = frac.to_string();
    if frac_norm.len() > decimals as usize {
        return None;
    }
    while frac_norm.len() < decimals as usize {
        frac_norm.push('0');
    }
    let combined = if whole.is_empty() {
        format!("0{}", frac_norm)
    } else {
        format!("{}{}", whole, frac_norm)
    };
    combined.parse::<u128>().ok()
}

#[cfg(test)]
mod tests {
    use candid::{Nat, Principal};

    use super::{ChainTokenAmount, decimal_to_units};
    use crate::tokens::chain_token::ChainToken;

    fn token(decimals: u8) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "TKN".to_string(),
            decimals,
            fee: Nat::from(0u8),
        }
    }

    #[test]
    fn decimal_to_units_rejects_empty_or_whitespace() {
        assert_eq!(decimal_to_units("", 8), None);
        assert_eq!(decimal_to_units("   ", 8), None);
        assert_eq!(decimal_to_units("\t\n", 8), None);
    }

    #[test]
    fn decimal_to_units_rejects_dot_only() {
        assert_eq!(decimal_to_units(".", 8), None);
        assert_eq!(decimal_to_units(" . ", 8), None);
    }

    #[test]
    fn decimal_to_units_accepts_trimmed_valid_input() {
        assert_eq!(decimal_to_units(" 1.23 ", 2), Some(123));
    }

    #[test]
    fn decimal_to_units_accepts_leading_dot_fraction() {
        assert_eq!(decimal_to_units(".5", 2), Some(50));
    }

    #[test]
    fn decimal_to_units_rejects_invalid_chars() {
        assert_eq!(decimal_to_units("1.a", 2), None);
        assert_eq!(decimal_to_units("a.1", 2), None);
        assert_eq!(decimal_to_units("1 2", 2), None);
    }

    #[test]
    fn decimal_to_units_rejects_too_many_fraction_digits() {
        assert_eq!(decimal_to_units("1.234", 2), None);
    }

    #[test]
    fn decimal_to_units_zero_decimals_behavior() {
        assert_eq!(decimal_to_units("12", 0), Some(12));
        assert_eq!(decimal_to_units("12.0", 0), None);
    }

    #[test]
    fn from_decimal_str_refuses_malformed_and_nonpositive_text() {
        for text in ["", "abc", "1,5", "1e3", "-1", "+1", "0", "0.000000", "1.2.3", "NaN"] {
            assert!(
                ChainTokenAmount::from_decimal_str(token(6), text).is_err(),
                "`{text}` was accepted"
            );
        }
    }

    #[test]
    fn from_decimal_str_keeps_every_unit_at_full_precision() {
        for decimals in [6u8, 8, 9, 18] {
            let one_unit = format!("0.{}1", "0".repeat(decimals as usize - 1));
            let parsed = ChainTokenAmount::from_decimal_str(token(decimals), &one_unit).expect("one unit");
            assert_eq!(parsed.value, Nat::from(1u8), "{decimals} decimals");

            let full = format!("123456.{}", "9".repeat(decimals as usize));
            let expected = 123_457u128 * 10u128.pow(decimals as u32) - 1;
            let parsed = ChainTokenAmount::from_decimal_str(token(decimals), &full).expect("full precision");
            assert_eq!(parsed.value, Nat::from(expected), "{decimals} decimals");

            let too_fine = format!("{one_unit}0");
            assert!(ChainTokenAmount::from_decimal_str(token(decimals), &too_fine).is_err());
        }
    }
}
