use candid::Nat;
use num_format::{Locale, ToFormattedString};
use num_traits::Pow;
use num_traits::ToPrimitive;

use crate::tokens::icrc::icrc_token::IcrcToken;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IcrcTokenAmount {
    pub token: IcrcToken,
    pub value: Nat,
}

#[allow(dead_code)]
impl IcrcTokenAmount {
    pub fn from_formatted(token: IcrcToken, formatted_value: f64) -> Self {
        let scaled = formatted_value * 10_f64.pow(token.decimals);
        let rounded = scaled.round();

        let value = if rounded.is_nan() || rounded.is_infinite() || rounded < 0.0 {
            Nat::from(0u8)
        } else if rounded > u128::MAX as f64 {
            Nat::from(u128::MAX)
        } else {
            Nat::from(rounded as u128)
        };

        Self { token, value }
    }

    pub fn formatted(&self) -> String {
        let raw_value = self.value.clone().0.to_u128().unwrap_or(0);

        let decimals = self.token.decimals as u32;

        let int_part = raw_value / 10u128.pow(decimals);
        let frac_part = raw_value % 10u128.pow(decimals);

        if decimals > 0 {
            let suffix = format!("{:0>width$}", frac_part, width = decimals as usize);
            format!(
                "{}: {}.{}",
                self.token.symbol,
                int_part.to_formatted_string(&Locale::en),
                suffix
            )
        } else {
            format!("{}: {}", self.token.symbol, int_part.to_formatted_string(&Locale::en),)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_to_nat_returns_zero_for_nan() {
        // given
        let nan = f64::NAN;

        // when
        let result = IcrcTokenAmount::clamp_to_nat(nan);

        // then
        assert_eq!(result, Nat::from(0u8));
    }

    #[test]
    fn clamp_to_nat_returns_zero_for_infinity() {
        // given
        let inf = f64::INFINITY;

        // when
        let result = IcrcTokenAmount::clamp_to_nat(inf);

        // then
        assert_eq!(result, Nat::from(0u8));
    }

    #[test]
    fn clamp_to_nat_returns_zero_for_negative() {
        // given
        let negative = -100.0;

        // when
        let result = IcrcTokenAmount::clamp_to_nat(negative);

        // then
        assert_eq!(result, Nat::from(0u8));
    }

    #[test]
    fn clamp_to_nat_returns_max_for_overflow() {
        // given
        let overflow = (u128::MAX as f64) * 2.0;

        // when
        let result = IcrcTokenAmount::clamp_to_nat(overflow);

        // then
        assert_eq!(result, Nat::from(u128::MAX));
    }

    #[test]
    fn clamp_to_nat_converts_valid_value() {
        // given
        let valid = 12345.0;

        // when
        let result = IcrcTokenAmount::clamp_to_nat(valid);

        // then
        assert_eq!(result, Nat::from(12345u128));
    }
}
