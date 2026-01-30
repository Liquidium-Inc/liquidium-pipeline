use std::str::FromStr;

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
        let value = if scaled.is_nan() || scaled.is_infinite() || scaled < 0.0 {
            Nat::from(0u8)
        } else {
            Nat::from_str(&scaled.to_string()).unwrap_or_else(|_| Nat::from(0u8))
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
