use std::str::FromStr;

use candid::CandidType;
use candid::Nat;

use num_format::{Locale, ToFormattedString};

use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;

use crate::tokens::chain_token::ChainToken;

#[derive(Debug, Clone, CandidType, Serialize, Deserialize)]
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

        let value = (formatted_value * scale).round().to_string();
        let value = Nat::from_str(&value).expect("invalid formatted value");

        Self { token, value }
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
