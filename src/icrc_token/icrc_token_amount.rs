use std::str::FromStr;

use candid::Nat;
use num_traits::Pow;

use super::icrc_token::IcrcToken;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IcrcTokenAmount {
    pub token: IcrcToken,
    pub value: Nat,
}

#[allow(dead_code)]
impl IcrcTokenAmount {
    pub fn from_formatted(token: IcrcToken, formatted_value: f64) -> Self {
        let value = (formatted_value * 10_f64.pow(token.decimals)).to_string();
        let value = Nat::from_str(&value).unwrap();
        Self { token, value }
    }
}
