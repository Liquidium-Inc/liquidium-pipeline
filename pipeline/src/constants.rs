pub const REPAY_BUFFER_CKETH_WEI: u128 = 300_000_000_000;
pub const REPAY_BUFFER_CKBTC_SATS: u128 = 10;
pub const REPAY_BUFFER_CKUSDC_E6: u128 = 1_000;
pub const REPAY_BUFFER_CKUSDT_E6: u128 = 1_000;
pub const REPAY_BUFFER_ICP_E8: u128 = 10_000;
pub const REPAY_BUFFER_CKSOL_E9: u128 = 1_000;

pub const REPAY_BUFFERS_NATIVE: &[(&str, u128)] = &[
    ("ckETH", REPAY_BUFFER_CKETH_WEI),
    ("ckBTC", REPAY_BUFFER_CKBTC_SATS),
    ("ckUSDC", REPAY_BUFFER_CKUSDC_E6),
    ("ckUSDT", REPAY_BUFFER_CKUSDT_E6),
    ("ICP", REPAY_BUFFER_ICP_E8),
    ("ckSOL", REPAY_BUFFER_CKSOL_E9),
];

/// Returns the amount that an offer adds to the debt, for the interest between the scan and the call.
/// An asset that is not in the table gets one millionth of a token, at least 1 native unit.
pub fn repay_buffer_native_units(symbol: &str, decimals: u8) -> u128 {
    REPAY_BUFFERS_NATIVE
        .iter()
        .find_map(|(buffer_symbol, buffer)| buffer_symbol.eq_ignore_ascii_case(symbol).then_some(*buffer))
        .unwrap_or_else(|| 10u128.pow(u32::from(decimals.saturating_sub(6))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listed_assets_use_the_table_value() {
        assert_eq!(repay_buffer_native_units("ckBTC", 8), 10);
        assert_eq!(repay_buffer_native_units("ckUSDC", 6), 1_000);
        assert_eq!(repay_buffer_native_units("ckUSDT", 6), 1_000);
        assert_eq!(repay_buffer_native_units("ckETH", 18), 300_000_000_000);
        assert_eq!(repay_buffer_native_units("ICP", 8), 10_000);
        assert_eq!(repay_buffer_native_units("cksol", 9), 1_000);
    }

    #[test]
    fn an_unlisted_asset_gets_a_buffer_of_at_least_one_unit() {
        assert_eq!(repay_buffer_native_units("NEW18", 18), 1_000_000_000_000);
        assert_eq!(repay_buffer_native_units("NEW8", 8), 100);
        assert_eq!(repay_buffer_native_units("NEW6", 6), 1);
        assert_eq!(repay_buffer_native_units("NEW2", 2), 1);
    }
}
