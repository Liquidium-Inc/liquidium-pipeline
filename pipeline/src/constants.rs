pub const REPAY_BUFFER_CKETH_WEI: u128 = 300_000_000_000;
pub const REPAY_BUFFER_CKBTC_SATS: u128 = 10;
pub const REPAY_BUFFER_CKUSDC_E6: u128 = 1_000;
pub const REPAY_BUFFER_CKUSDT_E6: u128 = 1_000;
pub const REPAY_BUFFER_ICP_E8: u128 = 10_000;

pub const REPAY_BUFFERS_NATIVE: &[(&str, u128)] = &[
    ("ckETH", REPAY_BUFFER_CKETH_WEI),
    ("ckBTC", REPAY_BUFFER_CKBTC_SATS),
    ("ckUSDC", REPAY_BUFFER_CKUSDC_E6),
    ("ckUSDT", REPAY_BUFFER_CKUSDT_E6),
    ("ICP", REPAY_BUFFER_ICP_E8),
];

pub fn repay_buffer_native_units(symbol: &str) -> Option<u128> {
    REPAY_BUFFERS_NATIVE
        .iter()
        .find_map(|(buffer_symbol, buffer)| buffer_symbol.eq_ignore_ascii_case(symbol).then_some(*buffer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repay_buffers_are_only_defined_for_explicit_assets() {
        assert_eq!(repay_buffer_native_units("ckBTC"), Some(10));
        assert_eq!(repay_buffer_native_units("ckUSDC"), Some(1_000));
        assert_eq!(repay_buffer_native_units("ckUSDT"), Some(1_000));
        assert_eq!(repay_buffer_native_units("ckETH"), Some(300_000_000_000));
        assert_eq!(repay_buffer_native_units("ICP"), Some(10_000));
        assert_eq!(repay_buffer_native_units("SOL"), None);
    }
}
