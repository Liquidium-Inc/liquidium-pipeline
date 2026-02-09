use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

pub(super) fn format_balance_result(res: Option<&Result<(AssetId, ChainTokenAmount), String>>) -> String {
    match res {
        Some(Ok((_, bal))) => format_chain_balance(bal),
        Some(Err(e)) => format!("err: {}", e),
        None => "-".to_string(),
    }
}

pub(super) fn format_chain_balance(bal: &ChainTokenAmount) -> String {
    let raw = bal.value.clone();
    let decimals = bal.token.decimals() as u32;

    if decimals == 0 {
        let int_str = raw.to_string().replace('_', "");
        return int_str;
    }

    // clamp to max 6 displayed decimals
    let display_decimals = decimals.min(6);
    let scale = 10u128.pow(decimals - display_decimals);
    let scaled = raw / scale;

    let int_part = scaled.clone() / 10u128.pow(display_decimals);
    let frac_part = scaled % 10u128.pow(display_decimals);

    let int_str = int_part.to_string().replace('_', "");
    let frac_str = format!(
        "{:0>width$}",
        frac_part.to_string().replace('_', ""),
        width = display_decimals as usize
    );

    format!("{}.{}", int_str, frac_str)
}

pub(super) fn format_i128_amount(amount: i128, decimals: Option<u8>) -> String {
    let Some(decimals) = decimals else {
        return amount.to_string();
    };

    let neg = amount.is_negative();
    let abs: u128 = amount.unsigned_abs();

    if decimals == 0 {
        return if neg { format!("-{}", abs) } else { abs.to_string() };
    }

    let display_decimals = (decimals as usize).min(6);
    let pow_full = 10u128.pow(decimals as u32);
    let int_part = abs / pow_full;
    let frac_part = abs % pow_full;

    let frac_str_full = format!("{:0>width$}", frac_part, width = decimals as usize);
    let frac_str = &frac_str_full[..display_decimals];

    if neg {
        format!("-{}.{}", int_part, frac_str)
    } else {
        format!("{}.{}", int_part, frac_str)
    }
}

pub(super) fn decimal_to_units(dec_str: &str, decimals: u8) -> Option<u128> {
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
    use super::decimal_to_units;

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
}
