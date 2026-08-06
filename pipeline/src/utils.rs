use std::time::{SystemTime, UNIX_EPOCH};

use candid::{Nat, Principal};

pub const ICP_LEDGER_PRINCIPAL: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";
pub const CKUSDT_LEDGER_PRINCIPAL: &str = "cngnf-vqaaa-aaaar-qag4q-cai";
pub const CKUSDC_LEDGER_PRINCIPAL: &str = "xevnm-gaaaa-aaaar-qafnq-cai";
pub const CKBTC_LEDGER_PRINCIPAL: &str = "mxzaz-hqaaa-aaaar-qaada-cai";
pub const CKETH_LEDGER_PRINCIPAL: &str = "ss2fx-dyaaa-aaaar-qacoq-cai";

pub const CKUSDT_MAX_ALLOWANCE: u128 = 340_282_366_920_938_463_463_374_607_431_768_211_455;

pub fn max_for_ledger(token: &Principal) -> Nat {
    if *token == Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("invalid ICP ledger principal") {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text(CKUSDT_LEDGER_PRINCIPAL).expect("invalid ckUSDT ledger principal") {
        return Nat::from(CKUSDT_MAX_ALLOWANCE);
    }

    if *token == Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).expect("invalid ckUSDC ledger principal") {
        return Nat::from(CKUSDT_MAX_ALLOWANCE);
    }

    if *token == Principal::from_text(CKBTC_LEDGER_PRINCIPAL).expect("invalid ckBTC ledger principal") {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text(CKETH_LEDGER_PRINCIPAL).expect("invalid ckETH ledger principal") {
        return Nat::from(CKUSDT_MAX_ALLOWANCE);
    }

    Nat::from(0u8)
}

pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Creates a restart-safe idempotency key for a venue execution prepared
/// before the parent multi-venue plan has assigned its persisted leg ID.
pub fn new_venue_execution_id(venue_id: &str) -> String {
    format!("{venue_id}-{}", uuid::Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cketh_has_nonzero_max_allowance() {
        let ledger = Principal::from_text(CKETH_LEDGER_PRINCIPAL).expect("valid ckETH ledger principal");
        assert!(max_for_ledger(&ledger) > Nat::from(0u8));
    }

    #[test]
    fn venue_execution_ids_are_unique_and_keep_the_venue_prefix() {
        let first = new_venue_execution_id("mexc");
        let second = new_venue_execution_id("mexc");

        assert!(first.starts_with("mexc-"));
        assert_ne!(first, second);
        uuid::Uuid::parse_str(first.trim_start_matches("mexc-")).expect("execution ID should contain a UUID");
    }
}
