use std::time::{SystemTime, UNIX_EPOCH};

use candid::{Nat, Principal};

pub const ICP_LEDGER_PRINCIPAL: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";
pub const CKUSDT_LEDGER_PRINCIPAL: &str = "cngnf-vqaaa-aaaar-qag4q-cai";
pub const CKBTC_LEDGER_PRINCIPAL: &str = "mxzaz-hqaaa-aaaar-qaada-cai";

pub const CKUSDT_MAX_ALLOWANCE: u128 = 340_282_366_920_938_463_463_374_607_431_768_211_455;

pub fn max_for_ledger(token: &Principal) -> Nat {
    if *token == Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("invalid ICP ledger principal") {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text(CKUSDT_LEDGER_PRINCIPAL).expect("invalid ckUSDT ledger principal") {
        return Nat::from(CKUSDT_MAX_ALLOWANCE);
    }

    if *token == Principal::from_text(CKBTC_LEDGER_PRINCIPAL).expect("invalid ckBTC ledger principal") {
        return Nat::from(u64::MAX);
    }

    Nat::from(0u8)
}

pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
