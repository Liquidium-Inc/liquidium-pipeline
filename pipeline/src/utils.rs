use std::time::{SystemTime, UNIX_EPOCH};

use candid::{Nat, Principal};

pub fn max_for_ledger(token: &Principal) -> Nat {
    if *token == Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").expect("invalid ICP ledger principal") {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text("cngnf-vqaaa-aaaar-qag4q-cai").expect("invalid ckUSDT ledger principal") {
        return Nat::from(340_282_366_920_938_463_463_374_607_431_768_211_455u128);
    }

    if *token == Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").expect("invalid ckBTC ledger principal") {
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
