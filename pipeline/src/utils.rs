use std::time::{SystemTime, UNIX_EPOCH};

use candid::{Nat, Principal};

pub const ICP_LEDGER_PRINCIPAL: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";
pub const CKUSDT_LEDGER_PRINCIPAL: &str = "cngnf-vqaaa-aaaar-qag4q-cai";
pub const CKUSDC_LEDGER_PRINCIPAL: &str = "xevnm-gaaaa-aaaar-qafnq-cai";
pub const CKETH_LEDGER_PRINCIPAL: &str = "ss2fx-dyaaa-aaaar-qacoq-cai";

/// The allowance the bot grants the lending canister on a debt ledger. Any
/// ledger outside the wide ones gets `u64::MAX`, which every ledger accepts.
pub fn max_for_ledger(token: &Principal) -> Nat {
    let wide_ledgers = [CKUSDT_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL];
    if wide_ledgers
        .iter()
        .any(|ledger| Principal::from_text(ledger).ok().as_ref() == Some(token))
    {
        return Nat::from(u128::MAX);
    }
    // Zero would read as "already approved" and block funding for a new debt
    // token (audit L20). ICP, ckBTC, ckSOL and custom ledgers all land here.
    Nat::from(u64::MAX)
}

/// An allowance below this gets a new approval of `max_for_ledger`.
pub fn approval_threshold(token: &Principal) -> Nat {
    max_for_ledger(token) / Nat::from(2u8)
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
    fn every_debt_ledger_gets_a_nonzero_allowance() {
        let cases = [
            (CKUSDT_LEDGER_PRINCIPAL, Nat::from(u128::MAX)),
            (CKETH_LEDGER_PRINCIPAL, Nat::from(u128::MAX)),
            (ICP_LEDGER_PRINCIPAL, Nat::from(u64::MAX)),
            // The default ckSOL ledger and a custom ledger.
            ("la34w-haaaa-aaaar-qb5na-cai", Nat::from(u64::MAX)),
            ("aaaaa-aa", Nat::from(u64::MAX)),
        ];
        for (ledger, max) in cases {
            let ledger = Principal::from_text(ledger).unwrap();
            assert_eq!(max_for_ledger(&ledger), max, "{ledger}");
            assert!(
                approval_threshold(&ledger) > 0u8,
                "a zero allowance must need approval: {ledger}"
            );
        }
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
