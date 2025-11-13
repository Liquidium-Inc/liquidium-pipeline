use std::str::FromStr;

use bip32::{DerivationPath, XPrv};
use candid::{Nat, Principal};

use ic_agent::{
    Identity,
    identity::{BasicIdentity, Secp256k1Identity},
};

pub fn max_for_ledger(token: &Principal) -> Nat {
    if *token == Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap() {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text("cngnf-vqaaa-aaaar-qag4q-cai").unwrap() {
        return Nat::from(340_282_366_920_938_463_463_374_607_431_768_211_455u128);
    }

    if *token == Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap() {
        return Nat::from(u64::MAX);
    }

    Nat::from(0u8)
}
