use candid::{Nat, Principal};
use ic_agent::{
    Identity,
    identity::{BasicIdentity, Secp256k1Identity},
};

pub fn create_identity_from_pem_file(pem_file: &str) -> Result<Box<dyn Identity>, String> {
    match BasicIdentity::from_pem_file(pem_file) {
        Ok(basic_identity) => Ok(Box::new(basic_identity)),
        Err(_) => match Secp256k1Identity::from_pem_file(pem_file) {
            Ok(secp256k1_identity) => Ok(Box::new(secp256k1_identity)),
            Err(err) => Err(format!(
                "Failed to create identity from pem file at {}. Unknown identity format. {}",
                pem_file, err
            )),
        },
    }
}

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
