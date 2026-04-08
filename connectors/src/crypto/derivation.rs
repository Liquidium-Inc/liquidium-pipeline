use bip32::{DerivationPath, XPrv};
use bip39::{Language, Mnemonic};
use k256::SecretKey;
use ripemd::{Digest as _, Ripemd160};
use sha2::Sha256;
use std::str::FromStr;

fn derive_private_key_with_path(mnemonic: &str, path_str: &str) -> Result<SecretKey, String> {
    let mnemonic = Mnemonic::parse_in(Language::English, mnemonic).map_err(|e| format!("invalid mnemonic: {e}"))?;
    let seed = mnemonic.to_seed("");
    let master = XPrv::new(seed).map_err(|e| format!("xprv error: {e}"))?;
    let derivation = DerivationPath::from_str(path_str).map_err(|e| format!("path error: {e}"))?;

    let mut node = master;
    for cn in derivation.into_iter() {
        node = node.derive_child(cn).map_err(|e| format!("derive_child error: {e}"))?;
    }

    let raw = node.private_key().to_bytes();
    SecretKey::from_bytes(&raw).map_err(|e| format!("secret key error: {e}"))
}

// Derive a raw Ethereum private key from a mnemonic and BIP32 path.
pub fn derive_evm_private_key(mnemonic: &str, account: u32, index: u32) -> Result<SecretKey, String> {
    let path_str = format!("m/44'/60'/{}'/0/{}", account, index);
    derive_private_key_with_path(mnemonic, &path_str)
}

// Derive a raw Bitcoin private key from a mnemonic and BIP84 path.
pub fn derive_btc_private_key(mnemonic: &str, account: u32, index: u32) -> Result<SecretKey, String> {
    let path_str = format!("m/84'/0'/{}'/0/{}", account, index);
    derive_private_key_with_path(mnemonic, &path_str)
}

// Derive a deterministic Bitcoin mainnet P2PKH address from mnemonic.
//
// This is used for bridge account wiring and operator visibility only in this phase.
pub fn derive_btc_p2pkh_address(mnemonic: &str, account: u32, index: u32) -> Result<String, String> {
    let private_key = derive_btc_private_key(mnemonic, account, index)?;
    let public_key = private_key.public_key();
    let pubkey_bytes = public_key.to_sec1_bytes();

    let sha = Sha256::digest(pubkey_bytes);
    let ripe = Ripemd160::digest(sha);

    // P2PKH payload: version(0x00) + HASH160(pubkey) + checksum.
    let mut payload = Vec::with_capacity(25);
    payload.push(0x00);
    payload.extend_from_slice(ripe.as_slice());
    let checksum_full = Sha256::digest(Sha256::digest(&payload));
    payload.extend_from_slice(&checksum_full[..4]);

    Ok(bs58::encode(payload).into_string())
}

#[cfg(test)]
mod tests {
    use super::{derive_btc_p2pkh_address, derive_btc_private_key, derive_evm_private_key};

    const TEST_MNEMONIC: &str =
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

    #[test]
    fn btc_derivation_changes_with_index() {
        let k0 = derive_btc_private_key(TEST_MNEMONIC, 1, 0).expect("key");
        let k1 = derive_btc_private_key(TEST_MNEMONIC, 1, 1).expect("key");
        assert_ne!(k0.to_bytes(), k1.to_bytes());
    }

    #[test]
    fn btc_and_evm_namespace_do_not_collide() {
        let evm = derive_evm_private_key(TEST_MNEMONIC, 1, 0).expect("key");
        let btc = derive_btc_private_key(TEST_MNEMONIC, 1, 0).expect("key");
        assert_ne!(evm.to_bytes(), btc.to_bytes());
    }

    #[test]
    fn btc_address_derivation_is_stable_and_valid_shape() {
        let a = derive_btc_p2pkh_address(TEST_MNEMONIC, 1, 0).expect("address");
        let b = derive_btc_p2pkh_address(TEST_MNEMONIC, 1, 0).expect("address");
        assert_eq!(a, b);
        assert!(a.starts_with('1'));
        assert!(a.len() >= 26 && a.len() <= 35);
    }
}
