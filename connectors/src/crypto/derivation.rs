use bip32::{DerivationPath, XPrv};
use bip39::{Language, Mnemonic};
use k256::SecretKey;
use std::str::FromStr;

// Derive a raw Ethereum private key from a mnemonic and BIP32 path.
pub fn derive_evm_private_key(mnemonic: &str, account: u32, index: u32) -> Result<SecretKey, String> {
    // Parse mnemonic
    let mnemonic = Mnemonic::parse_in(Language::English, mnemonic).map_err(|e| format!("invalid mnemonic: {e}"))?;

    // Seed from mnemonic (no passphrase)
    let seed = mnemonic.to_seed("");

    // Master extended private key
    let master = XPrv::new(seed).map_err(|e| format!("xprv error: {e}"))?;

    // Ethereum path: m/44'/60'/account'/0/index
    let path_str = format!("m/44'/60'/{}'/0/{}", account, index);
    let derivation = DerivationPath::from_str(&path_str).map_err(|e| format!("path error: {e}"))?;

    // Walk the path
    let mut node = master;
    for cn in derivation.into_iter() {
        node = node.derive_child(cn).map_err(|e| format!("derive_child error: {e}"))?;
    }

    // Final leaf private key
    let raw = node.private_key().to_bytes();
    SecretKey::from_bytes(&raw).map_err(|e| format!("secret key error: {e}"))
}
