use candid::{CandidType, Principal};
use ic_agent::{Identity, identity::Secp256k1Identity};
use liquidium_pipeline_connectors::account::icp_account::derive_icp_identity_at_path;
use serde::{Deserialize, Serialize};

/// Version of the persisted ICPSwap child-identity derivation contract.
pub const ICPSWAP_DERIVATION_VERSION: u32 = 1;

const ICPSWAP_DERIVATION_PREFIX: &str = "m/44'/223'/1001'/1'";
const BIP32_HARDENED_INDEX_MASK: u128 = 0x7fff_ffff;

/// Identifies the deterministic signing identity assigned to one liquidation.
/// The descriptor is safe to persist: it never contains a mnemonic or key.
#[derive(CandidType, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcpswapExecutionIdentity {
    pub scheme: IcpswapDerivationScheme,
    pub liquidation_id: String,
    pub derivation_path: String,
    pub principal: Principal,
}

/// Versioned derivation algorithms understood by this binary.
#[derive(CandidType, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IcpswapDerivationScheme {
    Bip32Secp256k1V1,
}

impl IcpswapExecutionIdentity {
    /// Derives the public descriptor and signing identity for a canonical
    /// decimal `u128` liquidation ID.
    pub fn derive(mnemonic: &str, liquidation_id: &str) -> Result<(Self, Secp256k1Identity), String> {
        let liquidation_id_value = parse_liquidation_id(liquidation_id)?;
        let derivation_path = derivation_path(liquidation_id_value);
        let identity = derive_icp_identity_at_path(mnemonic, &derivation_path)?;
        let principal = identity
            .sender()
            .map_err(|error| format!("failed to derive ICPSwap execution principal: {error}"))?;
        Ok((
            Self {
                scheme: IcpswapDerivationScheme::Bip32Secp256k1V1,
                liquidation_id: liquidation_id.to_string(),
                derivation_path,
                principal,
            },
            identity,
        ))
    }

    /// Re-derives all persisted identity fields before a caller uses the child
    /// principal for an external action. Any mismatch parks the execution.
    pub fn validate_and_derive(&self, mnemonic: &str) -> Result<Secp256k1Identity, String> {
        self.validate_descriptor()?;
        let (expected, identity) = Self::derive(mnemonic, &self.liquidation_id)?;
        if self.principal != expected.principal {
            return Err("persisted ICPSwap execution principal does not match the configured mnemonic".to_string());
        }
        Ok(identity)
    }

    /// Validates descriptor fields that do not require access to the mnemonic.
    pub fn validate_descriptor(&self) -> Result<(), String> {
        if self.scheme != IcpswapDerivationScheme::Bip32Secp256k1V1 {
            return Err("unsupported ICPSwap execution derivation scheme".to_string());
        }
        let liquidation_id = parse_liquidation_id(&self.liquidation_id)?;
        if self.derivation_path != derivation_path(liquidation_id) {
            return Err("persisted ICPSwap execution derivation path does not match the liquidation ID".to_string());
        }
        Ok(())
    }
}

/// Builds the hardened ICPSwap namespace path. Five base-2^31 limbs preserve
/// every bit of the liquidation's `u128` ID without exceeding BIP-32 indices.
pub fn derivation_path(liquidation_id: u128) -> String {
    let [l4, l3, l2, l1, l0]: [u32; 5] = std::array::from_fn(|index| {
        let shift = 31 * (4 - index);
        ((liquidation_id >> shift) & BIP32_HARDENED_INDEX_MASK) as u32
    });

    format!("{ICPSWAP_DERIVATION_PREFIX}/{l4}'/{l3}'/{l2}'/{l1}'/{l0}'")
}

fn parse_liquidation_id(liquidation_id: &str) -> Result<u128, String> {
    let value = liquidation_id
        .parse::<u128>()
        .map_err(|error| format!("invalid liquidation ID `{liquidation_id}`: {error}"))?;
    if value.to_string() != liquidation_id {
        return Err(format!(
            "liquidation ID `{liquidation_id}` is not canonical unsigned decimal"
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use candid::Principal;

    use super::{IcpswapExecutionIdentity, derivation_path};

    const TEST_MNEMONIC: &str =
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

    #[test]
    fn derivation_path_encodes_zero_and_complete_u128() {
        assert_eq!(derivation_path(0), "m/44'/223'/1001'/1'/0'/0'/0'/0'/0'");
        assert_eq!(
            derivation_path(u128::MAX),
            "m/44'/223'/1001'/1'/15'/2147483647'/2147483647'/2147483647'/2147483647'"
        );
    }

    #[test]
    fn derivation_is_stable_and_liquidation_scoped() {
        let (first, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1536").expect("derive identity");
        let (same, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1536").expect("derive identity");
        let (other, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1537").expect("derive identity");

        assert_eq!(first, same);
        assert_ne!(first.principal, other.principal);
        assert_eq!(first.derivation_path, "m/44'/223'/1001'/1'/0'/0'/0'/0'/1536'");
        assert_eq!(
            first.principal.to_text(),
            "jmrfu-64tin-2mc5y-nacom-tvhsm-dx5dq-hqh6m-uqf5k-nbpns-rq5qi-lqe"
        );
        assert_ne!(first.principal, Principal::anonymous());
    }

    #[test]
    fn validation_rejects_descriptor_mismatches() {
        let (descriptor, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1536").expect("derive identity");

        let mut wrong_path = descriptor.clone();
        wrong_path.derivation_path = derivation_path(1537);
        assert!(wrong_path.validate_and_derive(TEST_MNEMONIC).is_err());

        let mut wrong_id = descriptor.clone();
        wrong_id.liquidation_id = "1537".to_string();
        assert!(wrong_id.validate_and_derive(TEST_MNEMONIC).is_err());

        let mut wrong_principal = descriptor;
        wrong_principal.principal = Principal::anonymous();
        assert!(wrong_principal.validate_and_derive(TEST_MNEMONIC).is_err());
    }

    #[test]
    fn deserialization_rejects_an_unsupported_scheme() {
        let json = serde_json::json!({
            "scheme": "bip32_secp256k1_v2",
            "liquidation_id": "1536",
            "derivation_path": "m/44'/223'/1001'/1'/0'/0'/0'/0'/1536'",
            "principal": Principal::anonymous(),
        });

        assert!(serde_json::from_value::<IcpswapExecutionIdentity>(json).is_err());
    }

    #[test]
    fn derivation_rejects_noncanonical_or_out_of_range_ids() {
        assert!(IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "01536").is_err());
        assert!(IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "-1").is_err());
        assert!(IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "340282366920938463463374607431768211456").is_err());
    }
}
