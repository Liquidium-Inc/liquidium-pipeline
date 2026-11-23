//! Classifies collateral payouts using the requested profile and the canister result.
use candid::Principal;
use liquidium_pipeline_core::types::protocol_types::LiquidationResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollateralPayout {
    Underlying,
    Shares(Principal),
    NoCollateral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum CollateralPayoutError {
    #[error("collateral shares were credited without a share request")]
    SharesNotRequested,
    #[error("collateral shares were credited to {credited}, but the request named {requested}")]
    WrongProfile { requested: Principal, credited: Principal },
    #[error("nonzero collateral has no credited profile for a share request")]
    MissingShareProfile,
}

/// Returns the payout mode, rejecting results that do not match the requested profile.
pub fn classify_collateral_payout(
    requested: Option<Principal>,
    result: &LiquidationResult,
) -> Result<CollateralPayout, CollateralPayoutError> {
    match (requested, result.collateral_share_receiver) {
        (None, None) => Ok(CollateralPayout::Underlying),
        (None, Some(_)) => Err(CollateralPayoutError::SharesNotRequested),
        (Some(requested), Some(credited)) if requested != credited => {
            Err(CollateralPayoutError::WrongProfile { requested, credited })
        }
        (Some(_), Some(credited)) => Ok(CollateralPayout::Shares(credited)),
        (Some(_), None) if result.amounts.collateral_received == 0u8 => Ok(CollateralPayout::NoCollateral),
        (Some(_), None) => Err(CollateralPayoutError::MissingShareProfile),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Nat;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationStatus, TransferStatus, TxStatus,
    };

    fn result(profile: Option<Principal>, amount: u64) -> LiquidationResult {
        LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(amount),
                debt_repaid: Nat::from(100u8),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 0,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            id: 1,
            collateral_share_receiver: profile,
        }
    }

    #[test]
    fn classifies_payouts_and_rejects_profile_mismatches() {
        let profile = Principal::self_authenticating(b"share profile");
        let other = Principal::self_authenticating(b"other profile");
        let cases = [
            (None, None, 100, Ok(CollateralPayout::Underlying)),
            (None, None, 0, Ok(CollateralPayout::Underlying)),
            (Some(profile), Some(profile), 100, Ok(CollateralPayout::Shares(profile))),
            (Some(profile), None, 0, Ok(CollateralPayout::NoCollateral)),
            (
                Some(profile),
                None,
                100,
                Err(CollateralPayoutError::MissingShareProfile),
            ),
            (None, Some(profile), 100, Err(CollateralPayoutError::SharesNotRequested)),
            (
                Some(profile),
                Some(other),
                100,
                Err(CollateralPayoutError::WrongProfile {
                    requested: profile,
                    credited: other,
                }),
            ),
        ];
        for (requested, credited, amount, expected) in cases {
            assert_eq!(
                classify_collateral_payout(requested, &result(credited, amount)),
                expected
            );
        }
    }
}
