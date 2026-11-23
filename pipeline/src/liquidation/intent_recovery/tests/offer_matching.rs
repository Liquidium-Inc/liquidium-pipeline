//! Table tests of the pure decisions of the recovery.
use super::super::offer_matching::*;
use super::*;
use crate::liquidation::collateral_payout_utils::CollateralPayoutError;

fn query() -> OfferQuery {
    OfferQuery {
        liquidator: liquidator(),
        debt_pool: debt_pool(),
        offer: Nat::from(OFFER),
        created_at: CREATED_AT,
    }
}

#[test]
fn a_memo_is_a_liquidation_id_only_with_16_bytes() {
    assert_eq!(liquidation_id_from_memo(&1833u128.to_be_bytes()), Some(1833));
    assert_eq!(liquidation_id_from_memo(b"withdraw_7"), None);
    assert_eq!(liquidation_id_from_memo(&[]), None);
    assert_eq!(liquidation_id_from_memo(&[0u8; 17]), None);
}

/// One row for each reason that a transfer is, or is not, the offer of the entry.
#[test]
fn offer_candidates_follow_each_condition() {
    let good = || offer_transfer(1833, OFFER, 1);
    let other_sender = {
        let mut transfer = good();
        transfer.from = LedgerParty::Account(Account {
            owner: Principal::anonymous(),
            subaccount: None,
        });
        transfer
    };
    let from_a_subaccount = {
        let mut transfer = good();
        transfer.from = LedgerParty::Account(Account {
            owner: liquidator(),
            subaccount: Some([7; 32]),
        });
        transfer
    };
    let other_pool = {
        let mut transfer = good();
        transfer.to = LedgerParty::Account(Account {
            owner: Principal::anonymous(),
            subaccount: None,
        });
        transfer
    };
    let text_memo = {
        let mut transfer = good();
        transfer.memo = Some(b"withdraw_7".to_vec());
        transfer
    };
    let no_memo = {
        let mut transfer = good();
        transfer.memo = None;
        transfer
    };
    let no_time = {
        let mut transfer = good();
        transfer.timestamp_nanos = None;
        transfer
    };
    let cases: Vec<(&str, LedgerTransfer, Vec<u128>)> = vec![
        ("the offer transfer", good(), vec![1833]),
        (
            "inside the clock tolerance",
            offer_transfer(1833, OFFER, -100),
            vec![1833],
        ),
        ("older than the entry", offer_transfer(1833, OFFER, -121), vec![]),
        ("one unit less", offer_transfer(1833, OFFER - 1, 1), vec![]),
        ("one unit more", offer_transfer(1833, OFFER + 1, 1), vec![]),
        ("another sender", other_sender, vec![]),
        ("a subaccount of the liquidator", from_a_subaccount, vec![]),
        ("another receiver", other_pool, vec![]),
        ("a memo that is not an id", text_memo, vec![]),
        ("no memo", no_memo, vec![]),
        ("no time", no_time, vec![]),
    ];
    for (name, transfer, expected) in cases {
        assert_eq!(offer_candidates(&query(), &[transfer]), expected, "{name}");
    }
}

#[test]
fn two_transfers_with_one_id_are_one_candidate() {
    let transfers = [offer_transfer(1833, OFFER, 1), offer_transfer(1833, OFFER, 1)];
    assert_eq!(offer_candidates(&query(), &transfers), vec![1833]);
}

#[test]
fn choose_offer_needs_exactly_one_open_candidate() {
    assert_eq!(choose_offer(&[]), OfferTransfer::NotFound);
    assert_eq!(choose_offer(&[1833]), OfferTransfer::Found(1833));
    assert_eq!(choose_offer(&[1833, 1834]), OfferTransfer::Ambiguous);
}

/// One row for each reason that a canister result fits, or does not fit, the saved request.
#[test]
fn confirms_follows_each_condition() {
    let profile = Some(p("g4oj7-diaaa-aaaaa-aaaaa-aaaaa-aaaaa-phai"));
    let other_debt = {
        let mut result = result(1833, None);
        result.debt_asset = AssetType::CkAsset(collateral_ledger());
        result
    };
    let other_collateral = {
        let mut result = result(1833, None);
        result.collateral_asset = AssetType::Unknown;
        result
    };
    let cases: Vec<(&str, bool, LiquidationResult, Result<(), Mismatch>)> = vec![
        ("token payout", false, result(1833, None), Ok(())),
        ("share payout that was requested", true, result(1833, profile), Ok(())),
        (
            "missing credited profile with collateral",
            true,
            result(1833, None),
            Err(Mismatch::CollateralPayout(CollateralPayoutError::MissingShareProfile)),
        ),
        ("another id", false, result(1834, None), Err(Mismatch::LiquidationId)),
        ("another debt asset", false, other_debt, Err(Mismatch::DebtAsset)),
        (
            "another collateral asset",
            false,
            other_collateral,
            Err(Mismatch::CollateralAsset),
        ),
        (
            "share payout that was not requested",
            false,
            result(1833, profile),
            Err(Mismatch::CollateralPayout(CollateralPayoutError::SharesNotRequested)),
        ),
    ];
    for (name, asked_for_shares, result, expected) in cases {
        assert_eq!(confirms(&request(asked_for_shares), 1833, &result), expected, "{name}");
    }
}

#[test]
fn share_recovery_accepts_zero_seizure_and_rejects_another_profile() {
    let mut empty = result(1833, None);
    empty.amounts.collateral_received = Nat::from(0u8);
    assert_eq!(confirms(&request(true), 1833, &empty), Ok(()));
    let wrong = result(1833, Some(Principal::management_canister()));
    assert!(matches!(
        confirms(&request(true), 1833, &wrong),
        Err(Mismatch::CollateralPayout(CollateralPayoutError::WrongProfile { .. }))
    ));
}
