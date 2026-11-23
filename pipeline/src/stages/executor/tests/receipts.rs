use super::*;

/// The finalizer reads the pool again while the change is pending.
/// For a share payout, the swap must still use the amount that the withdraw paid.
#[test]
fn a_pool_view_does_not_undo_the_withdrawn_amount_of_a_share_payout() {
    let token = ChainToken::Icp {
        ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    };
    let mut seized = make_liquidation_result(7);
    seized.amounts.collateral_received = Nat::from(100_000u64);
    seized.collateral_share_receiver = Some(p("2vxsx-fae"));
    seized.change_tx.status = TransferStatus::Pending;

    let mut arrived = seized.clone();
    arrived.amounts.collateral_received = Nat::from(99_980u64);
    let mut request = make_request(p("2vxsx-fae"), token);
    request.liquidation.collateral_share_receiver = seized.collateral_share_receiver;
    let mut receipt = ExecutionReceipt {
        request,
        liquidation_result: Some(arrived),
        status: ExecutionStatus::Success,
        change_received: false,
    };

    let mut fresh = seized.clone();
    fresh.change_tx.status = TransferStatus::Success;
    assert!(receipt.apply_pool_view(fresh), "the change landed");

    let liquidation = receipt.liquidation_result.unwrap();
    assert_eq!(liquidation.amounts.collateral_received, Nat::from(99_980u64));
    assert_eq!(liquidation.change_tx.status, TransferStatus::Success);
    assert!(receipt.change_received);
}

/// A token payout keeps the old rule: the pool's view replaces the saved result.
#[test]
fn a_pool_view_replaces_the_result_of_a_token_payout() {
    let token = ChainToken::Icp {
        ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    };
    let mut receipt = ExecutionReceipt {
        request: make_request(p("2vxsx-fae"), token),
        liquidation_result: Some(make_liquidation_result(7)),
        status: ExecutionStatus::Success,
        change_received: true,
    };
    let mut fresh = make_liquidation_result(7);
    fresh.amounts.collateral_received = Nat::from(5u8);
    receipt.apply_pool_view(fresh);
    assert_eq!(
        receipt.liquidation_result.unwrap().amounts.collateral_received,
        Nat::from(5u8)
    );
}

#[test]
fn share_receipts_need_no_collateral_token_transfer() {
    let mut request = icp_request();
    request.liquidation.collateral_share_receiver = Some(Principal::management_canister());
    let mut result = make_liquidation_result(42);
    result.collateral_tx.status = TransferStatus::Pending;
    result.collateral_share_receiver = request.liquidation.collateral_share_receiver;
    result.amounts.collateral_received = Nat::from(100u8);
    assert!(matches!(
        ExecutionReceipt::from_result(request.clone(), result.clone()).status,
        ExecutionStatus::Success
    ));
    result.collateral_share_receiver = None;
    result.amounts.collateral_received = Nat::from(0u8);
    assert!(matches!(
        ExecutionReceipt::from_result(request, result).status,
        ExecutionStatus::Success
    ));
}
#[test]
fn mismatched_payouts_keep_the_result_and_a_typed_error() {
    let profile = Principal::self_authenticating(b"profile");
    let other = Principal::self_authenticating(b"other profile");
    let cases = [
        (None, Some(profile), CollateralPayoutError::SharesNotRequested),
        (Some(profile), None, CollateralPayoutError::MissingShareProfile),
        (
            Some(profile),
            Some(other),
            CollateralPayoutError::WrongProfile {
                requested: profile,
                credited: other,
            },
        ),
    ];
    for (requested, credited, expected_error) in cases {
        let mut request = icp_request();
        request.liquidation.collateral_share_receiver = requested;
        let mut result = make_liquidation_result(42);
        result.collateral_share_receiver = credited;
        result.amounts.collateral_received = Nat::from(100u8);
        let receipt = ExecutionReceipt::from_result(request, result.clone());
        assert!(
            matches!(&receipt.status, ExecutionStatus::CollateralPayoutMismatch(error) if error == &expected_error)
        );
        assert_eq!(receipt.liquidation_result, Some(result));
        let json = serde_json::to_string(&receipt).unwrap();
        let restored: ExecutionReceipt = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored.status, ExecutionStatus::CollateralPayoutMismatch(error) if error == expected_error));
    }
}

#[test]
fn underlying_receipts_wait_for_the_token_transfer() {
    let mut result = make_liquidation_result(42);
    result.collateral_tx.status = TransferStatus::Pending;
    let mut receipt = ExecutionReceipt::from_result(icp_request(), result.clone());
    assert!(matches!(receipt.status, ExecutionStatus::CollateralTransferFailed(_)));
    result.collateral_tx.status = TransferStatus::Success;
    assert!(receipt.apply_pool_view(result));
    assert!(matches!(receipt.status, ExecutionStatus::Success));
}

#[test]
fn share_pool_views_do_not_wait_for_a_token_transfer() {
    let mut request = icp_request();
    request.liquidation.collateral_share_receiver = Some(Principal::management_canister());
    let mut result = make_liquidation_result(42);
    result.collateral_share_receiver = request.liquidation.collateral_share_receiver;
    result.amounts.collateral_received = Nat::from(100u8);
    result.collateral_tx.status = TransferStatus::Pending;
    let mut receipt = ExecutionReceipt::from_result(request, result.clone());
    receipt.status = ExecutionStatus::CollateralTransferFailed("previous view".to_string());
    assert!(receipt.apply_pool_view(result));
    assert!(matches!(receipt.status, ExecutionStatus::Success));
}
