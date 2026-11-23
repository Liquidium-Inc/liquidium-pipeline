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
    let mut receipt = ExecutionReceipt {
        request: make_request(p("2vxsx-fae"), token),
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
