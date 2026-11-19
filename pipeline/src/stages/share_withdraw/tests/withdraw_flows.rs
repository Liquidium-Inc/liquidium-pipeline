//! Flow tests of the withdraw step on a real SQLite store.
use super::*;

#[tokio::test]
async fn a_held_row_is_withdrawn_and_enqueued_with_the_arrived_amount() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;

    // The only held row of the pool takes the position as it is, which includes 50 of interest.
    let mut positions = liquid_pool(1_000_000, SEIZED + 50);
    accept(&mut positions, SEIZED + 50, SEIZED + 50, "withdraw_1");
    fixture.tick(positions).await;

    let (row, _) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::HeldAsShares);
    assert!(matches!(
        fixture.holding("7").await.open,
        Some(OpenWithdraw::Accepted { ref outflow_id, after_index: 500, .. }) if outflow_id == "withdraw_1"
    ));

    // The payout is not on the ledger yet. The row stays held, and no new withdraw starts.
    fixture.tick(payout(None)).await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);

    // The pool paid the withdraw minus two ledger fees.
    let mut arrival = MockSharePositions::new();
    arrival.expect_arrived().times(1).returning(|query| {
        assert_eq!(query.outflow_id, "withdraw_1");
        assert_eq!(query.pool, pool());
        assert_eq!(query.destination, trader());
        assert_eq!(query.after_index, 500);
        assert_eq!(query.min_amount, Nat::from(SEIZED + 50 - 30));
        assert_eq!(query.max_amount, Nat::from(SEIZED + 50));
        Ok(Some(Nat::from(SEIZED + 30)))
    });
    fixture.tick(arrival).await;

    let (row, wrapper) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::Enqueued);
    let liquidation = wrapper.receipt.liquidation_result.unwrap();
    assert_eq!(liquidation.amounts.collateral_received, Nat::from(SEIZED + 30));
    let holding = wrapper.share_holding.unwrap();
    assert_eq!(holding.seized, Nat::from(SEIZED));
    assert_eq!(holding.arrived, Nat::from(SEIZED + 30));
    assert_eq!(holding.interest(), Nat::from(50u8));
    assert!(holding.released_at.is_some());
    assert_eq!(holding.open, None);
}

#[tokio::test]
async fn a_pool_without_liquidity_sends_no_withdraw() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = MockSharePositions::new();
    positions.expect_available_liquidity().returning(|_| Ok(Nat::from(0u8)));
    positions
        .expect_position()
        .returning(|_, _| Ok(position(SEIZED, SHARES)));
    positions.expect_withdraw().times(0);
    fixture.tick(positions).await;

    let holding = fixture.holding("7").await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);
    assert_eq!(holding.open, None);
    assert!(holding.next_try_at > now_ts());
    assert_eq!(holding.last_note.as_deref(), Some("the pool has no free liquidity"));

    // The wait is not over. Thus the next tick does not read the pool.
    fixture.tick(MockSharePositions::new()).await;
}

/// The canister traps when the liquidity is gone between the read and the call.
#[tokio::test]
async fn a_rejected_withdraw_is_tried_again_later() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;

    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::NotExecuted("canister trapped".to_string())));
    fixture.tick(positions).await;

    let holding = fixture.holding("7").await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);
    assert_eq!(holding.open, None);
    assert_eq!(holding.withdrawn, Nat::from(0u8));
    assert!(holding.next_try_at > now_ts());

    fixture.make_due("7").await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    accept(&mut positions, SEIZED, SEIZED, "withdraw_2");
    fixture.tick(positions).await;
    assert_eq!(fixture.holding("7").await.withdrawn, Nat::from(SEIZED));
}

#[tokio::test]
async fn a_refusal_that_can_pass_waits_and_another_one_goes_to_an_operator() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::Refused(ProtocolError::PoolFrozen)));
    fixture.tick(positions).await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);
    assert_eq!(fixture.holding("7").await.open, None);

    fixture.make_due("7").await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::Refused(ProtocolError::InvalidTargetPrincipal)));
    fixture.tick(positions).await;
    let (row, _) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::OperatorRequired);
    assert!(row.last_error.unwrap().contains("InvalidTargetPrincipal"));
}

#[tokio::test]
async fn liquidity_for_a_part_withdraws_the_part_and_the_rest_later() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;

    let mut positions = liquid_pool(40_000, SEIZED);
    accept(&mut positions, 40_000, 40_000, "withdraw_1");
    fixture.tick(positions).await;
    fixture.tick(payout(Some(39_980))).await;

    let holding = fixture.holding("7").await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);
    assert_eq!(holding.remaining(), Nat::from(60_000u64));
    assert_eq!(holding.arrived, Nat::from(39_980u64));

    // The rest is the whole position now, so the last withdraw asks for all of it, interest included.
    let mut positions = liquid_pool(1_000_000, 60_010);
    accept(&mut positions, 60_010, 60_010, "withdraw_2");
    fixture.tick(positions).await;
    fixture.tick(payout(Some(59_990))).await;

    let (row, wrapper) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::Enqueued);
    assert_eq!(
        wrapper.receipt.liquidation_result.unwrap().amounts.collateral_received,
        Nat::from(99_970u64)
    );
    assert_eq!(wrapper.share_holding.unwrap().interest(), Nat::from(10u8));
}

#[tokio::test]
async fn an_unknown_outcome_is_retried_only_with_proof_that_the_call_did_not_run() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::Ambiguous("timeout".to_string())));
    fixture.tick(positions).await;
    assert!(matches!(
        fixture.holding("7").await.open,
        Some(OpenWithdraw::Submitting { .. })
    ));

    // The share count did not change. Thus the call did not run, and the row is free again.
    let mut unchanged = MockSharePositions::new();
    unchanged
        .expect_position()
        .returning(|_, _| Ok(position(SEIZED + 1, SHARES)));
    unchanged.expect_withdraw().times(0);
    fixture.tick(unchanged).await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::HeldAsShares);
    assert_eq!(fixture.holding("7").await.open, None);
}

#[tokio::test]
async fn an_unknown_outcome_with_a_smaller_position_goes_to_an_operator() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::Ambiguous("timeout".to_string())));
    fixture.tick(positions).await;

    let mut emptied = MockSharePositions::new();
    emptied.expect_position().returning(|_, _| Ok(position(0, 0)));
    emptied.expect_withdraw().times(0);
    fixture.tick(emptied).await;
    let (row, _) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::OperatorRequired);
    assert!(row.last_error.unwrap().contains("not known"));
}

/// A small withdraw ran, and interest then made the value larger than before. Only the share count shows the withdraw.
#[tokio::test]
async fn interest_does_not_hide_a_withdraw_that_ran() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions
        .expect_withdraw()
        .times(1)
        .returning(|_, _, _, _| Err(WithdrawFailure::Ambiguous("timeout".to_string())));
    fixture.tick(positions).await;

    let mut grown = MockSharePositions::new();
    grown
        .expect_position()
        .returning(|_, _| Ok(position(SEIZED + 5, SHARES - 1)));
    grown.expect_withdraw().times(0);
    fixture.tick(grown).await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::OperatorRequired);
}

/// The payout of a parked row can still arrive. Until then, no other row of the pool starts a withdraw.
#[tokio::test]
async fn a_parked_row_with_an_open_withdraw_keeps_its_pool_busy() {
    let mut parked = held_record("7", SEIZED, 100, true);
    let mut wrapper = decode_receipt_wrapper(&parked).unwrap().unwrap();
    let holding = wrapper.share_holding.as_mut().unwrap();
    holding.withdrawn = Nat::from(SEIZED);
    holding.open = Some(OpenWithdraw::Accepted {
        outflow_id: "withdraw_1".to_string(),
        amount: Nat::from(SEIZED),
        after_index: 500,
        at: 1,
    });
    encode_meta(&mut parked, &wrapper).unwrap();
    parked.status = ResultStatus::OperatorRequired;
    let fixture = Fixture::with_rows(vec![parked, held_record("8", 30_000, 200, true)]).await;

    let mut positions = MockSharePositions::new();
    positions.expect_available_liquidity().times(0);
    positions.expect_withdraw().times(0);
    fixture.tick(positions).await;
    assert_eq!(fixture.holding("8").await.open, None);
}

#[tokio::test]
async fn two_rows_of_one_pool_withdraw_one_after_the_other() {
    let fixture = Fixture::with_rows(vec![
        held_record("7", SEIZED, 100, true),
        held_record("8", 30_000, 200, true),
    ])
    .await;

    // The older row goes first, with its own amount. The position also holds the shares of the other row.
    let mut positions = liquid_pool(1_000_000, SEIZED + 30_000);
    accept(&mut positions, SEIZED, SEIZED, "withdraw_1");
    fixture.tick(positions).await;
    assert!(fixture.holding("7").await.open.is_some());
    assert_eq!(fixture.holding("8").await.open, None);

    fixture.tick(payout(Some(SEIZED - 20))).await;
    assert_eq!(fixture.row("7").await.0.status, ResultStatus::Enqueued);

    let mut positions = liquid_pool(1_000_000, 30_000);
    accept(&mut positions, 30_000, 30_000, "withdraw_2");
    fixture.tick(positions).await;
    assert!(fixture.holding("8").await.open.is_some());
}

/// A parked row of the same pool keeps its shares. Thus the last held row must not take the whole position.
#[tokio::test]
async fn a_position_far_above_the_row_amount_is_not_withdrawn_as_a_whole() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let mut positions = liquid_pool(1_000_000, SEIZED * 2);
    accept(&mut positions, SEIZED, SEIZED, "withdraw_1");
    fixture.tick(positions).await;
    assert_eq!(fixture.holding("7").await.withdrawn, Nat::from(SEIZED));
}

#[tokio::test]
async fn a_row_without_a_swap_withdraws_to_the_main_account() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, false)]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut positions = liquid_pool(1_000_000, SEIZED);
    positions.expect_withdraw().returning(move |_, _, _, destination| {
        assert_eq!(destination, liquidator());
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(WithdrawAccepted {
            outflow_id: "withdraw_1".to_string(),
            amount: Nat::from(SEIZED),
        })
    });
    fixture.tick(positions).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_payout_that_never_shows_goes_to_an_operator() {
    let fixture = Fixture::with_rows(vec![held_record("7", SEIZED, 100, true)]).await;
    let (mut row, mut wrapper) = fixture.row("7").await;
    let holding = wrapper.share_holding.as_mut().unwrap();
    holding.withdrawn = Nat::from(SEIZED);
    holding.open = Some(OpenWithdraw::Accepted {
        outflow_id: "withdraw_1".to_string(),
        amount: Nat::from(SEIZED),
        after_index: 500,
        at: now_ts() - PAYOUT_PARK_SECS - 1,
    });
    encode_meta(&mut row, &wrapper).unwrap();
    fixture.wal.upsert_result(row).await.unwrap();

    fixture.tick(payout(None)).await;
    let (row, _) = fixture.row("7").await;
    assert_eq!(row.status, ResultStatus::OperatorRequired);
    assert!(row.last_error.unwrap().contains("withdraw_1"));
}
