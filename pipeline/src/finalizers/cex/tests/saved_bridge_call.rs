//! A bridge call, on either leg side, is saved before it runs, and a lost
//! answer is settled from the provider history before any new call.
use super::*;
use crate::bridge::call_sender::{BridgeCall, OpenBridgeCall, prepare_bridge_call};
use crate::bridge::claims::{BridgeClaims, MockBridgeClaimLookup};
use crate::bridge::legacy::{LegacyBridgeSubmitAttempt, LegacyBridgeSubmitPhase};
use crate::outbound::driver::{OperatorDecision, OutboundPhase, OutboundRecord};
use crate::utils::now_ts;
use liquidium_pipeline_connectors::backend::bridge_backend::BridgeSubmissionCandidate;

const DESTINATION: &str = "0x1111111111111111111111111111111111111111";

fn candidate(sequence: u64) -> BridgeSubmissionCandidate {
    BridgeSubmissionCandidate {
        bridge_id: format!("ic-withdraw:recovered:{sequence}"),
        sequence,
    }
}

fn zero_fee_bridge() -> MockBridgeBackend {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_fee_budget().returning(|_, _, _| {
        Ok(BridgeFeeBudget {
            source_fee_budget: 0.0,
            destination_fee_budget: 0.0,
            provider_fee_budget_native_units: None,
            ..BridgeFeeBudget::default()
        })
    });
    bridge
}

fn claims(open_calls: Vec<OpenBridgeCall>) -> MockBridgeClaimLookup {
    let mut claims = MockBridgeClaimLookup::new();
    claims.expect_claims_since().returning(move |_| {
        Ok(BridgeClaims {
            bridge_ids: vec!["ic-withdraw:1:2".into()],
            open_calls: open_calls.clone(),
            ..BridgeClaims::default()
        })
    });
    claims
}

fn finalizer(bridge: MockBridgeBackend, claims: Option<MockBridgeClaimLookup>) -> CexVenueFinalizer<MockCexBackend> {
    finalizer_with_cex(MockCexBackend::new(), bridge, claims)
}

fn finalizer_with_cex(
    cex: MockCexBackend,
    bridge: MockBridgeBackend,
    claims: Option<MockBridgeClaimLookup>,
) -> CexVenueFinalizer<MockCexBackend> {
    test_finalizer(Arc::new(cex), Arc::new(MockTransferActions::new())).with_bridge_runtime(match claims {
        Some(claims) => bridge_dependencies(Arc::new(bridge)).with_claims(Arc::new(claims)),
        None => bridge_dependencies(Arc::new(bridge)),
    })
}

/// A bridged ckUSDC deposit whose funding transfer landed.
async fn funded_state(finalizer: &CexVenueFinalizer<MockCexBackend>) -> CexState {
    let receipt = make_execution_receipt_with_assets(90, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("90", &receipt).await.expect("prepare");
    state.step = CexStep::DepositPending;
    state.deposit.deposit_txid = Some("tx-bridge-fund".into());
    state.deposit.bridge.deposit_bridge_submit_amount = Some(0.92);
    state.deposit.bridge.deposit_bridge_expected_amount = Some(0.91);
    state
}

/// A bridged withdrawal whose exchange withdrawal reached the bridge account,
/// on a route the provider runs as a call (ckUSDC@ICP -> USDC).
async fn withdrawn_state(finalizer: &CexVenueFinalizer<MockCexBackend>) -> CexState {
    let mut state = funded_state(finalizer).await;
    state.step = CexStep::WithdrawPending;
    state.withdraw.withdraw_asset = ChainToken::EvmErc20 {
        chain: "eth".into(),
        token_address: DESTINATION.into(),
        symbol: "USDC".into(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        0.92,
    ));
    state.withdraw.withdraw_id = Some("w-1".into());
    state.withdraw.withdraw_accepted_at_ts = Some(now_ts());
    let bridge = &mut state.withdraw.bridge;
    bridge.withdraw_planned_asset = Some("ckUSDC".into());
    bridge.withdraw_planned_network = Some("ICP".into());
    bridge.withdraw_bridge_required = true;
    bridge.withdraw_bridge_destination_snapshot = Some(DESTINATION.into());
    state
}

/// The exchange delivered the withdrawal to the bridge account.
fn completed_withdrawal(cex: &mut MockCexBackend, times: usize) {
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(times)
        .returning(|_, _| {
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("0xcompleted".into()),
                transaction_fee: None,
            })
        });
}

/// A row that the bridge journal saved before the outbound driver.
fn journal_attempt(called_at_ts: Option<i64>, parked: bool) -> LegacyBridgeSubmitPhase {
    LegacyBridgeSubmitPhase::Submitting(LegacyBridgeSubmitAttempt {
        id: "mine".into(),
        asset: "ckUSDC".into(),
        destination: DESTINATION.into(),
        amount: 0.92,
        cursor: Some(100),
        started_at_ts: now_ts() - 30,
        called_at_ts,
        parked_for_operator: parked,
    })
}

fn sent_call(at: i64) -> OutboundRecord {
    let call = BridgeCall {
        asset: "ckUSDC".into(),
        source_chain: "ICP".into(),
        source_address: Principal::anonymous().to_text(),
        target_asset: "USDC".into(),
        destination: DESTINATION.into(),
        amount: 0.92,
        provider_fee_budget_native_units: None,
        cursor: Some(100),
        started_at_ts: at,
    };
    let mut record = OutboundRecord::prepared(prepare_bridge_call("mine".into(), &call).unwrap());
    record.phase = OutboundPhase::Submitted { at };
    record
}

#[tokio::test]
async fn a_journal_attempt_is_migrated_and_settled_without_a_new_call() {
    let mut bridge = zero_fee_bridge();
    bridge.expect_submit_bridge().times(0);
    bridge
        .expect_find_submissions()
        .times(1)
        .returning(|request, cursor, claimed| {
            assert_eq!(request.asset, "ckUSDC");
            assert!((request.amount - 0.92).abs() < 1e-12);
            assert_eq!(cursor, 100);
            assert_eq!(claimed, ["ic-withdraw:1:2"]);
            Ok(vec![candidate(105)])
        });
    // The row's own attempt is still in the saved row the registry reads.
    let LegacyBridgeSubmitPhase::Submitting(own) = journal_attempt(None, false) else {
        unreachable!()
    };
    let finalizer = finalizer(bridge, Some(claims(vec![own.open_call()])));
    let mut state = funded_state(&finalizer).await;
    state.deposit.bridge.legacy_bridge_submit_phase = Some(journal_attempt(None, false));

    finalizer.deposit(&mut state).await.expect("the lookup attaches the handle");

    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("ic-withdraw:recovered:105")
    );
    assert!(state.deposit.bridge.deposit_bridge_call.is_none());
    assert!(state.deposit.bridge.legacy_bridge_submit_phase.is_none());
    assert_eq!(state.trade.trade_next_amount_in, Some(0.91));
}

#[tokio::test]
async fn a_parked_journal_attempt_waits_for_an_operator_answer() {
    let mut bridge = zero_fee_bridge();
    bridge.expect_submit_bridge().times(0);
    bridge.expect_find_submissions().times(2).returning(|_, _, _| Ok(vec![]));
    let finalizer = finalizer(bridge, Some(claims(vec![])));
    let mut state = funded_state(&finalizer).await;
    state.deposit.bridge.legacy_bridge_submit_phase = Some(journal_attempt(Some(now_ts() - 3_600), true));

    let error = finalizer.deposit(&mut state).await.expect_err("only an operator decides");
    assert!(matches!(error, CexBackendError::Ambiguous(_)), "{error}");
    assert!(matches!(
        state.deposit.bridge.deposit_bridge_call.as_ref().unwrap().phase,
        OutboundPhase::Parked { .. }
    ));

    // The operator found no record. The provider is searched once more.
    state.deposit.bridge.deposit_bridge_call.as_mut().unwrap().decide(OperatorDecision::Absent);
    finalizer.deposit(&mut state).await.expect_err("the absent call is dropped");
    assert!(state.deposit.bridge.deposit_bridge_call.is_none());
}

#[tokio::test]
async fn a_row_funded_by_an_older_build_parks_before_any_call() {
    let mut bridge = zero_fee_bridge();
    bridge.expect_submit_bridge().times(0);
    bridge.expect_prepare_submission().times(0);
    let finalizer = finalizer(bridge, Some(claims(vec![])));
    let mut state = funded_state(&finalizer).await;
    state.deposit.bridge.deposit_bridge_call_unrecorded = true;

    let error = finalizer.deposit(&mut state).await.expect_err("only an operator decides");
    assert!(matches!(error, CexBackendError::Ambiguous(_)), "{error}");
    assert!(!state.deposit.bridge.deposit_bridge_call_unrecorded);
    let questions = CexVenueFinalizer::<MockCexBackend>::operator_questions_for(&state);
    assert_eq!(questions.len(), 1, "{questions:?}");

    // The operator found the provider record.
    state
        .deposit
        .bridge
        .deposit_bridge_call
        .as_mut()
        .unwrap()
        .decide(OperatorDecision::Landed {
            receipt: "ic-withdraw:found:7".into(),
        });
    finalizer.deposit(&mut state).await.expect("the answer attaches the handle");
    assert_eq!(state.deposit.bridge.deposit_bridge_id.as_deref(), Some("ic-withdraw:found:7"));
}

#[tokio::test]
async fn a_failed_call_is_looked_up_before_any_new_call() {
    let mut bridge = zero_fee_bridge();
    bridge.expect_get_source_balance().times(1).returning(|_, _, _| Ok(5.0));
    bridge.expect_prepare_submission().times(1).returning(|_| Ok(Some(100)));
    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert!((request.amount - 0.92).abs() < 1e-12);
        Err("minter call timed out".into())
    });
    bridge
        .expect_find_submissions()
        .times(1)
        .returning(|_, _, _| Ok(vec![candidate(105)]));
    let mut cex = MockCexBackend::new();
    cex.expect_get_deposit_address().returning(|_, _| {
        Ok(DepositAddress {
            asset: "USDC".into(),
            network: "ETH".into(),
            address: DESTINATION.into(),
            tag: None,
        })
    });
    let finalizer = test_finalizer(Arc::new(cex), Arc::new(MockTransferActions::new()))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)).with_claims(Arc::new(claims(vec![]))));
    let mut state = funded_state(&finalizer).await;

    finalizer.deposit(&mut state).await.expect("the call is saved");
    finalizer.deposit(&mut state).await.expect("a failed call stays open");
    assert!(matches!(
        state.deposit.bridge.deposit_bridge_call.as_ref().unwrap().phase,
        OutboundPhase::Submitted { .. }
    ));
    finalizer.deposit(&mut state).await.expect("the lookup finds the record");
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("ic-withdraw:recovered:105")
    );
}

#[tokio::test]
async fn a_call_missing_past_the_grace_period_parks() {
    let recent = now_ts();
    let stale = now_ts() - 11 * 60;
    for at in [recent, stale] {
        let mut bridge = zero_fee_bridge();
        bridge.expect_submit_bridge().times(0);
        bridge.expect_find_submissions().times(1).returning(|_, _, _| Ok(vec![]));
        let finalizer = finalizer(bridge, Some(claims(vec![])));
        let mut state = funded_state(&finalizer).await;
        state.deposit.bridge.deposit_bridge_call = Some(sent_call(at));

        let result = finalizer.deposit(&mut state).await;

        assert!(state.deposit.bridge.deposit_bridge_id.is_none());
        if at == recent {
            result.expect("inside the grace period a miss is a wait");
        } else {
            let error = result.expect_err("a stale miss is not proof");
            assert!(matches!(error, CexBackendError::Ambiguous(_)), "{error}");
        }
    }
}

#[tokio::test]
async fn a_lost_return_bridge_call_is_looked_up_before_any_new_call() {
    let mut cex = MockCexBackend::new();
    completed_withdrawal(&mut cex, 1);
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_source_balance().times(1).returning(|_, _, _| Ok(5.0));
    bridge.expect_prepare_submission().times(1).returning(|request| {
        assert_eq!(request.asset, "ckUSDC");
        Ok(Some(100))
    });
    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert!((request.amount - 0.92).abs() < 1e-12);
        Err("minter call timed out".into())
    });
    bridge.expect_find_submissions().times(1).returning(|_, cursor, _| {
        assert_eq!(cursor, 100);
        Ok(vec![candidate(105)])
    });
    let finalizer = finalizer_with_cex(cex, bridge, Some(claims(vec![])));
    let mut state = withdrawn_state(&finalizer).await;

    finalizer.withdraw(&mut state).await.expect("the call is saved");
    assert!(state.send_saved);
    let call = state.withdraw.bridge.withdraw_bridge_call.as_ref().expect("saved call");
    assert_eq!(call.phase, OutboundPhase::Prepared);
    finalizer.withdraw(&mut state).await.expect("a failed call stays open");
    assert!(matches!(
        state.withdraw.bridge.withdraw_bridge_call.as_ref().unwrap().phase,
        OutboundPhase::Submitted { .. }
    ));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
    finalizer
        .withdraw(&mut state)
        .await
        .expect("the lookup finds the record");
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("ic-withdraw:recovered:105")
    );
    assert!(state.withdraw.bridge.withdraw_bridge_call.is_none());
    assert!(matches!(state.step, CexStep::WithdrawPending));
}

#[tokio::test]
async fn a_deferred_return_bridge_call_is_dropped_and_saved_again() {
    let mut cex = MockCexBackend::new();
    completed_withdrawal(&mut cex, 2);
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_source_balance().times(2).returning(|_, _, _| Ok(5.0));
    bridge.expect_prepare_submission().times(2).returning(|_| Ok(Some(100)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let seen = calls.clone();
    bridge.expect_submit_bridge().times(2).returning(move |_| {
        let mut calls = seen.lock().unwrap();
        *calls += 1;
        if *calls == 1 {
            Err(BridgeError::Deferred("bridge wallet has pending transactions".into()))
        } else {
            Ok(BridgeSubmission {
                bridge_id: "ic-withdraw:7:8".into(),
            })
        }
    });
    let finalizer = finalizer_with_cex(cex, bridge, Some(claims(vec![])));
    let mut state = withdrawn_state(&finalizer).await;

    finalizer.withdraw(&mut state).await.expect("the call is saved");
    finalizer.withdraw(&mut state).await.expect("a deferred call is a wait");
    // The backend proved nothing was sent, so the call is dropped.
    assert!(state.withdraw.bridge.withdraw_bridge_call.is_none());
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
    assert!(matches!(state.step, CexStep::WithdrawPending));
    finalizer.withdraw(&mut state).await.expect("a new call is saved");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("the call runs once the wallet is idle");
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("ic-withdraw:7:8")
    );
}

#[tokio::test]
async fn a_parked_return_bridge_call_waits_for_an_operator_answer() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_submit_bridge().times(0);
    bridge
        .expect_find_submissions()
        .times(2)
        .returning(|_, _, _| Ok(vec![]));
    let finalizer = finalizer(bridge, Some(claims(vec![])));
    let mut state = withdrawn_state(&finalizer).await;
    state.withdraw.bridge.withdraw_bridge_call = Some(sent_call(now_ts() - 11 * 60));

    let error = finalizer
        .withdraw(&mut state)
        .await
        .expect_err("a stale miss is not proof");
    assert!(matches!(error, CexBackendError::Ambiguous(_)), "{error}");
    let questions = CexVenueFinalizer::<MockCexBackend>::operator_questions_for(&state);
    assert_eq!(questions.len(), 1);
    assert!(
        questions[0].detail.contains("return bridge call"),
        "{}",
        questions[0].detail
    );

    CexVenueFinalizer::<MockCexBackend>::answer_operator(
        &mut state,
        Some(&OperatorDecision::Landed {
            receipt: "ic-withdraw:9:9".into(),
        }),
    )
    .unwrap();
    finalizer
        .withdraw(&mut state)
        .await
        .expect("the answer attaches the handle");
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("ic-withdraw:9:9")
    );
    assert!(state.withdraw.bridge.withdraw_bridge_call.is_none());
}
