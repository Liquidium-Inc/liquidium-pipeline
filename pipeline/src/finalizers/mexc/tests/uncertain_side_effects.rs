//! A call that may have moved funds without an answer is never repeated
//! automatically. Funding and withdrawals settle from the chain or the
//! MEXC history, and park for an operator when that stays undecided.
use super::*;
use liquidium_pipeline_core::outbound::SIDE_EFFECT_AMBIGUOUS_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::{WithdrawStatusSnapshot, WithdrawalReceipt};
use crate::outbound::driver::{OperatorDecision, OutboundPhase, OutboundRecord};
use liquidium_pipeline_core::outbound::{MockOutboundSender, PreparedSend, SendOutcome};

fn finalizer(cex: MockCexBackend, transfers: MockTransferActions) -> MexcFinalizer<MockCexBackend> {
    test_finalizer(Arc::new(cex), Arc::new(transfers))
}

fn receipt(id: &str) -> WithdrawalReceipt {
    WithdrawalReceipt {
        asset: "ckUSDC".into(),
        network: "ICP".into(),
        amount: 1.0,
        txid: None,
        internal_id: Some(id.into()),
    }
}

fn snapshot(status: WithdrawStatus, txid: Option<&str>) -> WithdrawStatusSnapshot {
    WithdrawStatusSnapshot {
        status,
        txid: txid.map(str::to_string),
        transaction_fee: None,
    }
}

async fn withdraw_state(finalizer: &MexcFinalizer<MockCexBackend>) -> CexState {
    let mut state = finalizer.prepare("uncertain", &make_execution_receipt(150)).await.unwrap();
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), 1.0));
    state
}

fn deposit_address(address: &'static str) -> MockCexBackend {
    let mut cex = MockCexBackend::new();
    // Measured once per prepared transfer, never on a resend.
    cex.expect_get_balance().times(1).returning(|_| Ok(10.0));
    cex.expect_get_deposit_address().times(1).returning(move |asset, network| {
        Ok(DepositAddress {
            asset: asset.into(),
            network: network.into(),
            address: address.into(),
            tag: None,
        })
    });
    cex
}

fn funding_sender(submit: Vec<SendOutcome>, settle: Vec<SendOutcome>, window: i64) -> MockOutboundSender {
    let mut sender = MockOutboundSender::new();
    sender.expect_prepare().times(1).returning(|intent| {
        assert!(matches!(intent.to, ChainAccount::Icp(_)), "{:?}", intent.to);
        Ok(PreparedSend {
            sender: "test".into(),
            identity: "funding-1".into(),
            payload: format!("{}", intent.amount_native),
            replay_safe: true,
        })
    });
    let mut submit = submit.into_iter();
    sender.expect_submit().returning(move |_| submit.next().expect("unexpected submit"));
    let mut settle = settle.into_iter();
    sender.expect_settle().returning(move |_| settle.next().expect("unexpected settle"));
    sender.expect_open_window_secs().returning(move |_| window);
    sender
}

fn funding_finalizer(cex: MockCexBackend, sender: MockOutboundSender) -> MexcFinalizer<MockCexBackend> {
    let mut transfers = MockTransferActions::new();
    transfers.expect_transfer().times(0);
    transfers.expect_approve().returning(|_, _, _| Ok("approve".into()));
    finalizer(cex, transfers).with_outbound(Arc::new(sender))
}

fn open() -> SendOutcome {
    SendOutcome::Open {
        reason: "replica timed out".into(),
    }
}

#[tokio::test]
async fn funding_is_saved_first_and_sent_on_the_next_cycle() {
    let sender = funding_sender(vec![SendOutcome::Landed { receipt: "block-7".into() }], vec![], 60);
    let finalizer = funding_finalizer(deposit_address("aaaaa-aa"), sender);
    let mut state = finalizer.prepare("funding", &make_execution_receipt(151)).await.unwrap();

    finalizer.deposit(&mut state).await.expect("the first cycle only saves");
    assert_eq!(state.step, CexStep::Deposit);
    assert!(state.deposit.deposit_txid.is_none());
    let record = state.deposit.deposit_funding.clone().expect("saved before the send");
    assert_eq!(record.phase, OutboundPhase::Prepared);
    let mut restarted = state.clone();
    assert!(restarted.resume_after_restart(now_ts()));
    assert_eq!(
        restarted.deposit.deposit_funding.unwrap().phase,
        OutboundPhase::Prepared,
        "a replay-safe record is sent again after a restart"
    );

    let mut state = serde_json::from_str::<CexState>(&serde_json::to_string(&state).unwrap()).unwrap();
    finalizer.deposit(&mut state).await.expect("the second cycle sends");
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("block-7"));
    assert!(state.deposit.deposit_funding.is_none());
    assert_eq!(state.step, CexStep::DepositPending);
}

#[tokio::test]
async fn an_open_funding_send_is_settled_and_never_measured_or_sent_again() {
    let sender = funding_sender(vec![open()], vec![open(), SendOutcome::Landed { receipt: "block-8".into() }], 600);
    let finalizer = funding_finalizer(deposit_address("aaaaa-aa"), sender);
    let mut state = finalizer.prepare("open", &make_execution_receipt(152)).await.unwrap();

    for _ in 0..3 {
        finalizer.deposit(&mut state).await.expect("an open send is a wait, not an error");
        assert!(state.deposit.deposit_txid.is_none());
    }
    finalizer.deposit(&mut state).await.unwrap();
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("block-8"));
}

#[tokio::test]
async fn a_parked_funding_send_moves_only_on_an_operator_decision() {
    let sender = funding_sender(vec![open()], vec![open(), open(), open()], 0);
    let finalizer = funding_finalizer(deposit_address("aaaaa-aa"), sender);
    let mut state = finalizer.prepare("parked", &make_execution_receipt(153)).await.unwrap();
    finalizer.deposit(&mut state).await.unwrap();
    finalizer.deposit(&mut state).await.unwrap();
    state.deposit.deposit_funding.as_mut().unwrap().phase = OutboundPhase::Submitted { at: 1 };

    let error = finalizer.deposit(&mut state).await.expect_err("the window ran out");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
    let error = finalizer.deposit(&mut state).await.expect_err("a release without a decision parks again");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");

    state.deposit.deposit_funding.as_mut().unwrap().decision =
        Some(OperatorDecision::Landed { receipt: "block-9".into() });
    finalizer.deposit(&mut state).await.expect("the decision completes the step");
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("block-9"));
}

#[tokio::test]
async fn a_send_that_never_landed_is_measured_and_prepared_again() {
    let mut cex = MockCexBackend::new();
    cex.expect_get_balance().times(2).returning(|_| Ok(10.0));
    cex.expect_get_deposit_address().times(2).returning(|asset, network| {
        Ok(DepositAddress {
            asset: asset.into(),
            network: network.into(),
            address: "aaaaa-aa".into(),
            tag: None,
        })
    });
    let mut sender = MockOutboundSender::new();
    sender.expect_prepare().times(2).returning(|_| {
        Ok(PreparedSend {
            sender: "test".into(),
            identity: "funding".into(),
            payload: String::new(),
            replay_safe: true,
        })
    });
    let mut submit = vec![
        SendOutcome::NeverLanded {
            reason: "insufficient funds".into(),
        },
        SendOutcome::Landed { receipt: "block-2".into() },
    ]
    .into_iter();
    sender.expect_submit().times(2).returning(move |_| submit.next().unwrap());
    let finalizer = funding_finalizer(cex, sender);
    let mut state = finalizer.prepare("refused", &make_execution_receipt(154)).await.unwrap();

    finalizer.deposit(&mut state).await.unwrap();
    let error = finalizer.deposit(&mut state).await.expect_err("refused");
    assert_eq!(error, "insufficient funds");
    assert!(state.deposit.deposit_funding.is_none());
    finalizer.deposit(&mut state).await.unwrap();
    finalizer.deposit(&mut state).await.unwrap();
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("block-2"));
}

#[tokio::test]
async fn plain_sends_do_not_resume_unsent_after_a_restart() {
    let mut state = finalizer(MockCexBackend::new(), MockTransferActions::new())
        .prepare("plain", &make_execution_receipt(155))
        .await
        .unwrap();
    state.deposit.deposit_funding = Some(OutboundRecord {
        prepared: PreparedSend {
            sender: "unsettled".into(),
            identity: "plain-1".into(),
            payload: String::new(),
            replay_safe: false,
        },
        phase: OutboundPhase::Prepared,
        decision: None,
    });
    assert!(state.resume_after_restart(5));
    assert_eq!(
        state.deposit.deposit_funding.unwrap().phase,
        OutboundPhase::Submitted { at: 5 },
        "it may have been sent before the crash, so it is only read back"
    );
}

/// The order id of the saved request, which the next cycle sends.
fn saved_order_id(state: &CexState) -> String {
    state
        .withdraw
        .withdraw_send
        .as_ref()
        .expect("the request is saved before it is sent")
        .prepared
        .identity
        .clone()
}

/// Moves the saved request's open clock past the MEXC window.
fn age_open_request(state: &mut CexState) {
    state.withdraw.withdraw_send.as_mut().unwrap().phase = OutboundPhase::Submitted { at: now_ts() - 16 * 60 };
}

#[tokio::test]
async fn an_unanswered_withdraw_is_settled_from_its_order_id_and_never_repeated() {
    let mut cex = MockCexBackend::new();
    cex.expect_withdraw()
        .times(1)
        .returning(|_, _, _, _, _| Err(WithdrawError::Ambiguous(format!("{SIDE_EFFECT_AMBIGUOUS_PREFIX}: MEXC withdraw timed out"))));
    let polls = Arc::new(std::sync::Mutex::new(vec![
        snapshot(WithdrawStatus::Completed, Some("ledger:7")),
        snapshot(WithdrawStatus::Pending, None),
    ]));
    let polled_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen = polled_ids.clone();
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(2)
        .returning(move |_, id| {
            seen.lock().unwrap().push(id.to_string());
            Ok(polls.lock().unwrap().pop().unwrap())
        });
    let finalizer = finalizer(cex, MockTransferActions::new());
    let mut state = withdraw_state(&finalizer).await;

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the request");
    let order_id = saved_order_id(&state);
    assert!(order_id.len() <= 32 && order_id.starts_with("lq") && order_id.ends_with("w0"), "{order_id}");
    assert_eq!(state.withdraw.withdraw_send.as_ref().unwrap().phase, OutboundPhase::Prepared);
    finalizer.withdraw(&mut state).await.expect("no error: the answer stays open");
    assert!(
        matches!(state.withdraw.withdraw_send.as_ref().unwrap().phase, OutboundPhase::Submitted { .. }),
        "a sent request is only read back"
    );
    finalizer.withdraw(&mut state).await.expect("the history shows the request");
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some(order_id.as_str()));
    assert_eq!(state.step, CexStep::WithdrawPending);
    finalizer.withdraw(&mut state).await.expect("the delivery arrives");
    assert_eq!(state.step, CexStep::Completed);
    assert_eq!(*polled_ids.lock().unwrap(), vec![order_id.clone(), order_id]);
}

#[tokio::test]
async fn an_absent_withdraw_parks_then_a_new_order_follows_an_absent_answer() {
    let order_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut cex = MockCexBackend::new();
    let seen = order_ids.clone();
    cex.expect_withdraw().times(2).returning(move |_, _, _, _, order_id| {
        let mut seen = seen.lock().unwrap();
        seen.push(order_id.to_string());
        if seen.len() == 1 {
            Err(WithdrawError::Ambiguous(format!("{SIDE_EFFECT_AMBIGUOUS_PREFIX}: MEXC withdraw timed out")))
        } else {
            Ok(receipt("mexc-internal-2"))
        }
    });
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(3)
        .returning(|_, _| Ok(snapshot(WithdrawStatus::Unknown, None)));
    let finalizer = finalizer(cex, MockTransferActions::new());
    let mut state = withdraw_state(&finalizer).await;

    finalizer.withdraw(&mut state).await.unwrap();
    finalizer.withdraw(&mut state).await.unwrap();
    // Inside the window an absent record is a wait.
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.step, CexStep::Withdraw);

    age_open_request(&mut state);
    let error = finalizer.withdraw(&mut state).await.expect_err("no record after the window");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");

    // The operator found no request. The leg reads the history once more.
    MexcFinalizer::<MockCexBackend>::answer_operator(&mut state, Some(&OperatorDecision::Absent)).unwrap();
    finalizer.withdraw(&mut state).await.expect_err("the absent request is dropped");
    assert!(state.withdraw.withdraw_send.is_none());
    assert_eq!(state.withdraw.withdraw_attempt, 1);

    finalizer.withdraw(&mut state).await.unwrap();
    finalizer.withdraw(&mut state).await.unwrap();
    let order_ids = order_ids.lock().unwrap();
    assert_ne!(order_ids[0], order_ids[1], "a new request gets a new order id");
    assert!(order_ids[1].ends_with("w1"), "{}", order_ids[1]);
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("mexc-internal-2"));
}

#[tokio::test]
async fn a_bridged_unanswered_withdraw_parks_instead_of_waiting_forever() {
    let mut cex = MockCexBackend::new();
    cex.expect_withdraw()
        .times(1)
        .returning(|_, _, _, _, _| Err(WithdrawError::Ambiguous(format!("{SIDE_EFFECT_AMBIGUOUS_PREFIX}: MEXC withdraw timed out"))));
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(2)
        .returning(|_, _| Ok(snapshot(WithdrawStatus::Unknown, None)));
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_submit_bridge().times(0);
    let finalizer = test_finalizer(Arc::new(cex), Arc::new(MockTransferActions::new()))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));
    let receipt = make_execution_receipt_with_assets(152, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("bridged-uncertain", &receipt).await.unwrap();
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), 1.0));

    finalizer.withdraw(&mut state).await.expect("saved");
    finalizer.withdraw(&mut state).await.expect("sent, open");
    finalizer.withdraw(&mut state).await.expect("inside the window");
    age_open_request(&mut state);
    let error = finalizer.withdraw(&mut state).await.expect_err("parks after the window");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
    assert!(state.withdraw.withdraw_id.is_none());
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
}

#[tokio::test]
async fn a_refused_withdraw_retries_under_a_new_order_id() {
    let order_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut cex = MockCexBackend::new();
    let seen = order_ids.clone();
    cex.expect_withdraw().times(2).returning(move |_, _, _, _, order_id| {
        let mut seen = seen.lock().unwrap();
        seen.push(order_id.to_string());
        if seen.len() == 1 {
            Err(WithdrawError::Refused("MEXC refused: insufficient balance".into()))
        } else {
            Ok(receipt("mexc-internal"))
        }
    });
    let finalizer = finalizer(cex, MockTransferActions::new());
    let mut state = withdraw_state(&finalizer).await;

    finalizer.withdraw(&mut state).await.unwrap();
    let error = finalizer.withdraw(&mut state).await.expect_err("refused");
    assert!(!error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX));
    assert_eq!(state.withdraw.withdraw_attempt, 1);
    finalizer.withdraw(&mut state).await.unwrap();
    finalizer.withdraw(&mut state).await.unwrap();

    let order_ids = order_ids.lock().unwrap();
    assert!(order_ids[0].ends_with("w0") && order_ids[1].ends_with("w1"), "{order_ids:?}");
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("mexc-internal"));
}
