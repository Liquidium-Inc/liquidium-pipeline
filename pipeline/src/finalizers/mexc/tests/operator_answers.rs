use super::*;
use crate::outbound::driver::{OperatorDecision, OutboundPhase, OutboundRecord};
use liquidium_pipeline_core::outbound::PreparedSend;

type Mexc = MexcFinalizer<MockCexBackend>;

fn state() -> CexState {
    cksol_repayment_state(&cksol_finalizer(MockCexBackend::new(), MockBridgeBackend::new()), 1.5)
}

fn parked_send() -> OutboundRecord {
    OutboundRecord {
        prepared: PreparedSend {
            sender: "icp".into(),
            identity: "send-1".into(),
            payload: "{}".into(),
            replay_safe: true,
        },
        phase: OutboundPhase::Parked {
            reason: "no decided answer".into(),
        },
        decision: None,
    }
}

fn landed(receipt: &str) -> OperatorDecision {
    OperatorDecision::Landed {
        receipt: receipt.into(),
    }
}

#[test]
fn a_parked_send_takes_a_decision_and_refuses_a_bare_release() {
    for answer in [landed("block-9"), OperatorDecision::Absent] {
        let mut state = state();
        state.deposit.deposit_funding = Some(parked_send());
        let questions = Mexc::operator_questions_for(&state);
        assert_eq!(questions.len(), 1);
        assert!(questions[0].detail.contains("send-1"), "{}", questions[0].detail);

        let error = Mexc::answer_operator(&mut state, None).unwrap_err();
        assert!(error.contains("waits for an answer"), "{error}");
        Mexc::answer_operator(&mut state, Some(&answer)).unwrap();
        assert_eq!(state.deposit.deposit_funding.unwrap().decision, Some(answer));
    }
}

#[test]
fn a_withdrawal_that_may_have_run_before_a_restart_takes_a_decision() {
    let mut state = state();
    let mut request = parked_send();
    request.prepared.sender = "mexc-withdraw".into();
    request.prepared.identity = "lq0000000000000001w0".into();
    request.prepared.replay_safe = false;
    request.phase = OutboundPhase::Prepared;
    state.withdraw.withdraw_send = Some(request.clone());
    let questions = Mexc::operator_questions_for(&state);
    assert_eq!(questions.len(), 1);
    assert!(questions[0].detail.contains("lq0000000000000001w0"), "{}", questions[0].detail);
    assert!(Mexc::answer_operator(&mut state, None).is_err(), "a bare release could withdraw twice");

    Mexc::answer_operator(&mut state, Some(&landed("7872730c87d04382b5c76da98359cd4c"))).unwrap();
    let decided = state.withdraw.withdraw_send.unwrap();
    assert!(matches!(decided.phase, OutboundPhase::Parked { .. }), "the next run reads MEXC first");
    assert_eq!(decided.decision, Some(landed("7872730c87d04382b5c76da98359cd4c")));

    // A prepared send that is safe to repeat resumes by itself.
    let mut state = super::operator_answers::state();
    request.prepared.replay_safe = true;
    state.withdraw.withdraw_send = Some(request);
    assert!(Mexc::operator_questions_for(&state).is_empty());
}

#[test]
fn a_parked_bridge_call_takes_the_found_handle_as_its_decision() {
    let mut state = state();
    let mut call = parked_send();
    call.prepared.sender = crate::bridge::call_sender::NAME.into();
    call.prepared.identity = "bridge-call-1".into();
    call.prepared.replay_safe = false;
    state.deposit.bridge.deposit_bridge_call = Some(call);
    let questions = Mexc::operator_questions_for(&state);
    assert_eq!(questions.len(), 1);
    assert!(questions[0].detail.contains("bridge id"), "{}", questions[0].detail);

    Mexc::answer_operator(&mut state, Some(&landed("ic-withdraw:1:2"))).unwrap();

    // The next run searches the provider first, then uses this handle.
    let decided = state.deposit.bridge.deposit_bridge_call.unwrap();
    assert_eq!(decided.decision, Some(landed("ic-withdraw:1:2")));
}

#[test]
fn a_park_without_a_question_takes_only_a_release() {
    let mut state = state();
    assert!(Mexc::operator_questions_for(&state).is_empty());
    Mexc::answer_operator(&mut state, None).unwrap();
    let error = Mexc::answer_operator(&mut state, Some(&OperatorDecision::Absent)).unwrap_err();
    assert!(error.contains("--release"), "{error}");
}
