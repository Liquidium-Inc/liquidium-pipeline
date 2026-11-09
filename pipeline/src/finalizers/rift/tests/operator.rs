//! What a parked Rift leg asks, where an answer lands, and what a release resets.
use liquidium_pipeline_core::outbound::PreparedSend;

use super::*;
use crate::{
    finalizers::multi_venue::MultiVenueAdapter,
    outbound::driver::{OperatorDecision, OutboundPhase, OutboundRecord},
    persistance::{VenueExecutionState, VenueLegFailure},
    utils::now_ts,
};

fn parked_send(identity: &str) -> OutboundRecord {
    OutboundRecord {
        prepared: PreparedSend {
            sender: "icp".into(),
            identity: identity.into(),
            payload: String::new(),
            replay_safe: false,
        },
        phase: OutboundPhase::Parked {
            reason: "no decided answer".into(),
        },
        decision: None,
    }
}

fn leg_with(state: &RiftState, operator_required: bool) -> VenueLegState {
    let execution = serde_json::json!({ "rift": state, "operator_required": operator_required });
    let request = request(100_010_000);
    VenueLegState {
        leg_id: "rift-leg-2".to_string(),
        venue_id: RIFT_VENUE_ID.to_string(),
        request: request.clone(),
        quote: VenueLegQuote {
            pay_amount: request.pay_amount.clone(),
            estimated_receive: ChainTokenAmount::from_formatted(ckusdt(), 99.5),
            conservative_receive: ChainTokenAmount::from_formatted(ckusdt(), 99.0),
            estimated_price_impact_bps: 0.0,
            route_id: "ethereum.usdc>ethereum.usdt".to_string(),
            side_costs: Vec::new(),
        },
        execution: VenueExecutionState {
            venue: RIFT_VENUE_ID.to_string(),
            state: execution,
        },
        status: VenueLegStatus::OperatorRequired,
        result: None,
        last_error: None,
        failure: None,
    }
}

#[test]
fn a_parked_funding_transfer_is_one_question_and_takes_one_answer() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut state = state_at(RiftStep::BridgeOut);
    state.funding = Some(parked_send("funding-7"));
    let leg = leg_with(&state, true);

    let questions = finalizer.operator_questions(&leg).unwrap();
    assert_eq!(questions.len(), 1);
    assert!(
        questions[0].detail.contains("bridge funding"),
        "{}",
        questions[0].detail
    );
    assert!(questions[0].detail.contains("funding-7"), "{}", questions[0].detail);

    // A release without an answer is refused while a question is open.
    let error = finalizer.operator_release(&leg, None).unwrap_err();
    assert!(error.contains("waits for an answer"), "{error}");

    let released = finalizer
        .operator_release(
            &leg,
            Some(&OperatorDecision::Landed {
                receipt: "block-77".into(),
            }),
        )
        .unwrap();
    let rift = released.state.get("rift").unwrap();
    assert_eq!(
        rift.pointer("/funding/decision/Landed/receipt")
            .and_then(|v| v.as_str()),
        Some("block-77")
    );
    assert_eq!(
        released.state.get("operator_required").and_then(|v| v.as_bool()),
        Some(false)
    );
}

/// A refunded order has no send to decide, but the refund transaction is a
/// fact only an operator can supply. It ends the leg as a refund, not a retry.
#[tokio::test]
async fn a_refunded_order_takes_its_refund_transaction_and_ends_the_leg() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut state = state_at(RiftStep::Wait);
    state.order.as_mut().unwrap().status = Some("refunded".to_string());
    let leg = leg_with(&state, true);

    let questions = finalizer.operator_questions(&leg).unwrap();
    assert_eq!(questions.len(), 1);
    assert!(questions[0].detail.contains("order-1"), "{}", questions[0].detail);
    assert!(questions[0].detail.contains("landed <tx>"), "{}", questions[0].detail);

    let error = finalizer.operator_release(&leg, None).unwrap_err();
    assert!(error.contains("waits for an answer"), "{error}");
    let error = finalizer
        .operator_release(&leg, Some(&OperatorDecision::Absent))
        .unwrap_err();
    assert!(error.contains("Rift support"), "{error}");

    let released = finalizer
        .operator_release(
            &leg,
            Some(&OperatorDecision::Landed {
                receipt: "0xrefund".into(),
            }),
        )
        .unwrap();
    assert_eq!(
        released.state.pointer("/rift/order/refund_tx").and_then(|v| v.as_str()),
        Some("0xrefund")
    );
    assert_eq!(
        released.state.pointer("/rift/failure").and_then(|v| v.as_str()),
        Some("refunded")
    );

    let mut ended = leg.clone();
    ended.execution = released;
    let progress = finalizer.advance(&ended, &NoopCheckpoint).await.unwrap();
    assert_eq!(progress.status, VenueLegStatus::FailedPermanent);
    assert_eq!(progress.failure, Some(VenueLegFailure::Refunded));
    assert!(progress.retryable_error.is_none());
    assert!(
        progress.last_error.as_deref().unwrap_or_default().contains("0xrefund"),
        "{:?}",
        progress.last_error
    );
}

#[test]
fn a_release_while_waiting_for_rift_starts_the_wait_again() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut state = state_at(RiftStep::Wait);
    state.order.as_mut().unwrap().wait_since_ts = Some(now_ts() - 7 * 60 * 60);
    let leg = leg_with(&state, true);

    let released = finalizer.operator_release(&leg, None).unwrap();

    let since = released
        .state
        .pointer("/rift/order/wait_since_ts")
        .and_then(|v| v.as_i64())
        .unwrap();
    assert!(now_ts() - since < 5, "the bounds count from the release");
}
