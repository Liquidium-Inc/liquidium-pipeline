//! The leg out on the Rift side: the order must still be payable, and the
//! bridge must deliver exactly what the order expects.
use super::*;
use crate::utils::now_ts;

fn deadline_in(secs: i64) -> String {
    chrono::DateTime::from_timestamp(now_ts() + secs, 0)
        .expect("timestamp")
        .to_rfc3339()
}

#[tokio::test]
async fn an_order_near_its_deadline_is_dropped_before_any_funding() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut state = state_at(RiftStep::BridgeOut);
    state.order.as_mut().unwrap().deposit_deadline = deadline_in(30 * 60);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert!(state.order.is_none());
    assert!(state.quote_id.is_none());
    assert_eq!(state.step, RiftStep::Order, "the next cycle quotes and orders again");
}

#[tokio::test]
async fn an_order_with_time_left_is_funded() {
    let mut bridge = free_bridge();
    bridge.expect_get_source_balance().times(0);
    let finalizer = finalizer(MockRiftBackend::new(), bridge, landing_sender());
    let mut state = state_at(RiftStep::BridgeOut);
    state.order.as_mut().unwrap().deposit_deadline = deadline_in(3 * 60 * 60);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert!(state.funding.is_some(), "the funding transfer is saved");
    assert!(state.send_saved);
    assert_eq!(state.step, RiftStep::BridgeOut);
}

#[tokio::test]
async fn a_sized_amount_that_drifts_from_the_order_takes_a_new_order_instead_of_sending() {
    let finalizer = finalizer(MockRiftBackend::new(), free_bridge(), landing_sender());
    let mut state = state_at(RiftStep::BridgeOut);
    // The order was taken for 100.5; the bridge sizes 100 today.
    state.amount_in = 100.5;
    state.order.as_mut().unwrap().amount_in = "100.5".to_string();

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert!(state.funding.is_none(), "the unsent transfer is dropped");
    assert!(state.order.is_none());
    assert_eq!(state.amount_in, 100.0, "the next order is for what the bridge delivers");
    assert_eq!(state.step, RiftStep::Order);
    assert!(!state.send_saved);
}

#[tokio::test]
async fn a_deadline_that_is_not_a_date_is_an_error_not_a_send() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut state = state_at(RiftStep::BridgeOut);
    state.order.as_mut().unwrap().deposit_deadline = "next week".to_string();

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(error.to_string().contains("not a date"), "{error}");
    assert!(state.order.is_some());
}
