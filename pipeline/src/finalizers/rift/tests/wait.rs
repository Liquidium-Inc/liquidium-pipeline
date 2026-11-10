//! The wait for Rift: each order status and what it decides.
use super::*;
use crate::finalizers::multi_venue::VenueStepError;
use crate::utils::now_ts;

fn waiting_state(bridge_completed_secs_ago: i64) -> RiftState {
    let mut state = state_at(RiftStep::Wait);
    state.order.as_mut().unwrap().wait_since_ts = Some(now_ts() - bridge_completed_secs_ago);
    state
}

async fn wait_with(
    status: RiftOrderStatus,
    amount_out: Option<&'static str>,
    state: &mut RiftState,
) -> Result<(), VenueStepError> {
    let mut rift = MockRiftBackend::new();
    rift.expect_order_state().times(1).returning(move |order_id| {
        assert_eq!(order_id, "order-1");
        Ok(order_state(status.clone(), amount_out))
    });
    let finalizer = finalizer(rift, MockBridgeBackend::new(), MockOutboundSender::new());
    finalizer.advance_current_step(state).await
}

#[tokio::test]
async fn an_order_that_awaits_its_deposit_waits_within_the_bound() {
    let mut state = waiting_state(60);
    wait_with(RiftOrderStatus::AwaitingDeposit, None, &mut state)
        .await
        .unwrap();
    assert_eq!(state.step, RiftStep::Wait);
    assert_eq!(
        state.order.as_ref().unwrap().status.as_deref(),
        Some("awaiting_deposit")
    );
}

#[tokio::test]
async fn an_order_that_never_sees_the_paid_deposit_parks_as_ambiguous() {
    let mut state = waiting_state(3 * 60 * 60);
    let error = wait_with(RiftOrderStatus::AwaitingDeposit, None, &mut state)
        .await
        .unwrap_err();
    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("still awaits")),
        "{error}"
    );
}

/// Rift counts its own Bitcoin confirmations after the minter's, so a BTC
/// deposit has a longer bound than an Ethereum one.
#[tokio::test]
async fn a_bitcoin_deposit_may_take_longer_to_be_seen() {
    let mut state = waiting_state(3 * 60 * 60);
    state.pay_chain = "bitcoin".to_string();
    wait_with(RiftOrderStatus::AwaitingDeposit, None, &mut state)
        .await
        .unwrap();
    assert_eq!(state.step, RiftStep::Wait);

    let mut state = waiting_state(5 * 60 * 60);
    state.pay_chain = "bitcoin".to_string();
    let error = wait_with(RiftOrderStatus::AwaitingDeposit, None, &mut state)
        .await
        .unwrap_err();
    assert!(matches!(error, VenueStepError::Ambiguous(_)), "{error}");
}

#[tokio::test]
async fn a_funded_order_waits_for_delivery() {
    let mut state = waiting_state(60);
    wait_with(RiftOrderStatus::Executing, None, &mut state).await.unwrap();
    assert_eq!(state.step, RiftStep::Wait);
}

#[tokio::test]
async fn a_delivered_order_moves_to_the_leg_back_with_its_amount() {
    let mut state = waiting_state(60);
    wait_with(RiftOrderStatus::Delivered, Some("99.4"), &mut state)
        .await
        .unwrap();
    assert_eq!(state.step, RiftStep::BridgeBack);
    let order = state.order.unwrap();
    assert_eq!(order.amount_out.as_deref(), Some("99.4"));
    assert_eq!(order.payout_tx.as_deref(), Some("0xpayout"));
    assert!(order.delivered_at_ts.is_some());
}

#[tokio::test]
async fn a_delivery_without_an_amount_is_ambiguous() {
    let mut state = waiting_state(60);
    let error = wait_with(RiftOrderStatus::Delivered, None, &mut state)
        .await
        .unwrap_err();
    assert!(matches!(error, VenueStepError::Ambiguous(_)), "{error}");
    assert_eq!(state.step, RiftStep::Wait);
}

#[tokio::test]
async fn a_refund_parks_with_the_refund_address() {
    let mut state = waiting_state(60);
    let error = wait_with(RiftOrderStatus::Refunded, None, &mut state)
        .await
        .unwrap_err();
    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains(MAIN_WALLET)),
        "{error}"
    );
}

#[tokio::test]
async fn every_other_status_means_rift_holds_the_funds() {
    for status in [
        RiftOrderStatus::Underfunded,
        RiftOrderStatus::Frozen,
        RiftOrderStatus::Expired,
        RiftOrderStatus::Unknown("settling".to_string()),
    ] {
        let mut state = waiting_state(60);
        let error = wait_with(status.clone(), None, &mut state).await.unwrap_err();
        assert!(matches!(error, VenueStepError::Ambiguous(_)), "{status:?}: {error}");
    }
}

#[tokio::test]
async fn a_delivery_of_nothing_is_ambiguous() {
    let mut state = waiting_state(60);
    let error = wait_with(RiftOrderStatus::Delivered, Some("0"), &mut state)
        .await
        .unwrap_err();
    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("delivery of 0")),
        "{error}"
    );
    assert_eq!(state.step, RiftStep::Wait);
}

#[tokio::test]
async fn a_delivery_under_the_floor_still_comes_back() {
    let mut state = waiting_state(60);
    state.min_amount_out = 99.0;
    wait_with(RiftOrderStatus::Delivered, Some("95"), &mut state)
        .await
        .unwrap();
    assert_eq!(
        state.step,
        RiftStep::BridgeBack,
        "the money is at the wallet, so it comes home"
    );
    assert_eq!(state.order.unwrap().amount_out.as_deref(), Some("95"));
}
