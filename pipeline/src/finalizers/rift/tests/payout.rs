//! The payout check before the leg back on Ethereum: the bridge wallet is
//! shared, so the payout transaction must show this delivery at the wallet.
use liquidium_pipeline_connectors::backend::bridge_backend::TransactionPayment;

use super::*;
use crate::finalizers::multi_venue::VenueStepError;
use crate::utils::now_ts;

/// A ckUSDC -> ckUSDT leg whose order Rift reports as delivered with 99.4 USDT.
fn delivered_on_ethereum(delivered_secs_ago: i64, payout_tx: Option<&str>) -> RiftState {
    let mut state = state_at(RiftStep::BridgeBack);
    let order = state.order.as_mut().unwrap();
    order.amount_out = Some("99.4".to_string());
    order.payout_tx = payout_tx.map(str::to_string);
    order.delivered_at_ts = Some(now_ts() - delivered_secs_ago);
    state
}

/// A bridge that answers the payout read and must not be asked for anything else.
fn bridge_reading(payment: Result<TransactionPayment, String>) -> MockBridgeBackend {
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_payment_in_transaction()
        .times(1)
        .returning(move |asset, chain, wallet, transaction| {
            assert_eq!((asset, chain), ("USDT", "ETH"));
            assert_eq!(wallet, "0x2222222222222222222222222222222222222222");
            assert_eq!(transaction, PAYOUT_TX);
            payment.clone()
        });
    bridge
}

/// The other row case: the wallet holds funds, but this payout is not on the chain yet.
#[tokio::test]
async fn a_payout_that_the_chain_does_not_show_waits_and_reads_no_wallet_balance() {
    // No balance expectation: a read of the shared wallet would fail the test.
    let bridge = bridge_reading(Ok(TransactionPayment::NotOnChain));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeBack);
    assert!(state.bridge_in.awaits_funds());
}

#[tokio::test]
async fn a_payout_still_not_on_the_chain_past_the_bound_errors() {
    let bridge = bridge_reading(Ok(TransactionPayment::NotOnChain));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(5 * 60 * 60, Some(PAYOUT_TX));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Other(ref m) if m.contains("does not show the delivery transaction")),
        "{error}"
    );
    assert!(state.bridge_in.awaits_funds());
}

#[tokio::test]
async fn a_payout_that_paid_less_than_the_delivery_asks_an_operator() {
    let bridge = bridge_reading(Ok(TransactionPayment::Paid(99.3)));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("99.4") && m.contains("paid 99.3")),
        "{error}"
    );
    assert!(state.bridge_in.awaits_funds());
}

/// A payment to another address or in another token reads as zero for this wallet and token.
#[tokio::test]
async fn a_payout_to_another_address_or_in_another_token_asks_an_operator() {
    let bridge = bridge_reading(Ok(TransactionPayment::Paid(0.0)));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(matches!(error, VenueStepError::Ambiguous(_)), "{error}");
}

#[tokio::test]
async fn a_failed_payout_transaction_asks_an_operator() {
    let bridge = bridge_reading(Ok(TransactionPayment::Reverted));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("failed on the chain")),
        "{error}"
    );
}

#[tokio::test]
async fn a_payout_read_that_fails_is_a_free_wait() {
    let bridge = bridge_reading(Err("rpc timeout".to_string()));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Unreachable(ref m) if m.contains("rpc timeout")),
        "{error}"
    );
}

/// The proof lets the leg go on: only then the wallet balance is read.
#[tokio::test]
async fn a_proven_payout_lets_the_bridge_back_read_the_wallet() {
    for payment in [TransactionPayment::Paid(99.4), TransactionPayment::NotVisible] {
        let mut bridge = bridge_reading(Ok(payment));
        // The wallet is short, so the leg waits after the read. The read is what the test checks.
        bridge.expect_get_source_balance().times(1).returning(|_, _, _| Ok(0.0));
        let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
        let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));

        finalizer.advance_current_step(&mut state).await.unwrap();

        assert_eq!(state.step, RiftStep::BridgeBack, "{payment:?}");
    }
}

#[tokio::test]
async fn a_delivery_without_a_payout_transaction_reads_the_order_again() {
    let mut rift = MockRiftBackend::new();
    rift.expect_order_state().times(1).returning(|_| {
        let mut answer = order_state(RiftOrderStatus::Delivered, Some("99.4"));
        answer.payout_tx = Some(PAYOUT_TX.to_string());
        Ok(answer)
    });
    let mut bridge = bridge_reading(Ok(TransactionPayment::Paid(99.4)));
    bridge.expect_get_source_balance().times(1).returning(|_, _, _| Ok(0.0));
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, None);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.order.unwrap().payout_tx.as_deref(), Some(PAYOUT_TX));
}

#[tokio::test]
async fn a_delivery_that_never_names_a_payout_transaction_waits_then_errors() {
    for (delivered_secs_ago, errors) in [(60, false), (5 * 60 * 60, true)] {
        let mut rift = MockRiftBackend::new();
        rift.expect_order_state().times(1).returning(|_| {
            let mut answer = order_state(RiftOrderStatus::Delivered, Some("99.4"));
            answer.payout_tx = None;
            Ok(answer)
        });
        // No bridge expectation: nothing on the chain or at the wallet is read.
        let finalizer = finalizer(rift, MockBridgeBackend::new(), MockOutboundSender::new());
        let mut state = delivered_on_ethereum(delivered_secs_ago, None);

        let outcome = finalizer.advance_current_step(&mut state).await;

        assert_eq!(outcome.is_err(), errors, "{outcome:?}");
        assert!(state.bridge_in.awaits_funds());
    }
}

/// A bridge back that already started does not read the payout again.
#[tokio::test]
async fn a_started_bridge_back_skips_the_payout_read() {
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_bridge_status()
        .times(1)
        .returning(|_| Ok(liquidium_pipeline_connectors::backend::bridge_backend::BridgeStatus::Pending));
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some(PAYOUT_TX));
    state.bridge_in.withdraw_bridge_id = Some("bridge-back-1".to_string());

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeBack);
}

/// An id that the chain cannot be asked about is refused at once: a wait for it would never end.
#[tokio::test]
async fn a_payout_id_that_is_not_a_hash_asks_an_operator_without_a_chain_read() {
    // No bridge expectation: nothing on the chain or at the wallet is read.
    let finalizer = finalizer(MockRiftBackend::new(), MockBridgeBackend::new(), MockOutboundSender::new());
    let mut state = delivered_on_ethereum(60, Some("payout-17"));

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("payout-17") && m.contains("not an Ethereum transaction hash")),
        "{error}"
    );
}
