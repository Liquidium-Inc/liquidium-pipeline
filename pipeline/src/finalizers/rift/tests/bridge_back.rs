//! The leg back for a Bitcoin payout: the minter mints into the final
//! account, and the leg only asks until it does.
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeMintReceipt, BridgeStatus, DepositNotice};

use super::*;
use crate::finalizers::multi_venue::VenueStepError;
use crate::utils::now_ts;

const PAYOUT_TX: &str = "53c022a60ed9cc64aa501ab3c0753dd90c39460b258df7b59b812e0d3294b6e7";

fn delivered_on_bitcoin(delivered_secs_ago: i64) -> RiftState {
    let mut state = state_receiving_ckbtc(RiftStep::BridgeBack);
    let order = state.order.as_mut().unwrap();
    order.amount_out = Some("0.0013".to_string());
    order.payout_tx = Some(PAYOUT_TX.to_string());
    order.delivered_at_ts = Some(now_ts() - delivered_secs_ago);
    state
}

#[tokio::test]
async fn a_deposit_the_minter_does_not_see_yet_waits_within_the_bound() {
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_track_deposit()
        .times(1)
        .returning(|asset, chain, _, _, receipt| {
            assert_eq!((asset, chain), ("BTC", "BTC"));
            assert_eq!(receipt, PAYOUT_TX);
            Ok(DepositNotice::NotYet {
                reason: "2 of 4 confirmations".to_string(),
            })
        });
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(60);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeBack);
    assert!(state.bridge_in.withdraw_bridge_id.is_none());
}

/// Past the bound each cycle errors, so the retry budget parks the row.
#[tokio::test]
async fn a_deposit_still_unseen_past_the_bound_errors_every_cycle() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_track_deposit().returning(|_, _, _, _, _| {
        Ok(DepositNotice::NotYet {
            reason: "nothing new".to_string(),
        })
    });
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(5 * 60 * 60);

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Other(ref m) if m.contains("still waits")),
        "{error}"
    );
}

#[tokio::test]
async fn a_minted_deposit_completes_the_leg_with_the_minted_amount() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(
            liquidium_pipeline_connectors::backend::bridge_backend::BridgeSubmission {
                bridge_id: format!("ckbtc-mint:81:{PAYOUT_TX}"),
            },
        ))
    });
    bridge.expect_get_bridge_status().times(1).returning(|bridge_id| {
        assert_eq!(bridge_id, format!("ckbtc-mint:81:{PAYOUT_TX}"));
        Ok(BridgeStatus::Minted(BridgeMintReceipt {
            transaction_hash: PAYOUT_TX.to_string(),
            log_index: Nat::from(0u8),
            mint_block_index: Nat::from(81u8),
            amount: Nat::from(129_900u64),
            recipient: icrc_ledger_types::icrc1::account::Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            token_symbol: "ckBTC".to_string(),
        }))
    });
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(60);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::Completed);
    assert_eq!(state.size_out.unwrap().value, Nat::from(129_900u64));
    assert_eq!(
        state.bridge_in.withdraw_bridge_id.as_deref(),
        Some(format!("ckbtc-mint:81:{PAYOUT_TX}").as_str())
    );
}

/// Once accepted, the handle is kept and only the ledger read repeats.
#[tokio::test]
async fn an_accepted_deposit_whose_mint_is_not_readable_yet_waits_with_its_handle() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(
            liquidium_pipeline_connectors::backend::bridge_backend::BridgeSubmission {
                bridge_id: format!("ckbtc-mint:81:{PAYOUT_TX}"),
            },
        ))
    });
    bridge.expect_get_bridge_status().times(2).returning(|_| {
        Err(liquidium_pipeline_connectors::backend::bridge_backend::BridgeError::Unreadable("ledger busy".to_string()))
    });
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(60);

    finalizer.advance_current_step(&mut state).await.unwrap();
    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeBack);
    assert_eq!(
        state.bridge_in.withdraw_bridge_id.as_deref(),
        Some(format!("ckbtc-mint:81:{PAYOUT_TX}").as_str())
    );
}

/// A quarantined deposit never mints on its own; only an operator moves it.
#[tokio::test]
async fn a_held_deposit_parks_the_leg() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_track_deposit().returning(|_, _, _, _, _| {
        Ok(DepositNotice::Held {
            reason: "quarantined".to_string(),
        })
    });
    let finalizer = finalizer(MockRiftBackend::new(), bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(60);

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Ambiguous(ref m) if m.contains("quarantined")),
        "{error}"
    );
}

/// The order is read again for the id; with the id, the mint is tracked in the same cycle.
#[tokio::test]
async fn a_bitcoin_delivery_without_a_payout_transaction_reads_the_order_again() {
    let mut rift = MockRiftBackend::new();
    rift.expect_order_state().times(1).returning(|_| {
        let mut answer = order_state(RiftOrderStatus::Delivered, Some("0.0013"));
        answer.payout_tx = Some(PAYOUT_TX.to_string());
        Ok(answer)
    });
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, receipt| {
        assert_eq!(receipt, PAYOUT_TX);
        Ok(DepositNotice::NotYet {
            reason: "0 of 4 confirmations".to_string(),
        })
    });
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut state = delivered_on_bitcoin(60);
    state.order.as_mut().unwrap().payout_tx = None;

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.order.unwrap().payout_tx.as_deref(), Some(PAYOUT_TX));
}

/// Without the id the leg waits within the return bound, then errors every cycle.
#[tokio::test]
async fn a_payout_transaction_rift_never_names_waits_then_errors() {
    for (delivered_secs_ago, waits) in [(60, true), (5 * 60 * 60, false)] {
        let mut rift = MockRiftBackend::new();
        rift.expect_order_state().times(1).returning(|_| {
            let mut answer = order_state(RiftOrderStatus::Delivered, Some("0.0013"));
            answer.payout_tx = None;
            Ok(answer)
        });
        let finalizer = finalizer(rift, MockBridgeBackend::new(), MockOutboundSender::new());
        let mut state = delivered_on_bitcoin(delivered_secs_ago);
        state.order.as_mut().unwrap().payout_tx = None;

        let outcome = finalizer.advance_current_step(&mut state).await;

        match outcome {
            Ok(()) => assert!(waits, "past the bound the leg must error"),
            Err(error) => assert!(
                !waits && matches!(error, VenueStepError::Other(ref m) if m.contains("still waits")),
                "{error}"
            ),
        }
        assert_eq!(state.step, RiftStep::BridgeBack);
    }
}
