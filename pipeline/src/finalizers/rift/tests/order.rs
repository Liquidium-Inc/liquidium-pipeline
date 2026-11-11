//! The order step: a saved quote first, then the order from it.
use super::*;
use crate::finalizers::multi_venue::VenueStepError;
use liquidium_pipeline_connectors::backend::rift_backend::RiftError;

fn fresh_state() -> RiftState {
    let mut state = state_at(RiftStep::Order);
    state.order = None;
    state
}

#[tokio::test]
async fn the_first_cycle_saves_the_quote_and_the_second_creates_the_order() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .times(1)
        .returning(|request| Ok(quote(&request.amount_in, "99.5")));
    rift.expect_create_order()
        .times(1)
        .returning(|quote_id, to_address, refund_address| {
            assert_eq!(quote_id, "quote-1");
            assert_eq!(to_address, "0x2222222222222222222222222222222222222222");
            assert_eq!(refund_address.as_deref(), Some(MAIN_WALLET));
            Ok(order("100"))
        });
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();

    finalizer.advance_current_step(&mut state).await.unwrap();
    assert_eq!(state.quote_id.as_deref(), Some("quote-1"));
    assert_eq!(
        state.quote_venues,
        ["uniswap"],
        "the route's venues are kept with the quote"
    );
    assert!(state.send_saved, "the quote id is written before the order is created");
    assert!(state.order.is_none());
    assert_eq!(state.step, RiftStep::Order);

    state.send_saved = false;
    finalizer.advance_current_step(&mut state).await.unwrap();
    let saved = state.order.expect("order saved");
    assert_eq!(saved.id, "order-1");
    assert_eq!(saved.deposit_address, DEPOSIT_ADDRESS);
    assert_eq!(saved.status.as_deref(), Some("awaiting_deposit"));
    assert_eq!(state.step, RiftStep::BridgeOut);
}

#[tokio::test]
async fn a_quote_under_the_planned_floor_is_refused_and_nothing_is_saved() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .returning(|request| Ok(quote(&request.amount_in, "98.9")));
    rift.expect_create_order().times(0);
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Other(ref m) if m.contains("under the planned floor")),
        "{error}"
    );
    assert!(state.quote_id.is_none());
    assert!(!state.send_saved);
}

#[tokio::test]
async fn an_expired_quote_is_dropped_so_the_next_cycle_quotes_again() {
    let mut rift = MockRiftBackend::new();
    rift.expect_create_order()
        .returning(|_, _, _| Err(RiftError::QuoteExpired("quote expired".to_string())));
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();
    state.quote_id = Some("quote-1".to_string());

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Other(ref m) if m.contains("expired")),
        "{error}"
    );
    assert!(state.quote_id.is_none());
    assert!(state.order.is_none());
}

#[tokio::test]
async fn an_order_for_another_amount_is_never_saved() {
    let mut rift = MockRiftBackend::new();
    rift.expect_create_order().returning(|_, _, _| Ok(order("99.9")));
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();
    state.quote_id = Some("quote-1".to_string());

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(
        matches!(error, VenueStepError::Other(ref m) if m.contains("expects 99.9")),
        "{error}"
    );
    assert!(state.order.is_none());
}

#[tokio::test]
async fn an_unreachable_rift_is_typed_so_the_leg_waits_without_spending_retries() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .returning(|_| Err(RiftError::Unreachable("connect timeout".to_string())));
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(matches!(error, VenueStepError::Unreachable(_)), "{error}");
}

#[tokio::test]
async fn a_throttled_rift_is_unreachable_too() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote().returning(|_| {
        Err(RiftError::RateLimited {
            retry_after_secs: Some(2),
        })
    });
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let mut state = fresh_state();

    let error = finalizer.advance_current_step(&mut state).await.unwrap_err();

    assert!(matches!(error, VenueStepError::Unreachable(_)), "{error}");
}

/// Rift refunds on the pay asset's chain. A ckBTC order names the minter's
/// address for the refund account, so a refund mints ckBTC and needs no Bitcoin key.
#[tokio::test]
async fn a_bitcoin_order_refunds_to_the_minter_address_of_the_refund_account() {
    use liquidium_pipeline_connectors::backend::bridge_backend::BridgeDestination;
    let mut rift = MockRiftBackend::new();
    rift.expect_create_order().times(1).returning(|_, _, refund_address| {
        assert_eq!(refund_address.as_deref(), Some(REFUND_BTC_ADDRESS));
        Ok(order("0.0006"))
    });
    let mut bridge = free_bridge();
    bridge
        .expect_deposit_address()
        .times(1)
        .returning(|asset, chain, destination| {
            assert_eq!((asset, chain), ("BTC", "BTC"));
            assert!(
                matches!(destination, BridgeDestination::IcpAccount(account) if account.owner == Principal::management_canister())
            );
            Ok(REFUND_BTC_ADDRESS.to_string())
        });
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut state = state_at(RiftStep::Order);
    state.order = None;
    state.quote_id = Some("quote-1".to_string());
    state.pay_chain = "bitcoin".to_string();
    state.pay_symbol = "BTC".to_string();
    state.size_in = ChainTokenAmount::from_raw(ckbtc(), Nat::from(60_010u64));
    state.amount_in = ChainTokenAmount::from_formatted(ckbtc(), 0.0006);

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeOut);
    assert_eq!(
        state.order.unwrap().refund_address.as_deref(),
        Some(REFUND_BTC_ADDRESS),
        "the order record keeps where a refund goes"
    );
}

/// A Bitcoin payout goes to the ckBTC minter's address for the final account,
/// so the mint lands there with no leg of ours in between.
#[tokio::test]
async fn an_order_paying_out_on_bitcoin_names_the_minter_address_of_the_final_account() {
    let mut rift = MockRiftBackend::new();
    rift.expect_create_order()
        .times(1)
        .returning(|_, to_address, refund_address| {
            assert_eq!(to_address, "bc1qminter");
            assert_eq!(refund_address.as_deref(), Some(MAIN_WALLET));
            Ok(order("100"))
        });
    let mut bridge = free_bridge();
    bridge
        .expect_deposit_address()
        .times(1)
        .returning(|asset, chain, destination| {
            assert_eq!((asset, chain), ("BTC", "BTC"));
            assert!(
                matches!(destination, liquidium_pipeline_connectors::backend::bridge_backend::BridgeDestination::IcpAccount(account) if account.owner == Principal::anonymous())
            );
            Ok("bc1qminter".to_string())
        });
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut state = state_receiving_ckbtc(RiftStep::Order);
    state.order = None;
    state.quote_id = Some("quote-1".to_string());

    finalizer.advance_current_step(&mut state).await.unwrap();

    assert_eq!(state.step, RiftStep::BridgeOut);
    assert_eq!(state.order.unwrap().to_address.as_deref(), Some("bc1qminter"));
}
