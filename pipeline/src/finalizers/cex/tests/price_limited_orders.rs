//! Audit S3: the exchange enforces the slippage cap on every slice order.
use super::*;
use liquidium_pipeline_connectors::backend::cex_backend::{PriceLimit, SwapExecutionOptions};

type Orders = Arc<std::sync::Mutex<Vec<SwapExecutionOptions>>>;

/// The test book's taker fee. A fill is checked net of it, so each order's
/// price room is the limit less this.
const FEE_BPS: f64 = 10.0;

/// A ckBTC -> ckUSDT finalizer whose first leg, CKBTC_BTC, answers each
/// order with the next fill; `None` is a fill of the whole request.
fn finalizer_with_fills(fills: Vec<Option<f64>>) -> (CexVenueFinalizer<MockCexBackend>, Orders) {
    finalizer_with_fee_and_fills(FEE_BPS, fills)
}

fn finalizer_with_fee_and_fills(
    taker_fee_bps: f64,
    fills: Vec<Option<f64>>,
) -> (CexVenueFinalizer<MockCexBackend>, Orders) {
    let mut backend = MockCexBackend::new();
    let book = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 1_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 1_000.0,
        }],
        taker_fee_bps,
    };
    backend.expect_get_orderbook().returning(move |_, _| Ok(book.clone()));
    let orders: Orders = Arc::default();
    let seen = orders.clone();
    let fills = std::sync::Mutex::new(fills.into_iter());
    backend
        .expect_execute_swap_detailed_with_options()
        .returning(move |market, _, amount_in, options| {
            assert_eq!(market, "CKBTC_BTC");
            seen.lock().unwrap().push(options);
            let consumed = fills
                .lock()
                .unwrap()
                .next()
                .expect("an unexpected order")
                .unwrap_or(amount_in);
            Ok(SwapFillReport {
                input_consumed: consumed,
                output_received: consumed * 0.9999,
            })
        });
    let finalizer = test_finalizer(Arc::new(backend), Arc::new(MockTransferActions::new()));
    (finalizer, orders)
}

async fn trade_state(finalizer: &CexVenueFinalizer<MockCexBackend>) -> CexState {
    let mut state = finalizer.prepare("42", &make_execution_receipt(42)).await.unwrap();
    state.step = CexStep::Trade;
    state
}

/// The price room of a sell slice at `limit_bps` with the test fee.
fn room(limit_bps: f64) -> f64 {
    CexVenueFinalizer::<MockCexBackend>::price_room_bps("sell", limit_bps, FEE_BPS)
}

fn client_ids(orders: &Orders) -> Vec<String> {
    orders
        .lock()
        .unwrap()
        .iter()
        .map(|o| o.client_order_id.clone().unwrap())
        .collect()
}

#[tokio::test]
async fn every_slice_order_carries_the_cap_less_the_fee_as_its_limit() {
    let (finalizer, orders) = finalizer_with_fills(vec![None]);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the leg fills");

    let orders = orders.lock().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].price_limit,
        Some(PriceLimit {
            reference_price: state.trade.trade_slices[0].mid_price,
            max_slippage_bps: room(TEST_MAX_SELL_SLIPPAGE_BPS),
        })
    );
}

#[tokio::test]
async fn an_order_that_fills_nothing_spends_its_id_and_previews_again() {
    let (finalizer, orders) = finalizer_with_fills(vec![Some(0.0), None]);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the second order fills");

    let ids = client_ids(&orders);
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "a spent id is never sent again");
    assert!(state.trade.trade_applied_client_order_ids.contains(&ids[0]));
    assert_eq!(state.trade.trade_slices.len(), 1, "an empty order records no slice");
}

#[tokio::test]
async fn a_partial_fill_leaves_the_rest_for_the_next_order() {
    let (finalizer, orders) = finalizer_with_fills(vec![Some(0.0001), None]);
    let mut state = trade_state(&finalizer).await;
    let amount_in = state.size_in.to_f64();

    finalizer.trade(&mut state).await.expect("the rest fills");

    assert_eq!(client_ids(&orders).len(), 2);
    let consumed: f64 = state.trade.trade_slices.iter().map(|slice| slice.amount_in).sum();
    assert!(consumed > 0.0001 && consumed <= amount_in, "{consumed} of {amount_in}");
}

#[tokio::test]
async fn empty_fills_in_a_row_pause_the_leg_without_an_error() {
    let (finalizer, orders) = finalizer_with_fills(vec![Some(0.0), Some(0.0), Some(0.0), None]);
    let mut state = trade_state(&finalizer).await;

    finalizer
        .trade(&mut state)
        .await
        .expect("a pause is a wait, not an error");

    let ids = client_ids(&orders);
    assert_eq!(ids.len(), 3);
    assert!(
        ids.iter()
            .all(|id| state.trade.trade_applied_client_order_ids.contains(id))
    );
    assert!(state.trade.trade_slices.is_empty());
    assert!(state.last_error.is_none());
    assert_eq!(state.trade.trade_leg_index.unwrap_or(0), 0, "the leg did not advance");

    // The next cycle resumes the same leg and fills it.
    finalizer.trade(&mut state).await.expect("the next cycle fills");
    assert_eq!(client_ids(&orders).len(), 4);
    assert_eq!(state.trade.trade_slices.len(), 1);
    assert_eq!(state.trade.trade_leg_index, Some(1));
}

fn limits(orders: &Orders) -> Vec<f64> {
    orders
        .lock()
        .unwrap()
        .iter()
        .map(|order| order.price_limit.expect("every order has a limit").max_slippage_bps)
        .collect()
}

/// After the empty fills at the cap, each order widens the limit by one step
/// up to the maximum, then the leg pauses. The next cycle starts at the cap again.
#[tokio::test]
async fn empty_fills_widen_the_limit_in_steps_up_to_the_maximum() {
    let (finalizer, orders) = finalizer_with_fills(vec![Some(0.0); 5].into_iter().chain([None]).collect());
    let finalizer = finalizer.with_widened_slippage_cap(TEST_MAX_SELL_SLIPPAGE_BPS + 200.0);
    let mut state = trade_state(&finalizer).await;

    finalizer
        .trade(&mut state)
        .await
        .expect("a pause is a wait, not an error");

    let cap = room(TEST_MAX_SELL_SLIPPAGE_BPS);
    let widened = |step: f64| room(TEST_MAX_SELL_SLIPPAGE_BPS + step);
    assert_eq!(limits(&orders), vec![cap, cap, cap, widened(100.0), widened(200.0)]);
    assert!(state.trade.trade_slices.is_empty());

    finalizer.trade(&mut state).await.expect("the next cycle fills");
    assert_eq!(limits(&orders)[5], cap, "a new cycle starts at the cap");
    assert_eq!(state.trade.trade_slices.len(), 1);
}

/// A fill at a widened limit passes the check for that limit, and the next
/// order is back at the cap.
#[tokio::test]
async fn a_fill_at_a_widened_limit_resets_the_limit_to_the_cap() {
    let (finalizer, orders) = finalizer_with_fills(vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0001), None]);
    let finalizer = finalizer.with_widened_slippage_cap(TEST_MAX_SELL_SLIPPAGE_BPS + 200.0);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the leg fills");

    let cap = room(TEST_MAX_SELL_SLIPPAGE_BPS);
    assert_eq!(
        limits(&orders),
        vec![cap, cap, cap, room(TEST_MAX_SELL_SLIPPAGE_BPS + 100.0), cap]
    );
    assert_eq!(state.trade.trade_slices.len(), 2);
}

#[test]
fn a_maximum_below_the_cap_leaves_the_cap_in_force() {
    let (finalizer, _) = finalizer_with_fills(vec![]);
    let finalizer = finalizer.with_widened_slippage_cap(TEST_MAX_SELL_SLIPPAGE_BPS - 50.0);
    assert_eq!(finalizer.max_widened_slippage_bps, TEST_MAX_SELL_SLIPPAGE_BPS);
}

/// Review finding 24: the fee comes off a buy's base output, so a buy that
/// filled at a limit of "cap less fee" failed the cap check net of the fee.
#[test]
fn a_fill_at_the_price_room_meets_the_limit_net_of_the_fee() {
    type F = CexVenueFinalizer<MockCexBackend>;
    let (mid, limit_bps, fee_bps) = (100.0, 200.0, 80.0);
    let fee = fee_bps / 10_000.0;
    for side in ["buy", "sell"] {
        let room = F::price_room_bps(side, limit_bps, fee_bps) / 10_000.0;
        let (limit_price, exec_price) = if side == "buy" {
            let price = mid * (1.0 + room);
            (price, price / (1.0 - fee))
        } else {
            let price = mid * (1.0 - room);
            (price, price * (1.0 - fee))
        };
        let slippage = F::slice_slippage_bps(side, mid, exec_price);
        assert!(slippage < limit_bps, "{side} at {limit_price}: {slippage} bps");
        assert!(
            slippage > limit_bps - 0.5,
            "{side} keeps most of its room: {slippage} bps"
        );
    }
}

/// Review finding 23: only the trade step compared the cap with the fee, so a
/// leg deposited and then never traded.
#[tokio::test]
async fn a_quote_refuses_a_cap_the_fee_uses_up() {
    let (finalizer, orders) = finalizer_with_fee_and_fills(TEST_MAX_SELL_SLIPPAGE_BPS, vec![]);
    let legs = [TradeLeg {
        market: "CKBTC_BTC".to_string(),
        side: "sell".to_string(),
    }];

    let error = finalizer
        .preview_resolved_trade_route(&legs, 0.01)
        .await
        .err()
        .expect("no price room");

    assert!(error.contains("taker fee"), "{error}");
    assert!(orders.lock().unwrap().is_empty());
}

/// A fee as large as the limit leaves the order no price room, so no order goes out.
#[tokio::test]
async fn a_limit_the_fee_uses_up_sends_no_order() {
    let (finalizer, orders) = finalizer_with_fee_and_fills(TEST_MAX_SELL_SLIPPAGE_BPS, vec![]);
    let mut state = trade_state(&finalizer).await;

    let error = finalizer.trade(&mut state).await.expect_err("no price room");

    assert!(error.to_string().contains("taker fee"), "{error}");
    assert!(orders.lock().unwrap().is_empty());
}
