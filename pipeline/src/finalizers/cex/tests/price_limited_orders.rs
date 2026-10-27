//! Audit S3: the exchange enforces the slippage cap on every slice order.
use super::*;
use liquidium_pipeline_connectors::backend::cex_backend::{PriceLimit, SwapExecutionOptions};

type Orders = Arc<std::sync::Mutex<Vec<SwapExecutionOptions>>>;

/// A ckBTC -> ckUSDT finalizer whose first leg, CKBTC_BTC, answers each
/// order with the next fill; `None` is a fill of the whole request.
fn finalizer_with_fills(fills: Vec<Option<f64>>) -> (CexVenueFinalizer<MockCexBackend>, Orders) {
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
        taker_fee_bps: 10.0,
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

fn client_ids(orders: &Orders) -> Vec<String> {
    orders
        .lock()
        .unwrap()
        .iter()
        .map(|o| o.client_order_id.clone().unwrap())
        .collect()
}

#[tokio::test]
async fn every_slice_order_carries_the_cap_as_its_limit() {
    let (finalizer, orders) = finalizer_with_fills(vec![None]);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the leg fills");

    let orders = orders.lock().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].price_limit,
        Some(PriceLimit {
            reference_price: state.trade.trade_slices[0].mid_price,
            max_slippage_bps: TEST_MAX_SELL_SLIPPAGE_BPS,
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
