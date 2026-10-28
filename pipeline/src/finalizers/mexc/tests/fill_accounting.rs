//! Each MEXC order counts once. A reused client order id makes MEXC return
//! the original fill, so a counted id must never go to MEXC again.
use super::*;

/// A sell book and a fill ledger that answer a known id with its old fill,
/// the way the adapter recovers a duplicate submission.
#[derive(Default)]
struct Exchange {
    requests: Vec<(String, f64)>,
    fills: HashMap<String, (f64, f64)>,
}

impl Exchange {
    fn consumed(&self) -> f64 {
        self.fills.values().map(|fill| fill.0).sum()
    }

    fn output(&self) -> f64 {
        self.fills.values().map(|fill| fill.1).sum()
    }
}

/// The first book allows a 4-unit slice at 1.0. After that order the book
/// sits at 0.75, so the first fill breaches the cap and later ones do not.
fn finalizer(
    exchange: Arc<std::sync::Mutex<Exchange>>,
    first_fill: Option<(f64, f64)>,
    calls: usize,
) -> MexcFinalizer<MockCexBackend> {
    let mut backend = MockCexBackend::new();
    let book = exchange.clone();
    backend.expect_get_orderbook().returning(move |market, _| {
        assert_eq!(market, "BTC_USDC");
        let first = book.lock().unwrap().requests.is_empty();
        Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: if first { 1.0 } else { 0.75 },
                quantity: if first { 4.0 } else { 100.0 },
            }],
            asks: vec![],
            taker_fee_bps: 10.0,
        })
    });
    backend
        .expect_execute_swap_detailed_with_options()
        .times(calls)
        .returning(move |market, side, requested, options| {
            assert_eq!((market, side), ("BTC_USDC", "sell"));
            let id = options.client_order_id.expect("every slice carries a client order id");
            let mut exchange = exchange.lock().unwrap();
            let first = exchange.requests.is_empty();
            exchange.requests.push((id.clone(), requested));
            let fill = *exchange.fills.entry(id).or_insert_with(|| match (first, first_fill) {
                (true, Some(fill)) => fill,
                _ => (requested, requested * 0.75),
            });
            Ok(SwapFillReport {
                input_consumed: fill.0,
                output_received: fill.1,
            })
        });
    test_finalizer(Arc::new(backend), Arc::new(MockTransferActions::new()))
}

fn leg() -> TradeLeg {
    TradeLeg {
        market: "BTC_USDC".into(),
        side: "sell".into(),
    }
}

async fn trade_state(finalizer: &MexcFinalizer<MockCexBackend>) -> CexState {
    let mut state = finalizer
        .prepare("fill-accounting", &make_execution_receipt(42))
        .await
        .unwrap();
    state.step = CexStep::Trade;
    state
}

fn restart(state: &CexState) -> CexState {
    serde_json::from_str(&serde_json::to_string(state).unwrap()).unwrap()
}

/// The reported totals must match the fills MEXC really made.
fn assert_totals_match_exchange(remaining: f64, output: f64, exchange: &Exchange) {
    assert!((output - exchange.output()).abs() < 1e-6, "output {output} vs {}", exchange.output());
    assert!(
        (remaining - (10.0 - exchange.consumed())).abs() < 1e-6,
        "remaining {remaining} vs {}",
        10.0 - exchange.consumed()
    );
}

#[tokio::test]
async fn an_adverse_slice_counts_once_and_its_id_is_spent() {
    let exchange = Arc::new(std::sync::Mutex::new(Exchange::default()));
    let finalizer = finalizer(exchange.clone(), None, 2);
    let mut state = trade_state(&finalizer).await;

    let error = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect_err("the 4 -> 3 fill breaches the 2% cap");

    assert!(error.to_string().contains("slice slippage too high"), "{error}");
    assert!((state.trade.trade_progress_remaining_in.unwrap() - 6.0).abs() < 1e-6);
    assert!((state.trade.trade_progress_total_out.unwrap() - 3.0).abs() < 1e-6);
    assert_eq!(state.trade.trade_slices.len(), 1, "the breaching slice is recorded");
    assert!(state.trade.trade_slices[0].slippage_bps > TEST_MAX_SELL_SLIPPAGE_BPS);
    let first_id = exchange.lock().unwrap().requests[0].0.clone();
    assert_eq!(state.trade.trade_applied_client_order_ids, vec![first_id.clone()]);
    assert!(state.trade.trade_pending_client_order_id.is_none());

    let mut state = restart(&state);
    let (remaining, output) = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect("the residual sells inside the cap")
        .expect("the leg finishes");

    let exchange = exchange.lock().unwrap();
    assert_eq!(exchange.requests.len(), 2);
    assert_ne!(exchange.requests[1].0, first_id, "a spent id never goes to MEXC again");
    assert!((exchange.requests[1].1 - 6.0).abs() < 1e-6);
    assert_totals_match_exchange(remaining, output, &exchange);
    assert!((output - 7.5).abs() < 1e-6);
    assert_eq!(state.trade.trade_slices.len(), 2);
}

#[tokio::test]
async fn a_fill_that_fails_its_price_check_still_spends_its_id() {
    let exchange = Arc::new(std::sync::Mutex::new(Exchange::default()));
    let finalizer = finalizer(exchange.clone(), Some((4.0, 0.0)), 2);
    let mut state = trade_state(&finalizer).await;

    let error = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect_err("zero output has no execution price");

    assert!(error.to_string().contains("invalid execution price"), "{error}");
    assert!((state.trade.trade_progress_remaining_in.unwrap() - 6.0).abs() < 1e-6);
    assert_eq!(state.trade.trade_applied_client_order_ids.len(), 1);

    let mut state = restart(&state);
    let (remaining, output) = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect("the residual sells")
        .expect("the leg finishes");

    let exchange = exchange.lock().unwrap();
    assert_ne!(exchange.requests[0].0, exchange.requests[1].0);
    assert_totals_match_exchange(remaining, output, &exchange);
    assert!((output - 4.5).abs() < 1e-6);
}

#[tokio::test]
async fn a_lost_reply_resumes_its_pending_id_once() {
    let exchange = Arc::new(std::sync::Mutex::new(Exchange::default()));
    // The order went out before the restart; only its reply was lost.
    let pending_id = "liq-fill-accounting-l1-s1-a".to_string();
    exchange.lock().unwrap().fills.insert(pending_id.clone(), (4.0, 3.0));
    let finalizer = finalizer(exchange.clone(), None, 2);
    let mut state = trade_state(&finalizer).await;
    state.trade.trade_pending_client_order_id = Some(pending_id.clone());
    state.trade.trade_pending_market = Some("BTC_USDC".into());
    state.trade.trade_pending_side = Some("sell".into());
    state.trade.trade_pending_requested_in = Some(4.0);
    // The book has already moved to the price of the lost order.
    exchange.lock().unwrap().requests.push(("before-restart".into(), 0.0));

    let mut state = restart(&state);
    let (remaining, output) = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect("the recovered fill and the residual both count")
        .expect("the leg finishes");

    let exchange = exchange.lock().unwrap();
    let sent: Vec<&str> = exchange.requests[1..].iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(sent[0], pending_id, "the pending id is resumed, not replaced");
    assert_ne!(sent[1], pending_id);
    assert_totals_match_exchange(remaining, output, &exchange);
}

#[tokio::test]
async fn a_pending_id_that_is_already_counted_is_replaced() {
    let exchange = Arc::new(std::sync::Mutex::new(Exchange::default()));
    let finalizer = finalizer(exchange.clone(), None, 1);
    let mut state = trade_state(&finalizer).await;
    let spent = "liq-fill-accounting-l1-s1-a".to_string();
    state.trade.trade_applied_client_order_ids = vec![spent.clone()];
    state.trade.trade_pending_client_order_id = Some(spent.clone());
    state.trade.trade_pending_market = Some("BTC_USDC".into());
    state.trade.trade_pending_side = Some("sell".into());
    state.trade.trade_pending_requested_in = Some(9.0);
    state.trade.trade_progress_remaining_in = Some(4.0);
    state.trade.trade_progress_total_out = Some(4.5);
    exchange.lock().unwrap().requests.push(("earlier-slices".into(), 0.0));

    let (remaining, output) = finalizer
        .execute_trade_leg_slices(&mut state, &leg(), 0, 1, 10.0, 140.0)
        .await
        .expect("the residual sells under a fresh id")
        .expect("the leg finishes");

    let exchange = exchange.lock().unwrap();
    assert_ne!(exchange.requests[1].0, spent);
    assert!((exchange.requests[1].1 - 4.0).abs() < 1e-6, "the stale request size is dropped");
    assert!(remaining.abs() < 1e-6);
    assert!((output - 7.5).abs() < 1e-6);
}
