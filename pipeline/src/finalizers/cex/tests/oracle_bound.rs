//! DEV-3463: the planner checks a quote against the oracle, but the slices
//! trade later on a fresh book. Every slice order is held to the oracle too.
use super::*;
use crate::price_oracle::price_oracle::MockPriceOracle;
use liquidium_pipeline_connectors::backend::cex_backend::{PriceLimit, SwapExecutionOptions};

type Orders = Arc<std::sync::Mutex<Vec<SwapExecutionOptions>>>;
type F = CexVenueFinalizer<MockCexBackend>;

const FEE_BPS: f64 = 10.0;
const DISCOUNT_BPS: u32 = 250;

/// A RAY price, from hundredths of a dollar.
fn ray(cents: u128) -> Nat {
    Nat::from(cents * 10u128.pow(25))
}

/// An oracle that prices each symbol from `prices` in cents, or fails on any other.
fn oracle(prices: &'static [(&'static str, u128)]) -> Arc<MockPriceOracle> {
    let mut oracle = MockPriceOracle::new();
    oracle.expect_get_price().returning(move |symbol, quote| {
        assert_eq!(quote, "USDT");
        prices
            .iter()
            .find(|(name, _)| *name == symbol)
            .map(|(_, cents)| (ray(*cents), 27))
            .ok_or_else(|| format!("no {symbol} price"))
    });
    Arc::new(oracle)
}

/// Oracle prices, in cents, for the assets the ckBTC -> ckUSDT route trades
/// after its first leg, so route admission can price every market.
const ROUTE_PRICES: &[(&str, u128)] = &[("BTC", 10_000_000), ("USDC", 100)];

/// A ckBTC -> ckUSDT finalizer whose first leg, CKBTC_BTC, has `bids`, and
/// whose orders each fill whole at `fill_price` net. CKBTC and BTC are both
/// BTC to the oracle, so that market prices at 1 without asking it.
fn finalizer_with_book(bids: Vec<(f64, f64)>, fill_price: f64) -> (F, Orders) {
    let mut backend = MockCexBackend::new();
    let book = OrderBook {
        bids: bids
            .into_iter()
            .map(|(price, quantity)| OrderBookLevel { price, quantity })
            .collect(),
        asks: vec![OrderBookLevel {
            price: 1.1,
            quantity: 1_000.0,
        }],
        taker_fee_bps: FEE_BPS,
    };
    backend.expect_get_orderbook().returning(move |_, _| Ok(book.clone()));
    let orders: Orders = Arc::default();
    let seen = orders.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .returning(move |market, _, amount_in, options| {
            assert_eq!(market, "CKBTC_BTC");
            seen.lock().unwrap().push(options);
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * fill_price,
            })
        });
    let finalizer = test_finalizer(Arc::new(backend), Arc::new(MockTransferActions::new()))
        .with_price_oracle(oracle(ROUTE_PRICES), DISCOUNT_BPS);
    (finalizer, orders)
}

fn finalizer_with_bid(bid: f64) -> (F, Orders) {
    finalizer_with_book(vec![(bid, 1_000.0)], bid * 0.9995)
}

async fn trade_state(finalizer: &F) -> CexState {
    let mut state = finalizer.prepare("42", &make_execution_receipt(42)).await.unwrap();
    state.step = CexStep::Trade;
    state
}

fn sent_limits(orders: &Orders) -> Vec<PriceLimit> {
    orders
        .lock()
        .unwrap()
        .iter()
        .map(|order| order.price_limit.expect("every order has a limit"))
        .collect()
}

/// The order price that leaves the first leg's bound, 0.975 at an oracle
/// price of 1, after the fee and the fill rounding margin.
fn oracle_order_price() -> f64 {
    0.975 / (1.0 - FEE_BPS / 10_000.0) * (1.0 + 0.1 / 10_000.0)
}

#[tokio::test]
async fn a_book_repriced_below_the_oracle_holds_before_any_order() {
    // The whole book sits 5% under the oracle: the slice impact is zero, and
    // the IOC limit and the slippage check would both measure from this book.
    let (finalizer, orders) = finalizer_with_bid(0.95);
    let mut state = trade_state(&finalizer).await;

    finalizer
        .trade(&mut state)
        .await
        .expect("a hold is a wait, not an error");

    assert!(orders.lock().unwrap().is_empty(), "no order goes out");
    assert!(state.trade.trade_slices.is_empty());
    assert_eq!(state.trade.trade_leg_index.unwrap_or(0), 0, "the leg did not advance");
    assert!(state.trade.trade_oracle_hold_since_ts.is_some());
    let error = state.last_error.clone().expect("the hold says why");
    assert!(error.contains("oracle"), "{error}");
}

#[tokio::test]
async fn a_hold_that_lasts_parks_for_an_operator() {
    let (finalizer, orders) = finalizer_with_bid(0.95);
    let mut state = trade_state(&finalizer).await;
    state.trade.trade_oracle_hold_since_ts = Some(now_ts() - 31 * 60);

    let error = finalizer.trade(&mut state).await.expect_err("an operator decides");

    assert!(matches!(error, CexBackendError::OperatorRequired(_)), "{error:?}");
    assert!(matches!(
        crate::finalizers::multi_venue::VenueStepError::from(error),
        crate::finalizers::multi_venue::VenueStepError::OperatorRequired(_)
    ));
    assert!(orders.lock().unwrap().is_empty());

    // A release starts the hold again, so the leg waits another full timeout.
    use crate::finalizers::multi_venue::VenueLeg;
    finalizer.after_release(&mut state);
    finalizer.trade(&mut state).await.expect("a released hold waits again");
    assert!(orders.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_oracle_bound_tighter_than_the_cap_prices_the_order() {
    // 1% under the oracle: the 2% cap would allow a fill 3% under it.
    let (finalizer, orders) = finalizer_with_bid(0.99);
    let mut state = trade_state(&finalizer).await;
    state.trade.trade_oracle_hold_since_ts = Some(now_ts() - 60);

    finalizer.trade(&mut state).await.expect("the leg fills");

    let limits = sent_limits(&orders);
    assert_eq!(limits.len(), 1);
    assert!(
        (limits[0].reference_price - oracle_order_price()).abs() < 1e-12,
        "{limits:?}"
    );
    assert_eq!(limits[0].max_slippage_bps, 0.0);
    assert_eq!(
        state.trade.trade_oracle_hold_since_ts, None,
        "a tradable book ends the hold"
    );
    assert_eq!(state.trade.trade_leg_index, Some(1));
    // The leg kept 0.99 * 0.9995 of oracle value; later legs get what is left.
    let kept = state.trade.trade_oracle_route_keep.expect("the leg's share is carried");
    assert!((kept - 0.99 * 0.9995).abs() < 1e-9, "{kept}");
}

#[tokio::test]
async fn earlier_legs_use_up_part_of_the_discount() {
    // Alone this book trades; after a leg that kept 99% it is past the bound.
    let (finalizer, orders) = finalizer_with_bid(0.98);
    let mut state = trade_state(&finalizer).await;
    state.trade.trade_oracle_route_keep = Some(0.99);

    finalizer.trade(&mut state).await.expect("a hold is a wait");

    assert!(orders.lock().unwrap().is_empty());
    assert!(state.trade.trade_oracle_hold_since_ts.is_some());
}

#[tokio::test]
async fn a_thin_top_level_still_trades_at_the_oracle_price() {
    // The slice average falls under the bound, but the best bid clears it,
    // so the order goes out at the oracle price instead of holding.
    let (finalizer, orders) = finalizer_with_book(vec![(1.0, 0.003), (0.968, 1_000.0)], 0.9995);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the slices fill");

    let limits = sent_limits(&orders);
    assert!(!limits.is_empty());
    assert!(
        (limits[0].reference_price - oracle_order_price()).abs() < 1e-12,
        "{limits:?}"
    );
    assert_eq!(state.trade.trade_oracle_hold_since_ts, None);
}

#[tokio::test]
async fn a_book_above_the_oracle_keeps_the_cap() {
    let (finalizer, orders) = finalizer_with_bid(1.02);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the leg fills");

    let cap = F::price_room_bps("sell", TEST_MAX_SELL_SLIPPAGE_BPS, FEE_BPS);
    let limits = sent_limits(&orders);
    assert_eq!(limits.len(), 1);
    assert_eq!(limits[0].max_slippage_bps, cap);
    assert_eq!(limits[0].reference_price, state.trade.trade_slices[0].mid_price);
}

#[tokio::test]
async fn a_market_order_fill_past_the_bound_stops_the_leg() {
    // An exchange that refuses the limit order type fills at market instead,
    // here 3% under the oracle: inside the cap from the book, past the bound.
    let (finalizer, _) = finalizer_with_book(vec![(0.98, 1_000.0)], 0.97);
    let mut state = trade_state(&finalizer).await;

    let error = finalizer
        .trade(&mut state)
        .await
        .expect_err("the fill breached the bound");

    // A retry would find no input left and carry the fill on, so the leg parks.
    assert!(matches!(error, CexBackendError::OperatorRequired(_)), "{error:?}");
    assert!(error.to_string().contains("oracle bound"), "{error}");
    assert_eq!(state.trade.trade_slices.len(), 1, "the breaching fill is recorded");
}

#[tokio::test]
async fn a_leg_that_beat_the_oracle_leaves_the_next_more_room() {
    // 2% over the oracle: the planner admitted the route on its end-to-end
    // value, so the next leg may use the extra 2%.
    let (finalizer, _) = finalizer_with_bid(1.02);
    let mut state = trade_state(&finalizer).await;

    finalizer.trade(&mut state).await.expect("the leg fills");

    let kept = state.trade.trade_oracle_route_keep.expect("the leg's share is carried");
    assert!((kept - 1.02 * 0.9995).abs() < 1e-9, "{kept}");
}

#[tokio::test]
async fn a_row_saved_before_the_bound_spends_no_room_twice() {
    let (finalizer, _) = finalizer_with_bid(1.0);
    let mut state = trade_state(&finalizer).await;

    // A later leg of an old row: its finished legs spent the whole discount.
    finalizer.adopt_legacy_oracle_record(&mut state, 1, 0.0);
    assert_eq!(state.trade.trade_oracle_route_keep, Some(0.975));
    assert_eq!(state.trade.trade_progress_oracle_out, None);

    // The first leg, part filled before the upgrade: the old fills count at the oracle price.
    let mut state = trade_state(&finalizer).await;
    finalizer.adopt_legacy_oracle_record(&mut state, 0, 5.0);
    assert_eq!(state.trade.trade_oracle_route_keep, None);
    assert_eq!(state.trade.trade_progress_oracle_out, Some(5.0));

    // A row with its own record keeps it.
    state.trade.trade_oracle_route_keep = Some(0.99);
    finalizer.adopt_legacy_oracle_record(&mut state, 1, 5.0);
    assert_eq!(state.trade.trade_oracle_route_keep, Some(0.99));
    assert_eq!(state.trade.trade_progress_oracle_out, Some(5.0));
}

#[tokio::test]
async fn a_route_the_oracle_cannot_price_is_not_admitted() {
    let (finalizer, _) = finalizer_with_bid(1.0);
    let leg = |market: &str| TradeLeg {
        market: market.to_string(),
        side: "sell".to_string(),
    };

    // CKBTC_BTC prices at 1 without the oracle, so it is admitted.
    finalizer
        .preview_resolved_trade_route(&[leg("CKBTC_BTC")], 1.0)
        .await
        .expect("a same-asset market needs no oracle price");
    let error = finalizer
        .preview_resolved_trade_route(&[leg("CKBTC_BTC"), leg("BTC_USD")], 1.0)
        .await
        .err()
        .expect("the oracle has no BTC or USD price");

    assert!(error.contains("price oracle cannot bound BTC_USD"), "{error}");
}

#[tokio::test]
async fn an_oracle_that_cannot_answer_is_a_wait() {
    let (finalizer, _) = finalizer_with_bid(1.0);
    let leg = TradeLeg {
        market: "ICP_USDT".to_string(),
        side: "sell".to_string(),
    };

    let error = finalizer.oracle_price_bound(&leg, 1.0).await.expect_err("no ICP price");

    assert!(matches!(error, CexBackendError::Unreachable(_)), "{error:?}");
}

#[tokio::test]
async fn a_buy_bound_prices_the_market_from_both_assets() {
    let finalizer = finalizer_with_bid(1.0)
        .0
        .with_price_oracle(oracle(&[("ICP", 500), ("USDC", 50)]), DISCOUNT_BPS);
    let buy = |market: &str| TradeLeg {
        market: market.to_string(),
        side: "buy".to_string(),
    };

    // A first leg may pay up to 5 / 0.975 USDT per ICP; USDT prices at 1 unasked.
    let (price, bound) = finalizer
        .oracle_price_bound(&buy("ICP_USDT"), 1.0)
        .await
        .unwrap()
        .unwrap();
    assert!((price - 5.0).abs() < 1e-9, "{price}");
    assert!((bound - 5.0 / 0.975).abs() < 1e-9, "{bound}");
    // After a leg that kept 99%, less is left; a chain-key symbol prices as its underlying.
    let (_, bound) = finalizer
        .oracle_price_bound(&buy("CKICP_USDC"), 0.99)
        .await
        .unwrap()
        .unwrap();
    assert!((bound - 10.0 / (0.975 / 0.99)).abs() < 1e-9, "{bound}");
}
