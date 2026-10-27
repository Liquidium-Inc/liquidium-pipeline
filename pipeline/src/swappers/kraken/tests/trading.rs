//! Orders: IOC at the limit, the buy sizing, the fill report, and the error kinds.
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackend, CexBackendError, PriceLimit, SwapExecutionOptions,
};
use rust_decimal::Decimal;

use super::support::{btc_usd_client, btc_usd_pair, level, one_coin_book};
use crate::swappers::kraken::{
    api::{KrakenApiError, KrakenBook, KrakenOrder, KrakenOrderStatus, KrakenSide, MockKrakenApi},
    trading::{
        ORDER_POLL_ATTEMPTS, TAKER_FEE_BPS, estimate_buy_cost, estimate_sell_output, fill_report, ioc_limit_price,
    },
};

fn closed(volume_executed: Decimal, cost: Decimal, fee: Decimal) -> KrakenOrder {
    KrakenOrder {
        status: KrakenOrderStatus::Closed,
        volume_executed,
        cost,
        fee,
    }
}

/// A 2% limit from a 60,000 mid.
fn ioc(client_order_id: &str) -> SwapExecutionOptions {
    SwapExecutionOptions {
        client_order_id: Some(client_order_id.into()),
        price_limit: Some(PriceLimit {
            reference_price: 60_000.0,
            max_slippage_bps: 200.0,
        }),
        ..SwapExecutionOptions::default()
    }
}

/// A client whose pairs, book and client-id lookup answer as a fresh order expects.
fn trading_api() -> MockKrakenApi {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_orderbook().returning(|_, _| Ok(one_coin_book()));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api
}

#[tokio::test]
async fn the_order_book_carries_the_measured_taker_fee() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_orderbook()
        .withf(|pair, depth| pair == "XXBTZUSD" && *depth == 25)
        .once()
        .returning(|_, _| Ok(one_coin_book()));

    let book = btc_usd_client(api).get_orderbook("btc/usd", Some(25)).await.unwrap();

    assert_eq!(book.bids[0].price, 60_000.0);
    assert_eq!(book.asks[0].quantity, 1.0);
    assert_eq!(book.taker_fee_bps, TAKER_FEE_BPS);
}

#[test]
fn the_ioc_price_rounds_toward_the_inside_of_the_limit() {
    let limit = PriceLimit {
        reference_price: 60_000.05,
        max_slippage_bps: 200.0,
    };
    // 58,800.049: a sell rounds up, so it never accepts less than the limit.
    assert_eq!(
        ioc_limit_price(KrakenSide::Sell, &limit, 1).unwrap(),
        Decimal::new(588_001, 1)
    );
    // 61,200.051: a buy rounds down, so it never pays more than the limit.
    assert_eq!(
        ioc_limit_price(KrakenSide::Buy, &limit, 1).unwrap(),
        Decimal::new(612_000, 1)
    );
}

#[tokio::test]
async fn an_ioc_sell_carries_the_limit_and_the_client_id_and_reports_the_net_fill() {
    let mut api = trading_api();
    api.expect_place_order()
        .withf(|request| {
            request.pair == "XXBTZUSD"
                && request.side == KrakenSide::Sell
                && request.volume == Decimal::new(1, 1)
                && request.limit_price == Some(Decimal::new(58_800, 0))
                && request.client_order_id.as_deref() == Some("liq-42")
        })
        .once()
        .returning(|_| Ok("ORDER-1".into()));
    api.expect_order()
        .withf(|id| id == "ORDER-1")
        .once()
        .returning(|_| Ok(closed(Decimal::new(1, 1), Decimal::new(6_000, 0), Decimal::new(48, 0))));

    let report = btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("liq-42"))
        .await
        .expect("filled sell");

    assert_eq!(report.input_consumed, 0.1);
    // 6,000 less the 48 fee, less the margin that keeps a fill under what Kraken credited.
    assert!(
        report.output_received <= 5_952.0 && report.output_received > 5_952.0 * (1.0 - 1e-5),
        "{}",
        report.output_received
    );
}

/// Every unit fills at or under the limit, so `budget / limit` never spends
/// more than the budget.
#[tokio::test]
async fn an_ioc_buy_never_spends_more_than_its_budget() {
    let mut api = trading_api();
    // 6,000 / 61,200 = 0.098039215..., cut to 8 decimals.
    api.expect_place_order()
        .withf(|request| {
            request.side == KrakenSide::Buy
                && request.volume == Decimal::new(9_803_921, 8)
                && request.limit_price == Some(Decimal::new(61_200, 0))
        })
        .once()
        .returning(|_| Ok("ORDER-BUY".into()));
    api.expect_order().once().returning(|_| {
        Ok(closed(
            Decimal::new(9_803_921, 8),
            Decimal::new(58_823_526, 4),
            Decimal::new(470_588, 4),
        ))
    });

    let report = btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "buy", 6_000.0, ioc("liq-buy"))
        .await
        .expect("filled buy");

    assert!(report.input_consumed <= 6_000.0, "{}", report.input_consumed);
    // The fee comes out of the bought base: 80 bps of 0.09803921.
    let credited = 0.098_039_21 * 0.992;
    assert!(
        report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
        "{} against {credited}",
        report.output_received
    );
}

#[tokio::test]
async fn an_ioc_order_that_fills_nothing_reports_an_empty_fill() {
    let mut api = trading_api();
    api.expect_place_order().once().returning(|_| Ok("ORDER-EMPTY".into()));
    api.expect_order().once().returning(|_| {
        Ok(KrakenOrder {
            status: KrakenOrderStatus::Canceled,
            volume_executed: Decimal::ZERO,
            cost: Decimal::ZERO,
            fee: Decimal::ZERO,
        })
    });

    let report = btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("liq-empty"))
        .await
        .expect("an empty IOC fill is an answer, not an error");

    assert_eq!((report.input_consumed, report.output_received), (0.0, 0.0));
}

#[tokio::test]
async fn a_sell_below_the_cost_minimum_is_refused_before_submission() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api.expect_orderbook().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![level(10, Decimal::ONE)],
            asks: vec![],
        })
    });
    api.expect_place_order().never();

    let error = btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("liq-small"))
        .await
        .expect_err("below the cost minimum");

    assert!(matches!(error, CexBackendError::BelowMinimum(_)), "{error}");
}

#[tokio::test]
async fn a_market_buy_sizes_its_volume_from_the_asks() {
    let mut api = trading_api();
    api.expect_place_order()
        .withf(|request| {
            request.side == KrakenSide::Buy && request.volume == Decimal::new(1, 1) && request.limit_price.is_none()
        })
        .once()
        .returning(|_| Ok("ORDER-MARKET".into()));
    api.expect_order()
        .once()
        .returning(|_| Ok(closed(Decimal::new(1, 1), Decimal::new(6_000, 0), Decimal::new(48, 0))));

    let report = btc_usd_client(api)
        .execute_swap_detailed_with_options(
            "BTC_USD",
            "buy",
            6_000.0,
            SwapExecutionOptions {
                client_order_id: Some("liq-market".into()),
                buy_mode: BuyOrderInputMode::BaseQuantity,
                ..SwapExecutionOptions::default()
            },
        )
        .await
        .expect("filled buy");

    assert_eq!(report.input_consumed, 6_000.0);
}

/// `Decimal` panics on a division by zero, so a zero-priced ask is stepped
/// over and the order is sized on the depth it can buy.
#[tokio::test]
async fn a_zero_priced_ask_is_stepped_over() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api.expect_orderbook().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![],
            asks: vec![level(0, Decimal::ONE), level(60_000, Decimal::ONE)],
        })
    });
    api.expect_place_order()
        .withf(|request| request.volume == Decimal::new(1, 1))
        .once()
        .returning(|_| Ok("ORDER-BUY".into()));
    api.expect_order()
        .once()
        .returning(|_| Ok(closed(Decimal::new(1, 1), Decimal::new(6_000, 0), Decimal::new(48, 0))));

    btc_usd_client(api)
        .execute_swap_detailed("BTC_USD", "buy", 6_000.0)
        .await
        .expect("a malformed level must not stop a fillable buy");
}

/// A walk that counted a zero-priced level as free volume would report a
/// cost of zero, and a spend cap checked against zero passes any order.
#[test]
fn an_untradable_level_never_fills_volume_for_free() {
    let book = KrakenBook {
        bids: vec![level(0, Decimal::ONE), level(60_000, Decimal::ONE)],
        asks: vec![level(0, Decimal::ONE), level(60_000, Decimal::ONE)],
    };
    let half = Decimal::new(5, 1);
    assert_eq!(estimate_buy_cost(&book, half).unwrap(), Decimal::new(30_000, 0));
    assert_eq!(estimate_sell_output(&book, half).unwrap(), Decimal::new(30_000, 0));
    assert!(estimate_buy_cost(&book, Decimal::new(15, 1)).is_err());
    assert!(estimate_sell_output(&book, Decimal::new(15, 1)).is_err());
}

/// Liquidation 1634: Kraken reported the fee in USD and took it from the
/// ETH. The reported amount must be what can be withdrawn, or every
/// withdrawal of it is refused for insufficient funds.
#[test]
fn a_buy_reports_the_base_it_can_withdraw() {
    let order = closed(
        Decimal::new(3_532_234, 8), // 0.03532234 ETH executed
        Decimal::new(6_707_536, 5), // 67.07536 USD cost
        Decimal::new(5_366, 4),     // 0.5366 USD fee, taken in ETH
    );
    let report = fill_report(KrakenSide::Buy, &order).unwrap();

    // The account held 0.0350477408 that day, dust included.
    let credited = 0.035_039_762_8;
    assert!(
        report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
        "{} against {credited}",
        report.output_received
    );
    assert!((report.input_consumed - 67.075_36).abs() < 1e-9);
}

/// Liquidation 1615: a resumed leg holds the client id it sent, and Kraken
/// has that order filled. The fill is adopted, and no second order goes out.
#[tokio::test]
async fn a_resumed_leg_adopts_the_order_it_already_placed() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id()
        .withf(|id| id == "lqd39ae79815a051f7")
        .once()
        .returning(|_| {
            Ok(Some(closed(
                Decimal::new(1, 1),
                Decimal::new(6_000, 0),
                Decimal::new(48, 0),
            )))
        });
    api.expect_place_order().never();

    let report = btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("lqd39ae79815a051f7"))
        .await
        .expect("the fill is adopted");

    assert!((report.input_consumed - 0.1).abs() < 1e-9);
}

/// An earlier IOC order under the id filled nothing, so a new order is safe.
#[tokio::test]
async fn a_resumed_leg_whose_order_filled_nothing_sends_again() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_orderbook().returning(|_, _| Ok(one_coin_book()));
    api.expect_closed_order_by_client_id()
        .once()
        .returning(|_| Ok(Some(closed(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO))));
    api.expect_place_order().once().returning(|_| Ok("ORDER-2".into()));
    api.expect_order()
        .once()
        .returning(|_| Ok(closed(Decimal::new(1, 1), Decimal::new(6_000, 0), Decimal::new(48, 0))));

    btc_usd_client(api)
        .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("liq-again"))
        .await
        .expect("the new order fills");
}

#[tokio::test]
async fn each_submit_failure_keeps_its_kind() {
    let cases: [(KrakenApiError, fn(&CexBackendError) -> bool); 3] = [
        (KrakenApiError::Rejected("EOrder:Insufficient funds".into()), |error| {
            matches!(error, CexBackendError::Other(_))
        }),
        (KrakenApiError::Unreachable("connection refused".into()), |error| {
            matches!(error, CexBackendError::Unreachable(_))
        }),
        (KrakenApiError::Transport("operation timed out".into()), |error| {
            matches!(error, CexBackendError::Ambiguous(_))
        }),
    ];
    for (failure, expected) in cases {
        let mut api = trading_api();
        api.expect_place_order().once().return_once(move |_| Err(failure));
        let error = btc_usd_client(api)
            .execute_swap_detailed_with_options("BTC_USD", "sell", 0.1, ioc("liq-fail"))
            .await
            .expect_err("the submit failed");
        assert!(expected(&error), "{error:?}");
    }
}

/// Liquidation 1615: Kraken accepted the order, then could not show it on
/// the first read. The order had filled, so the read is retried.
#[tokio::test(start_paused = true)]
async fn a_briefly_unreadable_order_is_read_again() {
    let mut api = MockKrakenApi::new();
    let mut reads = 0;
    api.expect_order().times(2).returning(move |_| {
        reads += 1;
        if reads == 1 {
            return Err(KrakenApiError::Transport("order was not returned".into()));
        }
        Ok(closed(
            Decimal::new(748_710_104, 8),
            Decimal::new(1_563_307, 5),
            Decimal::new(12_506, 5),
        ))
    });

    let report = btc_usd_client(api)
        .wait_for_order("OJKJQE-YONE3-2QEYQ2", KrakenSide::Sell)
        .await
        .expect("a filled order is reported, not parked");

    assert!((report.input_consumed - 7.487_101_04).abs() < 1e-8);
}

/// An order that may have filled and never becomes readable goes to an operator.
#[tokio::test(start_paused = true)]
async fn an_order_that_never_becomes_readable_is_ambiguous() {
    let mut api = MockKrakenApi::new();
    api.expect_order()
        .times(ORDER_POLL_ATTEMPTS)
        .returning(|_| Err(KrakenApiError::Transport("status request timed out".into())));

    let error = btc_usd_client(api)
        .wait_for_order("ORDER-2", KrakenSide::Sell)
        .await
        .expect_err("never readable");

    assert!(matches!(error, CexBackendError::Ambiguous(_)), "{error}");
}
