//! Tests for `kraken_trading`.

use super::super::test_support::{btc_usd_client, btc_usd_pair};
use super::*;
use crate::swappers::kraken::kraken_api::{KrakenOrder, MockKrakenApi};

#[tokio::test]
async fn translates_pair_and_orderbook_without_exposing_kraken_types() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_orderbook()
        .withf(|pair, depth| pair == "XXBTZUSD" && *depth == 25)
        .once()
        .returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::new(2, 0),
                }],
                asks: vec![KrakenBookLevel {
                    price: Decimal::new(60_010, 0),
                    quantity: Decimal::new(3, 0),
                }],
            })
        });
    let client = btc_usd_client(api);

    let book = client
        .get_orderbook("btc/usd", Some(25))
        .await
        .expect("normalized order book");

    assert_eq!(book.bids[0].price, 60_000.0);
    assert_eq!(book.bids[0].quantity, 2.0);
    assert_eq!(book.asks[0].price, 60_010.0);
}

#[tokio::test]
async fn trade_preflight_rejects_pair_minimum_before_submission() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_place_market_order().never();
    let client = btc_usd_client(api);

    let error = client
        .validate_trade_amounts("BTC_USD", "sell", 0.000_01, 1.0)
        .await
        .expect_err("order minimum must reject preview");

    assert!(error.contains("below order minimum"));
}

#[tokio::test]
async fn market_sell_attaches_client_id_and_reports_net_fill() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api.expect_place_market_order()
        .withf(|pair, side, volume, client_id| {
            pair == "XXBTZUSD"
                && side == "sell"
                && *volume == Decimal::new(1, 1)
                && client_id.as_deref() == Some("liq-42")
        })
        .once()
        .returning(|_, _, _, _| Ok("ORDER-1".into()));
    api.expect_orderbook().once().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![KrakenBookLevel {
                price: Decimal::new(60_000, 0),
                quantity: Decimal::new(1, 0),
            }],
            asks: vec![],
        })
    });
    api.expect_order().withf(|id| id == "ORDER-1").once().returning(|_| {
        Ok(KrakenOrder {
            status: KrakenOrderStatus::Closed,
            volume_executed: Decimal::new(1, 1),
            cost: Decimal::new(6_000, 0),
            fee: Decimal::new(24, 0),
        })
    });
    let client = btc_usd_client(api);

    let report = client
        .execute_swap_detailed_with_options(
            "BTC_USD",
            "sell",
            0.1,
            SwapExecutionOptions {
                client_order_id: Some("liq-42".into()),
                ..SwapExecutionOptions::default()
            },
        )
        .await
        .expect("filled sell");

    assert_eq!(report.input_consumed, 0.1);
    // 6000 quote less the 24 fee, less the rounding margin that keeps a
    // reported fill from ever exceeding what the venue credited.
    assert!(
        report.output_received <= 5_976.0 && report.output_received > 5_976.0 * (1.0 - 1e-5),
        "a sell must report at most the quote the venue credited: {}",
        report.output_received
    );
}

#[tokio::test]
async fn sell_below_cost_minimum_is_rejected_before_submission() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_orderbook().once().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![KrakenBookLevel {
                price: Decimal::TEN,
                quantity: Decimal::ONE,
            }],
            asks: vec![],
        })
    });
    api.expect_place_market_order().never();
    let client = btc_usd_client(api);

    let error = client
        .execute_swap_detailed("BTC_USD", "sell", 0.1)
        .await
        .expect_err("below cost minimum");

    assert!(error.contains("below cost minimum"));
}

#[tokio::test]
async fn market_buy_derives_precision_safe_base_volume_from_quote_budget() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api.expect_orderbook().once().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![],
            asks: vec![KrakenBookLevel {
                price: Decimal::new(60_000, 0),
                quantity: Decimal::ONE,
            }],
        })
    });
    api.expect_place_market_order()
        .withf(|pair, side, volume, client_id| {
            pair == "XXBTZUSD"
                && side == "buy"
                && *volume == Decimal::new(9_960_159, 8)
                && client_id.as_deref() == Some("liq-buy-1")
        })
        .once()
        .returning(|_, _, _, _| Ok("ORDER-BUY".into()));
    api.expect_order().once().returning(|_| {
        Ok(KrakenOrder {
            status: KrakenOrderStatus::Closed,
            volume_executed: Decimal::new(9_960_159, 8),
            cost: Decimal::new(59_760_954, 4),
            fee: Decimal::new(239_043_816, 7),
        })
    });
    let client = btc_usd_client(api);

    let report = client
        .execute_swap_detailed_with_options(
            "BTC_USD",
            "buy",
            6_000.0,
            SwapExecutionOptions {
                client_order_id: Some("liq-buy-1".into()),
                buy_mode: BuyOrderInputMode::BaseQuantity,
                max_quote_overspend_bps: Some(0.0),
            },
        )
        .await
        .expect("filled buy");

    // The fee is 40 bps of the order, and a buy pays out base, so it is 40
    // bps of the executed volume that never reaches the balance -- not a
    // quote-side surcharge on top of what was spent.
    assert!((report.input_consumed - 5_976.095_4).abs() < 1e-9);
    let credited = 0.099_601_59 * 0.996;
    assert!(
        report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
        "a buy must report at most the base the venue credited: {} against {credited}",
        report.output_received
    );
}

/// Order-book levels are copied from Kraken's response without validation,
/// and `Decimal` panics rather than erring when asked to divide by zero. A
/// zero-priced ask would therefore take the whole process down mid-buy, so
/// the sizing walk has to step over it and size on the depth it can buy.
#[tokio::test]
async fn a_zero_priced_ask_is_stepped_over_rather_than_dividing_by_it() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id().returning(|_| Ok(None));
    api.expect_orderbook().once().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![],
            asks: vec![
                KrakenBookLevel {
                    price: Decimal::ZERO,
                    quantity: Decimal::ONE,
                },
                KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::ONE,
                },
            ],
        })
    });
    // Sized purely from the priced level, exactly as if the junk one were
    // not in the book at all.
    api.expect_place_market_order()
        .withf(|pair, side, volume, _| pair == "XXBTZUSD" && side == "buy" && *volume == Decimal::new(9_960_159, 8))
        .once()
        .returning(|_, _, _, _| Ok("ORDER-BUY".into()));
    api.expect_order().once().returning(|_| {
        Ok(KrakenOrder {
            status: KrakenOrderStatus::Closed,
            volume_executed: Decimal::new(9_960_159, 8),
            cost: Decimal::new(59_760_954, 4),
            fee: Decimal::new(239_043_816, 7),
        })
    });
    let client = btc_usd_client(api);

    client
        .execute_swap_detailed_with_options(
            "BTC_USD",
            "buy",
            6_000.0,
            SwapExecutionOptions {
                client_order_id: Some("liq-buy-zero".into()),
                buy_mode: BuyOrderInputMode::BaseQuantity,
                max_quote_overspend_bps: Some(0.0),
            },
        )
        .await
        .expect("a malformed level must not stop an otherwise fillable buy");
}

/// The sizing walk skips levels it cannot buy from, so the repricing that
/// checks the spend cap has to skip them too. Counting a zero-priced level
/// as volume filled for nothing reports a cost of zero, and a cap check
/// against zero passes whatever the order actually costs.
#[test]
fn an_untradable_level_never_fills_volume_for_free() {
    let book = KrakenBook {
        bids: vec![
            KrakenBookLevel {
                price: Decimal::ZERO,
                quantity: Decimal::ONE,
            },
            KrakenBookLevel {
                price: Decimal::new(60_000, 0),
                quantity: Decimal::ONE,
            },
        ],
        asks: vec![
            KrakenBookLevel {
                price: Decimal::ZERO,
                quantity: Decimal::ONE,
            },
            KrakenBookLevel {
                price: Decimal::new(60_000, 0),
                quantity: Decimal::ONE,
            },
        ],
    };

    // Priced against the real level, not absorbed by the junk one.
    assert_eq!(
        estimate_buy_cost(&book, Decimal::new(5, 1)).expect("half a coin is covered"),
        Decimal::new(30_000, 0)
    );
    assert_eq!(
        estimate_sell_output(&book, Decimal::new(5, 1)).expect("half a coin is covered"),
        Decimal::new(30_000, 0)
    );

    // And once the priced depth runs out, that is a liquidity failure rather
    // than a free fill.
    assert!(estimate_buy_cost(&book, Decimal::new(15, 1)).is_err());
    assert!(estimate_sell_output(&book, Decimal::new(15, 1)).is_err());
}

/// The shape that stranded liquidation 1634: Kraken reports the fee in USD,
/// takes it out of the ETH, and the leg then tries to withdraw what it
/// believes it received. The received amount must already be net, or the
/// withdrawal asks for more ETH than the account holds -- forever, since a
/// retry changes neither the balance nor the request.
#[test]
fn a_market_buy_reports_the_base_it_can_actually_withdraw() {
    let report = fill_report(
        "buy",
        Decimal::new(3_532_234, 8),   // 0.03532234 ETH executed
        Decimal::new(6_707_536, 5),   // 67.07536 USD cost
        Decimal::new(5_366, 4),       // 0.53660 USD fee, charged in ETH
    )
    .expect("buy fill");

    // What Kraken credited: 0.03532234 less the 0.80% it withheld in ETH.
    // The balance on the account that day was 0.0350477408, dust included.
    let credited = 0.035_039_762_8;
    assert!(
        report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
        "the withdrawal must never ask for more than this: {} against {credited}",
        report.output_received
    );
    assert!((report.input_consumed - 67.075_36).abs() < 1e-9);
}

/// Liquidation 1615's recovery path: the leg resumes still holding the
/// client id it submitted under, and Kraken has that order settled. It must
/// adopt the existing fill rather than sell the same collateral twice.
#[tokio::test]
async fn a_resumed_leg_adopts_the_order_it_already_submitted() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
    api.expect_closed_order_by_client_id()
        .withf(|id| id == "lqd39ae79815a051f7")
        .once()
        .returning(|_| {
            Ok(Some(KrakenOrder {
                status: KrakenOrderStatus::Closed,
                volume_executed: Decimal::new(1, 1),
                cost: Decimal::new(6_000, 0),
                fee: Decimal::new(24, 0),
            }))
        });
    // The point of the test: no order is placed on the resumed attempt.
    api.expect_place_market_order().never();
    api.expect_orderbook().returning(|_, _| {
        Ok(KrakenBook {
            bids: vec![KrakenBookLevel {
                price: Decimal::new(60_000, 0),
                quantity: Decimal::new(1, 0),
            }],
            asks: vec![],
        })
    });
    let client = btc_usd_client(api);

    let report = client
        .execute_swap_detailed_with_options(
            "BTC_USD",
            "sell",
            0.1,
            SwapExecutionOptions {
                client_order_id: Some("lqd39ae79815a051f7".into()),
                ..SwapExecutionOptions::default()
            },
        )
        .await
        .expect("the settled order must be adopted");

    assert!((report.input_consumed - 0.1).abs() < 1e-9);
    // 6000 quote less the 24 fee, less the rounding margin that keeps a
    // reported fill from ever exceeding what the venue credited.
    assert!(
        report.output_received <= 5_976.0 && report.output_received > 5_976.0 * (1.0 - 1e-5),
        "a sell must report at most the quote the venue credited: {}",
        report.output_received
    );
}

/// Liquidation 1615: Kraken accepted the order, returned its id, then could
/// not read it back on the first attempt. The order had in fact filled, so
/// bailing there parked a completed sell for an operator. The read must be
/// retried to its deadline instead.
#[tokio::test(start_paused = true)]
async fn a_briefly_unreadable_order_is_polled_rather_than_parked() {
    let mut api = MockKrakenApi::new();
    let mut reads = 0;
    api.expect_order().times(2).returning(move |_| {
        reads += 1;
        if reads == 1 {
            return Err(KrakenApiError::Transport(
                "order OJKJQE-YONE3-2QEYQ2 was not returned".into(),
            ));
        }
        Ok(KrakenOrder {
            status: KrakenOrderStatus::Closed,
            volume_executed: Decimal::new(748_710_104, 8),
            cost: Decimal::new(1_563_307, 5),
            fee: Decimal::new(12_506, 5),
        })
    });
    let client = btc_usd_client(api);

    let report = client
        .wait_for_order("OJKJQE-YONE3-2QEYQ2", "sell")
        .await
        .expect("a filled order must be reported, not parked");

    assert!((report.input_consumed - 7.48710104).abs() < 1e-8);
    assert!(report.output_received > 0.0);
}

/// A lookup that never succeeds stays ambiguous: the order may have moved
/// funds, and nothing here can prove otherwise. The paused clock steps over
/// the poll interval so exhausting the budget costs no real time.
#[tokio::test(start_paused = true)]
async fn an_order_that_never_becomes_readable_is_ambiguous() {
    let mut api = MockKrakenApi::new();
    api.expect_order()
        .times(ORDER_POLL_ATTEMPTS)
        .returning(|_| Err(KrakenApiError::Transport("status request timed out".into())));
    let client = btc_usd_client(api);

    let error = client.wait_for_order("ORDER-2", "sell").await.expect_err("must park");

    assert!(matches!(
        client.classify_submission_error(&error),
        CexSubmissionError::Ambiguous(_)
    ));
}
