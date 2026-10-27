//! Trade route discovery and route previews.
use super::*;

#[tokio::test]
async fn mexc_resolves_icp_to_ckbtc_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "ICP_CKBTC" | "CKBTC_ICP" => Ok(empty.clone()),
            "ICP_USDC" | "BTC_USDC" | "CKBTC_BTC" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec!["ICP_USDC".to_string(), "BTC_USDC".to_string(), "CKBTC_BTC".to_string()],
        2,
    );

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ICP", "ckBTC")
        .await
        .expect("ICP -> ckBTC route should resolve");

    assert_eq!(legs.len(), 3);
    assert_eq!(legs[0].market, "ICP_USDC");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "BTC_USDC");
    assert_eq!(legs[1].side, "buy");
    assert_eq!(legs[2].market, "CKBTC_BTC");
    assert_eq!(legs[2].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_ckbtc_to_icp_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_ICP" | "ICP_CKBTC" => Ok(empty.clone()),
            "CKBTC_BTC" | "BTC_USDC" | "ICP_USDC" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec!["CKBTC_BTC".to_string(), "BTC_USDC".to_string(), "ICP_USDC".to_string()],
        2,
    );

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ckBTC", "ICP")
        .await
        .expect("ckBTC -> ICP route should resolve");

    assert_eq!(legs.len(), 3);
    assert_eq!(legs[0].market, "CKBTC_BTC");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "BTC_USDC");
    assert_eq!(legs[1].side, "sell");
    assert_eq!(legs[2].market, "ICP_USDC");
    assert_eq!(legs[2].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_icp_to_ckusdt_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "ICP_CKUSDT" | "CKUSDT_ICP" => Ok(empty.clone()),
            "ICP_USDT" | "CKUSDT_USDT" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_route_config(vec!["ICP_USDT".to_string(), "CKUSDT_USDT".to_string()], 1);

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ICP", "ckUSDT")
        .await
        .expect("ICP -> ckUSDT route should resolve");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].market, "ICP_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "CKUSDT_USDT");
    assert_eq!(legs[1].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_icp_to_eth_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "ICP_ETH" | "ETH_ICP" => Ok(empty.clone()),
            "ICP_USDT" | "ETH_USDT" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_route_config(vec!["ICP_USDT".to_string(), "ETH_USDT".to_string()], 1);

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ICP", "ETH")
        .await
        .expect("ICP -> ETH route should resolve");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].market, "ICP_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "ETH_USDT");
    assert_eq!(legs[1].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_ckbtc_to_eth_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_ETH" | "ETH_CKBTC" => Ok(empty.clone()),
            "CKBTC_BTC" | "BTC_USDT" | "ETH_USDT" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec!["CKBTC_BTC".to_string(), "BTC_USDT".to_string(), "ETH_USDT".to_string()],
        2,
    );

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ckBTC", "ETH")
        .await
        .expect("ckBTC -> ETH route should resolve");

    assert_eq!(legs.len(), 3);
    assert_eq!(legs[0].market, "CKBTC_BTC");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "BTC_USDT");
    assert_eq!(legs[1].side, "sell");
    assert_eq!(legs[2].market, "ETH_USDT");
    assert_eq!(legs[2].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_eth_to_ckbtc_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "ETH_CKBTC" | "CKBTC_ETH" => Ok(empty.clone()),
            "ETH_USDT" | "BTC_USDT" | "CKBTC_BTC" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec!["CKBTC_BTC".to_string(), "BTC_USDT".to_string(), "ETH_USDT".to_string()],
        2,
    );

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ETH", "ckBTC")
        .await
        .expect("ETH -> ckBTC route should resolve");

    assert_eq!(legs.len(), 3);
    assert_eq!(legs[0].market, "ETH_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "BTC_USDT");
    assert_eq!(legs[1].side, "buy");
    assert_eq!(legs[2].market, "CKBTC_BTC");
    assert_eq!(legs[2].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_ckusdt_to_icp_route_from_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquid = liquid_orderbook();
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKUSDT_ICP" | "ICP_CKUSDT" => Ok(empty.clone()),
            "CKUSDT_USDT" | "ICP_USDT" => Ok(liquid.clone()),
            other => Err(format!("unexpected market {}", other)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_route_config(vec!["CKUSDT_USDT".to_string(), "ICP_USDT".to_string()], 1);

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ckUSDT", "ICP")
        .await
        .expect("ckUSDT -> ICP route should resolve");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].market, "CKUSDT_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "ICP_USDT");
    assert_eq!(legs[1].side, "buy");
}

#[tokio::test]
async fn mexc_trade_prefers_direct_route_over_hops_when_both_available() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let observed_markets = Arc::new(std::sync::Mutex::new(HashMap::<String, usize>::new()));
    let observed_markets_handle = observed_markets.clone();
    let liquid = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 1_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 101.0,
            quantity: 1_000.0,
        }],
        taker_fee_bps: 10.0,
    };
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend.expect_get_orderbook().returning(move |market, _limit| {
        *observed_markets_handle
            .lock()
            .unwrap()
            .entry(market.to_string())
            .or_insert(0) += 1;
        match market {
            "BTC_USDT" => Ok(liquid.clone()),
            "USDT_BTC" => Ok(empty.clone()),
            "BTC_USDC" => Ok(liquid.clone()),
            "USDC_USDT" => Ok(liquid.clone()),
            _ => Err(format!("unexpected market {}", market)),
        }
    });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|market, side, amount_in, _opts| {
            assert_eq!(market, "BTC_USDT");
            assert_eq!(side, "sell");
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 100.0,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec!["BTC_USDT".to_string(), "BTC_USDC".to_string(), "USDC_USDT".to_string()],
        2,
    );

    let mut receipt = make_execution_receipt(1001);
    receipt.request.collateral_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    receipt.request.debt_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "USDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let mut state = finalizer
        .prepare("1001", &receipt)
        .await
        .expect("prepare should succeed");
    state.step = CexStep::Trade;

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert_eq!(state.trade.trade_leg_total, Some(1));
    assert_eq!(state.trade.trade_resolved_legs.len(), 1);
    assert_eq!(state.trade.trade_resolved_legs[0].market, "BTC_USDT");
    assert_eq!(state.trade.trade_resolved_legs[0].side, "sell");
    assert!(matches!(state.step, CexStep::Withdraw));

    let markets = observed_markets.lock().unwrap();
    assert_eq!(markets.get("BTC_USDC").copied().unwrap_or(0), 0);
    assert_eq!(markets.get("USDC_USDT").copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn mexc_trade_uses_configured_hop_route_when_direct_unavailable() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let liquid = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 1_000_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 1_000_000.0,
        }],
        taker_fee_bps: 10.0,
    };
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "BTC_CKUSDT" | "CKUSDT_BTC" => Ok(empty.clone()),
            "BTC_USDC" | "USDC_USDT" | "CKUSDT_USDT" => Ok(liquid.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|market, side, amount_in, _opts| {
            assert_eq!(market, "BTC_USDC");
            assert_eq!(side, "sell");
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec![
            "BTC_USDC".to_string(),
            "USDC_USDT".to_string(),
            "CKUSDT_USDT".to_string(),
        ],
        2,
    );

    let mut receipt = make_execution_receipt(1002);
    receipt.request.collateral_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    receipt.request.debt_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "CKUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let mut state = finalizer
        .prepare("1002", &receipt)
        .await
        .expect("prepare should succeed");
    state.step = CexStep::Trade;

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert_eq!(state.trade.trade_leg_total, Some(3));
    assert_eq!(state.trade.trade_leg_index, Some(1));
    assert_eq!(state.trade.trade_resolved_legs.len(), 3);
    assert_eq!(state.trade.trade_resolved_legs[0].market, "BTC_USDC");
    assert_eq!(state.trade.trade_resolved_legs[1].market, "USDC_USDT");
    assert_eq!(state.trade.trade_resolved_legs[2].market, "CKUSDT_USDT");
    assert!(matches!(state.step, CexStep::TradePending));
}

#[tokio::test]
async fn mexc_trade_reuses_persisted_route_without_reprobing_direct_markets() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let liquid = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 1_000_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 1_000_000.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .withf(|market, _| market == "BTC_CKUSDT")
        .times(1)
        .returning({
            let empty = empty.clone();
            move |_, _| Ok(empty.clone())
        });
    backend
        .expect_get_orderbook()
        .withf(|market, _| market == "CKUSDT_BTC")
        .times(1)
        .returning({
            let empty = empty.clone();
            move |_, _| Ok(empty.clone())
        });
    backend
        .expect_get_orderbook()
        .withf(|market, _| market != "BTC_CKUSDT" && market != "CKUSDT_BTC")
        .returning(move |market, _limit| match market {
            "BTC_USDC" | "USDC_USDT" | "CKUSDT_USDT" => Ok(liquid.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(2)
        .returning(|market, side, amount_in, _opts| {
            match market {
                "BTC_USDC" => assert_eq!(side, "sell"),
                "USDC_USDT" => assert_eq!(side, "sell"),
                _ => panic!("unexpected market {}", market),
            }
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(
        vec![
            "BTC_USDC".to_string(),
            "USDC_USDT".to_string(),
            "CKUSDT_USDT".to_string(),
        ],
        2,
    );

    let mut receipt = make_execution_receipt(1003);
    receipt.request.collateral_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    receipt.request.debt_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "CKUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let mut state = finalizer
        .prepare("1003", &receipt)
        .await
        .expect("prepare should succeed");
    state.step = CexStep::Trade;

    finalizer
        .trade(&mut state)
        .await
        .expect("first leg should resolve and execute");
    assert_eq!(state.trade.trade_resolved_legs.len(), 3);
    assert_eq!(state.trade.trade_leg_index, Some(1));

    finalizer
        .trade(&mut state)
        .await
        .expect("second leg should execute using persisted route");
    assert_eq!(state.trade.trade_leg_index, Some(2));
    assert_eq!(state.trade.trade_resolved_legs.len(), 3);
}

#[tokio::test]
async fn mexc_trade_errors_when_no_hop_route_exists_in_configured_pairs() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend
        .expect_get_orderbook()
        .returning(move |_market, _limit| Ok(empty.clone()));
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_route_config(vec!["BTC_USDC".to_string(), "USDC_USDT".to_string()], 2);

    let mut receipt = make_execution_receipt(1004);
    receipt.request.collateral_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    receipt.request.debt_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ETH".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let mut state = finalizer
        .prepare("1004", &receipt)
        .await
        .expect("prepare should succeed");
    state.step = CexStep::Trade;

    let err = finalizer
        .trade(&mut state)
        .await
        .expect_err("trade should fail when no configured hop route exists");
    assert!(err.to_string().contains("no configured hop route"));
}

#[tokio::test]
async fn mexc_preview_route_keeps_legacy_special_override_even_with_direct_pair_configured() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let observed_markets = Arc::new(std::sync::Mutex::new(HashMap::<String, usize>::new()));
    let observed_markets_handle = observed_markets.clone();
    let liquid = OrderBook {
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
    backend.expect_get_orderbook().returning(move |market, _limit| {
        *observed_markets_handle
            .lock()
            .unwrap()
            .entry(market.to_string())
            .or_insert(0) += 1;
        match market {
            "CKBTC_BTC" | "BTC_USDC" | "USDC_USDT" | "CKUSDT_USDT" | "CKBTC_CKUSDT" => Ok(liquid.clone()),
            _ => Err(format!("unexpected market {}", market)),
        }
    });

    let finalizer =
        test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(vec!["CKBTC_CKUSDT".to_string()], 2);

    let receipt = make_execution_receipt(1005);
    preview_receipt_route(&finalizer, &receipt)
        .await
        .expect("preview should succeed");

    let markets = observed_markets.lock().unwrap();
    assert_eq!(markets.get("CKBTC_CKUSDT").copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn mexc_resolves_eth_to_usdc_via_usdt_special_route_without_direct_probe() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    backend.expect_get_orderbook().times(0);

    let finalizer =
        test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(vec!["ETH_USDC".to_string()], 2);

    let legs = finalizer
        .resolve_trade_legs_for_symbols("ETH", "USDC")
        .await
        .expect("ETH -> USDC special route should resolve");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].market, "ETH_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "USDC_USDT");
    assert_eq!(legs[1].side, "buy");
}

#[tokio::test]
async fn mexc_resolves_usdc_to_eth_via_usdt_special_route_without_direct_probe() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    backend.expect_get_orderbook().times(0);

    let finalizer =
        test_finalizer(Arc::new(backend), Arc::new(transfers)).with_route_config(vec!["USDC_ETH".to_string()], 2);

    let legs = finalizer
        .resolve_trade_legs_for_symbols("USDC", "ETH")
        .await
        .expect("USDC -> ETH special route should resolve");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].market, "USDC_USDT");
    assert_eq!(legs[0].side, "sell");
    assert_eq!(legs[1].market, "ETH_USDT");
    assert_eq!(legs[1].side, "buy");
}

#[tokio::test]
async fn mexc_input_slice_usd_converts_non_hardcoded_quote_via_configured_hops() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let sol_eth = OrderBook {
        bids: vec![OrderBookLevel {
            price: 2.0,
            quantity: 1_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 2.1,
            quantity: 1_000.0,
        }],
        taker_fee_bps: 10.0,
    };
    let eth_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 0.05,
            quantity: 1_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 0.051,
            quantity: 1_000.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 70_000.0,
            quantity: 1_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 70_100.0,
            quantity: 1_000.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "SOL_ETH" => Ok(sol_eth.clone()),
            "ETH_BTC" => Ok(eth_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            // Direct stable probes that should fail and force hop conversion.
            "ETH_USDT" | "USDT_ETH" | "ETH_USDC" | "USDC_ETH" | "ETH_CKUSDT" | "CKUSDT_ETH" | "ETH_CKUSDC"
            | "CKUSDC_ETH" | "ETH_USD" | "USD_ETH" => Ok(empty.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_route_config(vec!["ETH_BTC".to_string(), "BTC_USDC".to_string()], 1);

    let usd = finalizer
        .input_slice_usd("SOL_ETH", "sell", 10.0)
        .await
        .expect("USD conversion should succeed");
    assert!((usd - 70_000.0).abs() < 1e-6);
}

#[tokio::test]
async fn mexc_preview_trade_slice_sell_binary_search_finds_largest_chunk_under_target() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Threshold shape:
    // - up to 1.0 base filled at 100
    // - remainder filled at 90
    // For target=500 bps, largest valid chunk is ~2.0.
    let orderbook = OrderBook {
        bids: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            },
            OrderBookLevel {
                price: 90.0,
                quantity: 10.0,
            },
        ],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .times(1)
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    let finalizer = CexVenueFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        2_000.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "BTC_USDC".to_string(),
        side: "sell".to_string(),
    };

    let preview = finalizer
        .preview_trade_slice(&leg, 3.0, 500.0)
        .await
        .expect("sell preview should succeed");

    assert!((preview.chunk_in - 2.0).abs() < 0.01);
    assert!(preview.preview_impact_bps <= 500.0 + 0.01);
    assert!(preview.preview_impact_bps > 450.0);
}

#[tokio::test]
async fn mexc_preview_route_resolves_direct_buy_leg_when_sell_book_empty() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Direct sell market BTC_CKBTC has no bids -> resolver should choose buy market CKBTC_BTC.
    let btc_ckbtc = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let ckbtc_btc = OrderBook {
        bids: vec![],
        asks: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 10.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 10.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "BTC_CKBTC" => Ok(btc_ckbtc.clone()),
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let mut receipt = make_execution_receipt(42);
    receipt.request.collateral_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    receipt.request.debt_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let preview = preview_receipt_route(&finalizer, &receipt)
        .await
        .expect("preview should succeed");
    assert!(preview.receive_amount > 0.0);
    assert!(preview.price_impact_bps >= 0.0);
}
