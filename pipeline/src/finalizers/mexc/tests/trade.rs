//! Trade legs: slices, previews, dust and the finished swap.
use super::*;

#[tokio::test]
async fn mexc_trade_skips_when_amount_in_zero_and_moves_to_withdraw() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // When amount_in <= 0, execute_swap must never be called.
    backend.expect_execute_swap_detailed_with_options().times(0);

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Force amount_in to zero and move state into Trade step.
    state.size_in.value = Nat::from(0u32);
    state.step = CexStep::Trade;

    finalizer
        .trade(&mut state)
        .await
        .expect("trade should succeed even when skipped");

    // No size_out set and step advanced to Withdraw.
    assert!(state.withdraw.size_out.is_none());
    assert!(matches!(state.step, CexStep::Withdraw));
}

#[tokio::test]
async fn mexc_trade_executes_swap_and_sets_size_out_and_step_withdraw() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let orderbook = OrderBook {
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
    backend
        .expect_get_orderbook()
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    let calls = std::sync::Arc::new(std::sync::Mutex::new(0usize));
    let calls_handle = calls.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .times(4)
        .returning(move |market, side, amount_in, _opts| {
            let mut idx = calls_handle.lock().unwrap();
            let cur = *idx;
            *idx += 1;

            match cur {
                0 => {
                    assert_eq!(market, "CKBTC_BTC");
                    assert_eq!(side, "sell");
                    assert!(amount_in > 0.0);
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * 0.9999,
                    })
                }
                1 => {
                    assert_eq!(market, "BTC_USDC");
                    assert_eq!(side, "sell");
                    assert!(amount_in > 0.0);
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * 0.9998,
                    })
                }
                2 => {
                    assert_eq!(market, "USDC_USDT");
                    assert_eq!(side, "sell");
                    assert!(amount_in > 0.0);
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * 0.9997,
                    })
                }
                3 => {
                    assert_eq!(market, "CKUSDT_USDT");
                    assert_eq!(side, "buy");
                    assert!(amount_in > 0.0);
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in / 1.0012,
                    })
                }
                _ => unreachable!("unexpected execute_swap call"),
            }
        });

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Move directly into Trade step; size_in is taken from receipt and should be > 0.
    state.step = CexStep::Trade;

    finalizer.trade(&mut state).await.expect("trade leg 1 should succeed");
    assert!(matches!(state.step, CexStep::TradePending));
    assert!(state.withdraw.size_out.is_none());

    finalizer.trade(&mut state).await.expect("trade leg 2 should succeed");
    assert!(matches!(state.step, CexStep::TradePending));
    assert!(state.withdraw.size_out.is_none());

    finalizer.trade(&mut state).await.expect("trade leg 3 should succeed");
    assert!(matches!(state.step, CexStep::TradePending));
    assert!(state.withdraw.size_out.is_none());

    finalizer.trade(&mut state).await.expect("trade leg 4 should succeed");

    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert_eq!(out.token, state.withdraw.withdraw_asset);
    assert!(out.to_f64() > 0.0);
    assert_eq!(state.trade.trade_slices.len(), 4);
    assert!(matches!(state.step, CexStep::Withdraw));
}

#[tokio::test]
async fn mexc_trade_propagates_backend_errors() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let orderbook = OrderBook {
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
    backend
        .expect_get_orderbook()
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, _amount_in, _opts| Err(CexBackendError::Other("boom".to_string())));

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    state.step = CexStep::Trade;

    let err = finalizer.trade(&mut state).await.expect_err("trade should fail");

    assert_eq!(err, CexBackendError::Other("boom".to_string()));
    // On error we expect the step to remain Trade.
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn mexc_trade_marks_dust_only_after_a_fill() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    // Dust path should break before any market execution.
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        // 0.01 ckBTC ~= 690 USD at mocked conversion, so this forces dust skip.
        1_000.0,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    // Force single-leg route: CKBTC -> BTC.
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    // Resume a partially filled leg: only its remaining input may be dust.
    state.trade.trade_progress_total_out = Some(0.02);

    finalizer
        .trade(&mut state)
        .await
        .expect("trade should succeed with dust skip");

    assert!(state.trade.trade_dust_skipped);
    assert!(state.trade.trade_dust_usd.unwrap_or_default() > 0.0);
    assert!(state.trade.trade_slices.is_empty());
    assert!(matches!(state.step, CexStep::Withdraw));
    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert_eq!(out.to_f64(), 0.02);
}

#[tokio::test]
async fn mexc_committed_leg_below_dust_floor_still_executes() {
    let mut backend = MockCexBackend::new();
    backend.expect_get_orderbook().returning(|_, _| {
        Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 100.0,
            }],
            taker_fee_bps: 10.0,
        })
    });
    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_, _, amount, _| {
            Ok(SwapFillReport {
                input_consumed: amount,
                output_received: amount,
            })
        });
    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        1_000.0,
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state = finalizer
        .prepare("dust-crossing", &make_execution_receipt(42))
        .await
        .unwrap();
    let leg = TradeLeg {
        market: "CKBTC_BTC".to_string(),
        side: "sell".to_string(),
    };
    let (remaining, output) = finalizer
        .execute_trade_leg_slices(&mut state, &leg, 0, 1, 0.01, 50.0)
        .await
        .unwrap()
        .expect("the leg finishes");
    assert_eq!(remaining, 0.0);
    assert_eq!(output, 0.01);
    assert!(!state.trade.trade_dust_skipped);
}

#[tokio::test]
async fn mexc_maybe_mark_trade_dust_honors_threshold_boundary() {
    // market CKBTC_BTC with best bid 1 and BTC_USDC best bid 100 => chunk_usd = chunk_in * 100
    let leg = TradeLeg {
        market: "CKBTC_BTC".to_string(),
        side: "sell".to_string(),
    };
    let receipt = make_execution_receipt(42);

    let mut backend_eq = MockCexBackend::new();
    let transfers_eq = MockTransferActions::new();
    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend_eq
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer_eq = MexcFinalizer::new(
        Arc::new(backend_eq),
        Arc::new(transfers_eq),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        1.0, // equal to chunk_usd when chunk_in=0.01
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state_eq = finalizer_eq
        .prepare("42", &receipt)
        .await
        .expect("prepare should succeed");
    let skipped_eq = finalizer_eq
        .maybe_mark_trade_dust(&mut state_eq, &leg, 1.0, 0.01, 0.01)
        .await
        .expect("dust check should succeed");
    assert!(!skipped_eq);
    assert!(!state_eq.trade.trade_dust_skipped);
    assert!(state_eq.trade.trade_dust_usd.is_none());

    let mut backend_below = MockCexBackend::new();
    let transfers_below = MockTransferActions::new();
    let ckbtc_btc_below = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let btc_usdc_below = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend_below
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc_below.clone()),
            "BTC_USDC" => Ok(btc_usdc_below.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer_below = MexcFinalizer::new(
        Arc::new(backend_below),
        Arc::new(transfers_below),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        1.0001, // slightly above chunk_usd when chunk_in=0.01
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state_below = finalizer_below
        .prepare("42", &receipt)
        .await
        .expect("prepare should succeed");
    let skipped_below = finalizer_below
        .maybe_mark_trade_dust(&mut state_below, &leg, 1.0, 0.01, 0.01)
        .await
        .expect("dust check should succeed");
    assert!(skipped_below);
    assert!(state_below.trade.trade_dust_skipped);
    assert!((state_below.trade.trade_dust_usd.unwrap_or_default() - 1.0).abs() < 1e-9);
}

#[tokio::test]
async fn mexc_maybe_mark_trade_dust_requires_small_chunk_and_small_residual() {
    // market CKBTC_BTC with best bid 1 and BTC_USDC best bid 100 => usd = amount * 100
    let leg = TradeLeg {
        market: "CKBTC_BTC".to_string(),
        side: "sell".to_string(),
    };
    let receipt = make_execution_receipt(42);

    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        1.0,
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // chunk_usd=0.5 (<1.0), residual_usd=2.0 (>=1.0) => should not be dust-skipped.
    let skipped = finalizer
        .maybe_mark_trade_dust(&mut state, &leg, 1.0, 0.005, 0.02)
        .await
        .expect("dust check should succeed");
    assert!(!skipped);
    assert!(!state.trade.trade_dust_skipped);
    assert!(state.trade.trade_dust_usd.is_none());
}

#[tokio::test]
async fn mexc_trade_fails_when_realized_slice_slippage_exceeds_cap() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, amount_in, _opts| {
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.9,
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        // Keep cap tight so the 10% execution drift fails loudly.
        50.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    let err = finalizer
        .trade(&mut state)
        .await
        .expect_err("trade should fail when realized slippage exceeds cap");

    assert!(err.to_string().contains("slice slippage too high"));
    assert!(
        state
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("slice slippage too high")
    );
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn mexc_trade_buy_truncation_mismatch_does_not_false_spike_slippage() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let sell_probe = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let buy_book = OrderBook {
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
            "BTC_CKBTC" => Ok(sell_probe.clone()),
            "CKBTC_BTC" => Ok(buy_book.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    // Simulate exchange truncation: requested quote_in=0.000185, actual consumed quote=0.0001.
    // Output chosen so realized exec price remains 1.0 (no true slippage spike).
    let seen_modes = Arc::new(std::sync::Mutex::new(Vec::<BuyOrderInputMode>::new()));
    let seen_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen_modes_handle = seen_modes.clone();
    let seen_ids_handle = seen_ids.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .times(2)
        .returning(move |_market, side, amount_in, opts| {
            assert_eq!(side, "buy");
            seen_modes_handle.lock().unwrap().push(opts.buy_mode);
            seen_ids_handle
                .lock()
                .unwrap()
                .push(opts.client_order_id.clone().unwrap_or_default());
            let consumed = amount_in.min(0.0001);
            Ok(SwapFillReport {
                input_consumed: consumed,
                output_received: consumed,
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.deposit.deposit_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.deposit.bridge.deposit_planned_asset = Some("BTC".to_string());
    state.deposit.bridge.deposit_planned_network = Some("ICP".to_string());
    state.deposit.bridge.deposit_bridge_required = false;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "CKBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("CKBTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;
    state.size_in = ChainTokenAmount::from_formatted(state.deposit.deposit_asset.clone(), 0.000185);
    state.trade.trade_next_amount_in = None;

    finalizer
        .trade(&mut state)
        .await
        .expect("trade should succeed without false slippage spike");

    assert!(matches!(state.step, CexStep::Withdraw));
    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert!((out.to_f64() - 0.000185).abs() < 1e-12);
    assert_eq!(state.trade.trade_slices.len(), 2);
    let modes = seen_modes.lock().unwrap();
    assert_eq!(
        modes.as_slice(),
        &[BuyOrderInputMode::Auto, BuyOrderInputMode::BaseQuantity]
    );
    let ids = seen_ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert!(!ids[0].is_empty());
    assert!(!ids[1].is_empty());
    assert_ne!(ids[0], ids[1]);
    assert!(state.last_error.is_none());
}

#[tokio::test]
async fn mexc_trade_slippage_error_includes_requested_and_actual_fill_details() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, amount_in, _opts| {
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.9,
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        50.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let err = finalizer.trade(&mut state).await.expect_err("trade should fail");
    let error = err.to_string();
    assert!(error.contains("slice slippage too high"));
    assert!(error.contains("requested_in="));
    assert!(error.contains("actual_in="));
    assert!(error.contains("actual_out="));
    assert!(error.contains("preview_mid="));
    assert!(error.contains("exec_price="));
}

#[tokio::test]
async fn mexc_trade_clamp_and_finish_under_consumed_buy_input_single_slice() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let sell_probe = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };
    let buy_book = OrderBook {
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
            "BTC_CKBTC" => Ok(sell_probe.clone()),
            "CKBTC_BTC" => Ok(buy_book.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let calls = Arc::new(std::sync::Mutex::new(0usize));
    let calls_handle = calls.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .times(2)
        .returning(move |_market, _side, amount_in, _opts| {
            *calls_handle.lock().unwrap() += 1;
            let consumed = amount_in.min(0.0001);
            Ok(SwapFillReport {
                input_consumed: consumed,
                output_received: consumed,
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.deposit.deposit_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.deposit.bridge.deposit_planned_asset = Some("BTC".to_string());
    state.deposit.bridge.deposit_planned_network = Some("ICP".to_string());
    state.deposit.bridge.deposit_bridge_required = false;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "CKBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("CKBTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;
    state.size_in = ChainTokenAmount::from_formatted(state.deposit.deposit_asset.clone(), 0.000185);

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert_eq!(*calls.lock().unwrap(), 2);
    assert!(matches!(state.step, CexStep::Withdraw));
    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert!((out.to_f64() - 0.000185).abs() < 1e-12);
}

#[tokio::test]
async fn mexc_trade_retries_current_leg_from_original_amount_after_mid_leg_error() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let call_count = Arc::new(std::sync::Mutex::new(0usize));
    let seen_amounts = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let call_count_handle = call_count.clone();
    let seen_amounts_handle = seen_amounts.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(move |_market, _side, amount_in, opts| {
            let mut idx = call_count_handle.lock().unwrap();
            *idx += 1;
            seen_amounts_handle.lock().unwrap().push(amount_in);
            assert!(opts.client_order_id.is_some());
            // First attempt intentionally fails post-trade slippage check.
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.95,
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    let first_err = finalizer
        .trade(&mut state)
        .await
        .expect_err("first trade attempt should fail on slippage");
    assert!(first_err.to_string().contains("slice slippage too high"));
    assert!(matches!(state.step, CexStep::Trade));

    // Retry same state; the leg should resume from persisted progress without replaying the order.
    finalizer
        .trade(&mut state)
        .await
        .expect("second trade attempt should succeed");

    let amounts = seen_amounts.lock().unwrap();
    assert_eq!(amounts.len(), 1);
    assert!((amounts[0] - 0.01).abs() < 1e-12);
    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert!((out.to_f64() - 0.0095).abs() < 1e-12);
    assert!(matches!(state.step, CexStep::Withdraw));
}

#[tokio::test]
async fn mexc_trade_uses_resume_amount_when_trade_next_amount_in_is_present() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let seen = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let seen_handle = seen.clone();
    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(move |_market, _side, amount_in, _opts| {
            seen_handle.lock().unwrap().push(amount_in);
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.999,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    // Force a direct single-leg route: CKBTC -> BTC.
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;
    state.trade.trade_next_amount_in = Some(0.0042);

    finalizer.trade(&mut state).await.expect("trade should succeed");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!((seen[0] - 0.0042).abs() < 1e-12);
    assert!((state.trade.trade_last_amount_in.unwrap_or_default() - 0.0042).abs() < 1e-12);
    assert!(matches!(state.step, CexStep::Withdraw));
}

#[tokio::test]
async fn mexc_trade_clears_legacy_pending_without_client_id_before_submit() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, amount_in, opts| {
            // Legacy stale requested amount must be ignored after pending-state cleanup.
            assert!((amount_in - 0.01).abs() < 1e-12);
            let id = opts.client_order_id.as_deref().unwrap_or_default();
            assert!(is_valid_mexc_client_order_id(id));
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.999,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    // Simulate legacy WAL residue: pending markers persisted without client id.
    state.trade.trade_pending_market = Some("CKBTC_BTC".to_string());
    state.trade.trade_pending_side = Some("sell".to_string());
    state.trade.trade_pending_requested_in = Some(999.0);
    state.trade.trade_pending_buy_mode = Some("base_quantity".to_string());

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert!(matches!(state.step, CexStep::Withdraw));
    assert!(state.trade.trade_pending_client_order_id.is_none());
    assert!(state.trade.trade_pending_market.is_none());
    assert!(state.trade.trade_pending_side.is_none());
    assert!(state.trade.trade_pending_requested_in.is_none());
}

#[tokio::test]
async fn mexc_trade_clears_stale_pending_from_different_leg_before_submit() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, amount_in, opts| {
            // stale pending requested input must be cleared and rebuilt.
            assert!((amount_in - 0.01).abs() < 1e-12);
            assert_ne!(opts.client_order_id.as_deref(), Some("legacy-pending-id"));
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.999,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    state.trade.trade_pending_client_order_id = Some("legacy-pending-id".to_string());
    state.trade.trade_pending_market = Some("OTHER_MARKET".to_string());
    state.trade.trade_pending_side = Some("buy".to_string());
    state.trade.trade_pending_requested_in = Some(0.123456);

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert!(matches!(state.step, CexStep::Withdraw));
    assert!(state.trade.trade_pending_client_order_id.is_none());
}

#[tokio::test]
async fn mexc_trade_regenerates_invalid_pending_client_order_id() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let ckbtc_btc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 69_000.0,
            quantity: 100.0,
        }],
        asks: vec![OrderBookLevel {
            price: 69_010.0,
            quantity: 100.0,
        }],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let invalid_pending_id = "legacy:invalid/client-order-id-that-is-way-too-long";
    let invalid_pending_id_owned = invalid_pending_id.to_string();

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(move |_market, _side, amount_in, opts| {
            let id = opts.client_order_id.as_deref().unwrap_or_default();
            assert_ne!(id, invalid_pending_id_owned.as_str());
            assert!(is_valid_mexc_client_order_id(id));
            Ok(SwapFillReport {
                input_consumed: amount_in,
                output_received: amount_in * 0.999,
            })
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;

    state.trade.trade_pending_client_order_id = Some(invalid_pending_id.to_string());
    state.trade.trade_pending_market = Some("CKBTC_BTC".to_string());
    state.trade.trade_pending_side = Some("sell".to_string());
    state.trade.trade_pending_requested_in = Some(0.01);

    finalizer.trade(&mut state).await.expect("trade should succeed");

    assert!(matches!(state.step, CexStep::Withdraw));
}

#[tokio::test]
async fn mexc_trade_when_leg_index_is_past_route_moves_to_withdraw_and_sets_size_out() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    backend.expect_get_orderbook().times(0);
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    // Route for CKBTC -> CKUSDT has 4 legs, so this index is out-of-range.
    state.trade.trade_leg_index = Some(99);
    state.trade.trade_next_amount_in = Some(12.345678);

    finalizer.trade(&mut state).await.expect("trade should short-circuit");

    assert!(matches!(state.step, CexStep::Withdraw));
    let out = state
        .withdraw
        .size_out
        .as_ref()
        .expect("size_out should be carried forward");
    assert_eq!(out.token, state.withdraw.withdraw_asset);
    assert!((out.to_f64() - 12.345678).abs() < 1e-12);
}

#[tokio::test]
async fn mexc_trade_errors_when_direct_market_cannot_be_resolved() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let empty_book = OrderBook {
        bids: vec![],
        asks: vec![],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .times(2)
        .returning(move |_market, _limit| Ok(empty_book.clone()));
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Trade;
    // Force non-special pair so resolver must probe direct books.
    state.deposit.deposit_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "BTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.deposit.bridge.deposit_planned_asset = Some("BTC".to_string());
    state.deposit.bridge.deposit_planned_network = Some("ICP".to_string());
    state.deposit.bridge.deposit_bridge_required = false;
    state.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ETH".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };
    state.withdraw.bridge.withdraw_planned_asset = Some("ETH".to_string());
    state.withdraw.bridge.withdraw_planned_network = Some("ICP".to_string());
    state.withdraw.bridge.withdraw_bridge_required = false;
    state.size_in = ChainTokenAmount::from_formatted(state.deposit.deposit_asset.clone(), 0.01);

    let err = finalizer
        .trade(&mut state)
        .await
        .expect_err("trade should fail when no direct leg can be resolved");

    assert!(
        err.to_string().contains("could not resolve direct market")
            || err.to_string().contains("no configured MEXC pairs for hop discovery")
    );
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn resolve_trade_legs_for_identical_symbols_is_noop() {
    let backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let legs = finalizer
        .resolve_trade_legs_for_symbols(" usdc ", "USDC")
        .await
        .expect("identical symbols should resolve as no-op");

    assert!(legs.is_empty());
}

#[tokio::test]
async fn mexc_preview_trade_slice_buy_binary_search_finds_largest_chunk_under_target() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Threshold shape:
    // - first 100 quote buys at 100
    // - remaining quote buys at 120
    // For target=500 bps, largest valid chunk is ~140 quote.
    let orderbook = OrderBook {
        bids: vec![],
        asks: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            },
            OrderBookLevel {
                price: 120.0,
                quantity: 10.0,
            },
        ],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .times(1)
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        2_000.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "CKUSDT_USDT".to_string(),
        side: "buy".to_string(),
    };

    let preview = finalizer
        .preview_trade_slice(&leg, 240.0, 500.0)
        .await
        .expect("buy preview should succeed");

    assert!((preview.chunk_in - 140.0).abs() < 0.05);
    assert!(preview.preview_impact_bps <= 500.0 + 0.01);
    assert!(preview.preview_impact_bps > 450.0);
}

#[tokio::test]
async fn mexc_preview_trade_slice_sell_fallback_uses_full_remaining_when_hard_cap_allows() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // First level has zero quantity at better price, so any positive amount fills at 90
    // while best bid is still 100, forcing 1000 bps impact for all >0 chunks.
    let orderbook = OrderBook {
        bids: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 0.0,
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        1_500.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "BTC_USDC".to_string(),
        side: "sell".to_string(),
    };

    // target=0 makes binary search return ~0 chunk; fallback should allow full remaining
    // because hard cap (1500 bps) is above realized impact (1000 bps).
    let preview = finalizer
        .preview_trade_slice(&leg, 1.0, 0.0)
        .await
        .expect("fallback should allow full remaining");

    assert!((preview.chunk_in - 1.0).abs() < 1e-12);
    assert!((preview.preview_impact_bps - 1_000.0).abs() < 1e-6);
}

#[tokio::test]
async fn mexc_preview_trade_slice_sell_fallback_errors_when_hard_cap_rejects() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let orderbook = OrderBook {
        bids: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 0.0,
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        500.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "BTC_USDC".to_string(),
        side: "sell".to_string(),
    };

    let err = finalizer
        .preview_trade_slice(&leg, 1.0, 0.0)
        .await
        .expect_err("fallback should reject when hard cap is breached");

    assert!(err.contains("cannot find sell chunk under impact target"));
}

#[tokio::test]
async fn mexc_preview_trade_slice_buy_fallback_uses_full_remaining_when_hard_cap_allows() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Mirror of sell fallback shape on ask side: best ask at 100 with zero quantity,
    // actual fills at 120 -> 2000 bps impact for any positive buy chunk.
    let orderbook = OrderBook {
        bids: vec![],
        asks: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 0.0,
            },
            OrderBookLevel {
                price: 120.0,
                quantity: 10.0,
            },
        ],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .times(1)
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        2_500.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "CKUSDT_USDT".to_string(),
        side: "buy".to_string(),
    };

    let preview = finalizer
        .preview_trade_slice(&leg, 120.0, 0.0)
        .await
        .expect("buy fallback should allow full remaining");

    assert!((preview.chunk_in - 120.0).abs() < 1e-12);
    assert!((preview.preview_impact_bps - 2_000.0).abs() < 1e-6);
}

#[tokio::test]
async fn mexc_preview_trade_slice_buy_fallback_errors_when_hard_cap_rejects() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let orderbook = OrderBook {
        bids: vec![],
        asks: vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 0.0,
            },
            OrderBookLevel {
                price: 120.0,
                quantity: 10.0,
            },
        ],
        taker_fee_bps: 10.0,
    };

    backend
        .expect_get_orderbook()
        .times(1)
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        500.0,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let leg = TradeLeg {
        market: "CKUSDT_USDT".to_string(),
        side: "buy".to_string(),
    };

    let err = finalizer
        .preview_trade_slice(&leg, 120.0, 0.0)
        .await
        .expect_err("buy fallback should reject when hard cap is breached");

    assert!(err.contains("cannot find buy chunk under impact target"));
}

#[tokio::test]
async fn mexc_approval_bump_retries_after_first_batch_failure_and_then_short_circuits() {
    let backend = Arc::new(MockCexBackend::new());
    let mut transfers = MockTransferActions::new();
    let approve_call_count = Arc::new(std::sync::Mutex::new(0usize));
    let approve_call_count_handle = approve_call_count.clone();

    // First invocation fails in the first batch on call #2 (no long sleep).
    // Second invocation succeeds all 6 approvals (marks liq as bumped).
    transfers
        .expect_approve()
        .times(8)
        .returning(move |_token, _spender, _amount| {
            let mut n = approve_call_count_handle.lock().unwrap();
            *n += 1;
            if *n == 2 {
                Err("approve failure in first batch".to_string())
            } else {
                Ok(format!("approve-{}", *n))
            }
        });

    let finalizer = test_finalizer(backend, Arc::new(transfers));

    let asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let first = finalizer.maybe_bump_mexc_approval("liq-first-batch", &asset).await;
    assert_eq!(first, 1);

    let second = finalizer.maybe_bump_mexc_approval("liq-first-batch", &asset).await;
    assert_eq!(second, 6);

    // After a full success the same liq_id should short-circuit and do no approvals.
    let third = finalizer.maybe_bump_mexc_approval("liq-first-batch", &asset).await;
    assert_eq!(third, 0);

    assert_eq!(*approve_call_count.lock().unwrap(), 8);
}

#[tokio::test]
async fn mexc_preview_route_returns_non_executable_for_zero_amount() {
    let backend = Arc::new(MockCexBackend::new());
    let transfers = Arc::new(MockTransferActions::new());
    let finalizer = test_finalizer(backend, transfers);

    let mut receipt = make_execution_receipt(42);
    if let Some(liq) = receipt.liquidation_result.as_mut() {
        liq.amounts.collateral_received = Nat::from(0u8);
    }

    let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
    assert!(!preview.is_executable);
    assert_eq!(preview.estimated_receive_amount, 0.0);
    assert_eq!(preview.estimated_price_impact_bps, 0.0);
    assert_eq!(preview.reason.as_deref(), Some("non-positive amount_in"));
}
