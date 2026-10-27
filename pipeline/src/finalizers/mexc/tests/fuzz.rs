use super::*;

proptest! {
    #[test]
    fn prop_trade_generated_client_order_id_is_mexc_compatible(liq_id in "[a-zA-Z0-9_-]{0,80}") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let generated_id = rt.block_on(async move {
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

            let seen_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let seen_ids_handle = seen_ids.clone();
            backend
                .expect_execute_swap_detailed_with_options()
                .times(1)
                .returning(move |_market, _side, amount_in, opts| {
                    seen_ids_handle
                        .lock()
                        .unwrap()
                        .push(opts.client_order_id.unwrap_or_default());
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * 0.999,
                    })
                });

            let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

            let receipt = make_execution_receipt(42);
            let mut state = finalizer
                .prepare(&liq_id, &receipt)
                .await
                .expect("prepare should succeed");
            state.step = CexStep::Trade;
            state.withdraw.withdraw_asset = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "BTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            };
            state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());

            finalizer.trade(&mut state).await.expect("trade should succeed");

            let ids = seen_ids.lock().unwrap();
            assert_eq!(ids.len(), 1);
            ids[0].clone()
        });
        prop_assert!(is_valid_mexc_client_order_id(&generated_id));
    }

    #[test]
    fn prop_trade_single_leg_success_invariants(
        amount_sats in 1_000u64..=2_000_000u64,
        btc_usd in 20_000u64..=120_000u64,
        exec_loss_bps in 0u64..=80u64
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut backend = MockCexBackend::new();
            let transfers = MockTransferActions::new();

            let ckbtc_btc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                taker_fee_bps: 10.0,
            };
            let btc_usdc_book = OrderBook {
                bids: vec![OrderBookLevel { price: btc_usd as f64, quantity: 1000.0 }],
                asks: vec![OrderBookLevel { price: btc_usd as f64 + 1.0, quantity: 1000.0 }],
                taker_fee_bps: 10.0,
            };

            backend.expect_get_orderbook().returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                "BTC_USDC" => Ok(btc_usdc_book.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

            let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
            backend
                .expect_execute_swap_detailed_with_options()
                .times(1)
                .returning(move |_market, _side, amount_in, _opts| {
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * factor,
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

            let mut receipt = make_execution_receipt(42);
            if let Some(liq) = receipt.liquidation_result.as_mut() {
                liq.amounts.collateral_received = Nat::from(amount_sats);
            }

            let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
            state.step = CexStep::Trade;
            // Force single leg CKBTC->BTC for deterministic fuzz invariants.
            state.withdraw.withdraw_asset = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "BTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            };
            state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());

            finalizer.trade(&mut state).await.expect("trade should succeed");

            let input = amount_sats as f64 / 100_000_000.0;
            let output = state.withdraw.size_out.as_ref().expect("size_out must exist").to_f64();

            assert!(matches!(state.step, CexStep::Withdraw));
            assert_eq!(state.trade.trade_slices.len(), 1);
            assert!(output > 0.0);
            assert!(output <= input + 1e-12);
            assert!(state.last_error.is_none());
        });
    }

    #[test]
    fn prop_trade_single_leg_slippage_breach_sets_error(
        amount_sats in 1_000u64..=2_000_000u64,
        exec_loss_bps in 250u64..=1500u64
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut backend = MockCexBackend::new();
            let transfers = MockTransferActions::new();

            let ckbtc_btc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                taker_fee_bps: 10.0,
            };
            let btc_usdc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                taker_fee_bps: 10.0,
            };

            backend.expect_get_orderbook().returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                "BTC_USDC" => Ok(btc_usdc_book.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

            let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
            backend
                .expect_execute_swap_detailed_with_options()
                .times(1)
                .returning(move |_market, _side, amount_in, _opts| {
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: amount_in * factor,
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

            let mut receipt = make_execution_receipt(42);
            if let Some(liq) = receipt.liquidation_result.as_mut() {
                liq.amounts.collateral_received = Nat::from(amount_sats);
            }

            let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
            state.step = CexStep::Trade;
            state.withdraw.withdraw_asset = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "BTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            };
            state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());

            let err = finalizer.trade(&mut state).await.expect_err("trade should fail");
            assert!(err.to_string().contains("slice slippage too high"));
            assert!(matches!(state.step, CexStep::Trade));
            assert!(state
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("slice slippage too high"));
        });
    }

    #[test]
    fn prop_trade_single_leg_multi_slice_progresses(
        first_level_sat in 50_000u64..=300_000u64,
        multiplier in 3u64..=8u64,
        exec_loss_bps in 0u64..=20u64
    ) {
        // Remaining amount is 3x..8x first-level depth, which should require multiple
        // slices under the configured target.
        let total_sats = first_level_sat * multiplier;
        let q1 = first_level_sat as f64 / 100_000_000.0;
        let total_btc = total_sats as f64 / 100_000_000.0;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut backend = MockCexBackend::new();
            let transfers = MockTransferActions::new();

            let ckbtc_btc_book = OrderBook {
                bids: vec![
                    OrderBookLevel { price: 1.0, quantity: q1 },
                    OrderBookLevel { price: 0.99, quantity: 10.0 },
                ],
                asks: vec![OrderBookLevel { price: 1.001, quantity: 10.0 }],
                taker_fee_bps: 10.0,
            };
            let btc_usdc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                taker_fee_bps: 10.0,
            };

            backend.expect_get_orderbook().returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                "BTC_USDC" => Ok(btc_usdc_book.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

            let calls = Arc::new(std::sync::Mutex::new(0usize));
            let calls_handle = calls.clone();
            let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
            backend
                .expect_execute_swap_detailed_with_options()
                .returning(move |_market, _side, amount_in, _opts| {
                *calls_handle.lock().unwrap() += 1;
                Ok(SwapFillReport {
                    input_consumed: amount_in,
                    output_received: amount_in * factor,
                })
            });

            let finalizer = MexcFinalizer::new(
                Arc::new(backend),
                Arc::new(transfers),
                Principal::anonymous(),
                200.0,
                TEST_CEX_MIN_EXEC_USD,
                // target=50 bps when max=200 bps, forcing chunking on this two-level book.
                0.25,
            );

            let mut receipt = make_execution_receipt(42);
            if let Some(liq) = receipt.liquidation_result.as_mut() {
                liq.amounts.collateral_received = Nat::from(total_sats);
            }

            let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
            state.step = CexStep::Trade;
            state.withdraw.withdraw_asset = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "BTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            };
            state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());

            finalizer.trade(&mut state).await.expect("trade should succeed");

            let call_count = *calls.lock().unwrap();
            assert!(call_count > 1);
            assert!(state.trade.trade_slices.len() > 1);
            assert!(matches!(state.step, CexStep::Withdraw));
            assert!(state.withdraw.size_out.as_ref().unwrap().to_f64() > 0.0);
            assert!(state.withdraw.size_out.as_ref().unwrap().to_f64() <= total_btc + 1e-12);
        });
    }

    #[test]
    fn prop_trade_retry_does_not_replay_consumed_input_after_first_failure(
        amount_sats in 1_000u64..=2_000_000u64,
        failing_loss_bps in 250u64..=2_000u64,
        recovery_loss_bps in 0u64..=80u64
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut backend = MockCexBackend::new();
            let transfers = MockTransferActions::new();

            let ckbtc_btc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                taker_fee_bps: 10.0,
            };
            let btc_usdc_book = OrderBook {
                bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                taker_fee_bps: 10.0,
            };

            backend.expect_get_orderbook().returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                "BTC_USDC" => Ok(btc_usdc_book.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

            let call_count = Arc::new(std::sync::Mutex::new(0usize));
            let seen_amounts = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
            let call_count_handle = call_count.clone();
            let seen_amounts_handle = seen_amounts.clone();
            let fail_factor = 1.0 - (failing_loss_bps as f64 / 10_000.0);
            let recovery_factor = 1.0 - (recovery_loss_bps as f64 / 10_000.0);
            backend
                .expect_execute_swap_detailed_with_options()
                .times(1)
                .returning(move |_market, _side, amount_in, _opts| {
                    let mut idx = call_count_handle.lock().unwrap();
                    seen_amounts_handle.lock().unwrap().push(amount_in);
                    let out = if *idx == 0 {
                        amount_in * fail_factor
                    } else {
                        amount_in * recovery_factor
                    };
                    *idx += 1;
                    Ok(SwapFillReport {
                        input_consumed: amount_in,
                        output_received: out,
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

            let mut receipt = make_execution_receipt(42);
            if let Some(liq) = receipt.liquidation_result.as_mut() {
                liq.amounts.collateral_received = Nat::from(amount_sats);
            }

            let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
            state.step = CexStep::Trade;
            // Force single-leg route for deterministic retry assertions. The plan
            // made at prepare time still names the old asset, so override it too.
            state.withdraw.withdraw_asset = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "BTC".to_string(),
                decimals: 8,
                fee: Nat::from(1_000u64),
            };
            state.withdraw.bridge.withdraw_planned_asset = Some("BTC".to_string());

            let first = finalizer.trade(&mut state).await.expect_err("first attempt should fail");
            assert!(first.to_string().contains("slice slippage too high"));
            assert!(matches!(state.step, CexStep::Trade));

            finalizer.trade(&mut state).await.expect("second attempt should recover");

            let seen = seen_amounts.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert!(state.trade.trade_progress_remaining_in.unwrap_or(0.0) <= 1e-12);
            assert!(matches!(state.step, CexStep::Withdraw));
            assert!(state.withdraw.size_out.as_ref().unwrap().to_f64() > 0.0);
        });
    }
}
