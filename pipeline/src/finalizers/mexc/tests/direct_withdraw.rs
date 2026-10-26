//! The MEXC withdrawal request: order id, delivery, below-minimum dust.
use super::*;

#[test]
fn mexc_withdraw_below_min_detection_uses_raw_code() {
    assert!(is_mexc_withdraw_below_min_error(
        r#"Error response: ErrorResponse { code: InvalidResponse, raw_code: 10254, msg: "localized or changed text", _extend: None }"#
    ));
    assert!(!is_mexc_withdraw_below_min_error(
        "Withdrawal shall not be less than the Min amount of:0.00002"
    ));
}

#[test]
fn the_withdraw_order_id_format_never_changes() {
    // A saved row polls MEXC history by this id, so a new release must build
    // the same id from the same row.
    assert_eq!(mexc_withdraw_order_id("1720", 3), "lq04733af0f350196bw3");
}

#[tokio::test]
async fn mexc_prepare_builds_initial_cex_state() {
    let backend = Arc::new(MockCexBackend::new());
    let transfer_service = Arc::new(MockTransferActions::new());
    let liquidator = Principal::anonymous();

    let finalizer = MexcFinalizer::new(
        backend,
        transfer_service,
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let state: CexState = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    assert_eq!(state.liq_id, "42");
    assert!(matches!(state.step, CexStep::Deposit));

    // deposit leg
    assert_eq!(state.deposit.deposit_asset, receipt.request.collateral_asset);
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.deposit_balance_before.is_none());
    assert!(state.deposit.deposit_sent_at_ts.is_none());

    // trade leg
    let expected_market = format!(
        "{}_{}",
        receipt.request.collateral_asset.symbol(),
        receipt.request.debt_asset.symbol()
    );
    assert_eq!(state.market, expected_market);
    assert_eq!(state.side, "sell");
    assert_eq!(state.size_in.token, receipt.request.collateral_asset);
    assert_eq!(
        state.size_in.value,
        receipt.liquidation_result.as_ref().unwrap().amounts.collateral_received
    );

    // withdraw leg
    assert_eq!(state.withdraw.withdraw_asset, receipt.request.debt_asset);
    assert_eq!(state.withdraw.withdraw_address, liquidator.to_text());
    assert!(state.withdraw.withdraw_id.is_none());
    assert!(state.withdraw.withdraw_txid.is_none());
    assert!(state.withdraw.size_out.is_none());
}

#[tokio::test]
async fn mexc_direct_withdraw_parks_when_delivery_never_arrives() {
    use liquidium_pipeline_connectors::backend::cex_backend::{WithdrawStatus, WithdrawStatusSnapshot, WithdrawalReceipt};
    let mut cex = MockCexBackend::new();
    cex.expect_withdraw().times(1).returning(|asset, network, _, amount, _withdraw_order_id| {
        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some("never-delivered".to_string()),
        })
    });
    cex.expect_get_withdraw_status_snapshot_by_id().times(2).returning(|_, _| {
        Ok(WithdrawStatusSnapshot {
            status: WithdrawStatus::Pending,
            txid: None,
            transaction_fee: None,
        })
    });
    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state = finalizer.prepare("stuck", &make_execution_receipt(131)).await.unwrap();
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), 1.0));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer.withdraw(&mut state).await.unwrap();
    assert!(matches!(state.step, CexStep::WithdrawPending));
    let accepted_at = state.withdraw.withdraw_accepted_at_ts.expect("accept time recorded");

    // Within the window the row keeps waiting.
    finalizer.withdraw(&mut state).await.unwrap();
    assert!(matches!(state.step, CexStep::WithdrawPending));

    // Past the window the row errors so the retry budget parks it with an alert.
    state.withdraw.withdraw_accepted_at_ts = Some(accepted_at - 2 * 60 * 60);
    let error = finalizer.withdraw(&mut state).await.unwrap_err();
    assert!(error.contains("not delivered"), "{error}");
    assert!(matches!(state.step, CexStep::WithdrawPending));
}

#[tokio::test]
async fn mexc_direct_withdraw_waits_for_delivery_and_deducts_fee_once() {
    use liquidium_pipeline_connectors::backend::cex_backend::{
        WithdrawStatus, WithdrawStatusSnapshot, WithdrawalReceipt,
    };
    let mut cex = MockCexBackend::new();
    cex.expect_withdraw().times(1).returning(|asset, network, _, amount, _withdraw_order_id| {
        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some("pending-direct".to_string()),
        })
    });
    let mut poll = 0;
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(2)
        .returning(move |_, id| {
            assert_eq!(id, "pending-direct");
            poll += 1;
            Ok(WithdrawStatusSnapshot {
                status: if poll == 1 {
                    WithdrawStatus::Pending
                } else {
                    WithdrawStatus::Completed
                },
                txid: if poll == 1 { None } else { Some("ledger:42".to_string()) },
                transaction_fee: Some(0.02),
            })
        });
    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let mut state = finalizer
        .prepare("pending", &make_execution_receipt(130))
        .await
        .unwrap();
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));
    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    for _ in 0..2 {
        finalizer.withdraw(&mut state).await.unwrap();
        assert!(matches!(state.step, CexStep::WithdrawPending));
        assert_eq!(state.withdraw.size_out.as_ref().unwrap().to_f64(), 1.0);
    }
    state = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    finalizer.withdraw(&mut state).await.unwrap();
    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.withdraw_txid.as_deref(), Some("ledger:42"));
    assert_eq!(state.withdraw.size_out.as_ref().unwrap().to_f64(), 0.98);
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.size_out.as_ref().unwrap().to_f64(), 0.98);
}

#[tokio::test]
async fn mexc_native_icp_non_bridge_withdraw_uses_liquidator_account_id_hex() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let liquidator = Principal::management_canister();
    let expected_account_id = AccountIdentifier::new(&liquidator, &Subaccount([0; 32])).to_hex();

    cex.expect_withdraw()
        .times(1)
        .returning(move |asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "ICP");
            assert_eq!(network, "ICP");
            assert_eq!(address, expected_account_id);
            assert!(amount > 0.0);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("tx-native-icp-withdraw".to_string()),
                internal_id: Some("withdraw-native-icp".to_string()),
            })
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt_with_assets(131, ckbtc_token(), native_icp_token());
    let mut state = finalizer
        .prepare("131", &receipt)
        .await
        .expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("native ICP direct withdraw should complete in one call");

    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-native-icp"));
    assert_ne!(state.withdraw.withdraw_address, liquidator.to_text());
}

#[test]
fn mexc_native_icp_withdraw_converts_the_persisted_per_leg_destination() {
    let finalizer = MexcFinalizer::new(
        Arc::new(MockCexBackend::new()),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );
    let requested = Account {
        owner: Principal::from_slice(&[42]),
        subaccount: Some([7; 32]),
    };
    let expected = AccountIdentifier::new(&requested.owner, &Subaccount([7; 32])).to_hex();

    assert_eq!(
        finalizer
            .native_icp_direct_withdraw_address(&requested.to_string())
            .expect("valid per-leg destination"),
        expected
    );
}

#[tokio::test]
async fn mexc_non_bridge_withdraw_below_min_completes_as_zero_output_dust() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    cex.expect_withdraw()
        .times(1)
        .returning(|_asset, _network, _address, _amount, _withdraw_order_id| {
            Err(
                r#"Error response: ErrorResponse { code: InvalidResponse, raw_code: 10254, msg: "Withdrawal shall not be less than the Min amount of:0.00002", _extend: None }"#
                    .to_string(),
            )
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(13);
    let mut state = finalizer.prepare("13", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        0.00001,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("below-min withdraw should be treated as terminal CEX dust");

    assert!(matches!(state.step, CexStep::Completed));
    assert!(state.withdraw.withdraw_id.is_none());
    assert!(state.withdraw.withdraw_txid.is_none());
    assert_eq!(state.withdraw.size_out.as_ref().unwrap().value, Nat::from(0u8));
    assert!(
        state
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("mexc withdraw below minimum")
    );
}

#[tokio::test]
async fn mexc_bridged_withdraw_below_min_completes_as_zero_output_dust() {
    let mut cex = MockCexBackend::new();
    let bridge = MockBridgeBackend::new();
    let transfers = MockTransferActions::new();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "USDC");
            assert_eq!(network, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            assert_eq!(amount, 0.00001);
            Err(
                r#"Error response: ErrorResponse { code: InvalidResponse, raw_code: 10254, msg: "localized or changed text", _extend: None }"#
                    .to_string(),
            )
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(14, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("14", &receipt).await.expect("prepare should succeed");
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        0.00001,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridged below-min withdraw should be treated as terminal CEX dust");

    assert!(matches!(state.step, CexStep::Completed));
    assert!(state.withdraw.withdraw_id.is_none());
    assert!(state.withdraw.withdraw_txid.is_none());
    assert_eq!(state.withdraw.size_out.as_ref().unwrap().value, Nat::from(0u8));
    assert!(
        state
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("mexc withdraw below minimum")
    );
}

#[tokio::test]
async fn mexc_non_bridge_withdraw_rejects_invalid_evm_destination_address() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    cex.expect_withdraw().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let debt_token = ChainToken::EvmErc20 {
        chain: "eth".to_string(),
        token_address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
        symbol: "USDC".to_string(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    let receipt = make_execution_receipt_with_assets(15, ckbtc_token(), debt_token);
    let mut state = finalizer.prepare("15", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.withdraw_address = "not-an-evm-address".to_string();
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    let err = finalizer
        .withdraw(&mut state)
        .await
        .expect_err("invalid EVM destination should fail");
    assert!(err.contains("invalid EVM withdraw destination"));
}

#[tokio::test]
async fn mexc_deposit_phase_b_without_baseline_sets_baseline_and_keeps_step() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    backend.expect_get_balance().returning(|_symbol| Ok(5.0));

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = MexcFinalizer::new(
        backend,
        transfer_service,
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Simulate that Phase A already ran and sent a tx, but baseline was never recorded.
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = None;
    state.step = CexStep::DepositPending;

    // Phase B: deposit() delegates to check_deposit, which should set the baseline
    // and keep the step unchanged.
    finalizer
        .deposit(&mut state)
        .await
        .expect("deposit (phase B) should succeed");

    assert_eq!(state.deposit.deposit_balance_before, Some(5.0));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-123"));
    assert!(state.deposit.deposit_sent_at_ts.is_some());
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_withdraw_fails_on_non_positive_amount() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Backend must not be called when amount <= 0.
    backend.expect_withdraw().times(0);

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = MexcFinalizer::new(
        backend,
        transfer_service,
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    state.step = CexStep::Withdraw;
    state.size_in.value = Nat::from(0u32);

    let err = finalizer
        .withdraw(&mut state)
        .await
        .expect_err("withdraw should fail for non-positive amount");

    assert!(err.contains("withdrawal amount is zero or negative"));
}

#[tokio::test]
async fn mexc_finish_builds_synthetic_swap_execution_from_state() {
    let backend = Arc::new(MockCexBackend::new());
    let transfers = Arc::new(MockTransferActions::new());

    let finalizer = MexcFinalizer::new(
        backend,
        transfers,
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Simulate that trade/withdraw legs have populated size_out.
    // Use a nice round native amount so we can reason about the price.
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        2.0,
    ));
    state.trade.trade_weighted_slippage_bps = Some(42.5);

    let swap = finalizer.finish(&receipt, &state).await.expect("finish should succeed");

    // Pay leg comes from seized collateral (size_in).
    assert_eq!(swap.pay_asset, state.deposit.deposit_asset.asset_id());
    assert_eq!(swap.pay_amount, state.size_in.value);

    // Receive leg comes from size_out.
    let expected_out = state.withdraw.size_out.as_ref().unwrap();
    assert_eq!(swap.receive_asset, state.withdraw.withdraw_asset.asset_id());
    assert_eq!(swap.receive_amount, expected_out.value);

    // Price is computed as receive / pay in native units.
    let pay_native = state.size_in.to_f64();
    let recv_native = expected_out.to_f64();
    let expected_price = if pay_native > 0.0 {
        recv_native / pay_native
    } else {
        0.0
    };

    assert!((swap.exec_price - expected_price).abs() < 1e-9);
    // At the mids the route returns 1 / (1 - 42.5 bps) times more.
    assert!((swap.mid_price - expected_price / (1.0 - 0.00425)).abs() < 1e-9);
    assert_eq!(swap.realized_slippage_bps, 42.5);

    // Status and legs should reflect a single synthetic CEX hop.
    assert_eq!(swap.status, "completed".to_string());
    assert!(swap.legs.is_empty());
}
