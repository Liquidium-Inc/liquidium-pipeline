//! The return bridge after a MEXC withdrawal (ckETH and ckERC20).
use super::*;

#[tokio::test]
async fn mexc_withdraw_bridge_waits_when_source_balance_below_expected_net() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let transfers = MockTransferActions::new();
    let liquidator = Principal::management_canister();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "USDC");
            assert_eq!(network, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            assert!((amount - 1.5).abs() < 1e-12);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("cex-tx-fee".to_string()),
                internal_id: Some("withdraw-fee".to_string()),
            })
        });

    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(1)
        .returning(|coin, withdraw_id| {
            assert_eq!(coin, "USDC");
            assert_eq!(withdraw_id, "withdraw-fee");
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("0xcompleted".to_string()),
                transaction_fee: None,
            })
        });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, address| {
            assert_eq!(asset, "USDC");
            assert_eq!(chain, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            // Simulate a stale/dust bridge source balance before the expected withdrawal credit arrives.
            Ok(1.4)
        });

    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(111, ckbtc_token(), ckusdc_token());
    let mut state = finalizer
        .prepare("111", &receipt)
        .await
        .expect("prepare should succeed");
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.5,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw submit should succeed");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-fee"));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge submit should wait for expected source balance");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_withdraw_bridge_deferral_keeps_step_pending_then_submits() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let liquidator = Principal::management_canister();

    cex.expect_withdraw().times(1).returning(|asset, network, _address, amount, _withdraw_order_id| {
        Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: Some("cex-tx-deferred".to_string()),
            internal_id: Some("withdraw-deferred".to_string()),
        })
    });
    cex.expect_get_withdraw_status_snapshot_by_id().times(2).returning(|_, _| {
        Ok(WithdrawStatusSnapshot {
            status: WithdrawStatus::Completed,
            txid: Some("0xcompleted".to_string()),
            transaction_fee: None,
        })
    });
    bridge.expect_get_source_balance().times(2).returning(|_, _, _| Ok(1.5));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let submit_calls = calls.clone();
    bridge.expect_prepare_bridge_send().times(2).returning(move |_| {
        let mut calls = submit_calls.lock().unwrap();
        *calls += 1;
        if *calls == 1 {
            Err(format!("{BRIDGE_SUBMISSION_DEFERRED_PREFIX}: bridge wallet has pending transactions"))
        } else {
            Ok(evm_bridge_send())
        }
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "bridge-after-wait".to_string(),
        }))
    });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)).with_outbound(Arc::new(landing_sender())));

    let receipt = make_execution_receipt_with_assets(112, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("112", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.5,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer.withdraw(&mut state).await.expect("cex withdraw submit");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("a deferred bridge submission is a wait, not an error");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    finalizer.withdraw(&mut state).await.expect("bridge submits once the wallet is idle");
    finalizer.withdraw(&mut state).await.expect("the saved bridge send goes out");
    finalizer.withdraw(&mut state).await.expect("the landed send is tracked");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("bridge-after-wait")
    );
}

#[tokio::test]
async fn mexc_deposit_bridge_deferral_keeps_step_pending_then_submits() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().returning(|_| Ok(10.0));
    cex.expect_get_deposit_address().returning(|_, _| {
        Ok(DepositAddress {
            asset: "USDC".to_string(),
            network: "ETH".to_string(),
            address: "0x1111111111111111111111111111111111111111".to_string(),
            tag: None,
        })
    });
    transfers
        .expect_transfer()
        .times(1)
        .returning(|_token, _to, _amount| Ok("tx-bridge-deferred".to_string()));
    bridge.expect_get_source_balance().returning(|_, _, _| Ok(10.0));
    bridge.expect_get_fee_budget().returning(|_, _, _| {
        Ok(BridgeFeeBudget {
            source_fee_budget: 0.0,
            destination_fee_budget: 0.0,
            provider_fee_budget_native_units: None,
            ..BridgeFeeBudget::default()
        })
    });
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.0));
    bridge.expect_prepare_submission().times(2).returning(|_| Ok(Some(1)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let submit_calls = calls.clone();
    bridge.expect_submit_bridge().times(2).returning(move |_| {
        let mut calls = submit_calls.lock().unwrap();
        *calls += 1;
        if *calls == 1 {
            Err(format!("{BRIDGE_SUBMISSION_DEFERRED_PREFIX}: bridge wallet has pending transactions"))
        } else {
            Ok(BridgeSubmission {
                bridge_id: "bridge-after-wait".to_string(),
            })
        }
    });

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(82, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("82", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer.deposit(&mut state).await.expect("transfer and record the attempt");
    finalizer
        .deposit(&mut state)
        .await
        .expect("a deferred bridge submission is a wait, not an error");
    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-bridge-deferred"));
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
    // The backend proved nothing was sent, so the call is dropped.
    assert!(state.deposit.bridge.deposit_bridge_call.is_none());

    finalizer.deposit(&mut state).await.expect("a new call is saved");
    finalizer.deposit(&mut state).await.expect("bridge submits once the wallet is idle");
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-after-wait")
    );
}

#[tokio::test]
async fn mexc_native_eth_withdraw_bridge_uses_bridgeable_balance_after_gas_reserve() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let transfers = MockTransferActions::new();
    let liquidator = Principal::management_canister();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "ETH");
            assert_eq!(network, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            assert!(amount > 0.0);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("cex-native-eth-tx".to_string()),
                internal_id: Some("withdraw-native-eth".to_string()),
            })
        });

    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(1)
        .returning(|coin, withdraw_id| {
            assert_eq!(coin, "ETH");
            assert_eq!(withdraw_id, "withdraw-native-eth");
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("0xcompleted".to_string()),
                transaction_fee: Some(0.000043),
            })
        });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, address| {
            assert_eq!(asset, "ETH");
            assert_eq!(chain, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            // ETH source balance is reported after reserving gas for the bridge transaction.
            Ok(0.016425721728152685)
        });

    bridge.expect_prepare_bridge_send().times(1).returning(|request| {
        assert_eq!(request.asset, "ETH");
        assert_eq!(request.source_chain, "ETH");
        assert_eq!(request.target_asset, "ckETH");
        assert_eq!(request.source_address, "0x2222222222222222222222222222222222222222");
        assert!((request.amount - 0.016425721728151685).abs() < 1e-18);
        Ok(evm_bridge_send())
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "bridge-native-eth".to_string(),
        }))
    });
    bridge.expect_get_bridge_status().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)).with_outbound(Arc::new(landing_sender())));

    let receipt = make_execution_receipt_with_assets(113, native_icp_token(), cketh_token());
    let mut state = finalizer
        .prepare("113", &receipt)
        .await
        .expect("prepare should succeed");
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        0.0200899299,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw submit should succeed");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-native-eth"));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge submit should use bridgeable source balance");
    finalizer.withdraw(&mut state).await.expect("the saved bridge send goes out");
    finalizer.withdraw(&mut state).await.expect("the landed send is tracked");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("bridge-native-eth")
    );
}

#[tokio::test]
async fn mexc_non_bridge_withdraw_behavior_unchanged() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "ckUSDT");
            assert_eq!(network, "ICP");
            assert_eq!(address, &Principal::anonymous().to_text());
            assert!(amount > 0.0);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("tx-direct".to_string()),
                internal_id: Some("withdraw-direct".to_string()),
            })
        });

    let finalizer = test_finalizer(Arc::new(cex), Arc::new(transfers));

    let receipt = make_execution_receipt(13);
    let mut state = finalizer.prepare("13", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("direct withdraw should still complete in one call");
    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-direct"));
}

#[tokio::test]
async fn mexc_non_bridge_withdraw_keeps_evm_destination_address() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let expected_address = "0x1111111111111111111111111111111111111111".to_string();

    cex.expect_withdraw()
        .times(1)
        .returning(move |asset, network, address, amount, _withdraw_order_id| {
            assert_eq!(asset, "USDC");
            assert_eq!(network, "evm-eth");
            assert_eq!(address, expected_address);
            assert!(amount > 0.0);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("tx-evm-direct".to_string()),
                internal_id: Some("withdraw-evm-direct".to_string()),
            })
        });

    let finalizer = test_finalizer(Arc::new(cex), Arc::new(transfers));

    let debt_token = ChainToken::EvmErc20 {
        chain: "eth".to_string(),
        token_address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
        symbol: "USDC".to_string(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    let receipt = make_execution_receipt_with_assets(14, ckbtc_token(), debt_token);
    let mut state = finalizer.prepare("14", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.withdraw_address = "0x1111111111111111111111111111111111111111".to_string();
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer
        .withdraw(&mut state)
        .await
        .expect("direct EVM withdraw should complete");
    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-evm-direct"));
}
