//! Deposits that reach MEXC through a bridge.
use super::*;

#[tokio::test]
async fn mexc_deposit_bridge_reserves_reverse_bridge_approval_fee_in_trade_amount() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    cex.expect_get_deposit_address().times(1).returning(|asset, network| {
        assert_eq!(asset, "USDC");
        assert_eq!(network, "ETH");
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
        .returning(|_token, _to, _amount| Ok("tx-bridge-fund".to_string()));

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, _address| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            Ok(0.99)
        });
    bridge.expect_prepare_submission().times(1).returning(|_| Ok(None));
    bridge
        .expect_get_fee_budget()
        .times(2)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.01,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: None,
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(0.0)
        });
    bridge.expect_submit_bridge().times(1).returning(move |request| {
        assert!(
            (request.amount - 0.98).abs() < 1e-9,
            "unexpected amount {}",
            request.amount
        );
        Ok(BridgeSubmission {
            bridge_id: "bridge-deposit-2".to_string(),
        })
    });

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(8, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("8", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit call should reserve reverse bridge approval fee before sizing trade");
    finalizer.deposit(&mut state).await.expect("the recorded attempt submits");

    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-deposit-2")
    );
    assert!((state.trade.trade_next_amount_in.unwrap_or_default() - 0.98).abs() < 1e-9);
}

#[tokio::test]
async fn mexc_deposit_bridge_rejects_source_fee_reserve_too_small_with_permanent_prefix() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    transfers.expect_transfer().times(0);

    bridge.expect_get_source_balance().times(0);
    bridge
        .expect_get_fee_budget()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(BridgeFeeBudget {
                source_fee_budget: 2.0,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: None,
                side_costs: Vec::new(),
            })
        });
    bridge.expect_get_minimum_bridge_amount().times(0);
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(84, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("84", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("source fee reserve consuming deposit must fail");

    assert!(err.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX));
    assert!(err.contains("too small to reserve reverse bridge source fee budget"));
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_deposit_bridge_reserves_native_cketh_withdrawal_fee_budget() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(10.0)
    });

    cex.expect_get_deposit_address().times(1).returning(|asset, network| {
        assert_eq!(asset, "ETH");
        assert_eq!(network, "ETH");
        Ok(DepositAddress {
            asset: "ETH".to_string(),
            network: "ETH".to_string(),
            address: "0x1111111111111111111111111111111111111111".to_string(),
            tag: None,
        })
    });

    transfers.expect_transfer().times(1).returning(|_token, _to, amount| {
        assert_eq!(amount, Nat::from(5_098_000_000_000_000u64));
        Ok("tx-bridge-fund-cketh".to_string())
    });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, _address| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            Ok(0.005_098)
        });
    bridge.expect_prepare_submission().times(1).returning(|_| Ok(Some(7)));
    bridge
        .expect_get_fee_budget()
        .times(2)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            // The minter takes gas out of the amount: only the approve fee stays behind.
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.000_002,
                destination_fee_budget: 0.000_049_900_885_02,
                provider_fee_budget_native_units: Some(Nat::from(49_900_885_020_000u64)),
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            Ok(0.005)
        });
    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "ckETH");
        assert_eq!(request.source_chain, "ICP");
        assert_eq!(request.target_asset, "ETH");
        assert!(
            (request.amount - 0.005_096).abs() < 1e-15,
            "unexpected amount {}",
            request.amount
        );
        assert_eq!(
            request.provider_fee_budget_native_units,
            Some(Nat::from(49_900_885_020_000u64))
        );
        Ok(BridgeSubmission {
            bridge_id: "bridge-deposit-cketh".to_string(),
        })
    });

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let mut receipt = make_execution_receipt_with_assets(82, cketh_token(), ckusdc_token());
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .collateral_received = Nat::from(5_100_000_000_000_000u64);
    let mut state = finalizer.prepare("82", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged native ckETH deposit should reserve the approval fee");
    finalizer.deposit(&mut state).await.expect("the recorded attempt submits");

    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-bridge-fund-cketh"));
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-deposit-cketh")
    );
    assert!((state.deposit.bridge.deposit_bridge_submit_amount.unwrap_or_default() - 0.005_096).abs() < 1e-15);
    assert!(
        (state.deposit.bridge.deposit_bridge_expected_amount.unwrap_or_default() - 0.005_046_099_114_98).abs() < 1e-15
    );
    assert!((state.trade.trade_next_amount_in.unwrap_or_default() - 0.005_046_099_114_98).abs() < 1e-15);
}

#[tokio::test]
async fn mexc_deposit_bridge_rejects_native_cketh_below_minimum_before_transfer() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(10.0)
    });

    transfers.expect_transfer().times(0);

    bridge.expect_get_source_balance().times(0);
    bridge
        .expect_get_fee_budget()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.000_051_900_885_02,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: Some(Nat::from(49_900_885_020_000u64)),
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            Ok(0.005)
        });
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let mut receipt = make_execution_receipt_with_assets(83, cketh_token(), ckusdc_token());
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .collateral_received = Nat::from(5_000_000_000_000_000u64);
    let mut state = finalizer.prepare("83", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("below minimum should fail before funding bridge source");

    assert!(err.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX));
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_deposit_bridge_rejects_native_cketh_when_destination_fee_consumes_expected_deposit() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(10.0)
    });

    transfers.expect_transfer().times(0);

    bridge.expect_get_source_balance().times(0);
    bridge
        .expect_get_fee_budget()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.000_051_900_885_02,
                destination_fee_budget: 0.006,
                provider_fee_budget_native_units: Some(Nat::from(49_900_885_020_000u64)),
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckETH");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "ETH");
            Ok(0.005)
        });
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let mut receipt = make_execution_receipt_with_assets(83, cketh_token(), ckusdc_token());
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .collateral_received = Nat::from(5_100_000_000_000_000u64);
    let mut state = finalizer.prepare("83", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("destination fee consuming expected deposit must fail");
    assert!(err.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX));
    assert!(err.contains("too small to cover destination fee budget"));
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_deposit_bridge_waits_for_source_funding_snapshot() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    cex.expect_get_deposit_address().times(1).returning(|asset, network| {
        assert_eq!(asset, "USDC");
        assert_eq!(network, "ETH");
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
        .returning(|_token, _to, _amount| Ok("tx-bridge-fund-wait".to_string()));

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, _address| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            Ok(0.000_001)
        });
    bridge
        .expect_get_fee_budget()
        .times(2)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.01,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: None,
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(0.0)
        });
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(81, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("81", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit should wait until source funding is visible");

    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-bridge-fund-wait"));
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_deposit_bridge_waits_for_cketh_fee_funding_snapshot() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    cex.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    cex.expect_get_deposit_address().times(1).returning(|asset, network| {
        assert_eq!(asset, "USDC");
        assert_eq!(network, "ETH");
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
        .returning(|_token, _to, _amount| Ok("tx-bridge-fund-fee-wait".to_string()));

    bridge
        .expect_get_source_balance()
        .times(2)
        .returning(|asset, chain, _address| {
            assert_eq!(chain, "ICP");
            match asset {
                "ckUSDC" => Ok(0.99),
                "ckETH" => Ok(0.000_080_356_126_532_001),
                other => panic!("unexpected source balance asset {other}"),
            }
        });
    bridge
        .expect_get_fee_budget()
        .times(2)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.01,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: Some(Nat::from(138_541_176_852_000u64)),
                side_costs: Vec::new(),
            })
        });
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|asset, chain, target| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            assert_eq!(target, "USDC");
            Ok(0.0)
        });
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(0);

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(84, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("84", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit should wait until ckETH fee funding is visible");

    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-bridge-fund-fee-wait"));
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
}

#[tokio::test]
async fn mexc_withdraw_bridge_submit_resume_complete_and_idempotent() {
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
            assert!(amount > 0.0);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: Some("cex-tx-1".to_string()),
                internal_id: Some("withdraw-1".to_string()),
            })
        });

    let withdraw_status_calls = Arc::new(std::sync::Mutex::new(0u8));
    let withdraw_status_calls_clone = withdraw_status_calls.clone();
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(2)
        .returning(move |coin, withdraw_id| {
            assert_eq!(coin, "USDC");
            assert_eq!(withdraw_id, "withdraw-1");
            let mut guard = withdraw_status_calls_clone.lock().expect("mutex");
            *guard += 1;
            if *guard == 1 {
                Ok(WithdrawStatusSnapshot {
                    status: WithdrawStatus::Pending,
                    txid: None,
                    transaction_fee: None,
                })
            } else {
                Ok(WithdrawStatusSnapshot {
                    status: WithdrawStatus::Completed,
                    txid: Some("0xcompleted".to_string()),
                    transaction_fee: None,
                })
            }
        });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, address| {
            assert_eq!(asset, "USDC");
            assert_eq!(chain, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            Ok(1.5)
        });

    bridge.expect_prepare_bridge_send().times(1).returning(|request| {
        assert_eq!(request.asset, "USDC");
        assert_eq!(request.source_chain, "ETH");
        assert_eq!(request.target_asset, "ckUSDC");
        assert_eq!(request.source_address, "0x2222222222222222222222222222222222222222");
        match &request.destination {
            BridgeDestination::IcpAccount(account) => {
                assert_eq!(account.owner, Principal::management_canister());
                assert!(account.subaccount.is_none());
            }
            other => panic!("unexpected bridge destination: {:?}", other),
        }
        Ok(evm_bridge_send())
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "bridge-withdraw-1".to_string(),
        }))
    });

    let bridge_status_calls = Arc::new(std::sync::Mutex::new(0u8));
    let bridge_status_calls_clone = bridge_status_calls.clone();
    bridge.expect_get_bridge_status().times(3).returning(move |bridge_id| {
        assert_eq!(bridge_id, "bridge-withdraw-1");
        let mut guard = bridge_status_calls_clone.lock().expect("mutex");
        *guard += 1;
        if *guard == 1 {
            Ok(BridgeStatus::Pending)
        } else {
            Ok(BridgeStatus::Minted(
                liquidium_pipeline_connectors::backend::bridge_backend::BridgeMintReceipt {
                    transaction_hash: "0xhelper".into(),
                    log_index: Nat::from(1u64),
                    mint_block_index: Nat::from(42u64),
                    amount: Nat::from(1_500_000u64),
                    recipient: Account {
                        owner: if *guard == 2 {
                            Principal::anonymous()
                        } else {
                            Principal::management_canister()
                        },
                        subaccount: None,
                    },
                    token_symbol: "ckUSDC".into(),
                },
            ))
        }
    });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        liquidator,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)).with_outbound(Arc::new(landing_sender())));

    let receipt = make_execution_receipt_with_assets(11, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("11", &receipt).await.expect("prepare should succeed");
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
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-1"));

    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw pending poll should keep pending");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw completion should submit bridge once");
    finalizer.withdraw(&mut state).await.expect("the saved bridge send goes out");
    finalizer.withdraw(&mut state).await.expect("the landed send is tracked");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("bridge-withdraw-1")
    );

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge pending should keep pending");
    assert!(matches!(state.step, CexStep::WithdrawPending));

    // Reconstruct durable state: polling must not resubmit either withdrawal.
    state = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    let error = finalizer.withdraw(&mut state).await.unwrap_err();
    assert!(error.contains("destination/token"));
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert!(state.withdraw.bridge.withdraw_bridge_mint_receipt.is_none());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge completion should finish");
    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.size_out.as_ref().unwrap().value, Nat::from(1_500_000u64));
    let saved: CexState = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    assert_eq!(
        saved
            .withdraw
            .bridge
            .withdraw_bridge_mint_receipt
            .unwrap()
            .mint_block_index,
        Nat::from(42u64)
    );
}

#[tokio::test]
async fn mexc_withdraw_bridge_applies_cex_withdraw_fee_to_submit_amount() {
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
                txid: Some("cex-tx-fee-apply".to_string()),
                internal_id: Some("withdraw-fee-apply".to_string()),
            })
        });

    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(1)
        .returning(|coin, withdraw_id| {
            assert_eq!(coin, "USDC");
            assert_eq!(withdraw_id, "withdraw-fee-apply");
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("0xcompleted".to_string()),
                transaction_fee: Some(0.1),
            })
        });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, address| {
            assert_eq!(asset, "USDC");
            assert_eq!(chain, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            // Available is higher than net amount after fee so this validates fee deduction (not source cap).
            Ok(1.45)
        });

    bridge.expect_prepare_bridge_send().times(1).returning(|request| {
        assert_eq!(request.asset, "USDC");
        assert_eq!(request.source_chain, "ETH");
        assert_eq!(request.target_asset, "ckUSDC");
        assert!((request.amount - 1.4).abs() < 1e-12);
        Ok(evm_bridge_send())
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "bridge-withdraw-fee-apply".to_string(),
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

    let receipt = make_execution_receipt_with_assets(112, ckbtc_token(), ckusdc_token());
    let mut state = finalizer
        .prepare("112", &receipt)
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
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-fee-apply"));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge submit should apply CEX withdraw fee to amount");
    finalizer.withdraw(&mut state).await.expect("the saved bridge send goes out");
    finalizer.withdraw(&mut state).await.expect("the landed send is tracked");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("bridge-withdraw-fee-apply")
    );
}
