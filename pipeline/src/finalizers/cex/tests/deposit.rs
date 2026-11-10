//! The deposit funding transfer, deposit confirmation and approval bumps.
use super::*;

#[tokio::test]
async fn mexc_deposit_phase_a_rejects_fee_consumed_deposit_with_permanent_prefix() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "ckUSDC");
        Ok(10.0)
    });
    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|asset, network| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(network, "ICP");
            Ok(DepositAddress {
                asset: "ckUSDC".to_string(),
                network: "ICP".to_string(),
                address: Principal::anonymous().to_text(),
                tag: None,
            })
        });
    transfers.expect_transfer().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let mut receipt = make_execution_receipt_with_assets(85, ckusdc_token(), ckbtc_token());
    receipt
        .liquidation_result
        .as_mut()
        .expect("liquidation result")
        .amounts
        .collateral_received = Nat::from(70_000u64);
    let mut state = finalizer.prepare("85", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("deposit consumed by fee must fail before transfer");

    assert!(matches!(err, CexBackendError::BelowMinimum(_)));
    assert!(err.to_string().contains("too small to reserve ledger fee"));
    assert!(state.deposit.deposit_txid.is_none());
}

#[tokio::test]
async fn mexc_deposit_phase_a_uses_evm_destination_for_evm_assets() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|asset, network| {
            assert_eq!(asset, "USDC");
            assert_eq!(network, "evm-eth");
            Ok(DepositAddress {
                asset: "USDC".to_string(),
                network: "ERC20".to_string(),
                address: "0x1111111111111111111111111111111111111111".to_string(),
                tag: None,
            })
        });

    transfers
        .expect_transfer()
        .times(1)
        .returning(|token, destination, amount| {
            assert!(matches!(token, ChainToken::EvmErc20 { symbol, .. } if symbol == "USDC"));
            match destination {
                ChainAccount::Evm(address) => {
                    assert_eq!(address, "0x1111111111111111111111111111111111111111");
                }
                other => panic!("expected EVM deposit destination, got {:?}", other),
            }
            assert_eq!(amount, Nat::from(1_000_000u64));
            Ok("tx-evm-123".to_string())
        });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let collateral = ChainToken::EvmErc20 {
        chain: "eth".to_string(),
        token_address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
        symbol: "USDC".to_string(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    let receipt = make_execution_receipt_with_assets(43, collateral, ckbtc_token());
    let mut state = finalizer.prepare("43", &receipt).await.expect("prepare should succeed");

    fund(&finalizer, &mut state).await;

    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-evm-123"));
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_deposit_phase_a_rejects_invalid_evm_deposit_address() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|_asset| Ok(10.0));
    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|_asset, _network| {
            Ok(DepositAddress {
                asset: "USDC".to_string(),
                network: "ERC20".to_string(),
                address: "not-an-evm-address".to_string(),
                tag: None,
            })
        });
    transfers.expect_transfer().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let collateral = ChainToken::EvmErc20 {
        chain: "eth".to_string(),
        token_address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
        symbol: "USDC".to_string(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    let receipt = make_execution_receipt_with_assets(44, collateral, ckbtc_token());
    let mut state = finalizer.prepare("44", &receipt).await.expect("prepare should succeed");

    let err = finalizer.deposit(&mut state).await.expect_err("deposit should fail");
    assert!(err.to_string().contains("invalid exchange EVM deposit address"));
}

#[tokio::test]
async fn mexc_native_icp_deposit_uses_ledger_account_id_destination() {
    AccountIdentifier::from_hex(MEXC_ICP_DEPOSIT_ACCOUNT_ID_HEX).expect("test MEXC ICP account id should be valid");

    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "ICP");
        Ok(10.0)
    });

    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|asset, network| {
            assert_eq!(asset, "ICP");
            assert_eq!(network, "ICP");
            Ok(DepositAddress {
                asset: "ICP".to_string(),
                network: "ICP".to_string(),
                address: MEXC_ICP_DEPOSIT_ACCOUNT_ID_HEX.to_string(),
                tag: None,
            })
        });

    transfers
        .expect_transfer()
        .times(1)
        .returning(|token, destination, amount| {
            assert!(CexVenueFinalizer::<MockCexBackend>::is_native_icp_token(token));
            match destination {
                ChainAccount::IcpLedger(account_id_hex) => {
                    assert_eq!(account_id_hex, MEXC_ICP_DEPOSIT_ACCOUNT_ID_HEX);
                }
                other => panic!("expected native ICP ledger account destination, got {:?}", other),
            }
            assert_eq!(amount, Nat::from(990_000u64));
            Ok("tx-native-icp".to_string())
        });
    transfers.expect_approve().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt_with_assets(45, native_icp_token(), ckbtc_token());
    let mut state = finalizer.prepare("45", &receipt).await.expect("prepare should succeed");

    fund(&finalizer, &mut state).await;

    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-native-icp"));
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_native_icp_deposit_rejects_invalid_ledger_account_id() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|_asset| Ok(10.0));
    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|_asset, _network| {
            Ok(DepositAddress {
                asset: "ICP".to_string(),
                network: "ICP".to_string(),
                address: "not-a-ledger-account-id".to_string(),
                tag: None,
            })
        });
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt_with_assets(46, native_icp_token(), ckbtc_token());
    let mut state = finalizer.prepare("46", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("invalid native ICP account id should fail");
    assert!(err.to_string().contains("invalid native ICP ledger account id"));
}

#[tokio::test]
async fn mexc_native_icp_deposit_rejects_memo_tag_before_transfer() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|_asset| Ok(10.0));
    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|_asset, _network| {
            Ok(DepositAddress {
                asset: "ICP".to_string(),
                network: "ICP".to_string(),
                address: MEXC_ICP_DEPOSIT_ACCOUNT_ID_HEX.to_string(),
                tag: Some("memo-not-supported".to_string()),
            })
        });
    transfers.expect_transfer().times(0);
    transfers.expect_approve().times(0);

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt_with_assets(47, native_icp_token(), ckbtc_token());
    let mut state = finalizer.prepare("47", &receipt).await.expect("prepare should succeed");

    let err = finalizer
        .deposit(&mut state)
        .await
        .expect_err("native ICP memo/tag should fail before transfer");
    assert!(err.to_string().contains("unsupported memo/tag"));
}

#[test]
fn mexc_sol_reverse_plan_reserves_approval_fee() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()))
        .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));
    let sol = ChainToken::Icp {
        ledger: Principal::anonymous(), symbol: "ckSOL".into(), decimals: 9, fee: Nat::from(500u64),
    };
    let stable = ChainToken::Icp {
        ledger: Principal::anonymous(), symbol: "ckUSDT".into(), decimals: 6, fee: Nat::from(10_000u64),
    };
    let plan = finalizer.resolve_bridge_plan_for_assets(&sol, &stable);
    assert!(plan.deposit.bridge_required);
    assert_eq!(plan.deposit.cex_asset, "SOL");
    assert_eq!(plan.deposit.cex_network, "SOL");
    assert!(!plan.withdraw.bridge_required);
    let route = liquidium_pipeline_connectors::backend::bridge_backend::resolve_route("ckSOL", "ICP", "SOL").unwrap();
    let amount = ChainTokenAmount::from_raw(sol.clone(), Nat::from(1_000_000_000u64));
    let (reserve, burn) = CexVenueFinalizer::<MockCexBackend>::compute_reverse_bridge_source_fee_reserve(
        route, &sol, &amount.value, 0.0000005,
    ).unwrap().unwrap();
    assert_eq!(reserve.value, Nat::from(500u64));
    assert_eq!(burn.value, Nat::from(999_999_500u64));
    assert!(
        crate::finalizers::bridge_planner::bridge_destination(route.destination_kind, "not-a-solana-address",).is_err()
    );
}

#[tokio::test]
async fn mexc_deposit_phase_b_moves_to_trade_when_balance_increased() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    backend.expect_get_balance().returning(|_symbol| Ok(5.1));

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Simulate that Phase A already ran, we have a baseline, and we are now in DepositPending.
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = Some(5.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("deposit (phase B) should succeed");

    // Baseline should remain unchanged, and we should advance to Trade when balance increased.
    assert_eq!(state.deposit.deposit_balance_before, Some(5.0));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-123"));
    assert!(matches!(state.step, CexStep::Trade));
}

/// Review finding 14: an uncredited deposit waited forever with no alert.
#[tokio::test]
async fn an_uncredited_deposit_errors_once_its_wait_is_over() {
    let mut backend = MockCexBackend::new();
    backend.expect_get_balance().returning(|_symbol| Ok(5.0));
    let finalizer = test_finalizer(Arc::new(backend), Arc::new(MockTransferActions::new()));
    let mut state = finalizer.prepare("42", &make_execution_receipt(42)).await.unwrap();
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = Some(5.0);
    state.step = CexStep::DepositPending;

    finalizer.deposit(&mut state).await.expect("the credit wait starts");
    let since = state.deposit.deposit_credit_since_ts.expect("the wait has a clock");

    state.deposit.deposit_credit_since_ts = Some(since - 3 * 60 * 60);
    let error = finalizer.deposit(&mut state).await.unwrap_err();
    assert!(error.to_string().contains("not credited"), "{error}");
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_deposit_phase_b_stays_in_deposit_when_balance_unchanged() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Balance stays the same as baseline.
    backend.expect_get_balance().returning(|_symbol| Ok(5.0));

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Simulate Phase A done, baseline recorded, and we are waiting in DepositPending.
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = Some(5.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("deposit (phase B) should succeed");

    // Since balance did not increase enough, we should still be in DepositPending.
    assert_eq!(state.deposit.deposit_balance_before, Some(5.0));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-123"));
    assert!(state.deposit.deposit_sent_at_ts.is_some());
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_deposit_phase_b_confirms_via_total_free_fallback_after_wait_window() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Total free covers expected, but delta from baseline does not.
    backend.expect_get_balance().returning(|_symbol| Ok(10.0));

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = Some(4.0);
    state.deposit.deposit_sent_at_ts = Some(crate::utils::now_ts().saturating_sub(30));
    state.trade.trade_next_amount_in = Some(10.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("deposit should confirm via total-free fallback");
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn mexc_deposit_phase_b_bridged_confirmation_uses_expected_amount_and_observed_delta() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let mut bridge = MockBridgeBackend::new();

    backend.expect_get_balance().returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(14.9)
    });
    bridge.expect_get_bridge_status().times(1).returning(|bridge_id| {
        assert_eq!(bridge_id, "bridge-deposit-cketh");
        Ok(BridgeStatus::Completed)
    });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(82, cketh_token(), ckusdc_token());
    let mut state = finalizer.prepare("82", &receipt).await.expect("prepare should succeed");
    state.deposit.deposit_txid = Some("tx-bridge-fund-cketh".to_string());
    state.deposit.deposit_balance_before = Some(5.0);
    state.deposit.bridge.deposit_bridge_id = Some("bridge-deposit-cketh".to_string());
    state.deposit.bridge.deposit_bridge_submit_amount = Some(10.0);
    state.deposit.bridge.deposit_bridge_expected_amount = Some(9.9);
    state.trade.trade_next_amount_in = Some(10.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit should confirm on expected net amount");

    assert!(matches!(state.step, CexStep::Trade));
    assert!((state.trade.trade_next_amount_in.unwrap_or_default() - 9.9).abs() < 1e-12);
}

/// A balance delta larger than this deposit could possibly have credited means
/// the reading is contaminated — typically a second deposit crediting the same
/// account concurrently. The trade must then be sized on the post-fee expected
/// amount, never the pre-fee submit amount, which the minter's withdrawal fee
/// makes unreachable. Sizing on the gross oversells what arrived and the venue
/// rejects the order as oversold.
#[tokio::test]
async fn mexc_deposit_phase_b_bridged_confirmation_caps_contaminated_delta_at_expected_amount() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let mut bridge = MockBridgeBackend::new();

    // 15.2 - 5.0 = 10.2 delta: more than the 10.0 submitted, so a concurrent
    // deposit is inflating it.
    backend.expect_get_balance().returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(15.2)
    });
    bridge.expect_get_bridge_status().times(1).returning(|bridge_id| {
        assert_eq!(bridge_id, "bridge-deposit-cketh");
        Ok(BridgeStatus::Completed)
    });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(82, cketh_token(), ckusdc_token());
    let mut state = finalizer.prepare("82", &receipt).await.expect("prepare should succeed");
    state.deposit.deposit_txid = Some("tx-bridge-fund-cketh".to_string());
    state.deposit.deposit_balance_before = Some(5.0);
    state.deposit.bridge.deposit_bridge_id = Some("bridge-deposit-cketh".to_string());
    state.deposit.bridge.deposit_bridge_submit_amount = Some(10.0);
    state.deposit.bridge.deposit_bridge_expected_amount = Some(9.9);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit should cap confirmed amount");

    assert!(matches!(state.step, CexStep::Trade));
    assert!(
        (state.trade.trade_next_amount_in.unwrap_or_default() - 9.9).abs() < 1e-12,
        "contaminated delta must cap at the post-fee expected amount, not the pre-fee submit amount"
    );
}

/// Regression for liq 1563: two ckETH withdrawals pre-credited to the MEXC
/// account in the same block, so this leg's balance delta picked up the other
/// deposit as well. The pre-fee cap then sized the sell at the full submitted
/// 0.00804851439432068 ETH while only 0.00801372 had actually credited, and
/// MEXC rejected it as oversold on every retry until the settlement wait
/// expired.
///
/// The fee budget was not at fault: 0.0000425 was reserved against an actual
/// 0.0000348 minter fee, so the credit landed slightly above forecast.
#[tokio::test]
async fn mexc_deposit_never_sizes_a_trade_above_what_the_bridge_could_credit() {
    let submitted = 0.008_048_514_394_320_68;
    let expected_after_fee = 0.008_005_979_746_399_88;
    let baseline = 0.000_018_374_568_812_442;
    // Baseline plus this leg's real credit plus a concurrent deposit.
    let contaminated_balance = baseline + 0.008_013_72 + 0.010_278_35;

    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let mut bridge = MockBridgeBackend::new();

    backend.expect_get_balance().returning(move |asset| {
        assert_eq!(asset, "ETH");
        Ok(contaminated_balance)
    });
    bridge.expect_get_bridge_status().times(1).returning(|bridge_id| {
        assert_eq!(bridge_id, "bridge-deposit-cketh");
        Ok(BridgeStatus::Completed)
    });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(82, cketh_token(), ckusdc_token());
    let mut state = finalizer
        .prepare("1563", &receipt)
        .await
        .expect("prepare should succeed");
    state.deposit.deposit_txid = Some("989_486".to_string());
    state.deposit.deposit_balance_before = Some(baseline);
    state.deposit.bridge.deposit_bridge_id = Some("bridge-deposit-cketh".to_string());
    state.deposit.bridge.deposit_bridge_submit_amount = Some(submitted);
    state.deposit.bridge.deposit_bridge_expected_amount = Some(expected_after_fee);
    state.step = CexStep::DepositPending;

    finalizer.deposit(&mut state).await.expect("deposit should confirm");

    let sized = state.trade.trade_next_amount_in.expect("trade amount should be set");
    assert!(
        sized <= expected_after_fee,
        "sized {sized} above the post-fee ceiling {expected_after_fee}; this is the oversold bug"
    );
    assert!(
        sized < submitted,
        "sized {sized} at the pre-fee submit amount {submitted}, which the minter fee makes unreachable"
    );
}

#[tokio::test]
async fn mexc_deposit_phase_b_bridged_total_free_fallback_keeps_expected_amount() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();
    let mut bridge = MockBridgeBackend::new();

    backend.expect_get_balance().returning(|asset| {
        assert_eq!(asset, "ETH");
        Ok(10.0)
    });
    bridge.expect_get_bridge_status().times(1).returning(|bridge_id| {
        assert_eq!(bridge_id, "bridge-deposit-cketh");
        Ok(BridgeStatus::Completed)
    });

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers))
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(82, cketh_token(), ckusdc_token());
    let mut state = finalizer.prepare("82", &receipt).await.expect("prepare should succeed");
    state.deposit.deposit_txid = Some("tx-bridge-fund-cketh".to_string());
    state.deposit.deposit_balance_before = Some(100.0);
    state.deposit.deposit_sent_at_ts = Some(crate::utils::now_ts().saturating_sub(30));
    state.deposit.bridge.deposit_bridge_id = Some("bridge-deposit-cketh".to_string());
    state.deposit.bridge.deposit_bridge_submit_amount = Some(10.0);
    state.deposit.bridge.deposit_bridge_expected_amount = Some(9.9);
    state.trade.trade_next_amount_in = Some(10.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit should confirm via fallback");

    assert!(matches!(state.step, CexStep::Trade));
    assert!((state.trade.trade_next_amount_in.unwrap_or_default() - 9.9).abs() < 1e-12);
}

#[tokio::test]
async fn mexc_deposit_phase_b_does_not_use_total_free_fallback_before_wait_window() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Total free covers expected, but fallback window has not elapsed yet.
    backend.expect_get_balance().returning(|_symbol| Ok(10.0));

    let finalizer = test_finalizer(Arc::new(backend), Arc::new(transfers));

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
    state.deposit.deposit_txid = Some("tx-123".to_string());
    state.deposit.deposit_balance_before = Some(4.0);
    state.deposit.deposit_sent_at_ts = Some(crate::utils::now_ts().saturating_sub(1));
    state.trade.trade_next_amount_in = Some(10.0);
    state.step = CexStep::DepositPending;

    finalizer
        .deposit(&mut state)
        .await
        .expect("deposit should stay pending before total-free fallback window");
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[test]
fn cex_deposit_state_deserialize_defaults_missing_sent_timestamp() {
    let original = CexDepositState {
        deposit_asset: ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        },
        deposit_txid: Some("tx-123".to_string()),
        deposit_balance_before: Some(5.0),
        deposit_sent_at_ts: Some(123),
        deposit_credit_since_ts: None,
        approval_bump_count: Some(2),
        deposit_funding: None,
        bridge: crate::finalizers::cex::state::CexDepositBridgeState {
            deposit_planned_asset: Some("ckBTC".to_string()),
            deposit_planned_network: Some("ICP".to_string()),
            deposit_bridge_required: false,
            deposit_bridge_id: None,
            deposit_bridge_submitted_at_ts: None,
            deposit_bridge_polled_at_ts: None,
            deposit_bridge_destination_snapshot: None,
            deposit_bridge_submit_amount: None,
            deposit_bridge_expected_amount: None,
            deposit_bridge_provider_fee_budget_native_units: None,
            deposit_bridge_call: None,
            legacy_bridge_submit_phase: None,
            deposit_bridge_call_unrecorded: false,
        },
    };

    let mut value = serde_json::to_value(&original).expect("serialize deposit state");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_sent_at_ts");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_planned_asset");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_planned_network");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_required");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_id");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_submitted_at_ts");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_polled_at_ts");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_destination_snapshot");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_submit_amount");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_expected_amount");
    value
        .as_object_mut()
        .expect("deposit state should serialize to object")
        .remove("deposit_bridge_provider_fee_budget_native_units");

    let decoded: CexDepositState = serde_json::from_value(value).expect("deserialize legacy payload");
    assert_eq!(decoded.deposit_sent_at_ts, None);
    assert_eq!(decoded.bridge.deposit_planned_asset, None);
    assert_eq!(decoded.bridge.deposit_planned_network, None);
    assert!(!decoded.bridge.deposit_bridge_required);
    assert_eq!(decoded.bridge.deposit_bridge_id, None);
    assert_eq!(decoded.bridge.deposit_bridge_submitted_at_ts, None);
    assert_eq!(decoded.bridge.deposit_bridge_polled_at_ts, None);
    assert_eq!(decoded.bridge.deposit_bridge_submit_amount, None);
    assert_eq!(decoded.bridge.deposit_bridge_expected_amount, None);
    assert_eq!(decoded.bridge.deposit_bridge_provider_fee_budget_native_units, None);
    assert_eq!(decoded.bridge.deposit_bridge_destination_snapshot, None);
}

#[tokio::test]
async fn mexc_deposit_phase_b_confirmation_uses_inclusive_epsilon_boundary() {
    // Exactly at `expected - 0.00001` should confirm.
    let mut backend_ok = MockCexBackend::new();
    let transfers_ok = MockTransferActions::new();
    backend_ok.expect_get_balance().returning(|_symbol| Ok(5.00999));

    let finalizer_ok = test_finalizer(Arc::new(backend_ok), Arc::new(transfers_ok));

    let receipt = make_execution_receipt(42);
    let mut state_ok = finalizer_ok
        .prepare("42", &receipt)
        .await
        .expect("prepare should succeed");
    state_ok.deposit.deposit_txid = Some("tx-123".to_string());
    state_ok.deposit.deposit_balance_before = Some(5.0);
    state_ok.trade.trade_next_amount_in = Some(0.01);
    state_ok.step = CexStep::DepositPending;

    finalizer_ok
        .deposit(&mut state_ok)
        .await
        .expect("deposit should confirm at epsilon boundary");
    assert!(matches!(state_ok.step, CexStep::Trade));

    // Slightly below `expected - 0.00001` should remain pending.
    let mut backend_pending = MockCexBackend::new();
    let transfers_pending = MockTransferActions::new();
    backend_pending.expect_get_balance().returning(|_symbol| Ok(5.009989));

    let finalizer_pending = test_finalizer(Arc::new(backend_pending), Arc::new(transfers_pending));

    let mut state_pending = finalizer_pending
        .prepare("42", &receipt)
        .await
        .expect("prepare should succeed");
    state_pending.deposit.deposit_txid = Some("tx-123".to_string());
    state_pending.deposit.deposit_balance_before = Some(5.0);
    state_pending.trade.trade_next_amount_in = Some(0.01);
    state_pending.step = CexStep::DepositPending;

    finalizer_pending
        .deposit(&mut state_pending)
        .await
        .expect("deposit should stay pending below epsilon boundary");
    assert!(matches!(state_pending.step, CexStep::DepositPending));
}

#[test]
fn mexc_target_slice_bps_is_clamped_for_extreme_inputs() {
    let backend = Arc::new(MockCexBackend::new());
    let transfers = Arc::new(MockTransferActions::new());

    let floor_by_zero_max = CexVenueFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        0.0,
        TEST_CEX_MIN_EXEC_USD,
        0.7,
    );
    assert!((floor_by_zero_max.target_slice_bps() - 1.0).abs() < 1e-12);

    let floor_by_negative_ratio = CexVenueFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        -1.0,
    );
    assert!((floor_by_negative_ratio.target_slice_bps() - 1.0).abs() < 1e-12);

    let mid = CexVenueFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        0.25,
    );
    assert!((mid.target_slice_bps() - 50.0).abs() < 1e-12);

    let capped = CexVenueFinalizer::new(
        backend,
        transfers,
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        99.0,
    );
    assert!((capped.target_slice_bps() - 200.0).abs() < 1e-12);
}

#[tokio::test]
async fn mexc_approval_bump_second_batch_failure_does_not_mark_as_completed() {
    let backend = Arc::new(MockCexBackend::new());
    let mut transfers = MockTransferActions::new();
    let approve_call_count = Arc::new(std::sync::Mutex::new(0usize));
    let approve_call_count_handle = approve_call_count.clone();

    // First invocation: fail in second batch at call #5 (after 4 approvals).
    // Second invocation: fail immediately on first approval to prove it retried
    // instead of short-circuiting.
    transfers
        .expect_approve()
        .times(6)
        .returning(move |_token, _spender, _amount| {
            let mut n = approve_call_count_handle.lock().unwrap();
            *n += 1;
            match *n {
                5 => Err("approve failure in second batch".to_string()),
                6 => Err("retry attempt reached approve again".to_string()),
                _ => Ok(format!("approve-{}", *n)),
            }
        });

    let finalizer = test_finalizer(backend, Arc::new(transfers));

    let asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let first = finalizer.maybe_bump_approval("liq-second-batch", &asset).await;
    assert_eq!(first, 4);

    // If first call had marked completion, this would have returned 0 with no approve call.
    let second = finalizer.maybe_bump_approval("liq-second-batch", &asset).await;
    assert_eq!(second, 0);

    assert_eq!(*approve_call_count.lock().unwrap(), 6);
}

#[tokio::test]
async fn mexc_withdraw_is_idempotent_when_already_recorded() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // No withdraw should be executed if we already have identifiers.
    backend.expect_withdraw().times(0);

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    state.step = CexStep::Withdraw;
    state.withdraw.withdraw_id = Some("internal-1".to_string());
    state.withdraw.withdraw_txid = Some("tx-1".to_string());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw should succeed idempotently");

    assert!(matches!(state.step, CexStep::Completed));
}
