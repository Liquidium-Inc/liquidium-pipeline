//! State preparation and the bridge transport plan.
use super::*;

#[tokio::test]
async fn mexc_prepare_maps_cketh_collateral_to_eth_deposit_bridge_plan() {
    let backend = Arc::new(MockCexBackend::new());
    let transfer_service = Arc::new(MockTransferActions::new());
    let finalizer = test_finalizer(backend, transfer_service)
    .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let receipt = make_execution_receipt_with_assets(43, cketh_token(), ckbtc_token());
    let state = finalizer.prepare("43", &receipt).await.expect("prepare should succeed");

    assert_eq!(state.deposit.bridge.deposit_planned_asset.as_deref(), Some("ETH"));
    assert_eq!(state.deposit.bridge.deposit_planned_network.as_deref(), Some("ETH"));
    assert!(state.deposit.bridge.deposit_bridge_required);
    assert_eq!(state.withdraw.bridge.withdraw_planned_asset.as_deref(), Some("ckBTC"));
    assert!(!state.withdraw.bridge.withdraw_bridge_required);
    assert_eq!(state.market, "ETH_ckBTC");
}

#[tokio::test]
async fn mexc_prepare_maps_cketh_debt_to_eth_withdraw_bridge_plan() {
    let backend = Arc::new(MockCexBackend::new());
    let transfer_service = Arc::new(MockTransferActions::new());
    let finalizer = test_finalizer(backend, transfer_service)
    .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let receipt = make_execution_receipt_with_assets(44, ckbtc_token(), cketh_token());
    let state = finalizer.prepare("44", &receipt).await.expect("prepare should succeed");

    assert_eq!(state.deposit.bridge.deposit_planned_asset.as_deref(), Some("ckBTC"));
    assert!(!state.deposit.bridge.deposit_bridge_required);
    assert_eq!(state.withdraw.bridge.withdraw_planned_asset.as_deref(), Some("ETH"));
    assert_eq!(state.withdraw.bridge.withdraw_planned_network.as_deref(), Some("ETH"));
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    assert_eq!(state.market, "ckBTC_ETH");
}

#[tokio::test]
async fn mexc_deposit_phase_a_snapshots_baseline_and_sends_transfer() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().returning(|_symbol| Ok(10.0));

    backend.expect_get_deposit_address().returning(|_symbol, _chain| {
        Ok(DepositAddress {
            asset: "CkBTC".to_string(),
            network: "ICP".to_string(),
            address: "aaaaa-aa".to_string(),
            tag: None,
        })
    });

    transfers
        .expect_transfer()
        .returning(|_token, _to, _amount| Ok("tx-123".to_string()));
    transfers
        .expect_approve()
        .times(6)
        .returning(|_token, _spender, _amount| Ok("approve-1".to_string()));

    let backend = Arc::new(backend);
    let transfer_service = Arc::new(transfers);

    let finalizer = test_finalizer(backend, transfer_service);

    let receipt = make_execution_receipt(42);
    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

    // Pre-conditions: no deposit has been sent yet
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.deposit_balance_before.is_none());
    assert!(matches!(state.step, CexStep::Deposit));

    // Phase A: snapshot baseline and send transfer
    fund(&finalizer, &mut state).await;

    assert_eq!(state.deposit.deposit_balance_before, Some(10.0));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-123"));
    assert!(state.deposit.deposit_sent_at_ts.is_some());
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[test]
fn mexc_bridge_planner_keeps_direct_path_when_no_bridge_route_matches() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()))
    .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let debt_token = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    };

    let plan = finalizer.resolve_bridge_plan_for_assets(&ckbtc_token(), &debt_token);
    assert!(!plan.deposit.bridge_required);
    assert_eq!(plan.deposit.cex_asset, "ckBTC");
    assert_eq!(plan.deposit.cex_network, "ICP");
    assert!(!plan.withdraw.bridge_required);
    assert_eq!(plan.withdraw.cex_asset, "ckUSDT");
    assert_eq!(plan.withdraw.cex_network, "ICP");
}

#[test]
fn mexc_bridge_planner_uses_bridge_fallback_for_ckusdc_deposit() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()))
    .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let plan = finalizer.resolve_bridge_plan_for_assets(&ckusdc_token(), &ckbtc_token());
    assert!(plan.deposit.bridge_required);
    assert_eq!(plan.deposit.cex_asset, "USDC");
    assert_eq!(plan.deposit.cex_network, "ETH");
    assert!(!plan.withdraw.bridge_required);
    assert_eq!(plan.withdraw.cex_asset, "ckBTC");
}

#[test]
fn mexc_bridge_planner_translates_ckusdc_symbol_both_directions() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()))
    .with_bridge_runtime(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let plan = finalizer.resolve_bridge_plan_for_assets(&ckusdc_token(), &ckusdc_token());
    assert!(plan.deposit.bridge_required);
    assert_eq!(plan.deposit.cex_asset, "USDC");
    assert_eq!(plan.deposit.cex_network, "ETH");
    assert!(plan.withdraw.bridge_required);
    assert_eq!(plan.withdraw.cex_asset, "USDC");
    assert_eq!(plan.withdraw.cex_network, "ETH");
}

#[tokio::test]
async fn mexc_deposit_bridge_submit_resume_and_complete() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let mut transfers = MockTransferActions::new();

    let balance_calls = Arc::new(std::sync::Mutex::new(0u8));
    let balance_calls_clone = balance_calls.clone();
    cex.expect_get_balance().times(2).returning(move |asset| {
        assert_eq!(asset, "USDC");
        let mut guard = balance_calls_clone.lock().expect("mutex");
        *guard += 1;
        if *guard == 1 { Ok(10.0) } else { Ok(100.0) }
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
    bridge.expect_prepare_submission().times(1).returning(|request| {
        assert!((request.amount - 0.98).abs() < 1e-9);
        Ok(Some(100))
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
    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "ckUSDC");
        assert_eq!(request.source_chain, "ICP");
        assert_eq!(request.target_asset, "USDC");
        assert!((request.amount - 0.98).abs() < 1e-9);
        match request.destination {
            BridgeDestination::EvmAddress(_) => {}
            other => panic!("unexpected bridge destination: {:?}", other),
        }
        Ok(BridgeSubmission {
            bridge_id: "bridge-deposit-1".to_string(),
        })
    });

    let status_calls = Arc::new(std::sync::Mutex::new(0u8));
    let status_calls_clone = status_calls.clone();
    bridge.expect_get_bridge_status().times(2).returning(move |bridge_id| {
        assert_eq!(bridge_id, "bridge-deposit-1");
        let mut guard = status_calls_clone.lock().expect("mutex");
        *guard += 1;
        if *guard == 1 {
            Ok(BridgeStatus::Pending)
        } else {
            Ok(BridgeStatus::Completed)
        }
    });

    let finalizer =
        test_finalizer(Arc::new(cex), Arc::new(transfers)).with_bridge_runtime(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(7, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("7", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    fund(&finalizer, &mut state).await;
    finalizer
        .deposit(&mut state)
        .await
        .expect("first bridged deposit call should save the bridge call");
    assert!(matches!(state.step, CexStep::DepositPending));
    assert!(state.deposit.bridge.deposit_bridge_id.is_none());
    let saved = state
        .deposit
        .bridge
        .deposit_bridge_call
        .clone()
        .expect("the call must be saved before it runs");
    assert_eq!(saved.phase, crate::outbound::driver::OutboundPhase::Prepared);
    assert!(saved.prepared.identity.starts_with("bridge-call-"), "{}", saved.prepared.identity);
    let call = crate::bridge::call_sender::saved_call(&saved.prepared).unwrap();
    assert_eq!(call.cursor, Some(100));
    assert_eq!(call.destination, "0x1111111111111111111111111111111111111111");
    assert!((call.amount - 0.98).abs() < 1e-9);

    finalizer
        .deposit(&mut state)
        .await
        .expect("second bridged deposit call should submit");
    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-deposit-1")
    );
    assert!(state.deposit.bridge.deposit_bridge_call.is_none());

    finalizer
        .deposit(&mut state)
        .await
        .expect("third bridged deposit call should poll pending");
    assert!(matches!(state.step, CexStep::DepositPending));

    finalizer
        .deposit(&mut state)
        .await
        .expect("fourth bridged deposit call should complete");
    assert!(matches!(state.step, CexStep::Trade));
    assert!(state.deposit.bridge.deposit_bridge_submitted_at_ts.is_some());
    assert!(state.deposit.bridge.deposit_bridge_polled_at_ts.is_some());
}
