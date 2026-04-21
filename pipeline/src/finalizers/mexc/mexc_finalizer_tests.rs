use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use candid::{Nat, Principal};
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeStatus, BridgeSubmission, MockBridgeBackend,
};
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, DepositAddress, MockCexBackend, OrderBook, OrderBookLevel, SwapFillReport, WithdrawStatus,
};
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::transfer::actions::MockTransferActions;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus, TxStatus,
};

use crate::executors::executor::ExecutorRequest;
use crate::finalizers::cex_finalizer::{CexDepositState, CexState, CexStep};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use proptest::prelude::*;

const TEST_MAX_SELL_SLIPPAGE_BPS: f64 = 200.0;
/// Minimum USD chunk used by test finalizer instances.
/// Keep tiny so tests do not trigger dust skipping unless explicitly intended.
const TEST_CEX_MIN_EXEC_USD: f64 = 0.0001;
/// Slice target ratio used by test finalizer instances.
const TEST_CEX_SLICE_TARGET_RATIO: f64 = 0.7;

fn is_valid_mexc_client_order_id(value: &str) -> bool {
    let len = value.len();
    if !(1..=32).contains(&len) {
        return false;
    }

    value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn make_execution_receipt(liq_id: u128) -> ExecutionReceipt {
    let collateral_token = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let debt_token = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(0u32),
        receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
        buy_bad_debt: false,
    };

    let liq_result = LiquidationResult {
        id: liq_id,
        timestamp: 0,
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(1_000_000u64),
            debt_repaid: Nat::from(2_000_000u64),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
    };

    let req = ExecutorRequest {
        liquidation,
        swap_args: None,
        debt_asset: debt_token.clone(),
        collateral_asset: collateral_token.clone(),
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    };

    ExecutionReceipt {
        request: req,
        liquidation_result: Some(liq_result),
        status: ExecutionStatus::Success,
        change_received: false,
    }
}

fn make_execution_receipt_with_assets(
    liq_id: u128,
    collateral_token: ChainToken,
    debt_token: ChainToken,
) -> ExecutionReceipt {
    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(0u32),
        receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
        buy_bad_debt: false,
    };

    let liq_result = LiquidationResult {
        id: liq_id,
        timestamp: 0,
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(1_000_000u64),
            debt_repaid: Nat::from(2_000_000u64),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
    };

    let req = ExecutorRequest {
        liquidation,
        swap_args: None,
        debt_asset: debt_token,
        collateral_asset: collateral_token,
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    };

    ExecutionReceipt {
        request: req,
        liquidation_result: Some(liq_result),
        status: ExecutionStatus::Success,
        change_received: false,
    }
}

fn ckusdc_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

fn ckbtc_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    }
}

fn bridge_dependencies(backend: Arc<MockBridgeBackend>) -> MexcBridgeDependencies {
    MexcBridgeDependencies {
        backend,
        config: MexcBridgeConfig {
            bridge_ic_source_account: icrc_ledger_types::icrc1::account::Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            bridge_evm_source_address: "0x2222222222222222222222222222222222222222".to_string(),
            bridge_btc_source_address: "bc1qbridge000000000000000000000000000000000".to_string(),
        },
    }
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

    // Pre-conditions: no deposit has been sent yet
    assert!(state.deposit.deposit_txid.is_none());
    assert!(state.deposit.deposit_balance_before.is_none());
    assert!(matches!(state.step, CexStep::Deposit));

    // Phase A: snapshot baseline and send transfer
    finalizer.deposit(&mut state).await.expect("deposit should succeed");

    assert_eq!(state.deposit.deposit_balance_before, Some(10.0));
    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-123"));
    assert!(state.deposit.deposit_sent_at_ts.is_some());
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_deposit_phase_a_uses_evm_destination_for_evm_assets() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|asset| {
        assert_eq!(asset, "USDC");
        Ok(10.0)
    });

    backend.expect_get_deposit_address().times(1).returning(|asset, network| {
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let collateral = ChainToken::EvmErc20 {
        chain: "eth".to_string(),
        token_address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
        symbol: "USDC".to_string(),
        decimals: 6,
        fee: Nat::from(0u8),
    };
    let receipt = make_execution_receipt_with_assets(43, collateral, ckbtc_token());
    let mut state = finalizer.prepare("43", &receipt).await.expect("prepare should succeed");

    finalizer.deposit(&mut state).await.expect("deposit should succeed");

    assert_eq!(state.deposit.deposit_txid.as_deref(), Some("tx-evm-123"));
    assert!(matches!(state.step, CexStep::DepositPending));
}

#[tokio::test]
async fn mexc_deposit_phase_a_rejects_invalid_evm_deposit_address() {
    let mut backend = MockCexBackend::new();
    let mut transfers = MockTransferActions::new();

    backend.expect_get_balance().times(1).returning(|_asset| Ok(10.0));
    backend.expect_get_deposit_address().times(1).returning(|_asset, _network| {
        Ok(DepositAddress {
            asset: "USDC".to_string(),
            network: "ERC20".to_string(),
            address: "not-an-evm-address".to_string(),
            tag: None,
        })
    });
    transfers.expect_transfer().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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
    assert!(err.contains("invalid MEXC EVM deposit address"));
}

#[test]
fn mexc_bridge_planner_keeps_direct_path_when_no_bridge_route_matches() {
    let finalizer = MexcFinalizer::new(
        Arc::new(MockCexBackend::new()),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_dependencies(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

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
    let finalizer = MexcFinalizer::new(
        Arc::new(MockCexBackend::new()),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_dependencies(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

    let plan = finalizer.resolve_bridge_plan_for_assets(&ckusdc_token(), &ckbtc_token());
    assert!(plan.deposit.bridge_required);
    assert_eq!(plan.deposit.cex_asset, "USDC");
    assert_eq!(plan.deposit.cex_network, "ETH");
    assert!(!plan.withdraw.bridge_required);
    assert_eq!(plan.withdraw.cex_asset, "ckBTC");
}

#[test]
fn mexc_bridge_planner_translates_ckusdc_symbol_both_directions() {
    let finalizer = MexcFinalizer::new(
        Arc::new(MockCexBackend::new()),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_dependencies(bridge_dependencies(Arc::new(MockBridgeBackend::new())));

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
        .returning(|asset, chain, _source| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            Ok(1.0)
        });

    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "ckUSDC");
        assert_eq!(request.source_chain, "ICP");
        assert_eq!(request.target_asset, "USDC");
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

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_dependencies(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(7, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("7", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    finalizer
        .deposit(&mut state)
        .await
        .expect("first bridged deposit call should submit");
    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-deposit-1")
    );

    finalizer
        .deposit(&mut state)
        .await
        .expect("second bridged deposit call should poll pending");
    assert!(matches!(state.step, CexStep::DepositPending));

    finalizer
        .deposit(&mut state)
        .await
        .expect("third bridged deposit call should complete");
    assert!(matches!(state.step, CexStep::Trade));
    assert!(state.deposit.bridge.deposit_bridge_submitted_at_ts.is_some());
    assert!(state.deposit.bridge.deposit_bridge_polled_at_ts.is_some());
}

#[tokio::test]
async fn mexc_deposit_bridge_retries_submit_with_reduced_amount_on_ckusdc_preflight_shortfall() {
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
        .returning(|asset, chain, _source| {
            assert_eq!(asset, "ckUSDC");
            assert_eq!(chain, "ICP");
            Ok(0.93)
        });

    bridge.expect_submit_bridge().times(1).returning(move |request| {
        assert!((request.amount - 0.92).abs() < 1e-9, "unexpected amount {}", request.amount);
        Ok(BridgeSubmission {
            bridge_id: "bridge-deposit-2".to_string(),
        })
    });

    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_dependencies(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(8, ckusdc_token(), ckbtc_token());
    let mut state = finalizer.prepare("8", &receipt).await.expect("prepare should succeed");
    assert!(state.deposit.bridge.deposit_bridge_required);

    finalizer
        .deposit(&mut state)
        .await
        .expect("bridged deposit call should recover from preflight shortfall");

    assert!(matches!(state.step, CexStep::DepositPending));
    assert_eq!(
        state.deposit.bridge.deposit_bridge_id.as_deref(),
        Some("bridge-deposit-2")
    );
    assert!((state.trade.trade_next_amount_in.unwrap_or_default() - 0.92).abs() < 1e-9);
}

#[tokio::test]
async fn mexc_withdraw_bridge_submit_resume_complete_and_idempotent() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let transfers = MockTransferActions::new();
    let liquidator = Principal::management_canister();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount| {
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
    cex.expect_get_withdraw_status_by_id()
        .times(2)
        .returning(move |coin, withdraw_id| {
            assert_eq!(coin, "USDC");
            assert_eq!(withdraw_id, "withdraw-1");
            let mut guard = withdraw_status_calls_clone.lock().expect("mutex");
            *guard += 1;
            if *guard == 1 {
                Ok(WithdrawStatus::Pending)
            } else {
                Ok(WithdrawStatus::Completed)
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

    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "USDC");
        assert_eq!(request.source_chain, "ETH");
        assert_eq!(request.target_asset, "ckUSDC");
        assert_eq!(request.source_address, "0x2222222222222222222222222222222222222222");
        match request.destination {
            BridgeDestination::IcpAccount(account) => {
                assert_eq!(account.owner, Principal::management_canister());
                assert!(account.subaccount.is_none());
            }
            other => panic!("unexpected bridge destination: {:?}", other),
        }
        Ok(BridgeSubmission {
            bridge_id: "bridge-withdraw-1".to_string(),
        })
    });

    let bridge_status_calls = Arc::new(std::sync::Mutex::new(0u8));
    let bridge_status_calls_clone = bridge_status_calls.clone();
    bridge.expect_get_bridge_status().times(2).returning(move |bridge_id| {
        assert_eq!(bridge_id, "bridge-withdraw-1");
        let mut guard = bridge_status_calls_clone.lock().expect("mutex");
        *guard += 1;
        if *guard == 1 {
            Ok(BridgeStatus::Pending)
        } else {
            Ok(BridgeStatus::Completed)
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
    .with_bridge_dependencies(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(11, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("11", &receipt).await.expect("prepare should succeed");
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.5,
    ));

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

    finalizer
        .withdraw(&mut state)
        .await
        .expect("bridge completion should finish");
    assert!(matches!(state.step, CexStep::Completed));
}

#[tokio::test]
async fn mexc_withdraw_bridge_caps_submit_amount_to_source_balance() {
    let mut cex = MockCexBackend::new();
    let mut bridge = MockBridgeBackend::new();
    let transfers = MockTransferActions::new();
    let liquidator = Principal::management_canister();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount| {
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

    cex.expect_get_withdraw_status_by_id()
        .times(1)
        .returning(|coin, withdraw_id| {
            assert_eq!(coin, "USDC");
            assert_eq!(withdraw_id, "withdraw-fee");
            Ok(WithdrawStatus::Completed)
        });

    bridge
        .expect_get_source_balance()
        .times(1)
        .returning(|asset, chain, address| {
            assert_eq!(asset, "USDC");
            assert_eq!(chain, "ETH");
            assert_eq!(address, "0x2222222222222222222222222222222222222222");
            // Simulate net credit after MEXC withdraw/network fee.
            Ok(1.4)
        });

    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "USDC");
        assert_eq!(request.source_chain, "ETH");
        assert_eq!(request.target_asset, "ckUSDC");
        assert!((request.amount - 1.4).abs() < 1e-12);
        Ok(BridgeSubmission {
            bridge_id: "bridge-withdraw-fee".to_string(),
        })
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
    .with_bridge_dependencies(bridge_dependencies(Arc::new(bridge)));

    let receipt = make_execution_receipt_with_assets(111, ckbtc_token(), ckusdc_token());
    let mut state = finalizer.prepare("111", &receipt).await.expect("prepare should succeed");
    assert!(state.withdraw.bridge.withdraw_bridge_required);
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.5,
    ));

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
        .expect("bridge submit should cap amount to available source balance");
    assert!(matches!(state.step, CexStep::WithdrawPending));
    assert_eq!(
        state.withdraw.bridge.withdraw_bridge_id.as_deref(),
        Some("bridge-withdraw-fee")
    );
}

#[tokio::test]
async fn mexc_non_bridge_withdraw_behavior_unchanged() {
    let mut cex = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount| {
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
        1.0,
    ));

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
        .returning(move |asset, network, address, amount| {
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
    let receipt = make_execution_receipt_with_assets(14, ckbtc_token(), debt_token);
    let mut state = finalizer.prepare("14", &receipt).await.expect("prepare should succeed");
    state.step = CexStep::Withdraw;
    state.withdraw.withdraw_address = "0x1111111111111111111111111111111111111111".to_string();
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    finalizer
        .withdraw(&mut state)
        .await
        .expect("direct EVM withdraw should complete");
    assert!(matches!(state.step, CexStep::Completed));
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("withdraw-evm-direct"));
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
async fn mexc_deposit_phase_b_moves_to_trade_when_balance_increased() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    backend.expect_get_balance().returning(|_symbol| Ok(5.1));

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

#[tokio::test]
async fn mexc_deposit_phase_b_stays_in_deposit_when_balance_unchanged() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Balance stays the same as baseline.
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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
async fn mexc_deposit_phase_b_does_not_use_total_free_fallback_before_wait_window() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Total free covers expected, but fallback window has not elapsed yet.
    backend.expect_get_balance().returning(|_symbol| Ok(10.0));

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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
        approval_bump_count: Some(2),
        bridge: crate::finalizers::cex_finalizer::CexDepositBridgeState {
            deposit_planned_asset: Some("ckBTC".to_string()),
            deposit_planned_network: Some("ICP".to_string()),
            deposit_bridge_required: false,
            deposit_bridge_id: None,
            deposit_bridge_submitted_at_ts: None,
            deposit_bridge_polled_at_ts: None,
            deposit_bridge_destination_snapshot: None,
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

    let decoded: CexDepositState = serde_json::from_value(value).expect("deserialize legacy payload");
    assert_eq!(decoded.deposit_sent_at_ts, None);
    assert_eq!(decoded.bridge.deposit_planned_asset, None);
    assert_eq!(decoded.bridge.deposit_planned_network, None);
    assert!(!decoded.bridge.deposit_bridge_required);
    assert_eq!(decoded.bridge.deposit_bridge_id, None);
    assert_eq!(decoded.bridge.deposit_bridge_submitted_at_ts, None);
    assert_eq!(decoded.bridge.deposit_bridge_polled_at_ts, None);
    assert_eq!(decoded.bridge.deposit_bridge_destination_snapshot, None);
}

#[tokio::test]
async fn mexc_deposit_phase_b_confirmation_uses_inclusive_epsilon_boundary() {
    // Exactly at `expected - 0.00001` should confirm.
    let mut backend_ok = MockCexBackend::new();
    let transfers_ok = MockTransferActions::new();
    backend_ok.expect_get_balance().returning(|_symbol| Ok(5.00999));

    let finalizer_ok = MexcFinalizer::new(
        Arc::new(backend_ok),
        Arc::new(transfers_ok),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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

    let finalizer_pending = MexcFinalizer::new(
        Arc::new(backend_pending),
        Arc::new(transfers_pending),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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

    let floor_by_zero_max = MexcFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        0.0,
        TEST_CEX_MIN_EXEC_USD,
        0.7,
    );
    assert!((floor_by_zero_max.target_slice_bps() - 1.0).abs() < 1e-12);

    let floor_by_negative_ratio = MexcFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        -1.0,
    );
    assert!((floor_by_negative_ratio.target_slice_bps() - 1.0).abs() < 1e-12);

    let mid = MexcFinalizer::new(
        backend.clone(),
        transfers.clone(),
        Principal::anonymous(),
        200.0,
        TEST_CEX_MIN_EXEC_USD,
        0.25,
    );
    assert!((mid.target_slice_bps() - 50.0).abs() < 1e-12);

    let capped = MexcFinalizer::new(
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
async fn mexc_trade_skips_when_amount_in_zero_and_moves_to_withdraw() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // When amount_in <= 0, execute_swap must never be called.
    backend.expect_execute_swap_detailed_with_options().times(0);

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
    };
    backend
        .expect_get_orderbook()
        .returning(move |_market, _limit| Ok(orderbook.clone()));

    backend
        .expect_execute_swap_detailed_with_options()
        .times(1)
        .returning(|_market, _side, _amount_in, _opts| Err("boom".to_string()));

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

    state.step = CexStep::Trade;

    let err = finalizer.trade(&mut state).await.expect_err("trade should fail");

    assert_eq!(err, "boom");
    // On error we expect the step to remain Trade.
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn mexc_trade_marks_dust_and_skips_execution_for_small_residual() {
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

    finalizer
        .trade(&mut state)
        .await
        .expect("trade should succeed with dust skip");

    assert!(state.trade.trade_dust_skipped);
    assert!(state.trade.trade_dust_usd.unwrap_or_default() > 0.0);
    assert!(state.trade.trade_slices.is_empty());
    assert!(matches!(state.step, CexStep::Withdraw));
    let out = state.withdraw.size_out.as_ref().expect("size_out should be set");
    assert_eq!(out.to_f64(), 0.0);
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
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
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
        .maybe_mark_trade_dust(&mut state_eq, &leg, 0.01, 0.01)
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
    };
    let btc_usdc_below = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
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
        .maybe_mark_trade_dust(&mut state_below, &leg, 0.01, 0.01)
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
    };
    let btc_usdc = OrderBook {
        bids: vec![OrderBookLevel {
            price: 100.0,
            quantity: 100.0,
        }],
        asks: vec![],
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
        .maybe_mark_trade_dust(&mut state, &leg, 0.005, 0.02)
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

    assert!(err.contains("slice slippage too high"));
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
    };
    let buy_book = OrderBook {
        bids: vec![],
        asks: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
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
    assert!(err.contains("slice slippage too high"));
    assert!(err.contains("requested_in="));
    assert!(err.contains("actual_in="));
    assert!(err.contains("actual_out="));
    assert!(err.contains("preview_mid="));
    assert!(err.contains("exec_price="));
}

#[tokio::test]
async fn mexc_trade_clamp_and_finish_under_consumed_buy_input_single_slice() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let sell_probe = OrderBook {
        bids: vec![],
        asks: vec![],
    };
    let buy_book = OrderBook {
        bids: vec![],
        asks: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
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
    assert!(first_err.contains("slice slippage too high"));
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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
    };

    backend
        .expect_get_orderbook()
        .times(2)
        .returning(move |_market, _limit| Ok(empty_book.clone()));
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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
        err.contains("could not resolve direct market") || err.contains("no configured MEXC pairs for hop discovery")
    );
    assert!(matches!(state.step, CexStep::Trade));
}

#[tokio::test]
async fn resolve_trade_legs_for_identical_symbols_is_noop() {
    let backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let legs = finalizer
        .resolve_trade_legs_for_symbols(" usdc ", "USDC")
        .await
        .expect("identical symbols should resolve as no-op");

    assert!(legs.is_empty());
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
    };
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_route_config(
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
    };
    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_route_config(
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_route_config(
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
    };
    backend
        .expect_get_orderbook()
        .returning(move |_market, _limit| Ok(empty.clone()));
    backend.expect_execute_swap_detailed_with_options().times(0);

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
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
    assert!(err.contains("no configured hop route"));
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_route_config(vec!["CKBTC_CKUSDT".to_string()], 2);

    let receipt = make_execution_receipt(1005);
    let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
    assert!(preview.is_executable);

    let markets = observed_markets.lock().unwrap();
    assert_eq!(markets.get("CKBTC_CKUSDT").copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn mexc_input_slice_usd_converts_non_hardcoded_quote_via_configured_hops() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    let empty = OrderBook {
        bids: vec![],
        asks: vec![],
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

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
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

    let finalizer = MexcFinalizer::new(
        backend,
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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

    let finalizer = MexcFinalizer::new(
        backend,
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

    let asset = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let first = finalizer.maybe_bump_mexc_approval("liq-second-batch", &asset).await;
    assert_eq!(first, 4);

    // If first call had marked completion, this would have returned 0 with no approve call.
    let second = finalizer.maybe_bump_mexc_approval("liq-second-batch", &asset).await;
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
    state.withdraw.withdraw_id = Some("internal-1".to_string());
    state.withdraw.withdraw_txid = Some("tx-1".to_string());

    finalizer
        .withdraw(&mut state)
        .await
        .expect("withdraw should succeed idempotently");

    assert!(matches!(state.step, CexStep::Completed));
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
    assert!((swap.mid_price - expected_price).abs() < 1e-9);

    // Status and legs should reflect a single synthetic CEX hop.
    assert_eq!(swap.status, "completed".to_string());
    assert!(swap.legs.is_empty());
}

#[tokio::test]
async fn mexc_preview_route_returns_non_executable_for_zero_amount() {
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

    let mut receipt = make_execution_receipt(42);
    if let Some(liq) = receipt.liquidation_result.as_mut() {
        liq.amounts.collateral_received = Nat::from(0u8);
    }

    let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
    assert!(!preview.is_executable);
    assert_eq!(preview.estimated_receive_amount, 0.0);
    assert_eq!(preview.estimated_slippage_bps, 0.0);
    assert_eq!(preview.reason.as_deref(), Some("non-positive amount_in"));
}

#[tokio::test]
async fn mexc_preview_route_resolves_direct_buy_leg_when_sell_book_empty() {
    let mut backend = MockCexBackend::new();
    let transfers = MockTransferActions::new();

    // Direct sell market BTC_CKBTC has no bids -> resolver should choose buy market CKBTC_BTC.
    let btc_ckbtc = OrderBook {
        bids: vec![],
        asks: vec![],
    };
    let ckbtc_btc = OrderBook {
        bids: vec![],
        asks: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
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
    };

    backend
        .expect_get_orderbook()
        .returning(move |market, _limit| match market {
            "BTC_CKBTC" => Ok(btc_ckbtc.clone()),
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    );

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

    let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
    assert!(preview.is_executable);
    assert!(preview.estimated_receive_amount > 0.0);
    assert!(preview.estimated_slippage_bps >= 0.0);
}

mod fuzz {
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

                let finalizer = MexcFinalizer::new(
                    Arc::new(backend),
                    Arc::new(transfers),
                    Principal::anonymous(),
                    TEST_MAX_SELL_SLIPPAGE_BPS,
                    TEST_CEX_MIN_EXEC_USD,
                    TEST_CEX_SLICE_TARGET_RATIO,
                );

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
                };
                let btc_usdc_book = OrderBook {
                    bids: vec![OrderBookLevel { price: btc_usd as f64, quantity: 1000.0 }],
                    asks: vec![OrderBookLevel { price: btc_usd as f64 + 1.0, quantity: 1000.0 }],
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
                };
                let btc_usdc_book = OrderBook {
                    bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                    asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
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
                assert!(err.contains("slice slippage too high"));
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
                };
                let btc_usdc_book = OrderBook {
                    bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                    asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
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
                };
                let btc_usdc_book = OrderBook {
                    bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                    asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
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
                // Force single-leg route for deterministic retry assertions.
                state.withdraw.withdraw_asset = ChainToken::Icp {
                    ledger: Principal::anonymous(),
                    symbol: "BTC".to_string(),
                    decimals: 8,
                    fee: Nat::from(1_000u64),
                };

                let first = finalizer.trade(&mut state).await.expect_err("first attempt should fail");
                assert!(first.contains("slice slippage too high"));
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
}
