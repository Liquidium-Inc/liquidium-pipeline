use std::{collections::HashMap, sync::Arc};

use candid::{Nat, Principal};
use ic_ledger_types::{AccountIdentifier, Subaccount};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::MockBridgeBackend,
    cex_backend::{
        MockCexBackend, OrderBook, OrderBookLevel, WithdrawStatus, WithdrawStatusSnapshot, WithdrawalReceipt,
    },
};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistry},
    transfer::actions::MockTransferActions,
};

use crate::{
    finalizers::{
        cex_finalizer::{
            CexBridgeConfig, CexBridgeDependencies, CexFinalizer, CexFinalizerLogic, CexStep, CexVenueProfile,
        },
        multi_venue::{MultiVenueAdapter, VenuePlanningContext},
    },
    swappers::model::SwapRequest,
    utils::ICP_LEDGER_PRINCIPAL,
};

const BRIDGE_EVM_ADDRESS: &str = "0x1111111111111111111111111111111111111111";

fn planning_context() -> VenuePlanningContext {
    VenuePlanningContext {
        liquidation_id: "42".to_string(),
    }
}

fn native_icp_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("valid ICP ledger"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    }
}

fn bridged_token(symbol: &str) -> ChainToken {
    let ledger = match symbol {
        "ckUSDC" => "xevnm-gaaaa-aaaar-qafnq-cai",
        "ckETH" => "ss2fx-dyaaa-aaaar-qacoq-cai",
        _ => panic!("unsupported test token"),
    };
    ChainToken::Icp {
        ledger: Principal::from_text(ledger).expect("valid token ledger"),
        symbol: symbol.to_string(),
        decimals: if symbol == "ckETH" { 18 } else { 6 },
        fee: Nat::from(10_000u64),
    }
}

fn bridge_dependencies() -> CexBridgeDependencies {
    CexBridgeDependencies {
        backend: Arc::new(MockBridgeBackend::new()),
        config: CexBridgeConfig {
            bridge_ic_source_account: Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            bridge_evm_source_address: BRIDGE_EVM_ADDRESS.to_string(),
            bridge_btc_source_address: "bc1qtest".to_string(),
        },
    }
}

fn finalizer(
    backend: MockCexBackend,
    pay: &ChainToken,
    receive: &ChainToken,
    pairs: Vec<String>,
) -> CexFinalizer<MockCexBackend> {
    let tokens = HashMap::from([(pay.asset_id(), pay.clone()), (receive.asset_id(), receive.clone())]);
    CexFinalizer::new(
        Arc::new(backend),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
    .with_profile(CexVenueProfile::kraken(40.0))
    .with_token_registry(Arc::new(TokenRegistry::new(tokens)))
    .with_bridge_dependencies(bridge_dependencies())
    .with_route_config(pairs, 2)
}

async fn assert_icp_to_bridged_asset(receive_symbol: &str, quote_market: &str) {
    let pay = native_icp_token();
    let receive = bridged_token(receive_symbol);
    let expected_withdraw_asset = receive_symbol.trim_start_matches("ck").to_ascii_uppercase();
    let mut backend = MockCexBackend::new();
    backend
        .expect_validate_trade_amounts()
        .times(2)
        .returning(|_, _, amount_in, amount_out| {
            assert!(amount_in > 0.0);
            assert!(amount_out > 0.0);
            Ok(())
        });
    backend
        .expect_validate_funding_route()
        .withf(move |preflight| {
            preflight.deposit_asset == "ICP"
                && preflight.deposit_network == "ICP"
                && preflight.withdraw_asset == expected_withdraw_asset
                && preflight.withdraw_network == "ETH"
                && preflight.withdraw_address == BRIDGE_EVM_ADDRESS
                && preflight.deposit_amount > 0.0
                && preflight.withdraw_amount > 0.0
        })
        .once()
        .returning(|_| Ok(()));
    backend.expect_get_orderbook().returning(|market, _| match market {
        "ICP_USD" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 5.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }),
        "USDC_USD" | "ETH_USD" => Ok(OrderBook {
            bids: vec![],
            asks: vec![OrderBookLevel {
                price: 1.0,
                quantity: 1_000_000.0,
            }],
        }),
        _ => Err(format!("unavailable direct market {market}")),
    });

    let finalizer = finalizer(
        backend,
        &pay,
        &receive,
        vec!["ICP_USD".to_string(), quote_market.to_string()],
    );
    let request = SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(pay, Nat::from(100_000_000u64)),
        receive_asset: receive.asset_id(),
        receive_address: Some(Principal::from_slice(&[42]).to_text()),
        max_slippage_bps: Some(200),
        venue_hint: Some("kraken".to_string()),
    };

    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect("Kraken ICP bridge preview should succeed");

    assert_eq!(preview.venue_id, "kraken");
    assert_eq!(
        preview.quote.legs[0].route_id,
        format!("ICP_USD:sell>{quote_market}:buy")
    );
}

async fn assert_bridged_asset_to_icp(pay_symbol: &str, base_market: &str) {
    let pay = bridged_token(pay_symbol);
    let receive = native_icp_token();
    let final_owner = Principal::from_slice(&[42]);
    let expected_account_id = AccountIdentifier::new(&final_owner, &Subaccount([0; 32])).to_hex();
    let expected_deposit_asset = pay_symbol.trim_start_matches("ck").to_ascii_uppercase();
    let mut backend = MockCexBackend::new();
    backend
        .expect_validate_trade_amounts()
        .times(2)
        .returning(|_, _, amount_in, amount_out| {
            assert!(amount_in > 0.0);
            assert!(amount_out > 0.0);
            Ok(())
        });
    backend
        .expect_validate_funding_route()
        .withf(move |preflight| {
            preflight.deposit_asset == expected_deposit_asset
                && preflight.deposit_network == "ETH"
                && preflight.withdraw_asset == "ICP"
                && preflight.withdraw_network == "ICP"
                && preflight.withdraw_address == expected_account_id
                && preflight.deposit_amount > 0.0
                && preflight.withdraw_amount > 0.0
        })
        .once()
        .returning(|_| Ok(()));
    backend.expect_get_orderbook().returning(|market, _| match market {
        "USDC_USD" | "ETH_USD" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }),
        "ICP_USD" => Ok(OrderBook {
            bids: vec![],
            asks: vec![OrderBookLevel {
                price: 5.0,
                quantity: 1_000_000.0,
            }],
        }),
        _ => Err(format!("unavailable direct market {market}")),
    });

    let finalizer = finalizer(
        backend,
        &pay,
        &receive,
        vec![base_market.to_string(), "ICP_USD".to_string()],
    );
    let raw_pay = if pay_symbol == "ckETH" {
        1_000_000_000_000_000_000u64
    } else {
        100_000_000u64
    };
    let request = SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(pay, Nat::from(raw_pay)),
        receive_asset: receive.asset_id(),
        receive_address: Some(final_owner.to_text()),
        max_slippage_bps: Some(200),
        venue_hint: Some("kraken".to_string()),
    };

    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect("Kraken bridge-to-ICP preview should succeed");

    assert_eq!(preview.venue_id, "kraken");
    assert_eq!(
        preview.quote.legs[0].route_id,
        format!("{base_market}:sell>ICP_USD:buy")
    );
}

#[tokio::test]
async fn kraken_routes_icp_to_usdc_through_usd_and_preflights_bridge_address() {
    assert_icp_to_bridged_asset("ckUSDC", "USDC_USD").await;
}

#[tokio::test]
async fn kraken_routes_icp_to_eth_through_usd_and_preflights_bridge_address() {
    assert_icp_to_bridged_asset("ckETH", "ETH_USD").await;
}

#[tokio::test]
async fn kraken_routes_usdc_to_icp_through_usd_and_preflights_account_id() {
    assert_bridged_asset_to_icp("ckUSDC", "USDC_USD").await;
}

#[tokio::test]
async fn kraken_routes_eth_to_icp_through_usd_and_preflights_account_id() {
    assert_bridged_asset_to_icp("ckETH", "ETH_USD").await;
}

#[tokio::test]
async fn kraken_falls_back_to_amount_fillable_hop_when_direct_book_is_too_thin() {
    let pay = native_icp_token();
    let receive = bridged_token("ckUSDC");
    let mut backend = MockCexBackend::new();
    backend
        .expect_validate_trade_amounts()
        .times(2)
        .returning(|_, _, _, _| Ok(()));
    backend
        .expect_validate_funding_route()
        .withf(|preflight| {
            preflight.deposit_asset == "ICP"
                && preflight.deposit_network == "ICP"
                && preflight.withdraw_asset == "USDC"
                && preflight.withdraw_network == "ETH"
                && preflight.withdraw_address == BRIDGE_EVM_ADDRESS
        })
        .once()
        .returning(|_| Ok(()));
    backend.expect_get_orderbook().returning(|market, _| match market {
        "ICP_USDC" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 5.0,
                quantity: 0.01,
            }],
            asks: vec![],
        }),
        "ICP_USD" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 5.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }),
        "USDC_USD" => Ok(OrderBook {
            bids: vec![],
            asks: vec![OrderBookLevel {
                price: 1.0,
                quantity: 1_000_000.0,
            }],
        }),
        _ => Err(format!("unexpected market {market}")),
    });

    let finalizer = finalizer(
        backend,
        &pay,
        &receive,
        vec!["ICP_USDC".to_string(), "ICP_USD".to_string(), "USDC_USD".to_string()],
    );
    let request = SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_formatted(pay, 1.0),
        receive_asset: receive.asset_id(),
        receive_address: Some(Principal::from_slice(&[42]).to_text()),
        max_slippage_bps: Some(200),
        venue_hint: Some("kraken".to_string()),
    };

    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect("fillable hop should replace thin direct route");
    assert_eq!(preview.quote.legs[0].route_id, "ICP_USD:sell>USDC_USD:buy");
}

#[tokio::test]
async fn kraken_direct_withdraw_waits_for_terminal_status_and_reports_net_receive() {
    let pay = bridged_token("ckUSDC");
    let receive = native_icp_token();
    let mut backend = MockCexBackend::new();
    backend
        .expect_withdraw()
        .once()
        .returning(|asset, network, address, amount| {
            assert_eq!(asset, "ICP");
            assert_eq!(network, "ICP");
            assert!(!address.is_empty());
            Ok(WithdrawalReceipt {
                asset: asset.to_string(),
                network: network.to_string(),
                amount,
                txid: None,
                internal_id: Some("kraken-withdraw-1".to_string()),
            })
        });
    backend
        .expect_get_withdraw_status_snapshot_by_id()
        .withf(|asset, id| asset == "ICP" && id == "kraken-withdraw-1")
        .once()
        .returning(|_, _| {
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("icp-tx-1".to_string()),
                transaction_fee: Some(0.01),
            })
        });

    let finalizer = finalizer(backend, &pay, &receive, vec![]);
    let mut state = finalizer
        .prepare_amount_scoped_state(
            "withdraw-test",
            ChainTokenAmount::from_formatted(pay, 1.0),
            receive,
            Some(Principal::from_slice(&[42]).to_text()),
        )
        .expect("state");
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        1.0,
    ));

    finalizer.withdraw(&mut state).await.expect("submit withdrawal");
    assert_eq!(state.step, CexStep::WithdrawPending);
    assert_eq!(state.withdraw.withdraw_id.as_deref(), Some("kraken-withdraw-1"));

    finalizer.withdraw(&mut state).await.expect("reconcile withdrawal");
    assert_eq!(state.step, CexStep::Completed);
    assert_eq!(state.withdraw.withdraw_txid.as_deref(), Some("icp-tx-1"));
    assert!((state.withdraw.size_out.expect("net output").to_f64() - 0.99).abs() < 1e-9);
}
