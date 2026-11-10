//! The preview: sized from the bridge arrival, quoted by Rift, haircut for the plan.
use super::*;
use crate::finalizers::multi_venue::{MultiVenueAdapter, VenueAmountError, VenuePlanningContext};
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::side_cost::SideCost;

fn context() -> VenuePlanningContext {
    VenuePlanningContext {
        liquidation_id: "42".to_string(),
    }
}

#[tokio::test]
async fn preview_quotes_the_amount_the_bridge_delivers_and_keeps_both_bridge_costs() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote().times(1).returning(|request| {
        assert_eq!(request.from.as_str(), "ethereum.usdc");
        assert_eq!(request.to.as_str(), "ethereum.usdt");
        // 100.01 ckUSDC less the 0.01 ledger fee.
        assert_eq!(request.amount_in, "100");
        Ok(quote(&request.amount_in, "99.5"))
    });
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_fee_budget()
        .times(2)
        .returning(|asset, chain, target| {
            let label = format!("{asset}@{chain}->{target}");
            Ok(BridgeFeeBudget {
                side_costs: vec![SideCost::new(label, "ETH", 18, Nat::from(1u8))],
                ..BridgeFeeBudget::default()
            })
        });
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.0));
    bridge.expect_get_gas_balance().times(1).returning(|chain, _| {
        assert_eq!(chain, "ETH");
        Ok(0.05)
    });
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());

    let preview = finalizer.preview(&context(), &request(100_010_000)).await.unwrap();

    assert_eq!(preview.venue_id, RIFT_VENUE_ID);
    assert_eq!(preview.quote.receive_amount, Nat::from(99_500_000u64));
    assert_eq!(preview.quote.estimated_price_impact_bps, 0.0);
    // 30 bps of quote costs off the output.
    assert_eq!(preview.conservative_receive.value, Nat::from(99_201_500u64));
    // The whole allocation at Rift's price, before the ledger fee.
    assert_eq!(preview.market_receive, Some(Nat::from(99_509_950u64)));
    let labels: Vec<&str> = preview.side_costs.iter().map(|cost| cost.label.as_str()).collect();
    assert_eq!(labels, vec!["ckUSDC@ICP->USDC", "USDT@ETH->ckUSDT"]);
    assert_eq!(preview.quote.legs[0].route_id, "ethereum.usdc>ethereum.usdt");
}

/// The return leg pays gas from the bridge wallet. A wallet that cannot pay
/// it must not receive funds that then sit on Ethereum.
#[tokio::test]
async fn preview_refuses_the_route_while_the_bridge_wallet_lacks_return_gas() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .returning(|request| Ok(quote(&request.amount_in, "99.5")));
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_fee_budget().returning(|_, chain, _| {
        let gas = if chain == "ETH" {
            // 0.01 ETH for the approve and the helper call.
            Nat::from(10_000_000_000_000_000u64)
        } else {
            Nat::from(0u8)
        };
        Ok(BridgeFeeBudget {
            side_costs: vec![SideCost::new("gas", "ETH", 18, gas)],
            ..BridgeFeeBudget::default()
        })
    });
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.0));
    bridge.expect_get_gas_balance().times(1).returning(|_, _| Ok(0.001));
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());

    let error = finalizer.preview(&context(), &request(100_010_000)).await.unwrap_err();

    // 0.01 ETH of gas at the quoted price, plus the 25 percent margin.
    assert!(error.contains("holds 0.001 ETH, below the 0.0125 ETH"), "{error}");
}

#[tokio::test]
async fn preview_refuses_a_quote_that_pays_nothing() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .returning(|request| Ok(quote(&request.amount_in, "0")));
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());

    let error = finalizer.preview(&context(), &request(100_010_000)).await.unwrap_err();

    assert!(error.contains("quotes nothing"), "{error}");
}

#[tokio::test]
async fn an_asset_rift_does_not_carry_is_unavailable_before_any_call() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let icp = ChainToken::Icp {
        ledger: Principal::from_slice(&[11]),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    };
    let mut request = request(100_010_000);
    request.pay_asset = icp.asset_id();
    request.pay_amount = ChainTokenAmount::from_raw(icp, Nat::from(100_000_000u64));

    let error = finalizer.check_amount(&request).await.unwrap_err();

    assert!(
        matches!(error, VenueAmountError::Unavailable(ref reason) if reason.contains("ICP")),
        "{error}"
    );
}

/// The ckETH minter takes the Ethereum gas out of the withdrawn amount at
/// execution time, so a ckETH order gets the same headroom as a ckBTC one.
#[tokio::test]
async fn a_cketh_leg_quotes_ethereum_with_headroom_under_the_expected_arrival() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote().times(1).returning(|request| {
        assert_eq!(request.from.as_str(), "ethereum.eth");
        assert_eq!(request.to.as_str(), "ethereum.usdt");
        // 0.05 ckETH, less the ledger fee and the approve reserve, less the
        // 0.0005 ETH withdrawal fee, less the same again as headroom.
        assert_eq!(request.amount_in, "0.048996");
        Ok(quote(&request.amount_in, "100"))
    });
    let bridge = minter_bridge("ckETH", 0.000_002, 0.000_5, 0.03);
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut request = request(1);
    request.pay_asset = cketh().asset_id();
    request.pay_amount = ChainTokenAmount::from_raw(cketh(), Nat::from(50_000_000_000_000_000u64));

    let preview = finalizer.preview(&context(), &request).await.unwrap();

    assert_eq!(preview.quote.legs[0].route_id, "ethereum.eth>ethereum.usdt");
    let saved: RiftState = serde_json::from_value(preview.initial_execution_state.state["rift"].clone()).unwrap();
    assert_eq!(saved.pay_chain, "ethereum");
    assert_eq!(saved.arrival_headroom.to_f64(), 0.000_5);
}

/// Only a minter that takes its fee out of the amount asks for headroom.
#[test]
fn only_routes_whose_minter_deducts_its_fee_ask_for_headroom() {
    use crate::finalizers::rift::preview::order_headroom;
    use liquidium_pipeline_connectors::backend::bridge_backend::resolve_route;
    let budget = BridgeFeeBudget {
        destination_fee_budget: 0.000_015,
        ..BridgeFeeBudget::default()
    };
    let ckbtc = resolve_route("ckBTC", "ICP", "BTC").unwrap();
    let cketh = resolve_route("ckETH", "ICP", "ETH").unwrap();
    let ckusdc = resolve_route("ckUSDC", "ICP", "USDC").unwrap();
    assert_eq!(order_headroom(ckbtc, &budget), 0.000_015);
    assert_eq!(order_headroom(cketh, &budget), 0.000_015);
    assert_eq!(order_headroom(ckusdc, &budget), 0.0);
}

/// The payout always comes back over the ckERC20 bridge, so ckBTC is a pay
/// asset only until the BTC deposit route exists.
#[tokio::test]
async fn ckbtc_is_not_a_receive_asset_yet() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut request = request(100_010_000);
    request.receive_asset = ckbtc().asset_id();

    let error = finalizer.check_amount(&request).await.unwrap_err();

    assert!(
        matches!(error, VenueAmountError::Unavailable(ref reason) if reason.contains("only to Ethereum")),
        "{error}"
    );
}

/// A ckBTC leg pays Rift on Bitcoin. The minter takes its fee out of the
/// amount, and the order asks for one more fee estimate less, so a fee rise
/// at burn time cannot underfund it.
#[tokio::test]
async fn a_ckbtc_leg_quotes_bitcoin_with_headroom_under_the_expected_arrival() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote().times(1).returning(|request| {
        assert_eq!(request.from.as_str(), "bitcoin.btc");
        assert_eq!(request.to.as_str(), "ethereum.usdt");
        // 0.01 ckBTC, less the ledger fee and the approve fee (10 satoshi each),
        // less the 1,500 satoshi withdrawal fee, less the same again as headroom.
        assert_eq!(request.amount_in, "0.0099698");
        Ok(quote(&request.amount_in, "600"))
    });
    let bridge = minter_bridge("ckBTC", 0.000_000_1, 0.000_015, 0.000_5);
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let mut request = request(1_000_000);
    request.pay_asset = ckbtc().asset_id();
    request.pay_amount = ChainTokenAmount::from_raw(ckbtc(), Nat::from(1_000_000u64));

    let preview = finalizer.preview(&context(), &request).await.unwrap();

    assert_eq!(preview.quote.legs[0].route_id, "bitcoin.btc>ethereum.usdt");
    let rift_state = preview.initial_execution_state.state.get("rift").unwrap();
    assert_eq!(rift_state.get("pay_chain").and_then(|v| v.as_str()), Some("bitcoin"));
    let saved: RiftState = serde_json::from_value(rift_state.clone()).unwrap();
    assert_eq!(saved.arrival_headroom.to_f64(), 0.000_015);
    assert_eq!(saved.amount_in.to_f64(), 0.009_969_8);
    assert_eq!(preview.quote.receive_amount, Nat::from(600_000_000u64));
}

#[tokio::test]
async fn a_pair_of_the_same_asset_is_unavailable() {
    let finalizer = finalizer(
        MockRiftBackend::new(),
        MockBridgeBackend::new(),
        MockOutboundSender::new(),
    );
    let mut request = request(100_010_000);
    request.receive_asset = ckusdc().asset_id();

    let error = finalizer.check_amount(&request).await.unwrap_err();

    assert!(
        matches!(error, VenueAmountError::Unavailable(ref reason) if reason.contains("itself")),
        "{error}"
    );
}
