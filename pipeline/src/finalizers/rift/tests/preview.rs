//! The preview: sized from the bridge arrival, quoted by Rift, haircut for the plan.
use super::*;
use crate::finalizers::multi_venue::{MultiVenueAdapter, VenueAmountError, VenuePlanningContext};
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
    let mut request = request(100_010_000);
    request.pay_asset = ckbtc().asset_id();
    request.pay_amount = ChainTokenAmount::from_raw(ckbtc(), Nat::from(1_000_000u64));

    let error = finalizer.check_amount(&request).await.unwrap_err();

    assert!(
        matches!(error, VenueAmountError::Unavailable(ref reason) if reason.contains("ckBTC")),
        "{error}"
    );
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
