//! The preview counts the costs execution pays (audit L10).
use super::*;

use num_traits::ToPrimitive;

fn return_gas() -> SideCost {
    SideCost::new(
        "ETH gas for the USDC helper deposit",
        "ETH",
        18,
        Nat::from(300_000_000_000_000u64),
    )
}

#[tokio::test]
async fn bridged_preview_counts_both_bridges_and_the_mexc_withdrawal() {
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms().returning(|asset, network| {
        assert_eq!((asset, network), ("USDC", "ETH"));
        Ok(WithdrawTerms { fee: 1.0, minimum: 5.0 })
    });
    // ETH reaches USDC through USDT, one taker fee per leg.
    cex.expect_get_orderbook().returning(|market, _| {
        let price = match market {
            "ETH_USDT" => 3_000.0,
            "USDC_USDT" => 1.0,
            _ => return Err(format!("unlisted market {market}")),
        };
        let level = OrderBookLevel {
            price,
            quantity: 1_000_000.0,
        };
        Ok(OrderBook {
            bids: vec![level.clone()],
            asks: vec![level],
            taker_fee_bps: 10.0,
        })
    });
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_fee_budget()
        .returning(|asset, chain, target| match (asset, chain, target) {
            ("ckETH", "ICP", "ETH") => Ok(BridgeFeeBudget {
                source_fee_budget: 0.000_002,
                destination_fee_budget: 0.000_05,
                provider_fee_budget_native_units: Some(Nat::from(50_000_000_000_000u64)),
                side_costs: Vec::new(),
            }),
            ("USDC", "ETH", "ckUSDC") => Ok(BridgeFeeBudget {
                side_costs: vec![return_gas()],
                ..BridgeFeeBudget::default()
            }),
            other => Err(format!("unexpected budget {other:?}")),
        });
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.005));
    let pay = cketh_token();
    let receive = ckusdc_token();
    let registry = liquidium_pipeline_core::tokens::token_registry::TokenRegistry::new(HashMap::from([
        (pay.asset_id(), pay.clone()),
        (receive.asset_id(), receive.clone()),
    ]));
    let finalizer = cksol_finalizer(cex, bridge)
        .with_token_registry(Arc::new(registry))
        .with_route_config(vec!["ETH_USDT".into(), "USDC_USDT".into()], 2);
    let request = crate::swappers::model::SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_formatted(pay.clone(), 0.1),
        receive_asset: receive.asset_id(),
        receive_address: None,
        max_slippage_bps: Some(200),
        venue_hint: Some("mexc".into()),
    };

    let preview = finalizer
        .preview_swap_request("route-costs", &request)
        .await
        .expect("the bridged route previews");

    // The deposit's own steps: ledger fee, approve reserve, then the minter's gas.
    let ledger_fee = ChainTokenAmount::from_raw(pay.clone(), pay.fee()).to_f64();
    let arrival = 0.1 - ledger_fee - 0.000_002 - 0.000_05;
    // MEXC's taker fee on each leg, then its withdrawal fee.
    let expected = (arrival * 3_000.0 * 0.999 * 0.999 - 1.0) * 1e6;
    let received = preview.quote.receive_amount.0.to_f64().unwrap();
    assert!(
        (received - expected).abs() <= 2.0,
        "received {received}, expected {expected}"
    );
    // The return's gas is paid from the bridge wallet, so it travels as a side cost.
    assert_eq!(preview.side_costs, vec![return_gas()]);
    assert_eq!(preview.state.side_costs, vec![return_gas()]);
}
