//! Audit L19, L23 and L25: the fee reserve, the route slippage and the per-slice amounts.
use super::*;
use crate::finalizers::cex_finalizer::CexTradeSlice;

fn slice(
    leg_index: u32,
    market: &str,
    side: &str,
    amount_in: f64,
    amount_out: f64,
    mid_price: f64,
) -> CexTradeSlice {
    CexTradeSlice {
        leg_index,
        market: market.into(),
        side: side.into(),
        amount_in,
        amount_out,
        mid_price,
        exec_price: 0.0,
        slippage_bps: 0.0,
    }
}

fn route_bps(slices: &[CexTradeSlice]) -> f64 {
    MexcFinalizer::<MockCexBackend>::route_slippage_bps(slices)
}

#[test]
fn a_buy_above_mid_and_a_sell_below_mid_are_both_adverse() {
    // Paid 101 for one unit with a mid of 100: 1.01 units at mid, 1 received.
    assert!((route_bps(&[slice(0, "ICP_USDT", "buy", 101.0, 1.0, 100.0)]) - 99.009_900_99).abs() < 1e-6);
    // Sold one unit for 99 with a mid of 100.
    assert!((route_bps(&[slice(0, "ICP_USDT", "sell", 1.0, 99.0, 100.0)]) - 100.0).abs() < 1e-9);
    // Better than mid is no slippage.
    assert_eq!(route_bps(&[slice(0, "ICP_USDT", "buy", 99.0, 1.0, 100.0)]), 0.0);
}

#[test]
fn legs_in_different_quote_currencies_combine_their_own_ratios() {
    let slices = [
        // 50 bps on a BTC-quoted leg of 0.01 BTC.
        slice(0, "CKBTC_BTC", "sell", 0.01, 0.00995, 1.0),
        // 30 bps on a USDC-quoted leg: 995 USDC at mid, 992.015 received.
        slice(1, "BTC_USDC", "sell", 0.00995, 992.015, 100_000.0),
    ];
    // 0.995 * 0.997 of the output at mid arrives: 79.85 bps, not 80.
    assert!((route_bps(&slices) - 79.85).abs() < 1e-6, "{}", route_bps(&slices));
}

#[test]
fn losses_on_sequential_legs_compound() {
    let slices = [
        slice(0, "ICP_USDT", "sell", 1.0, 90.0, 100.0),
        slice(1, "BTC_USDT", "buy", 90.0, 0.0009, 90_000.0),
    ];
    // Each leg keeps 90% of its output at mid, so the route keeps 81%.
    assert!((route_bps(&slices) - 1_900.0).abs() < 1e-6, "{}", route_bps(&slices));
}

#[tokio::test]
async fn a_finished_row_reports_slippage_from_its_slices_not_a_saved_figure() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()));
    let receipt = make_execution_receipt(44);
    let mut state = finalizer.prepare("44", &receipt).await.expect("prepare should succeed");
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        2.0,
    ));
    // An older build saved this figure under the old formula.
    state.trade.trade_weighted_slippage_bps = Some(999.0);
    state.trade.trade_slices = vec![slice(0, "ICP_USDT", "sell", 1.0, 99.0, 100.0)];

    let swap = finalizer.finish_state(&state).expect("finish should succeed");

    assert!(
        (swap.realized_slippage_bps - 100.0).abs() < 1e-9,
        "{}",
        swap.realized_slippage_bps
    );
}

#[tokio::test]
async fn slice_records_keep_token_decimals() {
    let finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()));
    let receipt = make_execution_receipt(43);
    let mut state = finalizer.prepare("43", &receipt).await.expect("prepare should succeed");
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        state.withdraw.withdraw_asset.clone(),
        2.0,
    ));
    state.trade.trade_slices = vec![slice(0, "BTC_USDC", "sell", 0.01, 500.0, 50_000.0)];

    let swap = finalizer.finish_state(&state).expect("finish should succeed");

    assert_eq!(swap.legs.len(), 1);
    assert_eq!(swap.legs[0].pay_amount, Nat::from(1_000_000u64), "0.01 BTC in sats");
    assert_eq!(
        swap.legs[0].receive_amount,
        Nat::from(500_000_000u64),
        "500 USDC in 6 decimals"
    );
}

/// Only a direct non-native ICP deposit sends approval bumps, so only it
/// holds back their fees; every other route keeps the transfer fee alone.
#[test]
fn the_deposit_reserve_covers_only_the_fees_its_route_pays() {
    type F = MexcFinalizer<MockCexBackend>;
    let sol = ChainToken::SolanaNative {
        symbol: "SOL".into(),
        decimals: 9,
        fee: Nat::from(5_000u64),
    };
    assert_eq!(F::deposit_fee_count(&ckusdc_token(), false), 7);
    assert_eq!(F::deposit_fee_count(&ckusdc_token(), true), 1);
    assert_eq!(F::deposit_fee_count(&native_icp_token(), false), 1);
    assert_eq!(F::deposit_fee_count(&sol, false), 1);

    let size_in = ChainTokenAmount::from_raw(ckusdc_token(), Nat::from(1_000_000u64));
    let fee = ckusdc_token().fee();
    let (direct, _) = F::compute_fee_adjusted_deposit_transfer(&ckusdc_token(), &size_in, false).unwrap();
    let (bridged, _) = F::compute_fee_adjusted_deposit_transfer(&ckusdc_token(), &size_in, true).unwrap();
    assert_eq!(direct, Nat::from(1_000_000u64) - fee.clone() * 7u8);
    assert_eq!(bridged, Nat::from(1_000_000u64) - fee);
}
