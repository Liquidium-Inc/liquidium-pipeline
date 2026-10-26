//! The record of executed slices, the route slippage computed from it, and
//! the move to the next leg.
use super::*;
use crate::finalizers::cex_finalizer::CexTradeSlice;
use crate::swappers::model::BPS_PER_RATIO_UNIT;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// Adverse slippage of the whole route against the preview mids, in bps.
    /// Each leg compares its output with its output at the mids, in its own
    /// currency; the legs' output ratios multiply along the route.
    pub(in crate::finalizers::mexc) fn route_slippage_bps(slices: &[CexTradeSlice]) -> f64 {
        let mut legs: std::collections::BTreeMap<u32, (f64, f64)> = std::collections::BTreeMap::new();
        for slice in slices {
            let output_at_mid = if slice.side.eq_ignore_ascii_case("sell") {
                slice.amount_in * slice.mid_price
            } else if slice.mid_price > 0.0 {
                slice.amount_in / slice.mid_price
            } else {
                0.0
            };
            let leg = legs.entry(slice.leg_index).or_default();
            leg.0 += slice.amount_out;
            leg.1 += output_at_mid;
        }
        let output_ratio: f64 = legs
            .values()
            .filter(|(_, output_at_mid)| *output_at_mid > LIQUIDITY_EPS)
            .map(|(output, output_at_mid)| output / output_at_mid)
            .product();
        ((1.0 - output_ratio) * BPS_PER_RATIO_UNIT).max(0.0)
    }

    /// Append one executed slice for telemetry/export.
    pub(super) fn record_trade_slice(
        state: &mut CexState,
        leg_idx: usize,
        leg: &TradeLeg,
        chunk_in: f64,
        filled_out: f64,
        preview_mid_price: f64,
        exec_price: f64,
        slice_slippage_bps: f64,
        preview_impact_bps: f64,
    ) {
        state.trade.trade_slices.push(CexTradeSlice {
            leg_index: leg_idx as u32,
            market: leg.market.clone(),
            side: leg.side.clone(),
            amount_in: chunk_in,
            amount_out: filled_out,
            mid_price: preview_mid_price,
            exec_price,
            slippage_bps: slice_slippage_bps.max(preview_impact_bps),
        });
        state.trade.trade_weighted_slippage_bps = Some(Self::route_slippage_bps(&state.trade.trade_slices));
    }

    /// Advance state to the next trade leg or to withdraw when route is fully processed.
    pub(super) fn advance_after_trade_leg(state: &mut CexState, leg_idx: usize, total_legs: usize, total_out: f64) {
        state.trade.trade_last_amount_out = Some(total_out);
        state.trade.trade_next_amount_in = Some(total_out);
        state.trade.trade_leg_index = Some((leg_idx + 1) as u32);
        state.trade.trade_progress_remaining_in = None;
        state.trade.trade_progress_total_out = None;
        Self::clear_pending_trade_order(state);
        state.trade.trade_pending_buy_mode = None;
        state.trade.trade_inverse_retry_count = 0;
        state.trade.trade_unexecutable_residual_in = None;

        if leg_idx + 1 < total_legs {
            state.step = CexStep::TradePending;
            return;
        }

        Self::finish_trade_route(state, Some(total_out.max(0.0)));
    }

    /// Ends the trade phase. A known route output becomes the withdrawal size.
    pub(super) fn finish_trade_route(state: &mut CexState, out_amt: Option<f64>) {
        state.step = CexStep::Withdraw;
        if let Some(out_amt) = out_amt {
            state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
                state.withdraw.withdraw_asset.clone(),
                out_amt,
            ));
        }
    }
}
