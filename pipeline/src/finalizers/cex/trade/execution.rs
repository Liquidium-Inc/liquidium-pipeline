//! Runs one trade leg as a loop of slices: preview, order, fill progress,
//! and the pause when nothing fills.
use super::*;
use crate::swappers::model::BPS_PER_RATIO_UNIT;

/// Hard stop to avoid unbounded per-leg slicing loops on pathological books.
const MAX_SLICE_EXECUTION_ROUNDS: usize = 128;

/// Orders in a row that fill nothing within the price limit before the leg
/// pauses until the next cycle. A pause is a wait, not an error.
const MAX_EMPTY_FILLS_IN_A_ROW: usize = 3;

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// Serialize executions per market to reduce self-induced impact from concurrent liquidations.
    pub(super) async fn acquire_market_lock(&self, market: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut guard = self.market_locks.lock().await;
            guard
                .entry(market.to_string())
                .or_insert_with(|| Arc::new(TokioMutex::new(())))
                .clone()
        };

        lock.lock_owned().await
    }

    /// Execute one route leg by slicing `amount_in` into impact-bounded chunks.
    ///
    /// Detailed algorithm:
    /// - `preview_trade_slice` proposes the next chunk from live orderbook depth:
    ///   soft target (`target_bps`) first, hard-cap fallback second.
    /// - `maybe_mark_trade_dust` stops execution when, after the leg's first fill,
    ///   the chunk and remainder notional are both below `cex_min_exec_usd`.
    /// - each accepted chunk is sent as an IOC limit order at the slippage cap.
    /// - realized `exec_price` is derived from actual fills and compared to preview
    ///   midpoint to compute realized slippage in bps.
    /// - if realized slippage exceeds `max_sell_slippage_bps`, the leg fails fast.
    /// - each executed slice is recorded, and the route slippage is computed from the records.
    ///
    /// Returns `Some((remaining_in, total_out))` once the leg is done, where
    /// `remaining_in` is the unexecuted residual (typically `0` or dust). `None`
    /// means the leg paused with its progress saved; the next cycle resumes it.
    pub(super) async fn execute_trade_leg_slices(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        leg_idx: usize,
        total_legs: usize,
        amount_in: f64,
        target_bps: f64,
    ) -> Result<Option<(f64, f64)>, CexBackendError> {
        // Resume from persisted per-leg progress if available.
        let mut remaining_in = state.trade.trade_progress_remaining_in.unwrap_or(amount_in);
        let mut total_out = state.trade.trade_progress_total_out.unwrap_or(0.0);
        // Hard guard against pathological loops when liquidity math cannot converge.
        let mut slice_round_count = 0usize;
        let mut empty_fills_in_a_row = 0usize;

        // One-shot override for the NEXT buy slice only.
        // Normal mode is quote-driven (`Auto`), but if quote truncation is large
        // we can force `BaseQuantity` on the next slice to consume more residual.
        let mut forced_next_buy_mode: Option<BuyOrderInputMode> = None;

        Self::sanitize_pending_trade_state_for_leg(state, leg);

        // Keep slicing until the leg input is consumed (or considered dust).
        while remaining_in > LIQUIDITY_EPS {
            slice_round_count += 1;
            // Prevent unbounded retries if chunk sizing keeps returning tiny progress.
            if slice_round_count > MAX_SLICE_EXECUTION_ROUNDS {
                let err = format!("trade slicing exceeded max rounds for market {}", leg.market);
                state.last_error = Some(err.clone());
                return Err(err.into());
            }

            // Preview the next executable chunk using live orderbook depth.
            // This gives us expected chunk size, midpoint proxy, and expected impact.
            let preview = match self.preview_trade_slice(leg, remaining_in, target_bps).await {
                Ok(preview) => preview,
                Err(err) => {
                    state.last_error = Some(err.clone());
                    return Err(err.into());
                }
            };

            if self
                .maybe_mark_trade_dust(state, leg, total_out, preview.chunk_in, remaining_in)
                .await?
            {
                state.trade.trade_unexecutable_residual_in = Some(remaining_in);
                Self::persist_trade_progress(state, remaining_in, total_out);
                break;
            }

            let buy_mode = Self::resolve_slice_buy_mode(state, leg, &mut forced_next_buy_mode);
            let pending = Self::prepare_pending_slice_request(state, leg, leg_idx, preview.chunk_in, buy_mode);

            // The exchange enforces the slippage cap with an IOC order. Only if
            // MEXC refuses IOC outright does the slice go out as a market order.
            let fill_report = self
                .backend
                .execute_swap_detailed_with_options(
                    &leg.market,
                    &leg.side,
                    pending.requested_in,
                    SwapExecutionOptions {
                        client_order_id: Some(pending.client_order_id.clone()),
                        buy_mode,
                        max_quote_overspend_bps: Some(self.cex_buy_inverse_overspend_bps as f64),
                        price_limit: Some(PriceLimit {
                            reference_price: preview.preview_mid_price,
                            max_slippage_bps: self.max_sell_slippage_bps,
                        }),
                    },
                )
                .await?;

            Self::clear_pending_trade_order(state);
            if fill_report.input_consumed <= LIQUIDITY_EPS {
                // The book moved past the limit and nothing filled. The id is
                // spent, and the next round previews the book again.
                state
                    .trade
                    .trade_applied_client_order_ids
                    .push(pending.client_order_id.clone());
                empty_fills_in_a_row += 1;
                if empty_fills_in_a_row >= MAX_EMPTY_FILLS_IN_A_ROW {
                    // The book keeps moving away. Waiting costs no retry budget,
                    // so the capital is never parked for this alone.
                    Self::persist_trade_progress(state, remaining_in, total_out);
                    warn!(
                        "[mexc] liq_id={} {} {}: {} orders in a row filled nothing within the limit; retrying next cycle",
                        state.liq_id, leg.market, leg.side, empty_fills_in_a_row
                    );
                    return Ok(None);
                }
                continue;
            }
            empty_fills_in_a_row = 0;
            let actual_input_consumed = fill_report.input_consumed;
            let actual_output_received = fill_report.output_received;

            // Advance by actual consumed input (not requested input), so truncation
            // is reflected in state and residual handling remains deterministic.
            Self::apply_slice_fill_progress(
                state,
                &mut remaining_in,
                &mut total_out,
                actual_input_consumed,
                actual_output_received,
            );
            // Spend the id in the same state write as the fill, so no later
            // error can let a retry submit it again.
            state
                .trade
                .trade_applied_client_order_ids
                .push(pending.client_order_id.clone());

            // Convert raw fill amounts into a realized execution price.
            // We need `exec_price` so slippage can be measured against preview mid-price.
            let exec_price =
                match Self::exec_price_from_fill(&leg.market, &leg.side, actual_input_consumed, actual_output_received)
                {
                    Ok(price) => price,
                    Err(err) => {
                        state.last_error = Some(err.clone());
                        return Err(err.into());
                    }
                };

            // Slippage is the realized price drift from preview midpoint, in bps.
            // This is the primary safety metric for adverse execution quality.
            let slice_slippage_bps = Self::slice_slippage_bps(&leg.side, preview.preview_mid_price, exec_price);

            // Record the executed slice before the cap check. A breaching slice
            // still happened, and its record is the durable trace of the breach.
            Self::record_trade_slice(
                state,
                leg_idx,
                leg,
                actual_input_consumed,
                actual_output_received,
                preview.preview_mid_price,
                exec_price,
                slice_slippage_bps,
                preview.preview_impact_bps,
            );

            // Enforce hard per-slice slippage limit before allowing leg continuation.
            if slice_slippage_bps > self.max_sell_slippage_bps {
                let err = format!(
                    "slice slippage too high for {} {}: {:.2} bps > {:.2} bps (client_id={} buy_mode={} requested_in={} actual_in={} actual_out={} preview_mid={} exec_price={})",
                    leg.market,
                    leg.side,
                    slice_slippage_bps,
                    self.max_sell_slippage_bps,
                    pending.client_order_id,
                    pending.buy_mode_label,
                    pending.requested_in,
                    actual_input_consumed,
                    actual_output_received,
                    preview.preview_mid_price,
                    exec_price
                );
                state.last_error = Some(err.clone());
                return Err(err.into());
            }

            // Emit slice execution log with route and quality context.
            info!(
                "[mexc] liq_id={} trade leg {}/{} slice={} market={} side={} in={} out={} slippage_bps={:.2} preview_impact_bps={:.2}",
                state.liq_id,
                leg_idx + 1,
                total_legs,
                slice_round_count,
                leg.market,
                leg.side,
                actual_input_consumed,
                actual_output_received,
                slice_slippage_bps,
                preview.preview_impact_bps
            );

            self.maybe_arm_adaptive_buy_fallback(
                state,
                leg,
                pending.requested_in,
                actual_input_consumed,
                remaining_in,
                &mut forced_next_buy_mode,
            )
            .await;
        }

        Self::persist_trade_progress(state, remaining_in, total_out);
        // Return any unexecuted residual (often 0 or dust) plus total output of this leg.
        Ok(Some((remaining_in, total_out)))
    }

    fn persist_trade_progress(state: &mut CexState, remaining_in: f64, total_out: f64) {
        state.trade.trade_progress_remaining_in = Some(remaining_in);
        state.trade.trade_progress_total_out = Some(total_out);
    }

    fn apply_slice_fill_progress(
        state: &mut CexState,
        remaining_in: &mut f64,
        total_out: &mut f64,
        actual_input_consumed: f64,
        actual_output_received: f64,
    ) {
        *remaining_in = (*remaining_in - actual_input_consumed).max(0.0);
        *total_out += actual_output_received;
        Self::persist_trade_progress(state, *remaining_in, *total_out);
    }

    /// Convert fill amounts into execution price according to side semantics.
    pub(super) fn exec_price_from_fill(
        market: &str,
        side: &str,
        chunk_in: f64,
        filled_out: f64,
    ) -> Result<f64, String> {
        let exec_price = if side.eq_ignore_ascii_case("sell") {
            if chunk_in > 0.0 { filled_out / chunk_in } else { 0.0 }
        } else if filled_out > 0.0 {
            chunk_in / filled_out
        } else {
            0.0
        };

        if exec_price <= 0.0 || !exec_price.is_finite() {
            return Err(format!("invalid execution price for {} {}", market, side));
        }
        Ok(exec_price)
    }

    /// Compute slippage in bps from preview mid and realized execution price.
    pub(super) fn slice_slippage_bps(side: &str, preview_mid_price: f64, exec_price: f64) -> f64 {
        if side.eq_ignore_ascii_case("sell") {
            ((preview_mid_price - exec_price) / preview_mid_price * BPS_PER_RATIO_UNIT).max(0.0)
        } else {
            ((exec_price - preview_mid_price) / preview_mid_price * BPS_PER_RATIO_UNIT).max(0.0)
        }
    }
}
