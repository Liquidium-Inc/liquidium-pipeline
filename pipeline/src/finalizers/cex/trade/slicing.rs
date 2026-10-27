//! Slice sizing from order book depth: the largest chunk under the impact
//! target, the preview of one slice, and the dust decision.
use super::*;

/// One leg simulated on the current order book.
pub(in crate::finalizers::cex) struct LegPreview {
    /// The output before the taker fee.
    pub gross_out: f64,
    pub impact_bps: f64,
    /// The market's taker fee, in bps.
    pub taker_fee_bps: f64,
}

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// Generic binary search used by buy/sell chunk sizing.
    ///
    /// The `simulate` callback must return `(impact_bps, residual_unfilled)` for a candidate chunk.
    /// A chunk is accepted only when:
    /// - residual is near-zero (fully executable on visible depth)
    /// - impact is at or below `target_bps`
    fn max_chunk_for_target_with_simulator<F>(max_input: f64, target_bps: f64, mut simulate: F) -> f64
    where
        F: FnMut(f64) -> Result<(f64, f64), String>,
    {
        if max_input <= LIQUIDITY_EPS {
            return 0.0;
        }

        // Fast path: if the full amount already passes constraints, avoid binary search.
        if let Ok((full_impact, full_residual)) = simulate(max_input)
            && full_residual <= LIQUIDITY_EPS
            && full_impact <= target_bps
        {
            return max_input;
        }

        // Binary search for the largest executable amount under target impact.
        let mut lower_bound = 0.0;
        let mut upper_bound = max_input;
        let mut best_valid_chunk = 0.0;
        for _ in 0..SLICE_SEARCH_STEPS {
            let candidate_chunk = (lower_bound + upper_bound) / 2.0;
            if candidate_chunk <= LIQUIDITY_EPS {
                break;
            }

            match simulate(candidate_chunk) {
                // Candidate is valid; keep it and try a larger chunk.
                Ok((impact, residual)) if residual <= LIQUIDITY_EPS && impact <= target_bps => {
                    best_valid_chunk = candidate_chunk;
                    lower_bound = candidate_chunk;
                }
                // Candidate is invalid (too much impact / not fully fillable / sim error); shrink.
                Ok(_) | Err(_) => {
                    upper_bound = candidate_chunk;
                }
            }
        }

        best_valid_chunk
    }

    /// Estimate the largest sell chunk under a target impact, in base units.
    fn max_sell_chunk_for_target(&self, bids: &[OrderBookLevel], max_amount: f64, target_bps: f64) -> f64 {
        Self::max_chunk_for_target_with_simulator(max_amount, target_bps, |candidate| {
            let (_, _, impact, unfilled) = simulate_sell_from_bids(bids, candidate)?;
            Ok((impact, unfilled))
        })
    }

    /// Estimate the largest buy chunk under a target impact, in quote units.
    fn max_buy_chunk_for_target(&self, asks: &[OrderBookLevel], max_quote: f64, target_bps: f64) -> f64 {
        Self::max_chunk_for_target_with_simulator(max_quote, target_bps, |candidate| {
            let (_, _, impact, unspent) = simulate_buy_from_asks(asks, candidate)?;
            Ok((impact, unspent))
        })
    }

    /// Simulate one leg with current orderbook for route previews.
    pub(in crate::finalizers::cex) async fn preview_leg(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
    ) -> Result<LegPreview, String> {
        let orderbook = self
            .backend
            .get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await?;

        if side.eq_ignore_ascii_case("sell") {
            let (out, _avg, impact, unfilled) = simulate_sell_from_bids(&orderbook.bids, amount_in)?;
            if unfilled > LIQUIDITY_EPS {
                return Err(format!(
                    "not enough bid liquidity for {} (needed {}, missing {})",
                    market, amount_in, unfilled
                ));
            }
            return Ok(LegPreview {
                gross_out: out,
                impact_bps: impact,
                taker_fee_bps: orderbook.taker_fee_bps,
            });
        }

        let (out, _avg, impact, unspent) = simulate_buy_from_asks(&orderbook.asks, amount_in)?;
        if unspent > LIQUIDITY_EPS {
            return Err(format!(
                "not enough ask liquidity for {} (needed {}, missing {})",
                market, amount_in, unspent
            ));
        }

        Ok(LegPreview {
            gross_out: out,
            impact_bps: impact,
            taker_fee_bps: orderbook.taker_fee_bps,
        })
    }

    /// Target impact threshold used for one execution slice.
    pub(super) fn target_slice_bps(&self) -> f64 {
        (self.max_sell_slippage_bps * self.cex_slice_target_ratio)
            .max(1.0)
            .min(self.max_sell_slippage_bps.max(1.0))
    }

    /// Build a one-slice preview under the configured impact target.
    pub(super) async fn preview_trade_slice(
        &self,
        leg: &TradeLeg,
        remaining_in: f64,
        target_bps: f64,
    ) -> Result<SlicePreview, String> {
        let orderbook = self
            .backend
            .get_orderbook(&leg.market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await?;

        if leg.side.eq_ignore_ascii_case("sell") {
            return self.preview_sell_trade_slice(leg, &orderbook.bids, remaining_in, target_bps);
        }

        self.preview_buy_trade_slice(leg, &orderbook.asks, remaining_in, target_bps)
    }

    /// Build one sell-side preview slice (base -> quote), including fallback to hard cap.
    fn preview_sell_trade_slice(
        &self,
        leg: &TradeLeg,
        bids: &[OrderBookLevel],
        remaining_in: f64,
        target_bps: f64,
    ) -> Result<SlicePreview, String> {
        let candidate = self.max_sell_chunk_for_target(bids, remaining_in, target_bps);
        let chunk =
            self.resolve_chunk_with_hard_cap_fallback(&leg.market, "sell", remaining_in, candidate, |amount| {
                let (_, _, impact, residual) = simulate_sell_from_bids(bids, amount)?;
                Ok((impact, residual))
            })?;

        self.build_slice_preview(&leg.market, "bid", chunk, |amount| {
            simulate_sell_from_bids(bids, amount)
        })
    }

    /// Build one buy-side preview slice (quote -> base), including fallback to hard cap.
    fn preview_buy_trade_slice(
        &self,
        leg: &TradeLeg,
        asks: &[OrderBookLevel],
        remaining_in: f64,
        target_bps: f64,
    ) -> Result<SlicePreview, String> {
        let candidate = self.max_buy_chunk_for_target(asks, remaining_in, target_bps);
        let chunk =
            self.resolve_chunk_with_hard_cap_fallback(&leg.market, "buy", remaining_in, candidate, |amount| {
                let (_, _, impact, residual) = simulate_buy_from_asks(asks, amount)?;
                Ok((impact, residual))
            })?;

        self.build_slice_preview(&leg.market, "ask", chunk, |amount| simulate_buy_from_asks(asks, amount))
    }

    /// Resolve final chunk size when soft-target sizing returns no usable chunk.
    ///
    /// Why this exists:
    /// - `candidate_chunk` is computed with a *soft* target (`target_bps`) used for preferred slice quality.
    /// - On thin/discrete books, that soft target can produce `0` even when a trade is still acceptable.
    /// - We do not want to reject such cases if the trade is still within the *hard* safety limit
    ///   (`max_sell_slippage_bps`) and fully fillable.
    ///
    /// Decision:
    /// 1. If binary search found a positive chunk, use it.
    /// 2. Otherwise simulate the full `remaining_in` once:
    ///    - accept it if fillable and within hard cap,
    ///    - reject with an error otherwise.
    fn resolve_chunk_with_hard_cap_fallback<F>(
        &self,
        market: &str,
        side: &str,
        remaining_in: f64,
        candidate_chunk: f64,
        mut simulate_impact: F,
    ) -> Result<f64, String>
    where
        F: FnMut(f64) -> Result<(f64, f64), String>,
    {
        if candidate_chunk > LIQUIDITY_EPS {
            return Ok(candidate_chunk);
        }

        // Soft target rejected everything; run a one-shot hard-cap check on full residual.
        let (impact, residual) = simulate_impact(remaining_in)?;
        if residual <= LIQUIDITY_EPS && impact <= self.max_sell_slippage_bps {
            return Ok(remaining_in);
        }

        Err(format!(
            "cannot find {} chunk under impact target for {} (remaining={}, impact={:.2}bps)",
            side, market, remaining_in, impact
        ))
    }

    /// Build final preview object and assert visible depth can fully fill `chunk`.
    fn build_slice_preview<F>(
        &self,
        market: &str,
        liquidity_side: &str,
        chunk: f64,
        mut simulate_fill: F,
    ) -> Result<SlicePreview, String>
    where
        F: FnMut(f64) -> Result<(f64, f64, f64, f64), String>,
    {
        let (_out, avg_price, impact, residual) = simulate_fill(chunk)?;
        if residual > LIQUIDITY_EPS {
            return Err(format!(
                "not enough {} liquidity for {} (needed {}, missing {})",
                liquidity_side, market, chunk, residual
            ));
        }

        Ok(SlicePreview {
            chunk_in: chunk,
            preview_mid_price: avg_price,
            preview_impact_bps: impact,
        })
    }

    /// Skip tiny residual chunks that are below configured minimum execution notional.
    ///
    /// The floor trims leftovers after a fill. It never discards a whole
    /// committed leg whose price fell during deposit settlement; the exchange's
    /// own lot and minimum checks still apply to that first fill.
    pub(super) async fn maybe_mark_trade_dust(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        leg_total_out: f64,
        chunk_in: f64,
        remaining_in: f64,
    ) -> Result<bool, String> {
        if leg_total_out <= 0.0 {
            return Ok(false);
        }
        let chunk_usd = self.input_slice_usd(&leg.market, &leg.side, chunk_in).await?;
        if chunk_usd >= self.cex_min_exec_usd {
            return Ok(false);
        }

        let residual_usd = self.input_slice_usd(&leg.market, &leg.side, remaining_in).await?;
        if residual_usd >= self.cex_min_exec_usd {
            return Ok(false);
        }

        state.trade.trade_dust_skipped = true;
        state.trade.trade_dust_usd = Some(residual_usd);
        info!(
            "[mexc] liq_id={} dust skipped market={} side={} residual_in={} residual_usd={}",
            state.liq_id, leg.market, leg.side, remaining_in, residual_usd
        );
        Ok(true)
    }
}
