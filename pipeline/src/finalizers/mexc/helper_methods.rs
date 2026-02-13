use super::*;
use crate::error::AppResult;

struct PendingSliceRequest {
    requested_in: f64,
    client_order_id: String,
    buy_mode_label: &'static str,
}

/// Deposit is considered confirmed once balance delta reaches expected amount minus this epsilon.
const DEPOSIT_CONFIRMATION_DELTA_EPSILON: f64 = 0.00001;
/// Hard stop to avoid unbounded per-leg slicing loops on pathological books.
const MAX_SLICE_EXECUTION_ROUNDS: usize = 128;
/// Basis points per 1.00 ratio value.
const BPS_PER_RATIO_UNIT: f64 = 10_000.0;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// Phase-B deposit confirmation by balance delta against the captured baseline.
    pub(super) async fn check_deposit(&self, state: &mut CexState) -> AppResult<()> {
        let symbol = state.deposit.deposit_asset.symbol();
        let current_balance = self.backend.get_balance(&symbol).await?;

        match state.deposit.deposit_balance_before {
            Some(baseline_balance) => {
                let expected_deposit_amount = state
                    .trade
                    .trade_next_amount_in
                    .unwrap_or_else(|| state.size_in.to_f64());

                let observed_balance_delta = current_balance - baseline_balance;
                if observed_balance_delta >= expected_deposit_amount - DEPOSIT_CONFIRMATION_DELTA_EPSILON {
                    info!(
                        "[mexc] liq_id={} deposit confirmed: before={} after={} expected={}",
                        state.liq_id, baseline_balance, current_balance, expected_deposit_amount
                    );
                    state.step = CexStep::Trade;
                    return Ok(());
                }
                info!(
                    "[mexc] liq_id={} deposit pending: before={} current={} delta={} expected={}",
                    state.liq_id, baseline_balance, current_balance, observed_balance_delta, expected_deposit_amount
                );
            }
            None => {
                // No baseline recorded yet (should normally be set when we send the transfer),
                // so record the current balance as the baseline and stay in Deposit.
                info!(
                    "[mexc] liq_id={} deposit baseline set: current={}",
                    state.liq_id, current_balance
                );
                state.deposit.deposit_balance_before = Some(current_balance);
            }
        }

        Ok(())
    }

    async fn resolve_direct_leg(&self, deposit_symbol: &str, withdraw_symbol: &str) -> AppResult<TradeLeg> {
        let deposit = deposit_symbol.to_ascii_uppercase();
        let withdraw = withdraw_symbol.to_ascii_uppercase();

        let sell_market = format!("{}_{}", deposit, withdraw);
        let buy_market = format!("{}_{}", withdraw, deposit);
        let mut errors: Vec<String> = Vec::new();

        match self
            .backend
            .get_orderbook(&sell_market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await
        {
            Ok(orderbook) => {
                if !orderbook.bids.is_empty() {
                    return Ok(TradeLeg {
                        market: sell_market,
                        side: "sell".to_string(),
                    });
                }
                errors.push(format!("{} has no bids", sell_market));
            }
            Err(err) => errors.push(format!("{}: {}", sell_market, err)),
        }

        match self
            .backend
            .get_orderbook(&buy_market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await
        {
            Ok(orderbook) => {
                if !orderbook.asks.is_empty() {
                    return Ok(TradeLeg {
                        market: buy_market,
                        side: "buy".to_string(),
                    });
                }
                errors.push(format!("{} has no asks", buy_market));
            }
            Err(err) => errors.push(format!("{}: {}", buy_market, err)),
        }

        Err(format!(
            "could not resolve direct market for {} -> {} ({})",
            deposit,
            withdraw,
            errors.join(" | ")
        )
        .into())
    }

    /// Resolve one or more market legs for deposit-asset -> withdraw-asset conversion.
    pub(super) async fn resolve_trade_legs(&self, state: &CexState) -> AppResult<Vec<TradeLeg>> {
        let deposit = state.deposit.deposit_asset.symbol();
        let withdraw = state.withdraw.withdraw_asset.symbol();

        if let Some(legs) = mexc_special_trade_legs(&deposit, &withdraw) {
            return Ok(legs);
        }

        let leg = self.resolve_direct_leg(&deposit, &withdraw).await?;
        Ok(vec![leg])
    }

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

    /// Convert an amount in `symbol` units to USD for min-notional checks.
    async fn amount_symbol_to_usd(&self, symbol: &str, amount: f64) -> AppResult<f64> {
        if amount <= 0.0 {
            return Ok(0.0);
        }
        let symbol = symbol.to_ascii_uppercase();
        if is_usd_stable_symbol(&symbol) {
            return Ok(amount);
        }

        match symbol.as_str() {
            "BTC" => {
                let orderbook = self
                    .backend
                    .get_orderbook("BTC_USDC", Some(DEFAULT_ORDERBOOK_LIMIT))
                    .await?;
                let best_bid = orderbook.bids.first().map(|l| l.price).unwrap_or(0.0);
                if best_bid <= 0.0 {
                    return Err("cannot convert BTC to USD: BTC_USDC has no bids".into());
                }
                Ok(amount * best_bid)
            }
            "CKBTC" => {
                // ckBTC -> BTC -> USD conversion.
                let ckb_btc = self
                    .backend
                    .get_orderbook("CKBTC_BTC", Some(DEFAULT_ORDERBOOK_LIMIT))
                    .await?;
                let best_ckbtc_bid_btc = ckb_btc.bids.first().map(|l| l.price).unwrap_or(0.0);
                if best_ckbtc_bid_btc <= 0.0 {
                    return Err("cannot convert CKBTC to USD: CKBTC_BTC has no bids".into());
                }
                let btc_amount = amount * best_ckbtc_bid_btc;
                let btc_usdc = self
                    .backend
                    .get_orderbook("BTC_USDC", Some(DEFAULT_ORDERBOOK_LIMIT))
                    .await?;
                let best_btc_bid = btc_usdc.bids.first().map(|l| l.price).unwrap_or(0.0);
                if best_btc_bid <= 0.0 {
                    return Err("cannot convert CKBTC to USD: BTC_USDC has no bids".into());
                }
                Ok(btc_amount * best_btc_bid)
            }
            "CKUSDT" => {
                let orderbook = self
                    .backend
                    .get_orderbook("CKUSDT_USDT", Some(DEFAULT_ORDERBOOK_LIMIT))
                    .await?;
                let best_bid = orderbook.bids.first().map(|l| l.price).unwrap_or(0.0);
                if best_bid <= 0.0 {
                    return Err("cannot convert CKUSDT to USD: CKUSDT_USDT has no bids".into());
                }
                Ok(amount * best_bid)
            }
            "CKUSDC" => {
                let orderbook = self
                    .backend
                    .get_orderbook("CKUSDC_USDC", Some(DEFAULT_ORDERBOOK_LIMIT))
                    .await?;
                let best_bid = orderbook.bids.first().map(|l| l.price).unwrap_or(0.0);
                if best_bid <= 0.0 {
                    return Err("cannot convert CKUSDC to USD: CKUSDC_USDC has no bids".into());
                }
                Ok(amount * best_bid)
            }
            _ => Err(format!("cannot convert {} to USD: unsupported quote", symbol).into()),
        }
    }

    /// Estimate USD notional of one input slice for a given market/side.
    pub(super) async fn input_slice_usd(&self, market: &str, side: &str, amount_in: f64) -> AppResult<f64> {
        let (_base, quote) =
            parse_market_symbols(market).ok_or_else(|| format!("invalid market format '{}'", market))?;

        if side.eq_ignore_ascii_case("sell") {
            let orderbook = self
                .backend
                .get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT))
                .await?;
            let best_bid = orderbook.bids.first().map(|l| l.price).unwrap_or(0.0);
            if best_bid <= 0.0 {
                return Err(format!("no bids available for market {}", market).into());
            }
            let quote_amount = amount_in * best_bid;
            return self.amount_symbol_to_usd(&quote, quote_amount).await;
        }

        // buy side amount_in is quote notional by backend contract
        self.amount_symbol_to_usd(&quote, amount_in).await
    }

    /// Generic binary search used by buy/sell chunk sizing.
    ///
    /// The `simulate` callback must return `(impact_bps, residual_unfilled)` for a candidate chunk.
    /// A chunk is accepted only when:
    /// - residual is near-zero (fully executable on visible depth)
    /// - impact is at or below `target_bps`
    fn max_chunk_for_target_with_simulator<F>(max_input: f64, target_bps: f64, mut simulate: F) -> f64
    where
        F: FnMut(f64) -> AppResult<(f64, f64)>,
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
    pub(super) async fn preview_leg(&self, market: &str, side: &str, amount_in: f64) -> AppResult<(f64, f64, f64)> {
        let orderbook = self
            .backend
            .get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await?;

        if side.eq_ignore_ascii_case("sell") {
            let (out, avg, impact, unfilled) = simulate_sell_from_bids(&orderbook.bids, amount_in)?;
            if unfilled > LIQUIDITY_EPS {
                return Err(format!(
                    "not enough bid liquidity for {} (needed {}, missing {})",
                    market, amount_in, unfilled
                )
                .into());
            }
            return Ok((out, avg, impact));
        }

        let (out, avg, impact, unspent) = simulate_buy_from_asks(&orderbook.asks, amount_in)?;
        if unspent > LIQUIDITY_EPS {
            return Err(format!(
                "not enough ask liquidity for {} (needed {}, missing {})",
                market, amount_in, unspent
            )
            .into());
        }

        Ok((out, avg, impact))
    }

    /// Target impact threshold used for one execution slice.
    pub(super) fn target_slice_bps(&self) -> f64 {
        (self.max_sell_slippage_bps * self.cex_slice_target_ratio)
            .max(1.0)
            .min(self.max_sell_slippage_bps.max(1.0))
    }

    /// Persist per-leg context on state before slicing starts.
    pub(super) fn set_trade_leg_context(state: &mut CexState, leg: &TradeLeg, amount_in: f64) {
        state.trade.trade_last_market = Some(leg.market.clone());
        state.trade.trade_last_side = Some(leg.side.clone());
        state.trade.trade_last_amount_in = Some(amount_in);
        state.trade.trade_last_amount_out = None;
        state.last_error = None;
        state.trade.trade_progress_remaining_in = Some(state.trade.trade_progress_remaining_in.unwrap_or(amount_in));
        state.trade.trade_progress_total_out = Some(state.trade.trade_progress_total_out.unwrap_or(0.0));
    }

    fn buy_mode_to_str(mode: BuyOrderInputMode) -> &'static str {
        match mode {
            BuyOrderInputMode::Auto => "auto",
            BuyOrderInputMode::QuoteOrderQty => "quote_order_qty",
            BuyOrderInputMode::BaseQuantity => "base_quantity",
        }
    }

    fn parse_buy_mode(mode: Option<&str>) -> BuyOrderInputMode {
        match mode.unwrap_or_default() {
            "base_quantity" => BuyOrderInputMode::BaseQuantity,
            "quote_order_qty" => BuyOrderInputMode::QuoteOrderQty,
            _ => BuyOrderInputMode::Auto,
        }
    }

    fn buy_mode_short_code(mode: BuyOrderInputMode) -> &'static str {
        match mode {
            BuyOrderInputMode::Auto => "a",
            BuyOrderInputMode::QuoteOrderQty => "q",
            BuyOrderInputMode::BaseQuantity => "b",
        }
    }

    fn is_valid_client_order_id(value: &str) -> bool {
        let len = value.len();
        if !(1..=32).contains(&len) {
            return false;
        }

        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }

    fn stable_hash_6hex(value: &str) -> String {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in value.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("{:06x}", hash & 0x00ff_ffff)
    }

    fn sanitize_client_order_id(value: &str) -> String {
        let mut sanitized: String = value
            .bytes()
            .filter(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
            .map(char::from)
            .collect();

        if sanitized.is_empty() {
            sanitized.push_str("liq");
        }

        if sanitized.len() > 32 {
            let hash = Self::stable_hash_6hex(&sanitized);
            sanitized.truncate(25);
            sanitized.push('-');
            sanitized.push_str(&hash);
        }

        sanitized
    }

    fn clear_pending_trade_order(state: &mut CexState) {
        state.trade.trade_pending_client_order_id = None;
        state.trade.trade_pending_market = None;
        state.trade.trade_pending_side = None;
        state.trade.trade_pending_requested_in = None;
    }

    /// Normalize pending-order WAL fields before processing a leg.
    ///
    /// Clears persisted pending state when it is:
    /// - tied to a different market/side tuple,
    /// - missing a client order id (legacy partial state),
    /// - carrying an invalid client order id for MEXC.
    fn sanitize_pending_trade_state_for_leg(state: &mut CexState, leg: &TradeLeg) {
        if state.trade.trade_pending_client_order_id.is_some()
            && (state.trade.trade_pending_market.as_deref() != Some(leg.market.as_str())
                || state.trade.trade_pending_side.as_deref() != Some(leg.side.as_str()))
        {
            warn!(
                "[mexc] liq_id={} clearing stale pending order state market={:?} side={:?}",
                state.liq_id, state.trade.trade_pending_market, state.trade.trade_pending_side
            );
            Self::clear_pending_trade_order(state);
            state.trade.trade_pending_buy_mode = None;
        }

        if state.trade.trade_pending_client_order_id.is_none()
            && (state.trade.trade_pending_market.is_some()
                || state.trade.trade_pending_side.is_some()
                || state.trade.trade_pending_requested_in.is_some())
        {
            warn!(
                "[mexc] liq_id={} clearing legacy pending state without client_order_id",
                state.liq_id
            );
            Self::clear_pending_trade_order(state);
            state.trade.trade_pending_buy_mode = None;
        }

        if let Some(existing) = state.trade.trade_pending_client_order_id.as_deref()
            && !Self::is_valid_client_order_id(existing)
        {
            warn!(
                "[mexc] liq_id={} clearing invalid pending client_order_id={}",
                state.liq_id, existing
            );
            Self::clear_pending_trade_order(state);
            state.trade.trade_pending_buy_mode = None;
        }
    }

    fn resolve_slice_buy_mode(
        state: &CexState,
        leg: &TradeLeg,
        forced_next_buy_mode: &mut Option<BuyOrderInputMode>,
    ) -> BuyOrderInputMode {
        if leg.side.eq_ignore_ascii_case("buy") {
            forced_next_buy_mode
                .take()
                .unwrap_or_else(|| Self::parse_buy_mode(state.trade.trade_pending_buy_mode.as_deref()))
        } else {
            BuyOrderInputMode::Auto
        }
    }

    fn next_slice_seq_for_leg(state: &CexState, leg_idx: usize) -> usize {
        state
            .trade
            .trade_slices
            .iter()
            .filter(|slice| slice.leg_index == leg_idx as u32)
            .count()
            + 1
    }

    /// Persist one pending slice request in state and return the resolved values.
    fn prepare_pending_slice_request(
        state: &mut CexState,
        leg: &TradeLeg,
        leg_idx: usize,
        preview_chunk_in: f64,
        buy_mode: BuyOrderInputMode,
    ) -> PendingSliceRequest {
        let slice_seq = Self::next_slice_seq_for_leg(state, leg_idx);
        let requested_in = state.trade.trade_pending_requested_in.unwrap_or(preview_chunk_in);
        let client_order_id = state
            .trade
            .trade_pending_client_order_id
            .clone()
            .unwrap_or_else(|| Self::build_client_order_id(state, leg_idx, slice_seq, buy_mode));
        let buy_mode_label = Self::buy_mode_to_str(buy_mode);

        state.trade.trade_pending_client_order_id = Some(client_order_id.clone());
        state.trade.trade_pending_market = Some(leg.market.clone());
        state.trade.trade_pending_side = Some(leg.side.clone());
        state.trade.trade_pending_requested_in = Some(requested_in);
        state.trade.trade_pending_buy_mode = Some(buy_mode_label.to_string());

        PendingSliceRequest {
            requested_in,
            client_order_id,
            buy_mode_label,
        }
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

    fn build_client_order_id(
        state: &CexState,
        leg_idx: usize,
        slice_seq: usize,
        buy_mode: BuyOrderInputMode,
    ) -> String {
        let raw = format!(
            "liq-{}-l{}-s{}-{}",
            state.liq_id,
            leg_idx + 1,
            slice_seq,
            Self::buy_mode_short_code(buy_mode)
        );
        Self::sanitize_client_order_id(&raw)
    }

    /// Apply adaptive buy fallback after one executed buy slice.
    ///
    /// Purpose:
    /// - Detect meaningful quote-side truncation (requested input > consumed input).
    /// - If residual is still executable, arm exactly one next-slice override to
    ///   `BaseQuantity` so we can consume more of the remaining input.
    ///
    /// Safety guards:
    /// - Runs only for buy legs.
    /// - Honors global enable/disable flag.
    /// - Requires truncation ratio threshold.
    /// - Requires retry budget (`trade_inverse_retry_count`) to remain.
    /// - Requires non-dust residual in USD.
    ///
    /// State effects:
    /// - Increments `trade_inverse_retry_count` when a fallback retry is armed.
    /// - Sets `forced_next_buy_mode` to `BaseQuantity` for one subsequent slice.
    /// - Clears persisted buy-mode hint when no forced retry is armed.
    async fn maybe_arm_adaptive_buy_fallback(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        requested_in: f64,
        actual_input_consumed: f64,
        remaining_in: f64,
        forced_next_buy_mode: &mut Option<BuyOrderInputMode>,
    ) {
        if !leg.side.eq_ignore_ascii_case("buy") {
            return;
        }

        if !self.cex_buy_inverse_enabled {
            state.trade.trade_pending_buy_mode = None;
            return;
        }

        let truncation_ratio = if requested_in > LIQUIDITY_EPS {
            ((requested_in - actual_input_consumed) / requested_in).max(0.0)
        } else {
            0.0
        };

        if truncation_ratio >= self.cex_buy_truncation_trigger_ratio
            && state.trade.trade_inverse_retry_count < self.cex_buy_inverse_max_retries
            && remaining_in > LIQUIDITY_EPS
        {
            let residual_usd = self
                .input_slice_usd(&leg.market, &leg.side, remaining_in)
                .await
                .unwrap_or(0.0);

            if residual_usd >= self.cex_min_exec_usd {
                state.trade.trade_inverse_retry_count += 1;
                *forced_next_buy_mode = Some(BuyOrderInputMode::BaseQuantity);
            }
        }

        if forced_next_buy_mode.is_none() {
            state.trade.trade_pending_buy_mode = None;
        }
    }

    /// Execute one route leg by slicing `amount_in` into impact-bounded chunks.
    ///
    /// Detailed algorithm:
    /// - `preview_trade_slice` proposes the next chunk from live orderbook depth:
    ///   soft target (`target_bps`) first, hard-cap fallback second.
    /// - `maybe_mark_trade_dust` stops execution when chunk/remainder notional is
    ///   below `cex_min_exec_usd` to avoid low-quality micro-fills.
    /// - each accepted chunk is sent as a market order (`execute_swap`).
    /// - realized `exec_price` is derived from actual fills and compared to preview
    ///   midpoint to compute realized slippage in bps.
    /// - if realized slippage exceeds `max_sell_slippage_bps`, the leg fails fast.
    /// - successful slices update weighted route stats and append per-slice telemetry.
    ///
    /// Returns `(remaining_in, total_out)` where:
    /// - `remaining_in` is unexecuted residual (typically `0` or dust),
    /// - `total_out` is total output produced by all executed slices in this leg.
    pub(super) async fn execute_trade_leg_slices(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        leg_idx: usize,
        total_legs: usize,
        amount_in: f64,
        target_bps: f64,
    ) -> AppResult<(f64, f64)> {
        // Resume from persisted per-leg progress if available.
        let mut remaining_in = state.trade.trade_progress_remaining_in.unwrap_or(amount_in);
        let mut total_out = state.trade.trade_progress_total_out.unwrap_or(0.0);
        // Hard guard against pathological loops when liquidity math cannot converge.
        let mut slice_round_count = 0usize;

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
                    state.last_error = Some(err.to_string());
                    return Err(err);
                }
            };

            // Skip very small residuals below min execution notional to avoid bad fills / fees.
            if self
                .maybe_mark_trade_dust(state, leg, preview.chunk_in, remaining_in)
                .await?
            {
                state.trade.trade_unexecutable_residual_in = Some(remaining_in);
                Self::persist_trade_progress(state, remaining_in, total_out);
                break;
            }

            let buy_mode = Self::resolve_slice_buy_mode(state, leg, &mut forced_next_buy_mode);
            let pending = Self::prepare_pending_slice_request(state, leg, leg_idx, preview.chunk_in, buy_mode);

            // Execute exactly this one slice as a market order.
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
                    },
                )
                .await?;

            Self::clear_pending_trade_order(state);
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

            // Convert raw fill amounts into a realized execution price.
            // We need `exec_price` so slippage can be measured against preview mid-price.
            let exec_price =
                match Self::exec_price_from_fill(&leg.market, &leg.side, actual_input_consumed, actual_output_received)
                {
                    Ok(price) => price,
                    Err(err) => {
                        state.last_error = Some(err.to_string());
                        return Err(err);
                    }
                };

            // Slippage is the realized price drift from preview midpoint, in bps.
            // This is the primary safety metric for adverse execution quality.
            let slice_slippage_bps = Self::slice_slippage_bps(&leg.side, preview.preview_mid_price, exec_price);

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

            // Update weighted route-level aggregates used by finish/export summaries.
            Self::update_trade_notional_stats(
                state,
                &leg.side,
                actual_input_consumed,
                actual_output_received,
                preview.preview_mid_price,
            );

            // Persist per-slice telemetry for observability and post-trade analysis.
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
        Ok((remaining_in, total_out))
    }

    /// Build a one-slice preview under the configured impact target.
    pub(super) async fn preview_trade_slice(
        &self,
        leg: &TradeLeg,
        remaining_in: f64,
        target_bps: f64,
    ) -> AppResult<SlicePreview> {
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
    ) -> AppResult<SlicePreview> {
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
    ) -> AppResult<SlicePreview> {
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
    ) -> AppResult<f64>
    where
        F: FnMut(f64) -> AppResult<(f64, f64)>,
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
        )
        .into())
    }

    /// Build final preview object and assert visible depth can fully fill `chunk`.
    fn build_slice_preview<F>(
        &self,
        market: &str,
        liquidity_side: &str,
        chunk: f64,
        mut simulate_fill: F,
    ) -> AppResult<SlicePreview>
    where
        F: FnMut(f64) -> AppResult<(f64, f64, f64, f64)>,
    {
        let (_out, avg_price, impact, residual) = simulate_fill(chunk)?;
        if residual > LIQUIDITY_EPS {
            return Err(format!(
                "not enough {} liquidity for {} (needed {}, missing {})",
                liquidity_side, market, chunk, residual
            )
            .into());
        }

        Ok(SlicePreview {
            chunk_in: chunk,
            preview_mid_price: avg_price,
            preview_impact_bps: impact,
        })
    }

    /// Skip tiny residual chunks that are below configured minimum execution notional.
    pub(super) async fn maybe_mark_trade_dust(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        chunk_in: f64,
        remaining_in: f64,
    ) -> AppResult<bool> {
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

    /// Convert fill amounts into execution price according to side semantics.
    pub(super) fn exec_price_from_fill(market: &str, side: &str, chunk_in: f64, filled_out: f64) -> AppResult<f64> {
        let exec_price = if side.eq_ignore_ascii_case("sell") {
            if chunk_in > 0.0 { filled_out / chunk_in } else { 0.0 }
        } else if filled_out > 0.0 {
            chunk_in / filled_out
        } else {
            0.0
        };

        if exec_price <= 0.0 || !exec_price.is_finite() {
            return Err(format!("invalid execution price for {} {}", market, side).into());
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

    /// Update route-level weighted slippage totals after one executed slice.
    pub(super) fn update_trade_notional_stats(
        state: &mut CexState,
        side: &str,
        chunk_in: f64,
        filled_out: f64,
        preview_mid_price: f64,
    ) {
        let mid_notional = if side.eq_ignore_ascii_case("sell") {
            chunk_in * preview_mid_price
        } else {
            filled_out * preview_mid_price
        };
        let exec_notional = if side.eq_ignore_ascii_case("sell") {
            filled_out
        } else {
            chunk_in
        };

        let mid_sum = state.trade.trade_mid_notional_sum.unwrap_or(0.0) + mid_notional;
        let exec_sum = state.trade.trade_exec_notional_sum.unwrap_or(0.0) + exec_notional;
        state.trade.trade_mid_notional_sum = Some(mid_sum);
        state.trade.trade_exec_notional_sum = Some(exec_sum);
        state.trade.trade_weighted_slippage_bps = if mid_sum > LIQUIDITY_EPS {
            Some(((mid_sum - exec_sum) / mid_sum * BPS_PER_RATIO_UNIT).max(0.0))
        } else {
            None
        };
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

        state.step = CexStep::Withdraw;
        state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
            state.withdraw.withdraw_asset.clone(),
            total_out.max(0.0),
        ));
    }

    // MEXC approvals are flaky; we send a burst of tiny approvals to increase the chance
    // the exchange notices the approval without blocking the main liquidation flow.
    pub(super) async fn maybe_bump_mexc_approval(&self, liq_id: &str, asset: &ChainToken) -> u32 {
        let already_bumped = {
            let Ok(approve_bumps) = self.approve_bumps.lock() else {
                warn!("[mexc] liq_id={} approve_bumps mutex poisoned, skipping bump", liq_id);
                return 0;
            };
            *approve_bumps.get(liq_id).unwrap_or(&0)
        };

        if already_bumped >= APPROVE_BUMP_MAX_COUNT {
            debug!(
                "[mexc] liq_id={} deposit transfer: approve bump already done (count={})",
                liq_id, already_bumped
            );
            return 0;
        }

        let approve_amount = Nat::from(1u8);
        let spender = ChainAccount::Icp(Account {
            owner: self.liquidator_principal,
            subaccount: None,
        });
        info!(
            "[mexc] liq_id={} deposit transfer: approve bump {}+{} amount={} asset={}",
            liq_id, APPROVE_BUMP_BATCH_SIZE, APPROVE_BUMP_BATCH_SIZE, approve_amount, asset
        );

        let mut ok = true;
        let mut approved_count: u32 = 0;
        let short_pause = Duration::from_millis(APPROVE_BUMP_DELAY_MS);
        for i in 0..APPROVE_BUMP_BATCH_SIZE {
            if let Err(err) = self
                .transfer_service
                .approve(asset, &spender, approve_amount.clone())
                .await
            {
                ok = false;
                info!("[mexc] liq_id={} approve bump failed: {}", liq_id, err);
                break;
            }
            approved_count += 1;
            if i < APPROVE_BUMP_BATCH_SIZE - 1 {
                sleep(short_pause).await;
            }
        }
        if ok {
            sleep(Duration::from_secs(APPROVE_BUMP_BATCH_DELAY_SECS)).await;
            for i in 0..APPROVE_BUMP_BATCH_SIZE {
                if let Err(err) = self
                    .transfer_service
                    .approve(asset, &spender, approve_amount.clone())
                    .await
                {
                    ok = false;
                    info!("[mexc] liq_id={} approve bump failed: {}", liq_id, err);
                    break;
                }
                approved_count += 1;
                if i < APPROVE_BUMP_BATCH_SIZE - 1 {
                    sleep(short_pause).await;
                }
            }
        }

        if ok && let Ok(mut approve_bumps) = self.approve_bumps.lock() {
            approve_bumps.insert(liq_id.to_string(), APPROVE_BUMP_MAX_COUNT);
        }
        approved_count
    }
}
