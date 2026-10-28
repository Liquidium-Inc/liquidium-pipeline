//! The order behind one slice: client order ids, the pending request saved
//! before the order, and the buy input mode.
use super::*;

pub(super) struct PendingSliceRequest {
    pub(super) requested_in: f64,
    pub(super) client_order_id: String,
    pub(super) buy_mode_label: &'static str,
}

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
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

    fn is_valid_client_order_id(&self, value: &str) -> bool {
        let len = value.len();
        if !(1..=self.profile.client_order_id_max_len()).contains(&len) {
            return false;
        }

        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }

    fn stable_hash_6hex(value: &str) -> String {
        format!("{:06x}", super::super::utils::fnv1a_64(value) & 0x00ff_ffff)
    }

    fn sanitize_client_order_id(&self, value: &str) -> String {
        let mut sanitized: String = value
            .bytes()
            .filter(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
            .map(char::from)
            .collect();

        if sanitized.is_empty() {
            sanitized.push_str("liq");
        }

        let max_len = self.profile.client_order_id_max_len();
        if sanitized.len() > max_len {
            // A cap under 32 keeps the whole 64-bit hash (18 characters), so ids stay distinct.
            if max_len < 32 {
                return format!("lq{:016x}", super::super::utils::fnv1a_64(&sanitized));
            }
            let hash = Self::stable_hash_6hex(&sanitized);
            sanitized.truncate(max_len - 7);
            sanitized.push('-');
            sanitized.push_str(&hash);
        }

        sanitized
    }

    pub(super) fn clear_pending_trade_order(state: &mut CexState) {
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
    /// - carrying an invalid client order id for the exchange.
    pub(super) fn sanitize_pending_trade_state_for_leg(&self, state: &mut CexState, leg: &TradeLeg) {
        if state.trade.trade_pending_client_order_id.is_some()
            && (state.trade.trade_pending_market.as_deref() != Some(leg.market.as_str())
                || state.trade.trade_pending_side.as_deref() != Some(leg.side.as_str()))
        {
            warn!(
                "[cex] liq_id={} clearing stale pending order state market={:?} side={:?}",
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
                "[cex] liq_id={} clearing legacy pending state without client_order_id",
                state.liq_id
            );
            Self::clear_pending_trade_order(state);
            state.trade.trade_pending_buy_mode = None;
        }

        if let Some(existing) = state.trade.trade_pending_client_order_id.as_deref()
            && !self.is_valid_client_order_id(existing)
        {
            warn!(
                "[cex] liq_id={} clearing invalid pending client_order_id={}",
                state.liq_id, existing
            );
            Self::clear_pending_trade_order(state);
            state.trade.trade_pending_buy_mode = None;
        }
    }

    pub(super) fn resolve_slice_buy_mode(
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
    pub(super) fn prepare_pending_slice_request(
        &self,
        state: &mut CexState,
        leg: &TradeLeg,
        leg_idx: usize,
        preview_chunk_in: f64,
        buy_mode: BuyOrderInputMode,
    ) -> PendingSliceRequest {
        let applied = &state.trade.trade_applied_client_order_ids;
        // A pending id is a resumed submission whose fill is not counted yet.
        // Once counted, the id is spent, so a new order needs a new id.
        let pending_id = state
            .trade
            .trade_pending_client_order_id
            .clone()
            .filter(|id| !applied.contains(id));
        if pending_id.is_none() && state.trade.trade_pending_client_order_id.is_some() {
            state.trade.trade_pending_requested_in = None;
        }
        let requested_in = state.trade.trade_pending_requested_in.unwrap_or(preview_chunk_in);
        let client_order_id = pending_id.unwrap_or_else(|| {
            let mut slice_seq = Self::next_slice_seq_for_leg(state, leg_idx);
            loop {
                let id = self.build_client_order_id(state, leg_idx, slice_seq, buy_mode);
                if !state.trade.trade_applied_client_order_ids.contains(&id) {
                    break id;
                }
                slice_seq += 1;
            }
        });
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

    fn build_client_order_id(
        &self,
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
        self.sanitize_client_order_id(&raw)
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
    pub(super) async fn maybe_arm_adaptive_buy_fallback(
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
}
