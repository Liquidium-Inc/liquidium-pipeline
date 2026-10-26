//! Trade route discovery: direct markets, hop search over the configured
//! pairs, saved routes, and the USD legs used to value a slice.
use super::*;
use crate::finalizers::bridge_planner::BridgePlanner;
use crate::finalizers::cex_finalizer::CexRouteLeg;
use crate::finalizers::mexc::mexc_utils::parse_market_symbols;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    fn normalize_market_pair(raw: &str) -> Option<String> {
        let normalized = raw.trim().replace(['/', '-'], "_").to_ascii_uppercase();
        if normalized.is_empty() {
            return None;
        }

        let mut parts = normalized.split('_').filter(|part| !part.is_empty());
        let base = parts.next()?;
        let quote = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        Some(format!("{}_{}", base, quote))
    }

    pub(super) fn normalize_market_pairs(markets: Vec<String>) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for market in markets {
            let Some(normalized) = Self::normalize_market_pair(&market) else {
                continue;
            };
            if seen.insert(normalized.clone()) {
                out.push(normalized);
            }
        }
        out
    }

    /// Resolve a single direct market leg for `deposit_symbol -> withdraw_symbol`.
    ///
    /// Probe order is intentional:
    /// 1) Prefer `DEPOSIT_WITHDRAW` on `sell` if there are bids.
    /// 2) Fallback to `WITHDRAW_DEPOSIT` on `buy` if there are asks.
    ///
    /// When neither side is tradable, returns a combined error with both probe outcomes.
    async fn resolve_direct_leg(&self, deposit_symbol: &str, withdraw_symbol: &str) -> Result<TradeLeg, String> {
        let deposit = deposit_symbol.to_ascii_uppercase();
        let withdraw = withdraw_symbol.to_ascii_uppercase();

        let sell_market = format!("{}_{}", deposit, withdraw);
        let buy_market = format!("{}_{}", withdraw, deposit);
        let mut errors: Vec<String> = Vec::new();

        // Preferred direct path: sell deposit asset into withdraw asset.
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

        // Fallback direct path: buy withdraw asset with deposit asset on the inverse market.
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
        ))
    }

    fn route_signature(legs: &[TradeLeg]) -> String {
        legs.iter()
            .map(|leg| format!("{}:{}", leg.market, leg.side))
            .collect::<Vec<_>>()
            .join(">")
    }

    fn normalize_symbol(symbol: &str) -> String {
        symbol.trim().to_ascii_uppercase()
    }

    /// Load previously resolved route legs from `state.trade.trade_resolved_legs`.
    ///
    /// This is the persistence boundary for route resolution:
    /// - source of truth in-state: `CexTradeState.trade_resolved_legs`
    /// - lifecycle: serialized inside `CexState` and checkpointed by the generic CEX
    ///   finalizer WAL write after each successful step.
    ///
    /// We normalize/validate every persisted leg before use so retries stay deterministic
    /// and malformed legacy state fails fast with a clear error.
    fn restore_persisted_trade_legs(state: &CexState) -> Result<Option<Vec<TradeLeg>>, String> {
        if state.trade.trade_resolved_legs.is_empty() {
            return Ok(None);
        }

        let mut out = Vec::with_capacity(state.trade.trade_resolved_legs.len());
        for (idx, leg) in state.trade.trade_resolved_legs.iter().enumerate() {
            let market = leg.market.trim().to_ascii_uppercase();
            if parse_market_symbols(&market).is_none() {
                return Err(format!(
                    "invalid persisted route leg {} market '{}'",
                    idx + 1,
                    leg.market
                ));
            }

            let side = leg.side.trim().to_ascii_lowercase();
            if side != "buy" && side != "sell" {
                return Err(format!("invalid persisted route leg {} side '{}'", idx + 1, leg.side));
            }

            out.push(TradeLeg { market, side });
        }

        Ok(Some(out))
    }

    /// Persist a resolved route into `state.trade.trade_resolved_legs`.
    ///
    /// After this assignment, the surrounding finalizer loop persists `CexState` into WAL.
    /// That makes subsequent retries/resumes reuse the same leg sequence instead of re-resolving.
    fn persist_trade_legs(state: &mut CexState, legs: &[TradeLeg]) {
        // Persist only minimal deterministic leg identity (market + side).
        state.trade.trade_resolved_legs = legs
            .iter()
            .map(|leg| CexRouteLeg {
                market: leg.market.clone(),
                side: leg.side.clone(),
            })
            .collect();
    }

    fn route_neighbors_from_pairs(&self, from_symbol: &str) -> Vec<(String, TradeLeg)> {
        let mut out = Vec::new();
        for market in &self.cex_mexc_available_pairs {
            let Some((base, quote)) = parse_market_symbols(market) else {
                continue;
            };

            if (base == "SOL" || quote == "SOL") && !(base == "SOL" && quote == "USDT") {
                continue;
            }

            if base == from_symbol {
                out.push((
                    quote.clone(),
                    TradeLeg {
                        market: market.clone(),
                        side: "sell".to_string(),
                    },
                ));
            }
            if quote == from_symbol {
                out.push((
                    base.clone(),
                    TradeLeg {
                        market: market.clone(),
                        side: "buy".to_string(),
                    },
                ));
            }
        }

        out.sort_by(|left, right| {
            (left.0.as_str(), left.1.market.as_str(), left.1.side.as_str()).cmp(&(
                right.0.as_str(),
                right.1.market.as_str(),
                right.1.side.as_str(),
            ))
        });
        out
    }

    /// Discover multi-leg route candidates from `from_symbol` to `to_symbol` using BFS.
    ///
    /// Semantics:
    /// - `max_hops` is the number of intermediate symbols allowed.
    /// - max leg count is `max_hops + 1`.
    /// - only multi-leg paths are returned (at least 2 legs).
    ///
    /// Safety/determinism:
    /// - each search branch tracks `visited` symbols to avoid cycles.
    /// - neighbor expansion order is deterministic (sorted in `route_neighbors_from_pairs`).
    ///
    /// Note: this function only discovers graph paths; liquidity/executability checks are
    /// performed later by `ensure_route_liquidity`.
    fn discover_hop_candidates(&self, from_symbol: &str, to_symbol: &str, max_hops: usize) -> Vec<Vec<TradeLeg>> {
        if max_hops == 0 {
            return vec![];
        }
        let max_legs = max_hops.saturating_add(1);
        if max_legs < 2 {
            return vec![];
        }

        struct SearchState {
            symbol: String,
            legs: Vec<TradeLeg>,
            visited: std::collections::HashSet<String>,
        }

        let mut queue = std::collections::VecDeque::new();
        let mut start_visited = std::collections::HashSet::new();
        start_visited.insert(from_symbol.to_string());
        queue.push_back(SearchState {
            symbol: from_symbol.to_string(),
            legs: vec![],
            visited: start_visited,
        });

        let mut candidates = Vec::new();

        while let Some(state) = queue.pop_front() {
            if state.legs.len() >= max_legs {
                continue;
            }

            for (next_symbol, next_leg) in self.route_neighbors_from_pairs(&state.symbol) {
                if state.visited.contains(&next_symbol) {
                    continue;
                }

                let mut next_legs = state.legs.clone();
                next_legs.push(next_leg);

                if next_symbol == to_symbol {
                    if next_legs.len() >= 2 {
                        candidates.push(next_legs);
                    }
                    continue;
                }

                if next_legs.len() < max_legs {
                    let mut next_visited = state.visited.clone();
                    next_visited.insert(next_symbol.clone());
                    queue.push_back(SearchState {
                        symbol: next_symbol,
                        legs: next_legs,
                        visited: next_visited,
                    });
                }
            }
        }

        candidates
    }

    /// Verify that one leg is currently executable on visible orderbook depth.
    ///
    /// Side semantics:
    /// - `sell` requires at least one bid level on `leg.market`
    /// - `buy` requires at least one ask level on `leg.market`
    ///
    /// This is a lightweight gate (presence check), not a full-size fill simulation.
    async fn ensure_leg_liquidity(&self, leg: &TradeLeg) -> Result<(), String> {
        let orderbook = self
            .backend
            .get_orderbook(&leg.market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await
            .map_err(|err| format!("{}: {}", leg.market, err))?;

        if leg.side.eq_ignore_ascii_case("sell") {
            if orderbook.bids.is_empty() {
                return Err(format!("{} has no bids", leg.market));
            }
            return Ok(());
        }

        if orderbook.asks.is_empty() {
            return Err(format!("{} has no asks", leg.market));
        }
        Ok(())
    }

    /// Verify that every leg in a candidate route is currently tradable.
    ///
    /// Returns early on the first failing leg and bubbles that error so callers can
    /// evaluate the next route candidate.
    async fn ensure_route_liquidity(&self, legs: &[TradeLeg]) -> Result<(), String> {
        for leg in legs {
            self.ensure_leg_liquidity(leg).await?;
        }
        Ok(())
    }

    /// Resolve a multi-leg hop route from `deposit_symbol` to `withdraw_symbol`.
    ///
    /// Flow:
    /// 1) Require hop config (`cex_mexc_available_pairs`) and enabled hop depth.
    /// 2) Discover candidate paths up to `cex_mexc_max_hops`.
    /// 3) Sort candidates by shortest route first, then deterministic signature.
    /// 4) Return the first route whose all legs pass `ensure_route_liquidity`.
    ///
    /// If no candidate is executable, returns an aggregated error that includes each
    /// rejected route signature and failure reason.
    async fn resolve_hop_route(&self, deposit_symbol: &str, withdraw_symbol: &str) -> Result<Vec<TradeLeg>, String> {
        if self.cex_mexc_available_pairs.is_empty() {
            return Err(format!(
                "no configured MEXC pairs for hop discovery ({} -> {})",
                deposit_symbol, withdraw_symbol
            ));
        }
        if self.cex_mexc_max_hops == 0 {
            return Err(format!(
                "hop discovery disabled by CEX_MEXC_MAX_HOPS=0 ({} -> {})",
                deposit_symbol, withdraw_symbol
            ));
        }

        let mut candidates = self.discover_hop_candidates(deposit_symbol, withdraw_symbol, self.cex_mexc_max_hops);
        if candidates.is_empty() {
            return Err(format!(
                "no configured hop route for {} -> {} within {} hops",
                deposit_symbol, withdraw_symbol, self.cex_mexc_max_hops
            ));
        }

        candidates.sort_by(|left, right| {
            left.len()
                .cmp(&right.len())
                .then_with(|| Self::route_signature(left).cmp(&Self::route_signature(right)))
        });

        let mut errors = Vec::new();
        for candidate in candidates {
            match self.ensure_route_liquidity(&candidate).await {
                Ok(_) => return Ok(candidate),
                Err(err) => {
                    errors.push(format!("{} ({})", Self::route_signature(&candidate), err));
                }
            }
        }

        Err(format!(
            "no executable hop route for {} -> {} ({})",
            deposit_symbol,
            withdraw_symbol,
            errors.join(" | ")
        ))
    }

    pub(crate) async fn resolve_trade_legs_for_symbols(
        &self,
        deposit_symbol: &str,
        withdraw_symbol: &str,
    ) -> Result<Vec<TradeLeg>, String> {
        let deposit = Self::normalize_symbol(deposit_symbol);
        let withdraw = Self::normalize_symbol(withdraw_symbol);

        if deposit == withdraw {
            return Ok(vec![]);
        }

        if deposit == "SOL" {
            let mut legs = vec![TradeLeg {
                market: "SOL_USDT".into(),
                side: "sell".into(),
            }];
            legs.extend(self.resolve_standard_trade_legs("USDT", &withdraw).await?);
            return Ok(legs);
        }
        if withdraw == "SOL" {
            let mut legs = self.resolve_standard_trade_legs(&deposit, "USDT").await?;
            legs.push(TradeLeg {
                market: "SOL_USDT".into(),
                side: "buy".into(),
            });
            return Ok(legs);
        }
        self.resolve_standard_trade_legs(&deposit, &withdraw).await
    }

    async fn resolve_standard_trade_legs(&self, deposit: &str, withdraw: &str) -> Result<Vec<TradeLeg>, String> {
        if deposit == withdraw {
            return Ok(vec![]);
        }

        // 1) High-priority compatibility override for legacy ck routes.
        if let Some(legs) = mexc_special_trade_legs(deposit, withdraw) {
            return Ok(legs);
        }

        // 2) Probe direct tradable market first.
        let direct_err = match self.resolve_direct_leg(deposit, withdraw).await {
            Ok(direct_leg) => return Ok(vec![direct_leg]),
            Err(err) => err,
        };

        // 3) Fallback to configured graph hops.
        self.resolve_hop_route(deposit, withdraw)
            .await
            .map_err(|hop_err| format!("{}; {}", direct_err, hop_err))
    }

    /// Resolve route legs once, then always reuse persisted legs on subsequent calls.
    ///
    /// Persistence location:
    /// - write: `state.trade.trade_resolved_legs` via `persist_trade_legs`
    /// - read: `restore_persisted_trade_legs`
    ///
    /// This keeps trade retries deterministic even if market availability changes mid-flow.
    pub(super) async fn resolve_or_load_trade_legs(&self, state: &mut CexState) -> Result<Vec<TradeLeg>, String> {
        if let Some(persisted) = Self::restore_persisted_trade_legs(state)? {
            return Ok(persisted);
        }

        let legs = self
            .resolve_trade_legs_for_symbols(
                &<Self as BridgePlanner>::planned_deposit_asset(state),
                &<Self as BridgePlanner>::planned_withdraw_asset(state),
            )
            .await?;
        Self::persist_trade_legs(state, &legs);
        Ok(legs)
    }

    async fn preview_through_legs(&self, legs: &[TradeLeg], mut amount_in: f64) -> Result<f64, String> {
        for leg in legs {
            let (out, _avg, _impact) = self.preview_leg(&leg.market, &leg.side, amount_in).await?;
            amount_in = out;
        }
        Ok(amount_in)
    }

    async fn resolve_symbol_to_usd_legs(&self, symbol: &str) -> Result<Vec<TradeLeg>, String> {
        const USD_STABLE_TARGETS: [&str; 5] = ["USDT", "USDC", "CKUSDT", "CKUSDC", "USD"];

        // Prefer direct conversion first.
        let mut direct_errors = Vec::new();
        for stable in USD_STABLE_TARGETS {
            if stable.eq_ignore_ascii_case(symbol) {
                continue;
            }
            match self.resolve_direct_leg(symbol, stable).await {
                Ok(leg) => return Ok(vec![leg]),
                Err(err) => direct_errors.push(format!("{} ({})", stable, err)),
            }
        }

        if self.cex_mexc_available_pairs.is_empty() {
            return Err(format!(
                "cannot convert {} to USD: no configured hop pairs and no direct stable market ({})",
                symbol,
                direct_errors.join(" | ")
            ));
        }
        if self.cex_mexc_max_hops == 0 {
            return Err(format!(
                "cannot convert {} to USD: hop discovery disabled and no direct stable market ({})",
                symbol,
                direct_errors.join(" | ")
            ));
        }

        let mut candidates = Vec::new();
        let mut stable_targets = std::collections::HashSet::new();
        for stable in USD_STABLE_TARGETS {
            if !stable.eq_ignore_ascii_case(symbol) {
                stable_targets.insert(stable.to_string());
            }
        }
        for stable in &stable_targets {
            candidates.extend(self.discover_hop_candidates(symbol, stable, self.cex_mexc_max_hops));
        }

        if candidates.is_empty() {
            return Err(format!(
                "cannot convert {} to USD: no configured path to stable symbols within {} hops",
                symbol, self.cex_mexc_max_hops
            ));
        }

        candidates.sort_by(|left, right| {
            left.len()
                .cmp(&right.len())
                .then_with(|| Self::route_signature(left).cmp(&Self::route_signature(right)))
        });

        let mut errors = Vec::new();
        for candidate in candidates {
            match self.ensure_route_liquidity(&candidate).await {
                Ok(_) => return Ok(candidate),
                Err(err) => errors.push(format!("{} ({})", Self::route_signature(&candidate), err)),
            }
        }

        Err(format!(
            "cannot convert {} to USD: no executable stable path ({})",
            symbol,
            errors.join(" | ")
        ))
    }

    /// Convert an amount in `symbol` units to USD for min-notional checks.
    async fn amount_symbol_to_usd(&self, symbol: &str, amount: f64) -> Result<f64, String> {
        if amount <= 0.0 {
            return Ok(0.0);
        }
        let symbol = symbol.trim().to_ascii_uppercase();
        if is_usd_stable_symbol(&symbol) {
            return Ok(amount);
        }

        let legs = self.resolve_symbol_to_usd_legs(&symbol).await?;
        self.preview_through_legs(&legs, amount).await
    }

    /// Estimate USD notional of one input slice for a given market/side.
    pub(super) async fn input_slice_usd(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        let (_base, quote) =
            parse_market_symbols(market).ok_or_else(|| format!("invalid market format '{}'", market))?;

        if side.eq_ignore_ascii_case("sell") {
            let orderbook = self
                .backend
                .get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT))
                .await?;
            let best_bid = orderbook.bids.first().map(|l| l.price).unwrap_or(0.0);
            if best_bid <= 0.0 {
                return Err(format!("no bids available for market {}", market));
            }
            let quote_amount = amount_in * best_bid;
            return self.amount_symbol_to_usd(&quote, quote_amount).await;
        }

        // buy side amount_in is quote notional by backend contract
        self.amount_symbol_to_usd(&quote, amount_in).await
    }
}
