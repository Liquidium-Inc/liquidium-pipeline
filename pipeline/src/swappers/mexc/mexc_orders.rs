//! One MEXC market order, end to end: sizing the amount to the symbol's
//! limits, submitting it under the leg's client order id, and reading back
//! what actually filled.
//!
//! MEXC accepts a buy either as a quote budget or as a base quantity; the
//! walks over the asks that size and reprice a base-quantity buy live here
//! together so they cannot drift apart.

use super::mexc_symbols::{SymbolFilters, truncate_to_step};
use super::*;

/// Default orderbook depth level used for quote-cost and preview estimations.
const DEFAULT_ORDERBOOK_DEPTH_LIMIT: u32 = 50;

impl MexcClient {
    fn apply_output_fee(amount: Decimal, fee_bps: f64) -> Decimal {
        if amount <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let fee_ratio = Decimal::from_f64_retain(fee_bps / BPS_PER_RATIO_UNIT)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO)
            .min(Decimal::ONE);

        amount * (Decimal::ONE - fee_ratio)
    }

    async fn fetch_ask_levels(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        api_symbol: &str,
    ) -> Result<Vec<(Decimal, Decimal)>, String> {
        let orderbook_depth = ex
            .depth(DepthParams {
                limit: Some(DEFAULT_ORDERBOOK_DEPTH_LIMIT),
                symbol: api_symbol,
            })
            .await
            .map_err(|e| mexc_error_message(&e))?;

        if orderbook_depth.asks.is_empty() {
            return Err("no asks".into());
        }

        Ok(orderbook_depth
            .asks
            .into_iter()
            .map(|level| (level.price, level.quantity))
            .collect())
    }

    fn estimate_buy_quantity_from_levels(
        ask_levels: &[(Decimal, Decimal)],
        quote_amount: Decimal,
    ) -> Result<Decimal, String> {
        if ask_levels.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining_quote = quote_amount;
        let mut base_out = Decimal::ZERO;
        for (price, quantity) in ask_levels {
            if remaining_quote <= Decimal::ZERO {
                break;
            }
            if *price <= Decimal::ZERO || *quantity <= Decimal::ZERO {
                continue;
            }
            let max_base = remaining_quote / *price;
            let take = (*quantity).min(max_base);
            base_out += take;
            remaining_quote -= take * *price;
        }

        if remaining_quote > Decimal::ZERO {
            return Err("not enough ask liquidity".into());
        }

        Ok(base_out)
    }

    fn estimate_buy_quote_cost_from_levels(
        ask_levels: &[(Decimal, Decimal)],
        base_quantity: Decimal,
    ) -> Result<Decimal, String> {
        if ask_levels.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining_base = base_quantity;
        let mut quote_cost = Decimal::ZERO;
        for (price, quantity) in ask_levels {
            if remaining_base <= Decimal::ZERO {
                break;
            }
            if *price <= Decimal::ZERO || *quantity <= Decimal::ZERO {
                continue;
            }
            let take = (*quantity).min(remaining_base);
            quote_cost += take * *price;
            remaining_base -= take;
        }

        if remaining_base > Decimal::ZERO {
            return Err("not enough ask liquidity".into());
        }

        Ok(quote_cost)
    }

    // Round quote amount to exchange precision and signal whether quote-based buys are viable.
    fn adjust_quote_amount(amount_dec: Decimal, filters: Option<&SymbolFilters>) -> (Decimal, bool) {
        let mut quote_amt = amount_dec;
        let mut use_quote_order = true;

        if let Some(f) = filters {
            if let Some(precision) = f.quote_precision {
                quote_amt = quote_amt
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }
            if quote_amt <= Decimal::ZERO {
                use_quote_order = false;
            }
        } else if quote_amt <= Decimal::ZERO {
            use_quote_order = false;
        }

        (quote_amt, use_quote_order)
    }

    // Enforce notional minimums against the intended spend amount.
    fn ensure_min_notional(filters: Option<&SymbolFilters>, amount: Decimal, symbol: &str) -> Result<(), String> {
        if let Some(f) = filters
            && let Some(min_notional) = f.min_notional
            && amount < min_notional
        {
            return Err(format!(
                "quote amount {} below min_notional {} for {}",
                amount, min_notional, symbol
            ));
        }
        Ok(())
    }

    // Estimate base output from orderbook for a quote-denominated buy.
    async fn estimate_buy_quantity(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        api_symbol: &str,
        quote_amount: Decimal,
    ) -> Result<Decimal, String> {
        let ask_levels = self.fetch_ask_levels(ex, api_symbol).await?;
        Self::estimate_buy_quantity_from_levels(&ask_levels, quote_amount)
    }

    // Apply step size/base precision and min_qty checks to a computed base amount.
    fn adjust_buy_quantity(qty: Decimal, filters: Option<&SymbolFilters>, symbol: &str) -> Result<Decimal, String> {
        let mut adjusted = qty;
        if let Some(f) = filters {
            if let Some(step) = f.step_size {
                let step_scale = step.scale();
                adjusted = truncate_to_step(adjusted, step)
                    .round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
                    .normalize();
            } else if let Some(precision) = f.base_precision {
                adjusted = adjusted
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }
            if let Some(min_qty) = f.min_qty
                && adjusted < min_qty
            {
                return Err(format!(
                    "quantity {} below min_qty {} for {}",
                    adjusted, min_qty, symbol
                ));
            }
        }

        if adjusted <= Decimal::ZERO {
            return Err(format!("quantity {} not valid for {}", adjusted, symbol));
        }

        Ok(adjusted)
    }

    pub(super) fn candidate_symbols(api_symbol: &str, market_symbol: &str, symbol: &str) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        for raw in [api_symbol, market_symbol, symbol] {
            let normalized = normalize_market_symbol(raw);
            if !normalized.is_empty() && !candidates.iter().any(|candidate| candidate == &normalized) {
                candidates.push(normalized);
            }
        }
        candidates
    }

    pub(super) fn prepare_sell_order(
        amount_dec: Decimal,
        filters: Option<&SymbolFilters>,
        symbol: &str,
    ) -> Result<(OrderSide, Option<Decimal>, Option<Decimal>), String> {
        let mut qty = amount_dec;
        if let Some(f) = filters {
            if let Some(step) = f.step_size {
                let step_scale = step.scale();
                let adjusted = truncate_to_step(qty, step)
                    .round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
                    .normalize();

                if adjusted != qty {
                    debug!(
                        "[mexc] adjust sell qty {} -> {} using step_size={}",
                        qty, adjusted, step
                    );
                }
                qty = adjusted;
            } else if let Some(precision) = f.base_precision {
                qty = qty
                    .round_dp_with_strategy(precision, RoundingStrategy::ToZero)
                    .normalize();
            }

            if let Some(min_qty) = f.min_qty
                && qty < min_qty
            {
                return Err(format!("quantity {} below min_qty {} for {}", qty, min_qty, symbol));
            }
        }

        if qty <= Decimal::ZERO {
            return Err(format!("quantity {} not valid for {}", qty, symbol));
        }

        Ok((OrderSide::Sell, Some(qty), None))
    }

    pub(super) async fn submit_market_order(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        candidates: &[String],
        order_side: OrderSide,
        quantity: Option<Decimal>,
        quote_order_quantity: Option<Decimal>,
        client_order_id: Option<&str>,
        market: &str,
        side: &str,
        _amount_in: f64,
    ) -> Result<(String, String), CexSubmissionError> {
        let mut last_err: Option<String> = None;
        for candidate in candidates {
            match ex
                .order(OrderParams {
                    symbol: candidate,
                    side: order_side,
                    order_type: v3::enums::OrderType::Market,
                    quantity,
                    new_client_order_id: client_order_id,
                    price: None,
                    quote_order_quantity,
                })
                .await
            {
                Ok(ok) => {
                    let order_id = ok.order_id.trim().to_string();
                    if order_id.is_empty() {
                        let details = format!(
                            "empty order_id returned for symbol={} market={} side={}",
                            candidate, market, side
                        );
                        warn!("[mexc] {}", details);
                        return Err(details.into());
                    }
                    return Ok((candidate.clone(), order_id));
                }
                Err(e) => {
                    let details = format_mexc_api_error(&e);
                    warn!("[mexc] order error response: {}", details);
                    if is_bad_symbol(&e) {
                        last_err = Some(format!("Swap err: {}", details));
                        continue;
                    }
                    if is_pending_settlement(&e) {
                        return Err(CexSubmissionError::PendingSettlement(format!("Swap err: {details}")));
                    }
                    return Err(format!("Swap err: {details}").into());
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "Swap err: bad symbol".to_string()).into())
    }

    /// Converts MEXC order fill fields into side-agnostic execution amounts.
    ///
    /// - `buy`: input is quote spent (`cummulative_quote_quantity`), output is base received (`executed_quantity`)
    /// - `sell`: input is base sold (`executed_quantity`), output is quote received (`cummulative_quote_quantity`)
    ///
    fn map_fill_report(
        side_norm: &str,
        executed_quantity: Decimal,
        cummulative_quote_quantity: Decimal,
        fee_bps: f64,
    ) -> Result<SwapFillReport, String> {
        if executed_quantity <= Decimal::ZERO {
            return Err("order has zero executed quantity".into());
        }

        let executed_base = executed_quantity
            .to_f64()
            .ok_or("cannot convert executed_quantity to f64".to_string())?;
        let cumulative_quote = cummulative_quote_quantity
            .to_f64()
            .ok_or("cannot convert cummulative_quote_quantity to f64".to_string())?;
        let net_executed_base = Self::apply_output_fee(executed_quantity, fee_bps)
            .to_f64()
            .ok_or("cannot convert fee-adjusted executed_quantity to f64".to_string())?;
        let net_cumulative_quote = Self::apply_output_fee(cummulative_quote_quantity, fee_bps)
            .to_f64()
            .ok_or("cannot convert fee-adjusted cummulative_quote_quantity to f64".to_string())?;

        match side_norm {
            "buy" => Ok(SwapFillReport {
                input_consumed: cumulative_quote,
                output_received: net_executed_base,
            }),
            "sell" => Ok(SwapFillReport {
                input_consumed: executed_base,
                output_received: net_cumulative_quote,
            }),
            _ => Err(format!(
                "invalid side_norm '{}' in map_fill_report(side_norm, executed_quantity, cummulative_quote_quantity)",
                side_norm
            )),
        }
    }

    pub(super) async fn fetch_fill_report(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        symbol: &str,
        order_id: &str,
        side_norm: &str,
        fee_bps: f64,
    ) -> Result<SwapFillReport, String> {
        let order_res = ex
            .get_order(GetOrderParams {
                symbol,
                order_id: Some(order_id),
                new_client_order_id: None,
                original_client_order_id: None,
            })
            .await
            .map_err(|e| format!("Get_order err: {}", format_mexc_api_error(&e)))?;

        match order_res.status {
            OrderStatus::Filled => {}
            other => {
                return Err(format!("order not executed, status: {:?}", other));
            }
        }

        Self::map_fill_report(
            side_norm,
            order_res.executed_quantity,
            order_res.cummulative_quote_quantity,
            fee_bps,
        )
    }

    async fn try_fetch_fill_report_by_client_order_id(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        symbol: &str,
        client_order_id: &str,
        side_norm: &str,
        fee_bps: f64,
    ) -> Result<Option<SwapFillReport>, String> {
        let order_res = ex
            .get_order(GetOrderParams {
                symbol,
                order_id: None,
                new_client_order_id: None,
                original_client_order_id: Some(client_order_id),
            })
            .await;

        let order_res = match order_res {
            Ok(res) => res,
            Err(err) => {
                let err_str = format_mexc_api_error(&err);
                // Only treat "order not found" as missing; propagate other errors.
                if is_bad_symbol(&err) || is_order_missing_lookup_error(&err_str) {
                    debug!(
                        "[mexc] get_order by client id not found symbol={} client_id={}",
                        symbol, client_order_id
                    );
                    return Ok(None);
                }
                debug!(
                    "[mexc] get_order by client id error symbol={} client_id={} err={}",
                    symbol, client_order_id, err_str
                );
                return Err(format!("get_order by client_id failed: {}", err_str));
            }
        };

        match order_res.status {
            OrderStatus::Filled => {}
            other => {
                return Err(format!(
                    "order {} not executed yet for {} (status={:?})",
                    client_order_id, symbol, other
                ));
            }
        }

        let report = Self::map_fill_report(
            side_norm,
            order_res.executed_quantity,
            order_res.cummulative_quote_quantity,
            fee_bps,
        )?;
        Ok(Some(report))
    }

    pub(super) async fn try_fetch_fill_report_by_client_order_id_candidates(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        candidates: &[String],
        client_order_id: &str,
        side_norm: &str,
        fee_bps: f64,
    ) -> Result<Option<SwapFillReport>, String> {
        for candidate in candidates {
            if let Some(report) = self
                .try_fetch_fill_report_by_client_order_id(ex, candidate, client_order_id, side_norm, fee_bps)
                .await?
            {
                return Ok(Some(report));
            }
        }

        Ok(None)
    }

    pub(super) async fn prepare_buy_order(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        amount_dec: Decimal,
        filters: Option<&SymbolFilters>,
        api_symbol: &str,
        symbol: &str,
        buy_mode: BuyOrderInputMode,
        max_quote_overspend_bps: Option<f64>,
    ) -> Result<(OrderSide, Option<Decimal>, Option<Decimal>), String> {
        if amount_dec <= Decimal::ZERO {
            return Err(format!("quote amount {} not valid for {}", amount_dec, symbol));
        }

        if buy_mode == BuyOrderInputMode::QuoteOrderQty {
            let (quote_amt, use_quote_order) = Self::adjust_quote_amount(amount_dec, filters);
            if !use_quote_order || quote_amt <= Decimal::ZERO {
                return Err(format!(
                    "quote-order mode selected but quote amount {} not valid for {}",
                    quote_amt, symbol
                ));
            }
            Self::ensure_min_notional(filters, quote_amt, symbol)?;
            return Ok((OrderSide::Buy, None, Some(quote_amt)));
        }

        if buy_mode == BuyOrderInputMode::BaseQuantity {
            let ask_levels = self.fetch_ask_levels(ex, api_symbol).await?;
            let base_out = Self::estimate_buy_quantity_from_levels(&ask_levels, amount_dec)?;
            let qty = Self::adjust_buy_quantity(base_out, filters, symbol)?;
            let quote_cost = Self::estimate_buy_quote_cost_from_levels(&ask_levels, qty)?;
            Self::ensure_min_notional(filters, quote_cost, symbol)?;

            if let Some(cap_bps) = max_quote_overspend_bps {
                let max_allowed = amount_dec
                    * (Decimal::ONE + Decimal::from_f64_retain(cap_bps / BPS_PER_RATIO_UNIT).unwrap_or(Decimal::ZERO));
                if quote_cost > max_allowed {
                    return Err(format!(
                        "base-quantity buy overspend too high for {}: est_quote_cost={} max_allowed={} cap_bps={}",
                        symbol, quote_cost, max_allowed, cap_bps
                    ));
                }
            }

            return Ok((OrderSide::Buy, Some(qty), None));
        }

        // Auto mode: if quote rounds to zero at exchange precision, fall back to base quantity.
        let (quote_amt, use_quote_order) = Self::adjust_quote_amount(amount_dec, filters);
        let check_amt = if use_quote_order { quote_amt } else { amount_dec };
        Self::ensure_min_notional(filters, check_amt, symbol)?;

        if !use_quote_order {
            let base_out = self.estimate_buy_quantity(ex, api_symbol, amount_dec).await?;
            let qty = Self::adjust_buy_quantity(base_out, filters, symbol)?;
            return Ok((OrderSide::Buy, Some(qty), None));
        }

        if quote_amt <= Decimal::ZERO {
            return Err(format!("quote amount {} not valid for {}", quote_amt, symbol));
        }

        Ok((OrderSide::Buy, None, Some(quote_amt)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::mexc_symbols::MEXC_SPOT_FEE_BPS;
    use super::*;

    // Buy fill mapping should keep quote as input-consumed and apply fee haircut to base output.
    #[test]
    fn map_fill_report_buy_uses_quote_as_input_and_base_as_output() {
        let report = MexcClient::map_fill_report("buy", Decimal::new(185, 6), Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
            .expect("map should work");
        let expected_output = MexcClient::apply_output_fee(Decimal::new(185, 6), MEXC_SPOT_FEE_BPS)
            .to_f64()
            .expect("convert expected buy output");
        assert!((report.input_consumed - 12.8).abs() < 1e-12);
        assert!((report.output_received - expected_output).abs() < 1e-12);
    }

    // Sell fill mapping should keep base as input-consumed and apply fee haircut to quote output.
    #[test]
    fn map_fill_report_sell_uses_base_as_input_and_quote_as_output() {
        let report =
            MexcClient::map_fill_report("sell", Decimal::new(185, 6), Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
                .expect("map should work");
        let expected_output = MexcClient::apply_output_fee(Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
            .to_f64()
            .expect("convert expected sell output");
        assert!((report.input_consumed - 0.000185).abs() < 1e-12);
        assert!((report.output_received - expected_output).abs() < 1e-12);
    }

    #[test]
    fn candidate_symbols_are_normalized_and_deduped() {
        let candidates = MexcClient::candidate_symbols("CKBTCBTC", "CKBTC_BTC", "ckbtc-btc");
        assert_eq!(candidates, vec!["CKBTCBTC".to_string()]);
    }
}
