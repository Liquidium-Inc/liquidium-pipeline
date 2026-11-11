//! Sizes one order from the book and the market rules: sell quantity, buy
//! quantity or quote amount, and the IOC price and quantity.
use super::*;

/// The market order of one slice: a base quantity, or for a buy a quote amount.
#[derive(Debug, Clone, Copy)]
pub(super) struct MarketOrder {
    pub(super) quantity: Option<Decimal>,
    pub(super) quote_order_quantity: Option<Decimal>,
}

/// The IOC limit order of one slice: nothing fills worse than `price`.
#[derive(Debug, Clone, Copy)]
pub(super) struct IocOrder {
    pub(super) quantity: Decimal,
    pub(super) price: Decimal,
}

impl MexcClient {
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
            .map_err(|e| e.to_string())?;

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

    /// The IOC order price: the slippage cap, kept inside MEXC's price band for
    /// the side and rounded toward the stricter price.
    pub(super) fn ioc_limit_price(
        side_norm: &str,
        limit: &PriceLimit,
        filters: Option<&SymbolFilters>,
    ) -> Result<Decimal, String> {
        let sell = side_norm == "sell";
        let band = filters.and_then(|f| if sell { f.sell_price_band } else { f.buy_price_band });
        // MEXC measures its band from its own price, so stay inside 90% of it.
        let max_distance = band.map(|band| band * Decimal::new(9, 1));
        let decimals = filters.and_then(|f| f.quote_precision).unwrap_or(8);
        crate::swappers::model::ioc_limit_price(sell, limit, max_distance, decimals)
    }

    /// The base quantity an IOC buy may take: every unit fills at or under the
    /// price, so the order spends at most `quote_amount`.
    pub(super) fn ioc_buy_quantity(
        quote_amount: Decimal,
        price: Decimal,
        filters: Option<&SymbolFilters>,
        symbol: &str,
    ) -> Result<Decimal, String> {
        let quantity = Self::adjust_buy_quantity(quote_amount / price, filters, symbol)?;
        Self::ensure_min_notional(filters, quantity * price, symbol)?;
        Ok(quantity)
    }
}
