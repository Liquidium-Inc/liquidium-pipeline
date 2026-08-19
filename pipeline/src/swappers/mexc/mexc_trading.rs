//! Trading on MEXC as the pipeline sees it: a cost quote, the order book, and
//! the swap itself. The swap resolves the symbol's limits, sizes and submits
//! one market order through `mexc_orders`, then reconciles the fill -- by
//! order id first, and by the persisted client order id when the venue
//! cannot read its own order back yet.

use super::*;

impl MexcClient {
    pub(super) async fn quote_buy_cost(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams {
                limit: None,
                symbol: &symbol,
            })
            .await
            .map_err(|e| mexc_error_message(&e))?;

        // asks: Vec<Vec<f64>> = [ [price, qty], ... ]
        let asks = &ob.asks;

        if asks.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining =
            Decimal::from_f64_retain(amount_in).ok_or_else(|| "could not convert amount to Decimal".to_string())?;
        let mut cost = Decimal::ZERO;

        for level in asks {
            let price = level.price;
            let qty = level.quantity;

            if remaining <= Decimal::ZERO {
                break;
            }

            let take = qty.min(remaining);
            cost += take * price;
            remaining -= take;
        }

        if remaining > Decimal::ZERO {
            return Err("not enough liquidity".into());
        }

        cost.to_f64().ok_or("f64 conversion failed".to_string())
    }

    pub(super) async fn execute_market_order(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexSubmissionError> {
        let market_symbol = market.trim().to_ascii_uppercase();
        let symbol = normalize_market_symbol(&market_symbol);
        // Determine order params (side, quantity vs quote quantity) using filters.
        let side_norm = side.to_ascii_lowercase();
        let amount_dec = Decimal::from_f64(amount_in).ok_or("could not convert amount_in to Decimal")?;
        let filters = self.get_symbol_filters(&market_symbol).await?;
        let api_symbol = filters
            .as_ref()
            .and_then(|f| f.resolved_symbol.as_deref())
            .unwrap_or(&symbol);
        let (fee_bps, fee_bps_fallback) = Self::resolve_taker_fee_bps(filters.as_ref());
        debug!(
            "[mexc] fee source market={} symbol={} taker_fee_bps={} fallback={}",
            market, api_symbol, fee_bps, fee_bps_fallback
        );

        info!("Swapping {} {} {} (symbol={})", market, side, amount_in, api_symbol);

        let candidates = Self::candidate_symbols(api_symbol, &market_symbol, &symbol);

        // Lock the authenticated client per exchange interaction, not for the full swap flow.
        let (order_side, quantity, quote_order_quantity) = match side_norm.as_str() {
            "sell" => Self::prepare_sell_order(amount_dec, filters.as_ref(), symbol.as_str())?,
            "buy" => {
                let ex = self.inner.lock().await;
                self.prepare_buy_order(
                    &ex,
                    amount_dec,
                    filters.as_ref(),
                    api_symbol,
                    symbol.as_str(),
                    options.buy_mode,
                    options.max_quote_overspend_bps,
                )
                .await?
            }
            _ => return Err(format!("unsupported side: {side}").into()),
        };

        // Try multiple candidate symbols for MEXC quirks, then fetch the filled amount.
        let submit_res = {
            let ex = self.inner.lock().await;
            self.submit_market_order(
                &ex,
                &candidates,
                order_side,
                quantity,
                quote_order_quantity,
                options.client_order_id.as_deref(),
                market,
                side,
                amount_in,
            )
            .await
        };

        match submit_res {
            Ok((chosen_symbol, order_id)) => {
                let fetch_res = {
                    let ex = self.inner.lock().await;
                    self.fetch_fill_report(&ex, &chosen_symbol, &order_id, &side_norm, fee_bps)
                        .await
                };
                match fetch_res {
                    Ok(report) => Ok(report),
                    Err(fetch_err) => {
                        if let Some(client_order_id) = options.client_order_id.as_deref() {
                            let recovered = {
                                let ex = self.inner.lock().await;
                                self.try_fetch_fill_report_by_client_order_id_candidates(
                                    &ex,
                                    &candidates,
                                    client_order_id,
                                    &side_norm,
                                    fee_bps,
                                )
                                .await
                            }?;
                            if let Some(report) = recovered {
                                info!(
                                    "[mexc] recovered filled order by client id after get_order miss market={} side={} client_id={}",
                                    market, side, client_order_id
                                );
                                return Ok(report);
                            }
                        }

                        if is_order_missing_lookup_error(&fetch_err) {
                            // The order was accepted but the venue cannot read it
                            // back yet. Same eventual-consistency wait as a
                            // settling deposit, so it must not spend a retry.
                            return Err(CexSubmissionError::PendingSettlement(format!(
                                "order lookup pending after submit market={market} side={side} symbol={chosen_symbol} order_id={order_id} err={fetch_err}"
                            )));
                        }

                        Err(fetch_err.into())
                    }
                }
            }
            Err(submit_err) => {
                if let Some(client_order_id) = options.client_order_id.as_deref() {
                    let recovered = {
                        let ex = self.inner.lock().await;
                        self.try_fetch_fill_report_by_client_order_id_candidates(
                            &ex,
                            &candidates,
                            client_order_id,
                            &side_norm,
                            fee_bps,
                        )
                        .await
                    }?;
                    if let Some(report) = recovered {
                        info!(
                            "[mexc] recovered filled order by client id after submit error market={} side={} client_id={}",
                            market, side, client_order_id
                        );
                        return Ok(report);
                    }
                }
                Err(submit_err)
            }
        }
    }

    pub(super) async fn read_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let ex = self.inner.lock().await;
        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams { limit, symbol: &symbol })
            .await
            .map_err(|e| mexc_error_message(&e))?;

        let bids = ob
            .bids
            .iter()
            .map(|level| {
                let price = level.price.to_f64().ok_or("orderbook bid price to f64 failed")?;
                let quantity = level.quantity.to_f64().ok_or("orderbook bid qty to f64 failed")?;
                Ok(OrderBookLevel { price, quantity })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let asks = ob
            .asks
            .iter()
            .map(|level| {
                let price = level.price.to_f64().ok_or("orderbook ask price to f64 failed")?;
                let quantity = level.quantity.to_f64().ok_or("orderbook ask qty to f64 failed")?;
                Ok(OrderBookLevel { price, quantity })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(OrderBook { bids, asks })
    }
}
