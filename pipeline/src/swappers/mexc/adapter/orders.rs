//! Places and reads orders: the IOC order with its market fallback, the fill
//! report of a final order, and recovery of a fill by client order id.
use super::*;

/// One limit order as MEXC reports it after placement.
#[derive(Debug, Clone)]
pub struct LimitOrderReport {
    pub order_id: String,
    pub status: OrderStatus,
    pub executed_quantity: Decimal,
    pub cumulative_quote_quantity: Decimal,
}

/// Reads after placement that may miss a just-created order.
const ORDER_LOOKUP_ATTEMPTS: u32 = 5;

const ORDER_LOOKUP_PAUSE: std::time::Duration = std::time::Duration::from_millis(400);

impl MexcClient {
    pub(super) fn apply_output_fee(amount: Decimal, fee_bps: f64) -> Decimal {
        if amount <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let fee_ratio = Decimal::from_f64_retain(fee_bps / BPS_PER_RATIO_UNIT)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO)
            .min(Decimal::ONE);

        amount * (Decimal::ONE - fee_ratio)
    }

    /// Places a limit order whose unfilled rest MEXC cancels at once (IOC), so
    /// no part fills beyond `price`. It then reads the order back.
    pub async fn place_ioc_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        client_order_id: &str,
    ) -> Result<LimitOrderReport, String> {
        let ex = self.inner.lock().await;
        let placed = ex
            .order(OrderParams {
                symbol,
                side,
                order_type: v3::enums::OrderType::ImmediateOrCancel,
                quantity: Some(quantity),
                quote_order_quantity: None,
                price: Some(price),
                new_client_order_id: Some(client_order_id),
            })
            .await
            .map_err(|error| format!("IOC order refused: {}", format_mexc_api_error(&error)))?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match ex
                .get_order(GetOrderParams {
                    symbol,
                    order_id: Some(&placed.order_id),
                    new_client_order_id: None,
                    original_client_order_id: None,
                })
                .await
            {
                Ok(order) => {
                    return Ok(LimitOrderReport {
                        order_id: placed.order_id,
                        status: order.status,
                        executed_quantity: order.executed_quantity,
                        cumulative_quote_quantity: order.cummulative_quote_quantity,
                    });
                }
                Err(error) if attempt < ORDER_LOOKUP_ATTEMPTS => {
                    debug!(
                        "[mexc] order {} not readable yet: {}",
                        placed.order_id,
                        format_mexc_api_error(&error)
                    );
                    tokio::time::sleep(ORDER_LOOKUP_PAUSE).await;
                }
                Err(error) => {
                    return Err(format!(
                        "IOC order {} placed but not readable: {}",
                        placed.order_id,
                        format_mexc_api_error(&error)
                    ));
                }
            }
        }
    }

    /// Cancels an open order and returns its final state.
    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<LimitOrderReport, String> {
        let ex = self.inner.lock().await;
        let cancelled = ex
            .cancel_order(CancelOrderParams {
                symbol,
                order_id: Some(order_id),
                original_client_order_id: None,
                new_client_order_id: None,
            })
            .await
            .map_err(|error| format!("cancel of {order_id} failed: {}", format_mexc_api_error(&error)))?;
        Ok(LimitOrderReport {
            order_id: cancelled.order_id,
            status: cancelled.status,
            executed_quantity: cancelled.executed_quantity,
            cumulative_quote_quantity: cancelled.cummulative_quote_quantity,
        })
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

    /// Submits the IOC order when there is one, else the market order. A
    /// definite refusal of the IOC order placed nothing, so the market order
    /// follows under the same client id.
    pub(super) async fn submit_order(
        &self,
        ex: &MexcSpotApiClientWithAuthentication,
        candidates: &[String],
        order_side: OrderSide,
        market_order: Option<MarketOrder>,
        ioc_order: Option<IocOrder>,
        client_order_id: Option<&str>,
        market: &str,
        side: &str,
    ) -> Result<(String, String), CexBackendError> {
        let mut last_err: Option<String> = None;
        for candidate in candidates {
            if let Some(ioc) = ioc_order {
                let placed = ex
                    .order(OrderParams {
                        symbol: candidate,
                        side: order_side,
                        order_type: v3::enums::OrderType::ImmediateOrCancel,
                        quantity: Some(ioc.quantity),
                        new_client_order_id: client_order_id,
                        price: Some(ioc.price),
                        quote_order_quantity: None,
                    })
                    .await;
                match placed {
                    Ok(ok) => return Self::placed_order(candidate, ok.order_id, market, side),
                    Err(e) if is_bad_symbol(&e) => {
                        last_err = Some(format!("Swap err: {}", format_mexc_api_error(&e)));
                        continue;
                    }
                    Err(e) if is_ioc_refusal(&e) && market_order.is_some() => {
                        warn!(
                            "[mexc] IOC order refused for {} {} ({}); sending a market order",
                            candidate,
                            side,
                            format_mexc_api_error(&e)
                        );
                    }
                    Err(e) => return Err(Self::order_error(&e)),
                }
            }
            let Some(market_order) = market_order else {
                return Err("Swap err: no order could be built".to_string().into());
            };
            match ex
                .order(OrderParams {
                    symbol: candidate,
                    side: order_side,
                    order_type: v3::enums::OrderType::Market,
                    quantity: market_order.quantity,
                    new_client_order_id: client_order_id,
                    price: None,
                    quote_order_quantity: market_order.quote_order_quantity,
                })
                .await
            {
                Ok(ok) => return Self::placed_order(candidate, ok.order_id, market, side),
                Err(e) if is_bad_symbol(&e) => {
                    last_err = Some(format!("Swap err: {}", format_mexc_api_error(&e)));
                    continue;
                }
                Err(e) => return Err(Self::order_error(&e)),
            }
        }

        Err(last_err.unwrap_or_else(|| "Swap err: bad symbol".to_string()).into())
    }

    fn placed_order(
        candidate: &str,
        order_id: String,
        market: &str,
        side: &str,
    ) -> Result<(String, String), CexBackendError> {
        let order_id = order_id.trim().to_string();
        if order_id.is_empty() {
            let details = format!("empty order_id returned for symbol={candidate} market={market} side={side}");
            warn!("[mexc] {}", details);
            return Err(details.into());
        }
        Ok((candidate.to_string(), order_id))
    }

    fn order_error(e: &v3::ApiError) -> CexBackendError {
        let details = format_mexc_api_error(e);
        warn!("[mexc] order error response: {}", details);
        if is_pending_settlement(e) {
            return CexBackendError::PendingSettlement(format!("Swap err: {details}"));
        }
        format!("Swap err: {details}").into()
    }

    /// Converts MEXC order fill fields into side-agnostic execution amounts.
    ///
    /// - `buy`: input is quote spent (`cummulative_quote_quantity`), output is base received (`executed_quantity`)
    /// - `sell`: input is base sold (`executed_quantity`), output is quote received (`cummulative_quote_quantity`)
    ///
    pub(super) fn map_fill_report(
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

        if !Self::is_final_order_status(order_res.status) {
            return Err(format!("order not executed, status: {:?}", order_res.status));
        }
        Self::final_fill_report(
            side_norm,
            order_res.executed_quantity,
            order_res.cummulative_quote_quantity,
            fee_bps,
        )
    }

    /// Filled, or cancelled after any fill: the order trades no further. An IOC
    /// order ends cancelled when the book had no more within its price.
    pub(super) fn is_final_order_status(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::PartiallyCanceled
        )
    }

    /// The fill of a final order. A cancelled order that filled nothing is a zero fill.
    pub(super) fn final_fill_report(
        side_norm: &str,
        executed_quantity: Decimal,
        cummulative_quote_quantity: Decimal,
        fee_bps: f64,
    ) -> Result<SwapFillReport, String> {
        if executed_quantity <= Decimal::ZERO {
            return Ok(SwapFillReport {
                input_consumed: 0.0,
                output_received: 0.0,
            });
        }
        Self::map_fill_report(side_norm, executed_quantity, cummulative_quote_quantity, fee_bps)
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

        if !Self::is_final_order_status(order_res.status) {
            return Err(format!(
                "order {} not executed yet for {} (status={:?})",
                client_order_id, symbol, order_res.status
            ));
        }

        let report = Self::final_fill_report(
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
}
