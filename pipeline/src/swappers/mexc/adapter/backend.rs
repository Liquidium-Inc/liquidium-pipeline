//! `CexBackend` for MEXC: quotes, swaps, balances, deposits and withdrawals.
use super::*;

#[async_trait]
impl CexBackend for MexcClient {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams {
                limit: None,
                symbol: &symbol,
            })
            .await
            .map_err(|e| e.to_string())?;

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

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        let report = self.execute_swap_detailed(market, side, amount_in).await?;
        Ok(report.output_received)
    }

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64) -> Result<SwapFillReport, String> {
        self.execute_swap_detailed_with_options(market, side, amount_in, SwapExecutionOptions::default())
            .await
            .map_err(|error| error.to_string())
    }

    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexBackendError> {
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

        let order_side = match side_norm.as_str() {
            "sell" => OrderSide::Sell,
            "buy" => OrderSide::Buy,
            _ => return Err(format!("unsupported side: {side}").into()),
        };
        // Lock the authenticated client per exchange interaction, not for the full swap flow.
        let market_order = if order_side == OrderSide::Sell {
            Self::prepare_sell_order(amount_dec, filters.as_ref(), symbol.as_str())
        } else {
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
            .await
        }
        .map(|(_, quantity, quote_order_quantity)| MarketOrder {
            quantity,
            quote_order_quantity,
        });
        // An IOC order that cannot be built here goes out as the market order.
        let ioc_order = options.price_limit.as_ref().and_then(|limit| {
            let built = Self::ioc_limit_price(&side_norm, limit, filters.as_ref()).and_then(|price| {
                let quantity = match order_side {
                    OrderSide::Sell => market_order
                        .as_ref()
                        .map_err(Clone::clone)?
                        .quantity
                        .ok_or("sell order has no quantity")?,
                    OrderSide::Buy => Self::ioc_buy_quantity(amount_dec, price, filters.as_ref(), &symbol)?,
                };
                Ok(IocOrder { quantity, price })
            });
            built
                .inspect_err(|error| warn!("[mexc] no IOC order for {market} {side} ({error}); sending a market order"))
                .ok()
        });
        let market_order = match (market_order, ioc_order) {
            (Ok(order), _) => Some(order),
            (Err(error), Some(_)) => {
                warn!("[mexc] no market fallback for {market} {side} ({error}); sending the IOC order alone");
                None
            }
            (Err(error), None) => return Err(error.into()),
        };

        // Try multiple candidate symbols for MEXC quirks, then fetch the filled amount.
        let submit_res = {
            let ex = self.inner.lock().await;
            self.submit_order(
                &ex,
                &candidates,
                order_side,
                market_order,
                ioc_order,
                options.client_order_id.as_deref(),
                market,
                side,
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
                            return Err(CexBackendError::PendingSettlement(format!(
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

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let ex = self.inner.lock().await;
        let symbol = normalize_market_symbol(market);
        let ob = ex
            .depth(DepthParams { limit, symbol: &symbol })
            .await
            .map_err(|e| e.to_string())?;

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
        drop(ex);

        // The same fee source the fills are accounted with.
        let filters = self.get_symbol_filters(&market.trim().to_ascii_uppercase()).await?;
        let (taker_fee_bps, _) = Self::resolve_taker_fee_bps(filters.as_ref());
        Ok(OrderBook {
            bids,
            asks,
            taker_fee_bps,
        })
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let ex = self.inner.lock().await;

        let candidates = mexc_network_candidates(asset, network);
        let asset_candidates = mexc_deposit_asset_candidates(asset);
        let mut last_err: Option<String> = None;
        let mut last_available: Option<Vec<String>> = None;

        let mut network_attempts: Vec<Option<String>> = candidates.iter().cloned().map(Some).collect();
        network_attempts.push(None);

        for coin in &asset_candidates {
            for net in &network_attempts {
                let res = match ex.get_deposit_address(coin.to_string(), net.as_deref()).await {
                    Ok(res) => res,
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                };

                if res.is_empty() {
                    last_err = Some(format!(
                        "no deposit addresses returned for coin={} network={:?}",
                        coin, net
                    ));
                    continue;
                }

                let addr = res
                    .iter()
                    .find(|item| deposit_network_matches(&item.network, &candidates));

                if let Some(v) = addr {
                    return Ok(DepositAddress {
                        asset: asset.to_string(),
                        network: v.network.clone(),
                        address: v.address.clone(),
                        tag: v.memo.clone(),
                    });
                }

                last_available = Some(res.iter().map(|item| item.network.clone()).collect());
                last_err = Some(format!(
                    "address not found for coin={} network={:?} candidates={:?} available={:?}",
                    coin, net, candidates, last_available
                ));
            }
        }

        Err(format!(
            "address not found for asset={} network={} candidates={:?} asset_candidates={:?} available={:?} err={}",
            asset,
            network,
            candidates,
            asset_candidates,
            last_available.unwrap_or_default(),
            last_err.unwrap_or_else(|| "no deposit address candidates matched".to_string())
        ))
    }

    async fn get_withdraw_terms(&self, asset: &str, network: &str) -> Result<WithdrawTerms, String> {
        find_withdraw_terms(&self.coin_config().await?, asset, network)
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let res = ex.account_information().await.map_err(|e| e.to_string())?;

        let asset_norm = asset.to_ascii_uppercase();
        let balance = match res
            .balances
            .iter()
            .find(|item| item.asset.to_ascii_uppercase() == asset_norm)
            .cloned()
        {
            Some(b) => b,
            None => {
                debug!(
                    "[mexc] balance not found for asset={}, available={:?}",
                    asset,
                    res.balances.iter().map(|item| item.asset.clone()).collect::<Vec<_>>()
                );
                AccountBalance {
                    asset: asset.to_string(),
                    free: Decimal::ZERO,
                    locked: Decimal::ZERO,
                }
            }
        };

        balance
            .free
            .to_f64()
            .ok_or_else(|| "could not convert balance to f64".to_string())
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
        withdraw_order_id: &str,
    ) -> Result<WithdrawalReceipt, WithdrawError> {
        let ex = self.inner.lock().await;
        let candidates = mexc_withdraw_coin_candidates(asset);
        let network_candidates = mexc_withdraw_network_order(asset, network);

        info!(
            "Withdraw request coin={} network_candidates={:?} amount={} address={} withdraw_order_id={}",
            asset, network_candidates, amount, address, withdraw_order_id
        );

        let mut last_err: Option<String> = None;
        let mut hard_err: Option<WithdrawError> = None;
        let mut res = None;
        for coin in &candidates {
            for net in &network_candidates {
                match ex
                    .withdraw(WithdrawRequest {
                        address: address.to_string(),
                        amount: amount.to_string(),
                        coin: coin.to_string(),
                        memo: None,
                        network: Some(net.clone()),
                        remark: None,
                        withdraw_order_id: Some(withdraw_order_id.to_string()),
                    })
                    .await
                {
                    Ok(ok) => {
                        res = Some((ok, coin.clone()));
                        break;
                    }
                    Err(e) => {
                        // A request MEXC may have executed ends the search: the
                        // next alias would be a second withdrawal of the same funds.
                        if !is_definite_rejection(&e) {
                            return Err(WithdrawError::Ambiguous(format!(
                                "{SIDE_EFFECT_AMBIGUOUS_PREFIX}: MEXC withdraw coin={coin} network={net} withdraw_order_id={withdraw_order_id} has no decided answer: {e}"
                            )));
                        }
                        last_err = Some(e.to_string());
                        if is_coin_missing(&e) {
                            continue;
                        }
                        if hard_err.is_none() {
                            hard_err = Some(if is_below_withdraw_minimum(&e) {
                                WithdrawError::BelowMinimum(e.to_string())
                            } else {
                                WithdrawError::Refused(e.to_string())
                            });
                        }
                    }
                }
            }
            if res.is_some() {
                break;
            }
        }

        let (res, accepted_coin) = match res {
            Some(res) => res,
            None => {
                return Err(hard_err.unwrap_or_else(|| {
                    WithdrawError::Refused(last_err.unwrap_or_else(|| "withdraw failed".to_string()))
                }));
            }
        };

        Ok(WithdrawalReceipt {
            asset: accepted_coin,
            network: mexc_withdraw_network(asset, network),
            amount,
            txid: None,
            internal_id: Some(res.id),
        })
    }

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        let snapshot = self.get_withdraw_status_snapshot_by_id(coin, withdraw_id).await?;
        Ok(snapshot.status)
    }

    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        let Some(rec) = self.find_withdraw_record(coin, withdraw_id).await? else {
            return Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Unknown,
                txid: None,
                transaction_fee: None,
            });
        };

        let status =
            withdraw_status_from_mexc_record(rec.status.as_str(), rec.tx_id.as_deref(), rec.trans_hash.as_deref());
        let txid = rec
            .tx_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .or_else(|| {
                rec.trans_hash
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
            });
        let transaction_fee = parse_mexc_transaction_fee(rec.transaction_fee.as_str());
        if transaction_fee.is_none() && !rec.transaction_fee.trim().is_empty() {
            warn!(
                "mexc withdraw_history returned unparseable transaction_fee='{}' for coin={} withdraw_id={} status={}",
                rec.transaction_fee, coin, withdraw_id, rec.status
            );
        }
        if transaction_fee.is_none() && matches!(status, WithdrawStatus::Completed) {
            warn!(
                "mexc withdraw_history missing transaction fee for completed withdraw: coin={} withdraw_id={} status={} txid_present={}",
                coin,
                withdraw_id,
                rec.status,
                txid.is_some()
            );
        }

        Ok(WithdrawStatusSnapshot {
            status,
            txid,
            transaction_fee,
        })
    }
}
