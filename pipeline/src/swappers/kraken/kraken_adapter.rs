use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CEX_VENUE_UNREACHABLE_PREFIX, CexBackend, CexSubmissionError, DepositAddress,
    FundingRoutePreflight, OrderBook, OrderBookLevel, SwapExecutionOptions, SwapFillReport, WithdrawStatus,
    WithdrawStatusSnapshot, WithdrawalReceipt, classify_cex_submission_error, is_cex_venue_unreachable_error,
};
use log::info;
use rust_decimal::{
    Decimal, RoundingStrategy,
    prelude::{FromPrimitive, ToPrimitive},
};
use tokio::sync::RwLock;

use super::kraken_api::{
    KrakenApi, KrakenApiError, KrakenBook, KrakenBookLevel, KrakenFundingMethod, KrakenOrderStatus, KrakenPair,
    KrakenRestApi, KrakenTransferStatus,
};

const KRAKEN_AMBIGUOUS_PREFIX: &str = "kraken ambiguous submission: ";
/// Prefix on the refusal this adapter raises when a withdrawal is under the
/// method minimum.
///
/// Kraken returns no numeric code for this, unlike MEXC: the amount is refused
/// here, before submission, so the message is the only signal the finalizer can
/// classify on. `CexVenueProfile::kraken` matches this exact constant and the
/// message is built from it, so the two cannot drift apart and leave a residual
/// withdrawal retrying forever in `Withdraw`.
pub const KRAKEN_WITHDRAW_BELOW_MIN: &str = "Kraken withdrawal is below the method minimum";
const ORDER_POLL_ATTEMPTS: usize = 20;
const ORDER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const PAIR_METADATA_TTL: Duration = Duration::from_secs(5 * 60);
/// One hundredth of a basis point, matching MEXC's `ROUND_HANDLER`: the margin
/// by which a reported fill is kept under the amount the venue credited.
const FILL_ROUNDING_MARGIN_RATIO: Decimal = Decimal::from_parts(1, 0, 0, false, 6);

#[derive(Clone)]
pub struct KrakenClient {
    api: Arc<dyn KrakenApi>,
    pairs: Arc<RwLock<Option<(Instant, Vec<KrakenPair>)>>>,
    allowed_pairs: Arc<Vec<String>>,
}

impl KrakenClient {
    pub fn new(api_key: String, api_secret: String, allowed_pairs: Vec<String>) -> Self {
        Self::with_api(Arc::new(KrakenRestApi::new(api_key, api_secret)), allowed_pairs)
    }

    pub fn with_api(api: Arc<dyn KrakenApi>, allowed_pairs: Vec<String>) -> Self {
        Self {
            api,
            pairs: Arc::new(RwLock::new(None)),
            allowed_pairs: Arc::new(allowed_pairs.into_iter().map(|pair| normalize_market(&pair)).collect()),
        }
    }

    async fn all_pairs(&self) -> Result<Vec<KrakenPair>, String> {
        if let Some((loaded_at, pairs)) = self.pairs.read().await.as_ref()
            && loaded_at.elapsed() < PAIR_METADATA_TTL
        {
            return Ok(pairs.clone());
        }
        let pairs = self.api.pairs().await.map_err(api_read_error)?;
        *self.pairs.write().await = Some((Instant::now(), pairs.clone()));
        Ok(pairs)
    }

    async fn pair(&self, market: &str) -> Result<KrakenPair, String> {
        let market = normalize_market(market);
        let matches = self
            .all_pairs()
            .await?
            .into_iter()
            .filter(|pair| format!("{}_{}", pair.base, pair.quote) == market)
            .filter(|_| self.allowed_pairs.is_empty() || self.allowed_pairs.iter().any(|allowed| allowed == &market))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Err(format!("Kraken pair {market} is unavailable or not allowed")),
            [pair] if !pair.online => Err(format!("Kraken pair {market} is not online")),
            [pair] => Ok(pair.clone()),
            _ => Err(format!("Kraken pair {market} is ambiguous")),
        }
    }

    async fn book(&self, pair: &KrakenPair, depth: u32) -> Result<KrakenBook, String> {
        self.api.orderbook(&pair.api_name, depth).await.map_err(api_read_error)
    }

    async fn resolve_deposit(
        &self,
        asset: &str,
        network: &str,
    ) -> Result<(KrakenFundingMethod, String, Option<String>), String> {
        let api_asset = api_asset(asset);
        let method = unique_method(
            self.api.deposit_methods(&api_asset).await.map_err(api_read_error)?,
            asset,
            network,
            "deposit",
        )?;
        let addresses = self
            .api
            .deposit_addresses(&api_asset, &method.method)
            .await
            .map_err(api_read_error)?;
        match addresses.as_slice() {
            [address] => Ok((method, address.address.clone(), address.tag.clone())),
            [] => Err(format!(
                "Kraken has no existing {asset} deposit address for network {network}"
            )),
            _ => Err(format!(
                "Kraken returned ambiguous {asset} deposit addresses for network {network}"
            )),
        }
    }

    async fn resolve_withdrawal(
        &self,
        asset: &str,
        network: &str,
        destination: &str,
    ) -> Result<(KrakenFundingMethod, String), String> {
        let api_asset = api_asset(asset);
        let method = unique_method(
            self.api.withdrawal_methods(&api_asset).await.map_err(api_read_error)?,
            asset,
            network,
            "withdrawal",
        )?;
        let matches = self
            .api
            .withdrawal_addresses(&api_asset)
            .await
            .map_err(api_read_error)?
            .into_iter()
            .filter(|entry| {
                entry.verified && entry.method == method.method && addresses_match(&entry.address, destination)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [entry] => Ok((method, entry.key.clone())),
            [] => Err(format!(
                "Kraken destination is not a verified {asset} withdrawal address for network {network}"
            )),
            _ => Err(format!("Kraken withdrawal destination for {asset} is ambiguous")),
        }
    }

    async fn wait_for_order(&self, order_id: &str, side: &str) -> Result<SwapFillReport, String> {
        let mut last_lookup_failure: Option<String> = None;
        for attempt in 0..ORDER_POLL_ATTEMPTS {
            let order = match self.api.order(order_id).await {
                Ok(order) => order,
                // Kraken already accepted this order and handed back its id, so
                // a read that cannot see it yet is the venue's own propagation
                // delay rather than a lost order. Re-reading submits nothing, so
                // polling on is safe -- and necessary: failing the first lookup
                // parked orders that had already filled, because the loop below
                // only ever retried on *status*, never on a failed read.
                Err(error) => {
                    last_lookup_failure = Some(api_reconciliation_error(error));
                    if attempt + 1 < ORDER_POLL_ATTEMPTS {
                        tokio::time::sleep(ORDER_POLL_INTERVAL).await;
                    }
                    continue;
                }
            };
            last_lookup_failure = None;
            match order.status {
                KrakenOrderStatus::Closed => return fill_report(side, order.volume_executed, order.cost, order.fee),
                KrakenOrderStatus::Canceled | KrakenOrderStatus::Expired => {
                    if order.volume_executed > Decimal::ZERO {
                        return fill_report(side, order.volume_executed, order.cost, order.fee);
                    }
                    return Err(format!("Kraken order {order_id} was {:?}", order.status));
                }
                KrakenOrderStatus::Pending | KrakenOrderStatus::Open if attempt + 1 < ORDER_POLL_ATTEMPTS => {
                    tokio::time::sleep(ORDER_POLL_INTERVAL).await;
                }
                KrakenOrderStatus::Pending | KrakenOrderStatus::Open => {}
            }
        }
        // Both exhaustion paths stay ambiguous: an order we still cannot read,
        // or one that never settled, may both have moved funds.
        Err(last_lookup_failure
            .unwrap_or_else(|| format!("{KRAKEN_AMBIGUOUS_PREFIX}order {order_id} did not reach a terminal state")))
    }
}

#[async_trait]
impl CexBackend for KrakenClient {
    fn classify_submission_error(&self, error: &str) -> CexSubmissionError {
        // Only the ambiguity marker is Kraken's own; every other case follows
        // the shared rule. Unreachability still wins over it, because that is
        // decided by the transport before any submission could become unclear.
        if error.starts_with(KRAKEN_AMBIGUOUS_PREFIX) && !is_cex_venue_unreachable_error(error) {
            return CexSubmissionError::Ambiguous(error.to_string());
        }
        classify_cex_submission_error(error)
    }

    async fn validate_funding_route(&self, preflight: &FundingRoutePreflight) -> Result<(), String> {
        ensure_positive(preflight.deposit_amount)?;
        ensure_positive(preflight.withdraw_amount)?;
        let (deposit_method, _, deposit_tag) = self
            .resolve_deposit(&preflight.deposit_asset, &preflight.deposit_network)
            .await?;

        if preflight.deposit_network.eq_ignore_ascii_case("ICP")
            && deposit_tag.as_deref().is_some_and(|tag| !tag.trim().is_empty())
        {
            return Err(format!(
                "Kraken {} deposit on ICP requires a memo/tag that the transfer layer cannot preserve",
                preflight.deposit_asset
            ));
        }

        let deposit_amount =
            Decimal::from_f64(preflight.deposit_amount).ok_or_else(|| "invalid Kraken deposit amount".to_string())?;

        if deposit_amount < deposit_method.minimum {
            return Err(format!(
                "Kraken deposit amount {deposit_amount} is below {} minimum {}",
                deposit_method.method, deposit_method.minimum
            ));
        }

        let (withdraw_method, _) = self
            .resolve_withdrawal(
                &preflight.withdraw_asset,
                &preflight.withdraw_network,
                &preflight.withdraw_address,
            )
            .await?;

        let withdraw_amount = Decimal::from_f64(preflight.withdraw_amount)
            .ok_or_else(|| "invalid Kraken withdrawal amount".to_string())?;

        if withdraw_amount < withdraw_method.minimum {
            return Err(format!(
                "Kraken withdrawal amount {withdraw_amount} is below {} minimum {}",
                withdraw_method.method, withdraw_method.minimum
            ));
        }
        Ok(())
    }

    async fn validate_trade_amounts(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> Result<(), String> {
        ensure_positive(amount_in)?;
        ensure_positive(amount_out)?;
        let pair = self.pair(market).await?;
        let input = Decimal::from_f64(amount_in).ok_or_else(|| "invalid Kraken trade input".to_string())?;
        let output = Decimal::from_f64(amount_out).ok_or_else(|| "invalid Kraken trade output".to_string())?;

        let (base_volume, quote_cost) = if side.eq_ignore_ascii_case("sell") {
            (truncate(input, pair.lot_decimals), output)
        } else if side.eq_ignore_ascii_case("buy") {
            (truncate(output, pair.lot_decimals), input)
        } else {
            return Err(format!("unsupported Kraken order side {side}"));
        };

        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            ));
        }

        if quote_cost < pair.cost_min {
            return Err(format!(
                "Kraken quote cost {quote_cost} is below cost minimum {}",
                pair.cost_min
            ));
        }
        Ok(())
    }

    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let pair = self.pair(market).await?;
        ensure_positive(amount_in)?;

        let mut remaining = amount_in;
        let mut output = 0.0;
        for level in self.book(&pair, 100).await?.bids {
            let take = remaining.min(decimal_f64(level.quantity, "bid quantity")?);
            output += take * decimal_f64(level.price, "bid price")?;
            remaining -= take;
            if remaining <= 1e-12 {
                break;
            }
        }

        if remaining > 1e-12 {
            return Err("not enough Kraken bid liquidity".to_string());
        }

        Ok(output)
    }

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        Ok(self
            .execute_swap_detailed(market, side, amount_in)
            .await?
            .output_received)
    }

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64) -> Result<SwapFillReport, String> {
        // The classification is flattened back into its message here so the
        // sentinel prefixes stay readable to `classify_submission_error`.
        self.execute_swap_detailed_with_options(market, side, amount_in, SwapExecutionOptions::default())
            .await
            .map_err(|error| error.to_string())
    }

    /// Submits one Kraken spot market order and reconciles its terminal fill.
    ///
    /// `amount_in` is expressed in the asset being spent by the route:
    /// - for a sell, it is base-asset volume;
    /// - for a buy, it is the quote-asset spending budget.
    ///
    /// Kraken accepts market-order volume in base units. Buy orders therefore
    /// derive a precision-safe base volume from the quote budget, visible asks,
    /// the pair's taker fee, and the configured quote overspend cap. Sell orders
    /// truncate the supplied base amount directly to Kraken's lot precision.
    ///
    /// Before submission this method enforces local liquidity, lot precision,
    /// order-minimum, cost-minimum, fee, and overspend constraints. It attaches
    /// the persisted client order ID when present, submits exactly one order,
    /// then polls Kraken until the order reaches a terminal state. Transport
    /// uncertainty during submission or reconciliation is classified by the
    /// Kraken adapter so the shared CEX state machine does not blindly replay it.
    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexSubmissionError> {
        // Reject zero, negative, NaN, and infinite inputs before reading Kraken state.
        ensure_positive(amount_in)?;

        // Resolve the canonical market to one unique, online Kraken pair and load
        // its precision, minimum-order, minimum-cost, and fee metadata.
        let pair = self.pair(market).await?;

        // Convert the route's floating-point input into decimal arithmetic before
        // performing any sizing or limit calculations.
        let amount = Decimal::from_f64(amount_in).ok_or_else(|| "invalid Kraken order amount".to_string())?;

        // Kraken's order endpoint always needs a base-asset volume, regardless
        // of whether this route spends base by selling or quote by buying.
        let base_volume = if side.eq_ignore_ascii_case("sell") {
            // A sell's route input is already base volume; truncate it so the
            // submitted quantity never exceeds the available amount.
            let base = truncate(amount, pair.lot_decimals);

            // Simulate the precision-adjusted sell against current bids to prove
            // liquidity and determine whether its quote proceeds satisfy costmin.
            let estimated_output = estimate_sell_output(&self.book(&pair, 100).await?, base)?;

            // Kraken rejects sells whose expected quote notional is below costmin.
            if estimated_output < pair.cost_min {
                return Err(format!(
                    "Kraken sell estimate {estimated_output} is below cost minimum {}",
                    pair.cost_min
                )
                .into());
            }

            // Submit the precision-safe base volume produced above.
            base
        } else if side.eq_ignore_ascii_case("buy") {
            // The shared quote-quantity mode cannot be sent directly because this
            // Kraken endpoint expects market-buy volume in base units.
            if options.buy_mode == BuyOrderInputMode::QuoteOrderQty {
                return Err("Kraken market buys require precision-safe base quantity mode"
                    .to_string()
                    .into());
            }

            // Reject malformed pair metadata before using its fee in budget math.
            if !pair.taker_fee_bps.is_finite() || !(0.0..=10_000.0).contains(&pair.taker_fee_bps) {
                return Err(format!("invalid Kraken taker fee {} bps", pair.taker_fee_bps).into());
            }

            // Default to no extra spending when the caller did not authorize an
            // overspend allowance for base-quantity buy sizing.
            let cap_bps = options.max_quote_overspend_bps.unwrap_or(0.0);

            // Keep the overspend cap within a finite zero-to-100% bps range.
            if !cap_bps.is_finite() || !(0.0..=10_000.0).contains(&cap_bps) {
                return Err(format!("invalid Kraken quote overspend cap {cap_bps} bps").into());
            }

            // Convert fee and overspend bps into exact decimal ratios.
            let fee_ratio = Decimal::from_f64(pair.taker_fee_bps / 10_000.0).unwrap_or_default();
            let cap_ratio = Decimal::from_f64(cap_bps / 10_000.0).unwrap_or_default();

            // The hard quote cap includes only the explicitly authorized overspend.
            let max_spend = amount * (Decimal::ONE + cap_ratio);

            // Reserve quote currency for the estimated taker fee before using the
            // remainder to purchase base volume.
            let trade_budget = max_spend / (Decimal::ONE + fee_ratio);

            // Read enough asks to derive the base quantity purchasable by that budget.
            let book = self.book(&pair, 100).await?;

            // Track the unallocated quote budget while walking asks best-price first.
            let mut budget = trade_budget;

            // Accumulate the corresponding base volume across visible ask levels.
            let mut base = Decimal::ZERO;

            // Consume each ask level until either the budget or visible depth ends.
            for level in &book.asks {
                // Stop once the entire quote trade budget has been allocated.
                if budget <= Decimal::ZERO {
                    break;
                }

                // Skipped rather than rejected outright, so one malformed level
                // deep in the book does not refuse an order the rest of the
                // depth can fill; the liquidity check below still refuses it if
                // it cannot. See `is_tradable_level` for why it cannot be used.
                if !is_tradable_level(level) {
                    continue;
                }

                // Buy no more than the level offers or the remaining budget affords.
                let take = level.quantity.min(budget / level.price);

                // Add this level's purchased quantity to the proposed base order.
                base += take;

                // Remove this level's quote cost from the remaining trade budget.
                budget -= take * level.price;
            }

            // Any material budget left after exhausting asks means visible depth
            // cannot support the requested quote-input order.
            if budget > Decimal::from_f64(1e-10).unwrap_or(Decimal::ZERO) {
                return Err("not enough Kraken ask liquidity".to_string().into());
            }

            // Truncate derived base volume to Kraken lot precision; rounding up
            // here could spend more quote currency than authorized.
            let base = truncate(base, pair.lot_decimals);

            // Reprice the exact truncated base quantity against the same asks.
            let estimated_cost = estimate_buy_cost(&book, base)?;

            // Include the estimated taker fee in the final quote spend check.
            let estimated_total = estimated_cost * (Decimal::ONE + fee_ratio);

            // Refuse submission if precision or fee math would cross the hard cap.
            if estimated_total > max_spend {
                return Err(format!(
                    "Kraken buy estimate with fee {estimated_total} exceeds quote-input cap {max_spend}"
                )
                .into());
            }

            // Submit the precision-safe base volume derived from the quote budget.
            base
        } else {
            // Only the two spot sides understood by Kraken are accepted locally.
            return Err(format!("unsupported Kraken order side {side}").into());
        };

        // Apply Kraken's minimum base-order volume after all precision truncation.
        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            )
            .into());
        }

        // For buys, the route input is the quote budget and must independently
        // satisfy Kraken's minimum quote cost.
        if side.eq_ignore_ascii_case("buy") && amount < pair.cost_min {
            return Err(format!("Kraken quote budget {amount} is below cost minimum {}", pair.cost_min).into());
        }

        // A leg that already submitted under this client id must adopt that
        // order rather than place a second one. The Kraken order id is returned
        // only once, so a leg interrupted between submission and reconciliation
        // knows the order solely by the client id persisted before it -- and
        // without this lookup the resumed attempt would re-sell collateral the
        // first order had already sold.
        if let Some(client_order_id) = options.client_order_id.as_deref() {
            match self.api.closed_order_by_client_id(client_order_id).await {
                Ok(Some(order)) if order.volume_executed > Decimal::ZERO => {
                    info!(
                        "[kraken] adopting order already settled under client id {client_order_id}: executed {} cost {}",
                        order.volume_executed, order.cost
                    );
                    return fill_report(side, order.volume_executed, order.cost, order.fee).map_err(Into::into);
                }
                Ok(_) => {}
                // Not knowing whether a prior order exists is not the same as
                // knowing there is none, so this must not fall through to a
                // submission that could duplicate it.
                Err(error) => {
                    return Err(self.classify_submission_error(&api_reconciliation_error(error)));
                }
            }
        }

        // Submit exactly one market order, carrying the persisted client order ID
        // that the shared CEX recovery logic prepared before this side effect.
        // Past this point a failure may have reached the exchange, so both the
        // submission and the reconciliation are classified here rather than
        // being flattened into a rejection the caller would be free to replay.
        let order_id = self
            .api
            .place_market_order(&pair.api_name, side, base_volume, options.client_order_id)
            .await
            .map_err(|error| self.classify_submission_error(&api_submit_error(error)))?;

        // Poll the returned Kraken order ID to a terminal state and translate its
        // consumed input, received output, and fees into the shared fill report.
        self.wait_for_order(&order_id, side)
            .await
            .map_err(|error| self.classify_submission_error(&error))
    }

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        let pair = self.pair(market).await?;
        let book = self.book(&pair, limit.unwrap_or(100)).await?;
        Ok(OrderBook {
            bids: convert_levels(book.bids)?,
            asks: convert_levels(book.asks)?,
        })
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let (_, address, tag) = self.resolve_deposit(asset, network).await?;
        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.to_string(),
            address,
            tag,
        })
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String> {
        ensure_positive(amount)?;
        let (method, key) = self.resolve_withdrawal(asset, network, address).await?;
        let amount_decimal = Decimal::from_f64(amount).ok_or_else(|| "invalid Kraken withdrawal amount".to_string())?;
        if amount_decimal < method.minimum {
            return Err(format!(
                "{KRAKEN_WITHDRAW_BELOW_MIN}: {amount_decimal} is below {} minimum {}",
                method.method, method.minimum
            ));
        }
        let ref_id = self
            .api
            .withdraw(&api_asset(asset), &key, address, amount_decimal)
            .await
            .map_err(api_submit_error)?;
        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some(ref_id),
        })
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        decimal_f64(
            self.api.balance(&api_asset(asset)).await.map_err(api_read_error)?,
            "balance",
        )
    }

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        Ok(self.get_withdraw_status_snapshot_by_id(coin, withdraw_id).await?.status)
    }

    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        let withdrawal = self
            .api
            .withdrawals(&api_asset(coin))
            .await
            .map_err(api_read_error)?
            .into_iter()
            .find(|item| item.ref_id == withdraw_id);
        let Some(withdrawal) = withdrawal else {
            return Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Unknown,
                txid: None,
                transaction_fee: None,
            });
        };
        let status = match withdrawal.status {
            KrakenTransferStatus::Pending => WithdrawStatus::Pending,
            KrakenTransferStatus::Completed => WithdrawStatus::Completed,
            KrakenTransferStatus::Failed => WithdrawStatus::Failed,
        };
        Ok(WithdrawStatusSnapshot {
            status,
            txid: withdrawal.tx_id,
            transaction_fee: Some(decimal_f64(withdrawal.fee, "withdrawal fee")?),
        })
    }
}

pub(crate) fn normalize_market(market: &str) -> String {
    market
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '-'], "_")
        .split('_')
        .map(|symbol| match symbol {
            "XBT" | "XXBT" => "BTC",
            value => value,
        })
        .collect::<Vec<_>>()
        .join("_")
}
/// Compares a stored withdrawal address against the destination we intend to
/// send to.
///
/// Hex addresses carry no information in their case: EIP-55 checksumming is a
/// display convention, so Kraken storing an address lowercased while the
/// pipeline holds the checksummed form describes one address, not two. Matching
/// those byte-for-byte rejects a destination the operator did verify, and the
/// failure surfaces only after the collateral has been seized.
///
/// Every other address format is compared exactly. Base58 in particular encodes
/// distinct values in `A` and `a`, so case-folding it would risk matching an
/// address that was never verified.
fn addresses_match(stored: &str, destination: &str) -> bool {
    let hex_address = |value: &str| {
        value.len() > 2 && value[..2].eq_ignore_ascii_case("0x") && value[2..].chars().all(|c| c.is_ascii_hexdigit())
    };
    if hex_address(stored) && hex_address(destination) {
        return stored.eq_ignore_ascii_case(destination);
    }
    stored == destination
}

fn api_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "BTC" => "XBT".to_string(),
        value => value.to_string(),
    }
}

/// Kraken's own label for a chain this pipeline names, when it has one.
///
/// Used to compare against `KrakenFundingMethod::network` exactly. A method
/// *name* cannot identify a chain: `Tether USD (SPL)` contains "eth" because
/// "Tether" does, and `Ethereum (Polygon)` contains "ethereum" while settling on
/// Polygon. Matching either by substring selects the wrong chain or several.
fn kraken_network_label(requested: &str) -> Option<&'static str> {
    match requested.trim().to_ascii_uppercase().as_str() {
        "ETH" | "ETHEREUM" | "ERC20" => Some("ethereum"),
        "BTC" | "BITCOIN" => Some("bitcoin"),
        _ => None,
    }
}

/// Chain names that appear inside a method name for the chain the pipeline asked
/// for, used only when Kraken reports no network field.
///
/// Deposit methods carry no network, so the name is the only signal. Kraken
/// spells the same chain several ways -- `Ether (Hex)`, `Tether USD (ERC20)`,
/// `USDC - Ethereum (Unified)` -- so each accepted spelling is listed rather
/// than inferred, and anything unlisted does not match.
fn method_name_names_chain(requested: &str, method_name: &str) -> bool {
    let name = method_name.to_ascii_lowercase();
    let Some(label) = kraken_network_label(requested) else {
        // Assets whose chain this pipeline does not translate, such as ICP.
        return name.contains(&requested.to_ascii_lowercase());
    };
    match label {
        "ethereum" => {
            // `Ether (Hex)` is Kraken's native ETH deposit; `erc20` covers the
            // token spellings. A chain qualifier means a different rollup.
            let ethereum_spelling = name.starts_with("ether") || name.contains("erc20") || name.contains("ethereum");
            ethereum_spelling && !OTHER_CHAIN_QUALIFIERS.iter().any(|other| name.contains(other))
        }
        "bitcoin" => name.contains("bitcoin") && !name.contains("lightning") && !name.contains("kbtc"),
        _ => false,
    }
}

/// Chain qualifiers that disqualify an otherwise Ethereum-looking method name.
///
/// Kraken names rollup methods after the token plus the rollup, so a name can
/// contain an Ethereum spelling while settling elsewhere entirely.
const OTHER_CHAIN_QUALIFIERS: [&str; 12] = [
    "polygon",
    "optimism",
    "arbitrum",
    "base",
    "unichain",
    "ink",
    "linea",
    "zksync",
    "sei",
    "avalanche",
    "solana",
    "tron",
];

fn network_matches(asset: &str, requested: &str, method: &KrakenFundingMethod) -> bool {
    let _ = asset;
    // Withdrawal methods report the settlement network, which is the only field
    // that identifies a chain unambiguously. Compare it exactly.
    if let Some(expected) = kraken_network_label(requested)
        && let Some(network) = method.network.as_deref()
    {
        return network.trim().eq_ignore_ascii_case(expected);
    }
    // Deposit methods report no network, leaving only the method name.
    if let Some(network) = method.network.as_deref() {
        return network.to_ascii_lowercase().contains(&requested.to_ascii_lowercase());
    }
    method_name_names_chain(requested, &method.method)
}

fn unique_method(
    methods: Vec<KrakenFundingMethod>,
    asset: &str,
    network: &str,
    operation: &str,
) -> Result<KrakenFundingMethod, String> {
    let matches = methods
        .into_iter()
        .filter(|method| network_matches(asset, network, method))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [method] => Ok(method.clone()),
        [] => Err(format!(
            "Kraken does not support {operation} for {asset} on network {network}"
        )),
        _ => Err(format!(
            "Kraken {operation} method for {asset} on network {network} is ambiguous"
        )),
    }
}

fn truncate(value: Decimal, decimals: u32) -> Decimal {
    value.round_dp_with_strategy(decimals, RoundingStrategy::ToZero)
}

/// Whether a book level is one this pipeline can trade against at all.
///
/// Levels arrive exactly as Kraken sent them, and a price at or below zero is
/// not a price: dividing a budget by it panics `Decimal`, and a negative one
/// would hand back budget that was never spent. Every walk over a book answers
/// this the same way, because sizing an order against depth that the repricing
/// then treats differently is how an order slips past its own spend cap.
fn is_tradable_level(level: &KrakenBookLevel) -> bool {
    level.price > Decimal::ZERO
}

fn estimate_buy_cost(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    let mut remaining = base_volume;
    let mut cost = Decimal::ZERO;
    for level in &book.asks {
        if !is_tradable_level(level) {
            continue;
        }
        let take = remaining.min(level.quantity);
        cost += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if remaining > Decimal::ZERO {
        Err("not enough Kraken ask liquidity".to_string())
    } else {
        Ok(cost)
    }
}

fn estimate_sell_output(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    let mut remaining = base_volume;
    let mut output = Decimal::ZERO;
    for level in &book.bids {
        if !is_tradable_level(level) {
            continue;
        }
        let take = remaining.min(level.quantity);
        output += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if remaining > Decimal::ZERO {
        Err("not enough Kraken bid liquidity".to_string())
    } else {
        Ok(output)
    }
}

fn convert_levels(levels: Vec<super::kraken_api::KrakenBookLevel>) -> Result<Vec<OrderBookLevel>, String> {
    levels
        .into_iter()
        .map(|level| {
            Ok(OrderBookLevel {
                price: decimal_f64(level.price, "orderbook price")?,
                quantity: decimal_f64(level.quantity, "orderbook quantity")?,
            })
        })
        .collect()
}

/// Splits a settled order into what it consumed and what it paid out.
///
/// Kraken charges the fee in the currency the order pays out, and no order
/// flags are set to change that, so both sides here take the fee off the
/// received amount. What differs is the units: Kraken reports `fee` in the
/// quote currency whichever side it actually took it from. A sell pays out
/// quote, so the number is already usable. A buy pays out base, so the reported
/// fee has to be converted at this fill's own price before it can be taken off
/// the volume the order says it executed.
///
/// Reading a buy as "quote spent plus a quote fee" instead left `executed`
/// standing as the amount received, which is a fee more than Kraken credits.
/// Every withdrawal of that amount was then rejected for insufficient funds,
/// and no retry could close a gap where neither side moves (liquidation 1634).
fn fill_report(side: &str, executed: Decimal, cost: Decimal, fee: Decimal) -> Result<SwapFillReport, String> {
    // A sell pays out quote and a buy pays out base; the fee comes out of
    // whichever that is. Kraken reports it in the quote currency either way, so
    // only a buy has to convert, and `fee / cost` -- the fee as a fraction of
    // the order -- is the one form that survives the change of units. A
    // zero-cost fill has no price to convert through and nothing to charge a
    // fee against.
    let (consumed, received, fee_in_received) = if side.eq_ignore_ascii_case("sell") {
        (executed, cost, fee)
    } else if cost > Decimal::ZERO {
        let base_fee = executed
            .checked_mul(fee)
            .and_then(|weighted| weighted.checked_div(cost))
            .ok_or_else(|| "Kraken order fee cannot be expressed in base units".to_string())?;
        (cost, executed, base_fee)
    } else {
        (cost, executed, Decimal::ZERO)
    };

    // Kraken rounds every figure it reports, so a fee derived from them can land
    // a rounding step under what was really withheld -- and a received amount
    // even slightly above what the venue credited is the whole failure this
    // fixes: the leg then asks to withdraw or spend money that is not there, and
    // retrying cannot close a gap where neither side moves. The same hundredth
    // of a basis point MEXC's `ROUND_HANDLER` shaves off a fill covers it here,
    // three orders of magnitude below the dust the venue leaves behind anyway.
    let rounding_margin = received
        .checked_mul(FILL_ROUNDING_MARGIN_RATIO)
        .unwrap_or(Decimal::ZERO);

    Ok(SwapFillReport {
        input_consumed: decimal_f64(consumed, "order input")?,
        output_received: decimal_f64(
            (received - fee_in_received - rounding_margin).max(Decimal::ZERO),
            "order output net of fee",
        )?,
    })
}

fn decimal_f64(value: Decimal, field: &str) -> Result<f64, String> {
    value
        .to_f64()
        .ok_or_else(|| format!("Kraken {field} cannot be represented as f64"))
}
fn ensure_positive(value: f64) -> Result<(), String> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err("amount must be finite and positive".to_string())
    }
}
fn api_read_error(error: KrakenApiError) -> String {
    match error {
        KrakenApiError::Unreachable(message) => format!("{CEX_VENUE_UNREACHABLE_PREFIX}{message}"),
        other => other.to_string(),
    }
}
/// An unreachable venue leaves the accepted order exactly as it was, so the
/// read is simply retried rather than handed to an operator.
fn api_reconciliation_error(error: KrakenApiError) -> String {
    match error {
        KrakenApiError::Unreachable(message) => format!("{CEX_VENUE_UNREACHABLE_PREFIX}{message}"),
        other => format!("{KRAKEN_AMBIGUOUS_PREFIX}could not reconcile accepted order: {other}"),
    }
}
fn api_submit_error(error: KrakenApiError) -> String {
    match error {
        // Nothing was submitted, so this is not the ambiguity `Transport` is.
        KrakenApiError::Unreachable(message) => format!("{CEX_VENUE_UNREACHABLE_PREFIX}{message}"),
        KrakenApiError::Ambiguous(message) | KrakenApiError::Transport(message) => {
            format!("{KRAKEN_AMBIGUOUS_PREFIX}{message}")
        }
        KrakenApiError::Rejected(message) => message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swappers::kraken::kraken_api::{
        KrakenBookLevel, KrakenDepositAddress, KrakenOrder, KrakenWithdrawalAddress, MockKrakenApi,
    };

    fn method(name: &str, network: Option<&str>) -> KrakenFundingMethod {
        KrakenFundingMethod {
            method: name.to_string(),
            network: network.map(str::to_string),
            minimum: Decimal::ZERO,
        }
    }

    /// Every method name and network below is what Kraken returns today.
    ///
    /// A method name cannot identify a chain, and both counterexamples are live:
    /// `Tether USD (SPL)` matches a substring search for "eth" because "Tether"
    /// does, and `Ethereum (Polygon)` contains "ethereum" while settling on
    /// Polygon. Selecting on the reported network instead is what makes each of
    /// these resolve to exactly one method.
    #[test]
    fn withdrawal_methods_resolve_to_the_one_ethereum_settlement() {
        let usdt = vec![
            method("USDT - Avalanche C-Chain", Some("Avalanche C-Chain")),
            method("Tether USD (ERC20)", Some("Ethereum")),
            method("Tether USD (TRC20)", Some("Tron")),
            method("Tether USD (SPL)", Some("Solana")),
            method("Tether USD (Aptos)", Some("Aptos")),
            method("Tether USD (Polygon)", Some("Polygon")),
            method("USDT0 - Sei (EVM)", Some("Sei - EVM")),
        ];
        let chosen = unique_method(usdt, "USDT", "ETH", "withdrawal").expect("exactly one Ethereum method");
        assert_eq!(chosen.method, "Tether USD (ERC20)");

        let eth = vec![
            method("Ether", Some("Ethereum")),
            method("Ethereum (Polygon)", Some("Polygon")),
            method("Arbitrum One", Some("Arbitrum One")),
            method("ETH - Base", Some("Base")),
        ];
        let chosen = unique_method(eth, "ETH", "ETH", "withdrawal").expect("exactly one Ethereum method");
        assert_eq!(chosen.method, "Ether");

        let usdc = vec![
            method("USDC", Some("Ethereum")),
            method("USDC (Polygon)", Some("Polygon (USDC.e)")),
            method("Optimism", Some("Optimism (USDC.e)")),
        ];
        let chosen = unique_method(usdc, "USDC", "ETH", "withdrawal").expect("exactly one Ethereum method");
        assert_eq!(chosen.method, "USDC");
    }

    /// Deposit methods report no network at all, so the name is the only signal
    /// and each accepted spelling has to be recognised deliberately.
    #[test]
    fn deposit_methods_resolve_without_a_reported_network() {
        let eth = vec![
            method("Ether (Hex)", None),
            method("ETH - Polygon (Unified)", None),
            method("ETH - Arbitrum One (Unified)", None),
            method("zkSync Era", None),
            method("Linea", None),
        ];
        let chosen = unique_method(eth, "ETH", "ETH", "deposit").expect("native ETH deposit");
        assert_eq!(chosen.method, "Ether (Hex)");

        let usdc = vec![
            method("USDC - Ethereum (Unified)", None),
            method("USDC.e - Optimism (Unified)", None),
            method("USDC - Stellar XLM", None),
        ];
        let chosen = unique_method(usdc, "USDC", "ETH", "deposit").expect("Ethereum USDC deposit");
        assert_eq!(chosen.method, "USDC - Ethereum (Unified)");

        // An asset whose chain this pipeline does not translate falls back to a
        // plain name match.
        let icp = vec![method("Internet Computer Protocol (ICP)", None)];
        let chosen = unique_method(icp, "ICP", "ICP", "deposit").expect("ICP deposit");
        assert_eq!(chosen.method, "Internet Computer Protocol (ICP)");
    }

    /// Wrapped BTC on Ethereum and Lightning both live under the BTC asset, and
    /// neither is the on-chain Bitcoin settlement the bridge expects.
    #[test]
    fn bitcoin_selection_excludes_lightning_and_wrapped_variants() {
        let btc = vec![
            method("Bitcoin", Some("Bitcoin")),
            method("Bitcoin Lightning", Some("Lightning")),
            method("kBTC - Ethereum", Some("Ethereum (kBTC)")),
        ];
        let chosen = unique_method(btc.clone(), "BTC", "BTC", "withdrawal").expect("on-chain Bitcoin");
        assert_eq!(chosen.method, "Bitcoin");

        // Deposits carry no network, so the name-based path must exclude them too.
        let deposits = vec![
            method("Bitcoin", None),
            method("Bitcoin Lightning", None),
            method("kBTC - Ethereum (Unified)", None),
        ];
        let chosen = unique_method(deposits, "BTC", "BTC", "deposit").expect("on-chain Bitcoin");
        assert_eq!(chosen.method, "Bitcoin");

        // Asking for ETH must never select the kBTC-on-Ethereum wrapper.
        assert!(unique_method(btc, "BTC", "ETH", "withdrawal").is_err());
    }

    /// The real pairing that failed liquidation 1615: Kraken had stored the
    /// verified address lowercased, the pipeline held the EIP-55 checksummed
    /// form, and the exact comparison reported a verified destination as
    /// unverified after the collateral was already seized.
    #[test]
    fn a_checksummed_destination_matches_its_lowercased_stored_address() {
        assert!(addresses_match(
            "0xa21522b91e8ed11a9afcc09f718319277b75c381",
            "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
        ));
    }

    #[test]
    fn a_different_hex_address_still_does_not_match() {
        assert!(!addresses_match(
            "0xd9a5b3a87e971e09c30829206506b4cc0548984c",
            "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
        ));
    }

    /// Base58 encodes different values in `A` and `a`, so folding case there
    /// could match an address the operator never verified.
    #[test]
    fn non_hex_addresses_are_compared_exactly() {
        assert!(addresses_match(
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
        ));
        assert!(!addresses_match(
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
            "1bvbmseystwetqtfn5au4m4gfg7xjanvn2"
        ));
    }

    fn btc_usd_pair() -> KrakenPair {
        KrakenPair {
            api_name: "XXBTZUSD".into(),
            base: "BTC".into(),
            quote: "USD".into(),
            price_decimals: 1,
            lot_decimals: 8,
            order_min: Decimal::new(1, 4),
            cost_min: Decimal::new(5, 0),
            taker_fee_bps: 40.0,
            online: true,
        }
    }

    #[test]
    fn canonicalizes_kraken_btc_and_market_separators() {
        assert_eq!(api_asset("BTC"), "XBT");
        assert_eq!(normalize_market("xbt/usd"), "BTC_USD");
        assert_eq!(normalize_market("xxbt-usd"), "BTC_USD");
    }

    #[test]
    fn bitcoin_network_does_not_select_lightning() {
        let methods = vec![
            KrakenFundingMethod {
                method: "Bitcoin".into(),
                network: Some("Bitcoin".into()),
                minimum: Decimal::ZERO,
            },
            KrakenFundingMethod {
                method: "Lightning".into(),
                network: Some("Lightning".into()),
                minimum: Decimal::ZERO,
            },
        ];
        assert_eq!(
            unique_method(methods, "BTC", "bitcoin", "deposit").unwrap().method,
            "Bitcoin"
        );
    }

    #[tokio::test]
    async fn translates_pair_and_orderbook_without_exposing_kraken_types() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_orderbook()
            .withf(|pair, depth| pair == "XXBTZUSD" && *depth == 25)
            .once()
            .returning(|_, _| {
                Ok(KrakenBook {
                    bids: vec![KrakenBookLevel {
                        price: Decimal::new(60_000, 0),
                        quantity: Decimal::new(2, 0),
                    }],
                    asks: vec![KrakenBookLevel {
                        price: Decimal::new(60_010, 0),
                        quantity: Decimal::new(3, 0),
                    }],
                })
            });
        let client = KrakenClient::with_api(Arc::new(api), vec!["BTC_USD".into()]);

        let book = client
            .get_orderbook("btc/usd", Some(25))
            .await
            .expect("normalized order book");

        assert_eq!(book.bids[0].price, 60_000.0);
        assert_eq!(book.bids[0].quantity, 2.0);
        assert_eq!(book.asks[0].price, 60_010.0);
    }

    #[tokio::test]
    async fn funding_uses_existing_deposit_address_and_exact_verified_withdrawal() {
        let mut api = MockKrakenApi::new();
        api.expect_deposit_methods()
            .withf(|asset| asset == "XBT")
            .times(2)
            .returning(|_| {
                Ok(vec![KrakenFundingMethod {
                    method: "Bitcoin".into(),
                    network: Some("Bitcoin".into()),
                    minimum: Decimal::ZERO,
                }])
            });
        api.expect_deposit_addresses()
            .withf(|asset, method| asset == "XBT" && method == "Bitcoin")
            .times(2)
            .returning(|_, _| {
                Ok(vec![KrakenDepositAddress {
                    address: "bc1qdeposit".into(),
                    tag: Some("memo-7".into()),
                }])
            });
        api.expect_withdrawal_methods()
            .withf(|asset| asset == "XBT")
            .once()
            .returning(|_| {
                Ok(vec![KrakenFundingMethod {
                    method: "Bitcoin".into(),
                    network: Some("Bitcoin".into()),
                    minimum: Decimal::ZERO,
                }])
            });
        api.expect_withdrawal_addresses()
            .withf(|asset| asset == "XBT")
            .once()
            .returning(|_| {
                Ok(vec![KrakenWithdrawalAddress {
                    address: "bc1qdestination".into(),
                    method: "Bitcoin".into(),
                    key: "verified-key".into(),
                    verified: true,
                }])
            });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let deposit = client
            .get_deposit_address("BTC", "bitcoin")
            .await
            .expect("existing address");
        assert_eq!(deposit.address, "bc1qdeposit");
        assert_eq!(deposit.tag.as_deref(), Some("memo-7"));
        client
            .validate_funding_route(&FundingRoutePreflight {
                deposit_asset: "BTC".into(),
                deposit_network: "bitcoin".into(),
                withdraw_asset: "BTC".into(),
                withdraw_network: "bitcoin".into(),
                withdraw_address: "bc1qdestination".into(),
                deposit_amount: 0.1,
                withdraw_amount: 0.1,
            })
            .await
            .expect("verified route");
    }

    #[tokio::test]
    async fn market_sell_attaches_client_id_and_reports_net_fill() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_closed_order_by_client_id().returning(|_| Ok(None));
        api.expect_place_market_order()
            .withf(|pair, side, volume, client_id| {
                pair == "XXBTZUSD"
                    && side == "sell"
                    && *volume == Decimal::new(1, 1)
                    && client_id.as_deref() == Some("liq-42")
            })
            .once()
            .returning(|_, _, _, _| Ok("ORDER-1".into()));
        api.expect_orderbook().once().returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::new(1, 0),
                }],
                asks: vec![],
            })
        });
        api.expect_order().withf(|id| id == "ORDER-1").once().returning(|_| {
            Ok(KrakenOrder {
                status: KrakenOrderStatus::Closed,
                volume_executed: Decimal::new(1, 1),
                cost: Decimal::new(6_000, 0),
                fee: Decimal::new(24, 0),
            })
        });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let report = client
            .execute_swap_detailed_with_options(
                "BTC_USD",
                "sell",
                0.1,
                SwapExecutionOptions {
                    client_order_id: Some("liq-42".into()),
                    ..SwapExecutionOptions::default()
                },
            )
            .await
            .expect("filled sell");

        assert_eq!(report.input_consumed, 0.1);
        // 6000 quote less the 24 fee, less the rounding margin that keeps a
        // reported fill from ever exceeding what the venue credited.
        assert!(
            report.output_received <= 5_976.0 && report.output_received > 5_976.0 * (1.0 - 1e-5),
            "a sell must report at most the quote the venue credited: {}",
            report.output_received
        );
    }

    /// Liquidation 1615's recovery path: the leg resumes still holding the
    /// client id it submitted under, and Kraken has that order settled. It must
    /// adopt the existing fill rather than sell the same collateral twice.
    #[tokio::test]
    async fn a_resumed_leg_adopts_the_order_it_already_submitted() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_closed_order_by_client_id()
            .withf(|id| id == "lqd39ae79815a051f7")
            .once()
            .returning(|_| {
                Ok(Some(KrakenOrder {
                    status: KrakenOrderStatus::Closed,
                    volume_executed: Decimal::new(1, 1),
                    cost: Decimal::new(6_000, 0),
                    fee: Decimal::new(24, 0),
                }))
            });
        // The point of the test: no order is placed on the resumed attempt.
        api.expect_place_market_order().never();
        api.expect_orderbook().returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::new(1, 0),
                }],
                asks: vec![],
            })
        });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let report = client
            .execute_swap_detailed_with_options(
                "BTC_USD",
                "sell",
                0.1,
                SwapExecutionOptions {
                    client_order_id: Some("lqd39ae79815a051f7".into()),
                    ..SwapExecutionOptions::default()
                },
            )
            .await
            .expect("the settled order must be adopted");

        assert!((report.input_consumed - 0.1).abs() < 1e-9);
        // 6000 quote less the 24 fee, less the rounding margin that keeps a
        // reported fill from ever exceeding what the venue credited.
        assert!(
            report.output_received <= 5_976.0 && report.output_received > 5_976.0 * (1.0 - 1e-5),
            "a sell must report at most the quote the venue credited: {}",
            report.output_received
        );
    }

    /// A lookup that never succeeds stays ambiguous: the order may have moved
    /// funds, and nothing here can prove otherwise. The paused clock steps over
    /// the poll interval so exhausting the budget costs no real time.
    #[tokio::test(start_paused = true)]
    async fn an_order_that_never_becomes_readable_is_ambiguous() {
        let mut api = MockKrakenApi::new();
        api.expect_order()
            .times(ORDER_POLL_ATTEMPTS)
            .returning(|_| Err(KrakenApiError::Transport("status request timed out".into())));
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client.wait_for_order("ORDER-2", "sell").await.expect_err("must park");

        assert!(matches!(
            client.classify_submission_error(&error),
            CexSubmissionError::Ambiguous(_)
        ));
    }

    /// A request that never reached Kraken submitted nothing, so it must not be
    /// parked alongside the submissions whose outcome is genuinely unknown.
    #[test]
    fn an_unreachable_kraken_is_not_an_ambiguous_submission() {
        let client = KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![]);

        for rendered in [
            api_submit_error(KrakenApiError::Unreachable("dns error".into())),
            api_read_error(KrakenApiError::Unreachable("dns error".into())),
            api_reconciliation_error(KrakenApiError::Unreachable("dns error".into())),
        ] {
            assert!(
                matches!(
                    client.classify_submission_error(&rendered),
                    CexSubmissionError::Unreachable(_)
                ),
                "{rendered}"
            );
        }

        // A transport failure that may have arrived keeps its ambiguity.
        let timed_out = api_submit_error(KrakenApiError::Transport("operation timed out".into()));
        assert!(matches!(
            client.classify_submission_error(&timed_out),
            CexSubmissionError::Ambiguous(_)
        ));
    }

    /// `execute_swap_detailed` flattens a classification into its message and
    /// this classifier reads it back, so that pass has to be idempotent: a
    /// pending settlement must not gain a second marker each time round.
    #[test]
    fn flattening_a_pending_settlement_and_classifying_it_again_is_idempotent() {
        let client = KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![]);
        let original = CexSubmissionError::PendingSettlement("order OJKJQE-YONE3-2QEYQ2 has not settled".to_string());

        let flattened = original.to_string();
        let reclassified = client.classify_submission_error(&flattened);

        assert_eq!(reclassified, original);
        assert_eq!(reclassified.to_string(), flattened);
    }

    /// Liquidation 1615: Kraken accepted the order, returned its id, then could
    /// not read it back on the first attempt. The order had in fact filled, so
    /// bailing there parked a completed sell for an operator. The read must be
    /// retried to its deadline instead.
    #[tokio::test(start_paused = true)]
    async fn a_briefly_unreadable_order_is_polled_rather_than_parked() {
        let mut api = MockKrakenApi::new();
        let mut reads = 0;
        api.expect_order().times(2).returning(move |_| {
            reads += 1;
            if reads == 1 {
                return Err(KrakenApiError::Transport(
                    "order OJKJQE-YONE3-2QEYQ2 was not returned".into(),
                ));
            }
            Ok(KrakenOrder {
                status: KrakenOrderStatus::Closed,
                volume_executed: Decimal::new(748_710_104, 8),
                cost: Decimal::new(1_563_307, 5),
                fee: Decimal::new(12_506, 5),
            })
        });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let report = client
            .wait_for_order("OJKJQE-YONE3-2QEYQ2", "sell")
            .await
            .expect("a filled order must be reported, not parked");

        assert!((report.input_consumed - 7.48710104).abs() < 1e-8);
        assert!(report.output_received > 0.0);
    }

    #[tokio::test]
    async fn withdrawal_below_method_minimum_is_rejected_before_submission() {
        let mut api = MockKrakenApi::new();
        api.expect_withdrawal_methods().once().returning(|_| {
            Ok(vec![KrakenFundingMethod {
                method: "Bitcoin".into(),
                network: Some("Bitcoin".into()),
                minimum: Decimal::new(1, 2),
            }])
        });
        api.expect_withdrawal_addresses().once().returning(|_| {
            Ok(vec![KrakenWithdrawalAddress {
                address: "bc1qdestination".into(),
                method: "Bitcoin".into(),
                key: "verified-key".into(),
                verified: true,
            }])
        });
        api.expect_withdraw().never();
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client
            .withdraw("BTC", "bitcoin", "bc1qdestination", 0.001)
            .await
            .expect_err("below minimum");

        assert!(error.contains("below Bitcoin minimum"));
        // The finalizer writes this residual off as dust rather than retrying a
        // withdrawal Kraken will never accept, and it recognises it by this
        // marker, so the refusal actually raised here has to carry it.
        assert!(error.contains(KRAKEN_WITHDRAW_BELOW_MIN));
    }

    #[tokio::test]
    async fn funding_preflight_rejects_below_minimum_deposit_before_planning() {
        let mut api = MockKrakenApi::new();
        api.expect_deposit_methods().once().returning(|_| {
            Ok(vec![KrakenFundingMethod {
                method: "Bitcoin".into(),
                network: Some("Bitcoin".into()),
                minimum: Decimal::new(1, 2),
            }])
        });
        api.expect_deposit_addresses().once().returning(|_, _| {
            Ok(vec![KrakenDepositAddress {
                address: "bc1qdeposit".into(),
                tag: None,
            }])
        });
        api.expect_withdrawal_methods().never();
        api.expect_withdrawal_addresses().never();
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client
            .validate_funding_route(&FundingRoutePreflight {
                deposit_asset: "BTC".into(),
                deposit_network: "bitcoin".into(),
                withdraw_asset: "BTC".into(),
                withdraw_network: "bitcoin".into(),
                withdraw_address: "bc1qdestination".into(),
                deposit_amount: 0.001,
                withdraw_amount: 0.1,
            })
            .await
            .expect_err("deposit minimum must reject preview");

        assert!(error.contains("deposit amount"));
        assert!(error.contains("below Bitcoin minimum"));
    }

    #[tokio::test]
    async fn trade_preflight_rejects_pair_minimum_before_submission() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_place_market_order().never();
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client
            .validate_trade_amounts("BTC_USD", "sell", 0.000_01, 1.0)
            .await
            .expect_err("order minimum must reject preview");

        assert!(error.contains("below order minimum"));
    }

    /// The sizing walk skips levels it cannot buy from, so the repricing that
    /// checks the spend cap has to skip them too. Counting a zero-priced level
    /// as volume filled for nothing reports a cost of zero, and a cap check
    /// against zero passes whatever the order actually costs.
    #[test]
    fn an_untradable_level_never_fills_volume_for_free() {
        let book = KrakenBook {
            bids: vec![
                KrakenBookLevel {
                    price: Decimal::ZERO,
                    quantity: Decimal::ONE,
                },
                KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::ONE,
                },
            ],
            asks: vec![
                KrakenBookLevel {
                    price: Decimal::ZERO,
                    quantity: Decimal::ONE,
                },
                KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::ONE,
                },
            ],
        };

        // Priced against the real level, not absorbed by the junk one.
        assert_eq!(
            estimate_buy_cost(&book, Decimal::new(5, 1)).expect("half a coin is covered"),
            Decimal::new(30_000, 0)
        );
        assert_eq!(
            estimate_sell_output(&book, Decimal::new(5, 1)).expect("half a coin is covered"),
            Decimal::new(30_000, 0)
        );

        // And once the priced depth runs out, that is a liquidity failure rather
        // than a free fill.
        assert!(estimate_buy_cost(&book, Decimal::new(15, 1)).is_err());
        assert!(estimate_sell_output(&book, Decimal::new(15, 1)).is_err());
    }

    /// Order-book levels are copied from Kraken's response without validation,
    /// and `Decimal` panics rather than erring when asked to divide by zero. A
    /// zero-priced ask would therefore take the whole process down mid-buy, so
    /// the sizing walk has to step over it and size on the depth it can buy.
    #[tokio::test]
    async fn a_zero_priced_ask_is_stepped_over_rather_than_dividing_by_it() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_closed_order_by_client_id().returning(|_| Ok(None));
        api.expect_orderbook().once().returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![],
                asks: vec![
                    KrakenBookLevel {
                        price: Decimal::ZERO,
                        quantity: Decimal::ONE,
                    },
                    KrakenBookLevel {
                        price: Decimal::new(60_000, 0),
                        quantity: Decimal::ONE,
                    },
                ],
            })
        });
        // Sized purely from the priced level, exactly as if the junk one were
        // not in the book at all.
        api.expect_place_market_order()
            .withf(|pair, side, volume, _| pair == "XXBTZUSD" && side == "buy" && *volume == Decimal::new(9_960_159, 8))
            .once()
            .returning(|_, _, _, _| Ok("ORDER-BUY".into()));
        api.expect_order().once().returning(|_| {
            Ok(KrakenOrder {
                status: KrakenOrderStatus::Closed,
                volume_executed: Decimal::new(9_960_159, 8),
                cost: Decimal::new(59_760_954, 4),
                fee: Decimal::new(239_043_816, 7),
            })
        });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        client
            .execute_swap_detailed_with_options(
                "BTC_USD",
                "buy",
                6_000.0,
                SwapExecutionOptions {
                    client_order_id: Some("liq-buy-zero".into()),
                    buy_mode: BuyOrderInputMode::BaseQuantity,
                    max_quote_overspend_bps: Some(0.0),
                },
            )
            .await
            .expect("a malformed level must not stop an otherwise fillable buy");
    }

    #[tokio::test]
    async fn market_buy_derives_precision_safe_base_volume_from_quote_budget() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_closed_order_by_client_id().returning(|_| Ok(None));
        api.expect_orderbook().once().returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![],
                asks: vec![KrakenBookLevel {
                    price: Decimal::new(60_000, 0),
                    quantity: Decimal::ONE,
                }],
            })
        });
        api.expect_place_market_order()
            .withf(|pair, side, volume, client_id| {
                pair == "XXBTZUSD"
                    && side == "buy"
                    && *volume == Decimal::new(9_960_159, 8)
                    && client_id.as_deref() == Some("liq-buy-1")
            })
            .once()
            .returning(|_, _, _, _| Ok("ORDER-BUY".into()));
        api.expect_order().once().returning(|_| {
            Ok(KrakenOrder {
                status: KrakenOrderStatus::Closed,
                volume_executed: Decimal::new(9_960_159, 8),
                cost: Decimal::new(59_760_954, 4),
                fee: Decimal::new(239_043_816, 7),
            })
        });
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let report = client
            .execute_swap_detailed_with_options(
                "BTC_USD",
                "buy",
                6_000.0,
                SwapExecutionOptions {
                    client_order_id: Some("liq-buy-1".into()),
                    buy_mode: BuyOrderInputMode::BaseQuantity,
                    max_quote_overspend_bps: Some(0.0),
                },
            )
            .await
            .expect("filled buy");

        // The fee is 40 bps of the order, and a buy pays out base, so it is 40
        // bps of the executed volume that never reaches the balance -- not a
        // quote-side surcharge on top of what was spent.
        assert!((report.input_consumed - 5_976.095_4).abs() < 1e-9);
        let credited = 0.099_601_59 * 0.996;
        assert!(
            report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
            "a buy must report at most the base the venue credited: {} against {credited}",
            report.output_received
        );
    }

    /// The shape that stranded liquidation 1634: Kraken reports the fee in USD,
    /// takes it out of the ETH, and the leg then tries to withdraw what it
    /// believes it received. The received amount must already be net, or the
    /// withdrawal asks for more ETH than the account holds -- forever, since a
    /// retry changes neither the balance nor the request.
    #[test]
    fn a_market_buy_reports_the_base_it_can_actually_withdraw() {
        let report = fill_report(
            "buy",
            Decimal::new(3_532_234, 8),   // 0.03532234 ETH executed
            Decimal::new(6_707_536, 5),   // 67.07536 USD cost
            Decimal::new(5_366, 4),       // 0.53660 USD fee, charged in ETH
        )
        .expect("buy fill");

        // What Kraken credited: 0.03532234 less the 0.80% it withheld in ETH.
        // The balance on the account that day was 0.0350477408, dust included.
        let credited = 0.035_039_762_8;
        assert!(
            report.output_received <= credited && report.output_received > credited * (1.0 - 1e-5),
            "the withdrawal must never ask for more than this: {} against {credited}",
            report.output_received
        );
        assert!((report.input_consumed - 67.075_36).abs() < 1e-9);
    }

    #[tokio::test]
    async fn sell_below_cost_minimum_is_rejected_before_submission() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
        api.expect_orderbook().once().returning(|_, _| {
            Ok(KrakenBook {
                bids: vec![KrakenBookLevel {
                    price: Decimal::TEN,
                    quantity: Decimal::ONE,
                }],
                asks: vec![],
            })
        });
        api.expect_place_market_order().never();
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client
            .execute_swap_detailed("BTC_USD", "sell", 0.1)
            .await
            .expect_err("below cost minimum");

        assert!(error.contains("below cost minimum"));
    }

    #[tokio::test]
    async fn offline_and_ambiguous_pairs_are_rejected_locally() {
        let mut offline = btc_usd_pair();
        offline.online = false;
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().return_once(move || Ok(vec![offline]));
        let client = KrakenClient::with_api(Arc::new(api), vec![]);
        assert!(
            client
                .get_orderbook("BTC_USD", None)
                .await
                .unwrap_err()
                .contains("not online")
        );

        let pair = btc_usd_pair();
        let mut api = MockKrakenApi::new();
        api.expect_pairs()
            .once()
            .return_once(move || Ok(vec![pair.clone(), pair]));
        let client = KrakenClient::with_api(Arc::new(api), vec![]);
        assert!(
            client
                .get_orderbook("BTC_USD", None)
                .await
                .unwrap_err()
                .contains("ambiguous")
        );
    }
}
