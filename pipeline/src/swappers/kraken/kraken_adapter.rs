use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CEX_PENDING_SETTLEMENT_PREFIX, CexBackend, CexSubmissionError, DepositAddress,
    FundingRoutePreflight, OrderBook, OrderBookLevel, SwapExecutionOptions, SwapFillReport, WithdrawStatus,
    WithdrawStatusSnapshot, WithdrawalReceipt,
};
use rust_decimal::{
    Decimal, RoundingStrategy,
    prelude::{FromPrimitive, ToPrimitive},
};
use tokio::sync::RwLock;

use super::kraken_api::{
    KrakenApi, KrakenApiError, KrakenBook, KrakenFundingMethod, KrakenOrderStatus, KrakenPair, KrakenRestApi,
    KrakenTransferStatus,
};

const KRAKEN_AMBIGUOUS_PREFIX: &str = "kraken ambiguous submission: ";
const ORDER_POLL_ATTEMPTS: usize = 20;
const ORDER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const PAIR_METADATA_TTL: Duration = Duration::from_secs(5 * 60);

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
            .filter(|entry| entry.verified && entry.method == method.method && entry.address == destination)
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
        for attempt in 0..ORDER_POLL_ATTEMPTS {
            let order = self.api.order(order_id).await.map_err(api_reconciliation_error)?;
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
        Err(format!(
            "{KRAKEN_AMBIGUOUS_PREFIX}order {order_id} did not reach a terminal state"
        ))
    }
}

#[async_trait]
impl CexBackend for KrakenClient {
    fn classify_submission_error(&self, error: &str) -> CexSubmissionError {
        if error.starts_with(KRAKEN_AMBIGUOUS_PREFIX) {
            CexSubmissionError::Ambiguous(error.to_string())
        } else if error.starts_with(CEX_PENDING_SETTLEMENT_PREFIX) {
            CexSubmissionError::PendingSettlement(error.to_string())
        } else {
            CexSubmissionError::Rejected(error.to_string())
        }
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
        self.execute_swap_detailed_with_options(market, side, amount_in, SwapExecutionOptions::default())
            .await
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
    ) -> Result<SwapFillReport, String> {
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
                ));
            }

            // Submit the precision-safe base volume produced above.
            base
        } else if side.eq_ignore_ascii_case("buy") {
            // The shared quote-quantity mode cannot be sent directly because this
            // Kraken endpoint expects market-buy volume in base units.
            if options.buy_mode == BuyOrderInputMode::QuoteOrderQty {
                return Err("Kraken market buys require precision-safe base quantity mode".to_string());
            }

            // Reject malformed pair metadata before using its fee in budget math.
            if !pair.taker_fee_bps.is_finite() || !(0.0..=10_000.0).contains(&pair.taker_fee_bps) {
                return Err(format!("invalid Kraken taker fee {} bps", pair.taker_fee_bps));
            }

            // Default to no extra spending when the caller did not authorize an
            // overspend allowance for base-quantity buy sizing.
            let cap_bps = options.max_quote_overspend_bps.unwrap_or(0.0);

            // Keep the overspend cap within a finite zero-to-100% bps range.
            if !cap_bps.is_finite() || !(0.0..=10_000.0).contains(&cap_bps) {
                return Err(format!("invalid Kraken quote overspend cap {cap_bps} bps"));
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
                return Err("not enough Kraken ask liquidity".to_string());
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
                ));
            }

            // Submit the precision-safe base volume derived from the quote budget.
            base
        } else {
            // Only the two spot sides understood by Kraken are accepted locally.
            return Err(format!("unsupported Kraken order side {side}"));
        };

        // Apply Kraken's minimum base-order volume after all precision truncation.
        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            ));
        }

        // For buys, the route input is the quote budget and must independently
        // satisfy Kraken's minimum quote cost.
        if side.eq_ignore_ascii_case("buy") && amount < pair.cost_min {
            return Err(format!(
                "Kraken quote budget {amount} is below cost minimum {}",
                pair.cost_min
            ));
        }

        // Submit exactly one market order, carrying the persisted client order ID
        // that the shared CEX recovery logic prepared before this side effect.
        let order_id = self
            .api
            .place_market_order(&pair.api_name, side, base_volume, options.client_order_id)
            .await
            .map_err(api_submit_error)?;

        // Poll the returned Kraken order ID to a terminal state and translate its
        // consumed input, received output, and fees into the shared fill report.
        self.wait_for_order(&order_id, side).await
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
                "Kraken withdrawal amount {amount_decimal} is below {} minimum {}",
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
fn api_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "BTC" => "XBT".to_string(),
        value => value.to_string(),
    }
}

fn network_matches(asset: &str, requested: &str, method: &KrakenFundingMethod) -> bool {
    let requested = requested.to_ascii_lowercase();
    let descriptor =
        format!("{} {}", method.method, method.network.as_deref().unwrap_or_default()).to_ascii_lowercase();
    match asset.trim().to_ascii_uppercase().as_str() {
        "BTC" => {
            (requested.contains("btc") || requested.contains("bitcoin"))
                && descriptor.contains("bitcoin")
                && !descriptor.contains("lightning")
        }
        "ETH" => {
            (requested.contains("eth") || requested.contains("erc20"))
                && (descriptor.contains("ethereum") || descriptor.contains("erc20"))
        }
        "USDC" | "USDT" => {
            descriptor.contains(&requested) || (descriptor.contains("ethereum") && requested.contains("erc20"))
        }
        _ => descriptor.contains(&requested),
    }
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

fn estimate_buy_cost(book: &KrakenBook, base_volume: Decimal) -> Result<Decimal, String> {
    let mut remaining = base_volume;
    let mut cost = Decimal::ZERO;
    for level in &book.asks {
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

fn fill_report(side: &str, executed: Decimal, cost: Decimal, fee: Decimal) -> Result<SwapFillReport, String> {
    let executed = decimal_f64(executed, "executed volume")?;
    let cost = decimal_f64(cost, "order cost")?;
    let fee = decimal_f64(fee, "order fee")?;
    if side.eq_ignore_ascii_case("sell") {
        Ok(SwapFillReport {
            input_consumed: executed,
            output_received: (cost - fee).max(0.0),
        })
    } else {
        Ok(SwapFillReport {
            input_consumed: cost + fee,
            output_received: executed,
        })
    }
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
    error.to_string()
}
fn api_reconciliation_error(error: KrakenApiError) -> String {
    format!("{KRAKEN_AMBIGUOUS_PREFIX}could not reconcile accepted order: {error}")
}
fn api_submit_error(error: KrakenApiError) -> String {
    match error {
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
        assert_eq!(report.output_received, 5_976.0);
    }

    #[tokio::test]
    async fn accepted_order_with_failed_status_query_is_ambiguous() {
        let mut api = MockKrakenApi::new();
        api.expect_order()
            .once()
            .returning(|_| Err(KrakenApiError::Transport("status request timed out".into())));
        let client = KrakenClient::with_api(Arc::new(api), vec![]);

        let error = client.wait_for_order("ORDER-2", "sell").await.expect_err("must park");

        assert!(matches!(
            client.classify_submission_error(&error),
            CexSubmissionError::Ambiguous(_)
        ));
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

    #[tokio::test]
    async fn market_buy_derives_precision_safe_base_volume_from_quote_budget() {
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().returning(|| Ok(vec![btc_usd_pair()]));
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

        assert!((report.input_consumed - 5_999.999_781_6).abs() < 1e-9);
        assert_eq!(report.output_received, 0.099_601_59);
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
