use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CEX_PENDING_SETTLEMENT_PREFIX, CexBackend, CexSubmissionError, DepositAddress, OrderBook,
    OrderBookLevel, SwapExecutionOptions, SwapFillReport, WithdrawStatus, WithdrawStatusSnapshot, WithdrawalReceipt,
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

#[derive(Clone)]
pub struct KrakenClient {
    api: Arc<dyn KrakenApi>,
    pairs: Arc<RwLock<Option<Vec<KrakenPair>>>>,
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
        if let Some(pairs) = self.pairs.read().await.as_ref() {
            return Ok(pairs.clone());
        }
        let pairs = self.api.pairs().await.map_err(api_read_error)?;
        *self.pairs.write().await = Some(pairs.clone());
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

    async fn resolve_deposit(&self, asset: &str, network: &str) -> Result<(String, String, Option<String>), String> {
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
            [address] => Ok((method.method, address.address.clone(), address.tag.clone())),
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

    async fn validate_funding_route(
        &self,
        deposit_asset: &str,
        deposit_network: &str,
        withdraw_asset: &str,
        withdraw_network: &str,
        withdraw_address: &str,
    ) -> Result<(), String> {
        self.resolve_deposit(deposit_asset, deposit_network).await?;
        self.resolve_withdrawal(withdraw_asset, withdraw_network, withdraw_address)
            .await?;
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

    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, String> {
        ensure_positive(amount_in)?;
        let pair = self.pair(market).await?;
        let amount = Decimal::from_f64(amount_in).ok_or_else(|| "invalid Kraken order amount".to_string())?;
        let base_volume = if side.eq_ignore_ascii_case("sell") {
            let base = truncate(amount, pair.lot_decimals);
            let estimated_output = estimate_sell_output(&self.book(&pair, 100).await?, base)?;
            if estimated_output < pair.cost_min {
                return Err(format!(
                    "Kraken sell estimate {estimated_output} is below cost minimum {}",
                    pair.cost_min
                ));
            }
            base
        } else if side.eq_ignore_ascii_case("buy") {
            if options.buy_mode == BuyOrderInputMode::QuoteOrderQty {
                return Err("Kraken market buys require precision-safe base quantity mode".to_string());
            }
            if !pair.taker_fee_bps.is_finite() || !(0.0..=10_000.0).contains(&pair.taker_fee_bps) {
                return Err(format!("invalid Kraken taker fee {} bps", pair.taker_fee_bps));
            }
            let cap_bps = options.max_quote_overspend_bps.unwrap_or(0.0);
            if !cap_bps.is_finite() || !(0.0..=10_000.0).contains(&cap_bps) {
                return Err(format!("invalid Kraken quote overspend cap {cap_bps} bps"));
            }
            let fee_ratio = Decimal::from_f64(pair.taker_fee_bps / 10_000.0).unwrap_or_default();
            let cap_ratio = Decimal::from_f64(cap_bps / 10_000.0).unwrap_or_default();
            let max_spend = amount * (Decimal::ONE + cap_ratio);
            let trade_budget = max_spend / (Decimal::ONE + fee_ratio);
            let book = self.book(&pair, 100).await?;
            let mut budget = trade_budget;
            let mut base = Decimal::ZERO;
            for level in &book.asks {
                if budget <= Decimal::ZERO {
                    break;
                }
                let take = level.quantity.min(budget / level.price);
                base += take;
                budget -= take * level.price;
            }
            if budget > Decimal::from_f64(1e-10).unwrap_or(Decimal::ZERO) {
                return Err("not enough Kraken ask liquidity".to_string());
            }
            let base = truncate(base, pair.lot_decimals);
            let estimated_cost = estimate_buy_cost(&book, base)?;
            let estimated_total = estimated_cost * (Decimal::ONE + fee_ratio);
            if estimated_total > max_spend {
                return Err(format!(
                    "Kraken buy estimate with fee {estimated_total} exceeds quote-input cap {max_spend}"
                ));
            }
            base
        } else {
            return Err(format!("unsupported Kraken order side {side}"));
        };
        if base_volume < pair.order_min {
            return Err(format!(
                "Kraken base volume {base_volume} is below order minimum {}",
                pair.order_min
            ));
        }
        if side.eq_ignore_ascii_case("buy") && amount < pair.cost_min {
            return Err(format!(
                "Kraken quote budget {amount} is below cost minimum {}",
                pair.cost_min
            ));
        }
        let order_id = self
            .api
            .place_market_order(&pair.api_name, side, base_volume, options.client_order_id)
            .await
            .map_err(api_submit_error)?;
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

fn normalize_market(market: &str) -> String {
    market.trim().to_ascii_uppercase().replace(['/', '-'], "_")
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
        assert_eq!(normalize_market("xbt/usd"), "XBT_USD");
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
            .validate_funding_route("BTC", "bitcoin", "BTC", "bitcoin", "bc1qdestination")
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
        api.expect_order().once().returning(|_| {
            Err(KrakenApiError::Transport("status request timed out".into()))
        });
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
        assert!(client.get_orderbook("BTC_USD", None).await.unwrap_err().contains("not online"));

        let pair = btc_usd_pair();
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().return_once(move || Ok(vec![pair.clone(), pair]));
        let client = KrakenClient::with_api(Arc::new(api), vec![]);
        assert!(client.get_orderbook("BTC_USD", None).await.unwrap_err().contains("ambiguous"));
    }
}
