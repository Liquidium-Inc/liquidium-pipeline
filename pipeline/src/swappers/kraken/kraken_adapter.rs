//! [`KrakenClient`]: Kraken behind the venue-neutral [`CexBackend`].
//!
//! This file holds what every Kraken operation shares -- the client itself,
//! the market allowlist and pair lookup, the mapping of API errors onto the
//! shared classification -- and the `CexBackend` impl, whose methods only
//! delegate. The work is split by concern into two child modules so each can
//! be audited on its own: `kraken_trading` (quotes, preflight, order
//! sizing, submission and reconciliation) and `kraken_funding` (deposit and
//! withdrawal method resolution, funding preflight, withdrawals).

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CEX_VENUE_UNREACHABLE_PREFIX, CexBackend, CexSubmissionError, CexWithdrawError, DepositAddress,
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
    KrakenTransferStatus,
};
use super::kraken_rest::KrakenRestApi;

#[path = "kraken_funding.rs"]
mod kraken_funding;
#[path = "kraken_trading.rs"]
mod kraken_trading;

const KRAKEN_AMBIGUOUS_PREFIX: &str = "kraken ambiguous submission: ";
const PAIR_METADATA_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub struct KrakenClient {
    api: Arc<dyn KrakenApi>,
    pairs: Arc<RwLock<Option<(Instant, Vec<KrakenPair>)>>>,
    allowed_pairs: Arc<Vec<String>>,
}

impl KrakenClient {
    /// Builds a client that trades only the markets in `allowed_pairs`.
    ///
    /// An empty allowlist is refused rather than read as "no restriction":
    /// there is no reason to route through every market Kraken lists, and a
    /// setting that went missing must not quietly become one.
    pub fn new(api_key: String, api_secret: String, allowed_pairs: Vec<String>) -> Result<Self, String> {
        Self::with_api(Arc::new(KrakenRestApi::new(api_key, api_secret)), allowed_pairs)
    }

    pub fn with_api(api: Arc<dyn KrakenApi>, allowed_pairs: Vec<String>) -> Result<Self, String> {
        let allowed_pairs = allowed_pairs
            .into_iter()
            .map(|pair| normalize_market(&pair))
            .filter(|pair| !pair.is_empty())
            .collect::<Vec<_>>();
        if allowed_pairs.is_empty() {
            return Err("Kraken needs an explicit market allowlist; an empty one is not \"no markets\" and must not mean \"every market\"".to_string());
        }
        Ok(Self {
            api,
            pairs: Arc::new(RwLock::new(None)),
            allowed_pairs: Arc::new(allowed_pairs),
        })
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
            .filter(|_| self.allowed_pairs.iter().any(|allowed| allowed == &market))
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
        self.preflight_funding_route(preflight).await
    }

    async fn validate_trade_amounts(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> Result<(), String> {
        self.preflight_trade_amounts(market, side, amount_in, amount_out).await
    }

    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        self.quote_sell(market, amount_in).await
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

    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, CexSubmissionError> {
        self.execute_market_order(market, side, amount_in, options).await
    }

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        self.read_orderbook(market, limit).await
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        self.deposit_address(asset, network).await
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, CexWithdrawError> {
        self.submit_withdrawal(asset, network, address, amount).await
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
        self.withdrawal_status(coin, withdraw_id).await
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

/// Fixtures shared by this file's tests and its children's.
#[cfg(test)]
mod test_support {
    use super::*;
    use crate::swappers::kraken::kraken_api::MockKrakenApi;

    pub(super) fn btc_usd_pair() -> KrakenPair {
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

    /// A client allowed to trade the one market these tests use.
    pub(super) fn btc_usd_client(api: MockKrakenApi) -> KrakenClient {
        KrakenClient::with_api(Arc::new(api), vec!["BTC_USD".into()]).expect("a non-empty allowlist")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{btc_usd_client, btc_usd_pair};
    use super::*;
    use crate::swappers::kraken::kraken_api::MockKrakenApi;

    /// The allowlist is the only thing keeping routing off every market Kraken
    /// lists, so a client cannot exist without one; a constructor that read an
    /// empty list as "unrestricted" would hand that behaviour to any caller
    /// that lost the setting.
    #[test]
    fn a_client_cannot_be_built_without_a_market_allowlist() {
        let error = match KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![]) {
            Ok(_) => panic!("an empty allowlist must be refused"),
            Err(error) => error,
        };
        assert!(error.contains("allowlist"), "unexpected error: {error}");
        assert!(KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![" ".into(), "".into()]).is_err());
    }

    #[test]
    fn canonicalizes_kraken_btc_and_market_separators() {
        assert_eq!(api_asset("BTC"), "XBT");
        assert_eq!(normalize_market("xbt/usd"), "BTC_USD");
        assert_eq!(normalize_market("xxbt-usd"), "BTC_USD");
    }

    #[tokio::test]
    async fn offline_and_ambiguous_pairs_are_rejected_locally() {
        let mut offline = btc_usd_pair();
        offline.online = false;
        let mut api = MockKrakenApi::new();
        api.expect_pairs().once().return_once(move || Ok(vec![offline]));
        let client = btc_usd_client(api);
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
        let client = btc_usd_client(api);
        assert!(
            client
                .get_orderbook("BTC_USD", None)
                .await
                .unwrap_err()
                .contains("ambiguous")
        );
    }

    /// A request that never reached Kraken submitted nothing, so it must not be
    /// parked alongside the submissions whose outcome is genuinely unknown.
    #[test]
    fn an_unreachable_kraken_is_not_an_ambiguous_submission() {
        let client = btc_usd_client(MockKrakenApi::new());

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
        let client = btc_usd_client(MockKrakenApi::new());
        let original = CexSubmissionError::PendingSettlement("order OJKJQE-YONE3-2QEYQ2 has not settled".to_string());

        let flattened = original.to_string();
        let reclassified = client.classify_submission_error(&flattened);

        assert_eq!(reclassified, original);
        assert_eq!(reclassified.to_string(), flattened);
    }
}
