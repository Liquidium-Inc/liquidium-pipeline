//! The live [`KrakenApi`] over `kraken_async_rs`. No `kraken_async_rs` type
//! leaves this file.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;
use kraken_async_rs::{
    clients::{
        core_kraken_client::CoreKrakenClient,
        errors::{ClientError, KrakenError},
        http_response_types::ResultErrorResponse,
        kraken_client::KrakenClient,
        rate_limited_kraken_client::RateLimitedKrakenClient,
    },
    crypto::nonce_provider::{IncreasingNonceProvider, NonceProvider},
    request_types::{
        AddOrderRequest, ClosedOrdersRequest, DepositAddressesRequest, DepositMethodsRequest, OrderFlags, OrderRequest,
        OrderbookRequest, StatusOfDepositWithdrawRequest, StringCSV, TimeInForce, TradableAssetPairsRequest,
        WithdrawFundsRequest, WithdrawalAddressesRequest, WithdrawalInfoRequest, WithdrawalMethodsRequest,
    },
    response_types::{BuySell, OrderFlag, OrderStatus, OrderType, TradableAssetStatus, TransferStatus},
    secrets::secrets_provider::{Secrets, SecretsProvider},
};
use rust_decimal::Decimal;
use tokio::sync::Mutex;

use super::api::{
    KrakenApi, KrakenApiError, KrakenBook, KrakenBookLevel, KrakenDepositAddress, KrakenFundingMethod, KrakenOrder,
    KrakenOrderRequest, KrakenOrderStatus, KrakenPair, KrakenSide, KrakenTransferStatus, KrakenWithdrawal,
    KrakenWithdrawalAddress,
};
use super::client::canonical_symbol;

/// Only a failed connection proves the request never left, and only Kraken's
/// own refusal proves it did nothing. A timeout, a broken response or an
/// internal error may come after Kraken acted.
fn transport_error(error: ClientError) -> KrakenApiError {
    match &error {
        ClientError::HyperClient(inner) if inner.is_connect() => KrakenApiError::Unreachable(error.to_string()),
        ClientError::Kraken(KrakenError::InternalError) => KrakenApiError::Transport(error.to_string()),
        ClientError::Kraken(refusal) => KrakenApiError::Rejected(format!("Kraken refused the request: {refusal}")),
        _ => KrakenApiError::Transport(error.to_string()),
    }
}

/// Kraken errors after which the request may still have run.
const UNDECIDED_ERRORS: [&str; 2] = ["EGeneral:Internal error", "EService:Deadline elapsed"];

struct OwnedSecretsProvider {
    key: String,
    secret: String,
}

impl Debug for OwnedSecretsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedSecretsProvider")
            .field("key", &"[redacted]")
            .field("secret", &"[redacted]")
            .finish()
    }
}

impl SecretsProvider for OwnedSecretsProvider {
    fn get_secrets(&mut self) -> Secrets {
        Secrets {
            key: self.key.clone().into(),
            secret: self.secret.clone().into(),
        }
    }
}

type Client = RateLimitedKrakenClient<CoreKrakenClient>;

pub struct KrakenRestApi {
    client: Mutex<Client>,
}

impl Debug for KrakenRestApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KrakenRestApi").finish_non_exhaustive()
    }
}

impl KrakenRestApi {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let secrets: Box<Arc<Mutex<dyn SecretsProvider>>> = Box::new(Arc::new(Mutex::new(OwnedSecretsProvider {
            key: api_key,
            secret: api_secret,
        })));
        let nonce: Box<Arc<Mutex<dyn NonceProvider>>> = Box::new(Arc::new(Mutex::new(IncreasingNonceProvider::new())));
        Self {
            client: Mutex::new(Client::new(secrets, nonce)),
        }
    }

    /// Runs one request under the client lock and reads Kraken's answer.
    async fn call<T>(
        &self,
        request: impl AsyncFnOnce(&mut Client) -> Result<ResultErrorResponse<T>, ClientError>,
    ) -> Result<T, KrakenApiError> {
        let mut client = self.client.lock().await;
        let response = request(&mut client).await.map_err(transport_error)?;
        read_answer(response.error, response.result)
    }
}

/// Kraken marks each `error` entry `E` (the request failed) or `W` (a warning
/// on a request it carried out). A result next to an error is unclear, so the
/// request may have acted.
fn read_answer<T>(errors: Vec<String>, result: Option<T>) -> Result<T, KrakenApiError> {
    let warnings_only = errors.iter().all(|entry| entry.starts_with('W'));
    let undecided = errors
        .iter()
        .any(|entry| UNDECIDED_ERRORS.iter().any(|prefix| entry.starts_with(prefix)));
    match result {
        Some(result) if warnings_only => {
            if !errors.is_empty() {
                log::warn!("[kraken] the answer carries warnings: {}", errors.join(" | "));
            }
            Ok(result)
        }
        None if !warnings_only && !undecided => Err(KrakenApiError::Rejected(errors.join(" | "))),
        result => Err(KrakenApiError::Transport(format!(
            "Kraken gave no clear answer (result {}): [{}]",
            if result.is_some() { "present" } else { "missing" },
            errors.join(" | ")
        ))),
    }
}

/// Kraken keys a one-item answer by its own name for the item, so a lone
/// entry is ours. Several entries without our key belong to something else.
fn take_requested<T>(mut entries: HashMap<String, T>, key: &str) -> Option<T> {
    match entries.remove(key) {
        Some(entry) => Some(entry),
        None if entries.len() == 1 => entries.into_values().next(),
        None => None,
    }
}

fn to_kraken_order(order: kraken_async_rs::response_types::Order) -> KrakenOrder {
    KrakenOrder {
        status: match order.status {
            OrderStatus::Pending => KrakenOrderStatus::Pending,
            OrderStatus::Open => KrakenOrderStatus::Open,
            OrderStatus::Closed => KrakenOrderStatus::Closed,
            OrderStatus::Canceled => KrakenOrderStatus::Canceled,
            OrderStatus::Expired => KrakenOrderStatus::Expired,
        },
        volume_executed: order.volume_executed,
        cost: order.cost,
        fee: order.fee,
    }
}

fn book_levels(levels: Vec<kraken_async_rs::response_types::BidAsk>) -> Vec<KrakenBookLevel> {
    levels
        .into_iter()
        .map(|level| KrakenBookLevel {
            price: level.price,
            quantity: level.volume,
        })
        .collect()
}

#[async_trait]
impl KrakenApi for KrakenRestApi {
    async fn pairs(&self) -> Result<Vec<KrakenPair>, KrakenApiError> {
        let request = TradableAssetPairsRequest::builder().build();
        let response = self
            .call(async |client| client.get_tradable_asset_pairs(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .filter_map(|(api_name, pair)| {
                let (base, quote) = pair.ws_name.split_once('/')?;
                Some(KrakenPair {
                    api_name,
                    base: canonical_symbol(base),
                    quote: canonical_symbol(quote),
                    price_decimals: u32::try_from(pair.pair_decimals).ok()?,
                    lot_decimals: u32::try_from(pair.lot_decimals).ok()?,
                    order_min: pair.order_min,
                    cost_min: pair.cost_min,
                    online: pair.status == TradableAssetStatus::Online,
                })
            })
            .collect())
    }

    async fn orderbook(&self, pair: &str, depth: u32) -> Result<KrakenBook, KrakenApiError> {
        let request = OrderbookRequest::builder(pair.to_string())
            .count(i64::from(depth))
            .build();
        let response = self.call(async |client| client.get_orderbook(&request).await).await?;
        let book = take_requested(response, pair)
            .ok_or_else(|| KrakenApiError::Transport(format!("no order book returned for {pair}")))?;
        Ok(KrakenBook {
            bids: book_levels(book.bids),
            asks: book_levels(book.asks),
        })
    }

    async fn balance(&self, asset: &str) -> Result<Decimal, KrakenApiError> {
        let response = self.call(async |client| client.get_account_balance().await).await?;
        Ok(response
            .iter()
            .find(|(name, _)| canonical_symbol(name) == canonical_symbol(asset))
            .map(|(_, value)| *value)
            .unwrap_or(Decimal::ZERO))
    }

    async fn deposit_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        let request = DepositMethodsRequest::builder(asset.to_string()).build();
        let response = self
            .call(async |client| client.get_deposit_methods(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .map(|method| KrakenFundingMethod {
                method: method.method,
                network: None,
                minimum: method.minimum,
            })
            .collect())
    }

    async fn deposit_addresses(&self, asset: &str, method: &str) -> Result<Vec<KrakenDepositAddress>, KrakenApiError> {
        let request = DepositAddressesRequest::builder(asset.to_string(), method.to_string())
            .is_new(false)
            .build();
        let response = self
            .call(async |client| client.get_deposit_addresses(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .map(|address| KrakenDepositAddress {
                address: address.address,
                tag: address.tag.or(address.memo),
            })
            .collect())
    }

    async fn withdrawal_methods(&self, asset: &str) -> Result<Vec<KrakenFundingMethod>, KrakenApiError> {
        let request = WithdrawalMethodsRequest::builder().asset(asset.to_string()).build();
        let response = self
            .call(async |client| client.get_withdrawal_methods(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .map(|method| KrakenFundingMethod {
                method: method.method,
                network: method.network,
                minimum: method.minimum,
            })
            .collect())
    }

    async fn withdrawal_addresses(&self, asset: &str) -> Result<Vec<KrakenWithdrawalAddress>, KrakenApiError> {
        let request = WithdrawalAddressesRequest::builder()
            .asset(asset.to_string())
            .verified(true)
            .build();
        let response = self
            .call(async |client| client.get_withdrawal_addresses(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .map(|address| KrakenWithdrawalAddress {
                address: address.address,
                method: address.method,
                key: address.key,
                verified: address.verified,
            })
            .collect())
    }

    async fn withdrawal_fee(&self, asset: &str, key: &str, amount: Decimal) -> Result<Decimal, KrakenApiError> {
        let request = WithdrawalInfoRequest::builder(asset.to_string(), key.to_string(), amount).build();
        let response = self
            .call(async |client| client.get_withdrawal_info(&request).await)
            .await?;
        Ok(response.fee)
    }

    async fn place_order(&self, request: &KrakenOrderRequest) -> Result<String, KrakenApiError> {
        // The fee comes from what the order pays out, so the spent side is
        // exactly the order's cost and the fill report can say what arrived.
        let (side, fee_flag) = match request.side {
            KrakenSide::Buy => (BuySell::Buy, OrderFlag::FeesInBase),
            KrakenSide::Sell => (BuySell::Sell, OrderFlag::FeesInQuote),
        };
        let mut builder = AddOrderRequest::builder(OrderType::Limit, side, request.volume, request.pair.clone());
        builder
            .price(request.limit_price)
            .time_in_force(TimeInForce::IOC)
            .order_flags(OrderFlags::new(vec![fee_flag]));
        if let Some(client_order_id) = &request.client_order_id {
            builder.client_order_id(client_order_id.clone());
        }
        let request = builder.build();
        let response = self.call(async |client| client.add_order(&request).await).await?;
        response
            .tx_id
            .into_iter()
            .next()
            .ok_or_else(|| KrakenApiError::Transport("Kraken accepted the order but returned no id".to_string()))
    }

    async fn order(&self, order_id: &str) -> Result<KrakenOrder, KrakenApiError> {
        let request = OrderRequest::builder(StringCSV::new(vec![order_id.to_string()])).build();
        let response = self
            .call(async |client| client.query_orders_info(&request).await)
            .await?;
        let order = take_requested(response, order_id)
            .ok_or_else(|| KrakenApiError::Transport(format!("order {order_id} was not returned")))?;
        Ok(to_kraken_order(order))
    }

    async fn closed_orders_by_client_id(&self, client_order_id: &str) -> Result<Vec<KrakenOrder>, KrakenApiError> {
        let request = ClosedOrdersRequest::builder()
            .client_order_id(client_order_id.to_string())
            .build();
        let response = self
            .call(async |client| client.get_closed_orders(&request).await)
            .await?;
        // The id is checked again here, so a filter Kraken ignored cannot adopt another order's fill.
        Ok(response
            .closed
            .into_values()
            .filter(|order| order.client_order_id.as_deref() == Some(client_order_id))
            .map(to_kraken_order)
            .collect())
    }

    async fn withdraw(&self, asset: &str, key: &str, address: &str, amount: Decimal) -> Result<String, KrakenApiError> {
        let request = WithdrawFundsRequest::builder(asset.to_string(), key.to_string(), amount)
            .address(address.to_string())
            .build();
        let response = self.call(async |client| client.withdraw_funds(&request).await).await?;
        Ok(response.ref_id)
    }

    async fn withdrawals(&self, asset: &str, limit: i64) -> Result<Vec<KrakenWithdrawal>, KrakenApiError> {
        let request = StatusOfDepositWithdrawRequest::builder()
            .asset(asset.to_string())
            .limit(limit)
            .build();
        let response = self
            .call(async |client| client.get_status_of_recent_withdrawals(&request).await)
            .await?;
        Ok(response
            .into_iter()
            .map(|withdrawal| KrakenWithdrawal {
                ref_id: withdrawal.ref_id,
                // Absent while pending, and blank on some assets once present.
                tx_id: withdrawal.tx_id.filter(|tx_id| !tx_id.trim().is_empty()),
                fee: withdrawal.fee,
                status: match withdrawal.status {
                    TransferStatus::Success | TransferStatus::Settled => KrakenTransferStatus::Completed,
                    TransferStatus::Failure => KrakenTransferStatus::Failed,
                    TransferStatus::Initial | TransferStatus::Pending => KrakenTransferStatus::Pending,
                },
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_keyed_response_only_answers_for_the_item_asked_about() {
        let map = |pairs: &[(&str, u8)]| {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), *value))
                .collect::<HashMap<String, u8>>()
        };

        assert_eq!(
            take_requested(map(&[("XXBTZUSD", 1), ("XETHZUSD", 2)]), "XXBTZUSD"),
            Some(1)
        );
        // Kraken's alias for the same item: one entry, so it is ours.
        assert_eq!(take_requested(map(&[("XBTUSD", 1)]), "XXBTZUSD"), Some(1));
        // Other items only: adopting one would reconcile against the wrong row.
        assert_eq!(
            take_requested(map(&[("XETHZUSD", 2), ("XXRPZUSD", 3)]), "XXBTZUSD"),
            None
        );
        assert_eq!(take_requested(map(&[]), "XXBTZUSD"), None);
    }

    /// Review finding 5: a warning comes with a request Kraken carried out, so
    /// reading it as a refusal would send the same withdrawal again.
    #[test]
    fn only_an_error_without_a_result_is_a_refusal() {
        let strings = |entries: &[&str]| entries.iter().map(|entry| entry.to_string()).collect::<Vec<_>>();

        assert_eq!(read_answer(strings(&[]), Some(1)), Ok(1));
        assert_eq!(read_answer(strings(&["WGeneral:Deprecated"]), Some(1)), Ok(1));
        assert!(matches!(
            read_answer::<u8>(strings(&["EFunding:Unknown withdraw key"]), None),
            Err(KrakenApiError::Rejected(_))
        ));
        // Mixed signals, empty answers and undecided errors may follow an action.
        for (errors, result) in [
            (strings(&["EGeneral:Internal error"]), Some(1)),
            (strings(&["EService:Deadline elapsed"]), None),
            (strings(&["WGeneral:Deprecated"]), None),
            (strings(&[]), None),
        ] {
            assert!(
                matches!(read_answer(errors, result), Err(KrakenApiError::Transport(_))),
                "may have acted"
            );
        }
    }

    /// Review finding 6: the client turns Kraken's own refusals into errors
    /// before the answer is read. Kraken did not act on them.
    #[test]
    fn kraken_refusals_are_rejections_and_only_an_internal_error_is_undecided() {
        for refusal in [
            KrakenError::TradingRateLimitExceeded,
            KrakenError::RateLimitExceeded,
            KrakenError::InvalidNonce,
            KrakenError::PermissionDenied,
            KrakenError::ServiceBusy,
        ] {
            assert!(matches!(
                transport_error(ClientError::Kraken(refusal)),
                KrakenApiError::Rejected(_)
            ));
        }
        assert!(matches!(
            transport_error(ClientError::Kraken(KrakenError::InternalError)),
            KrakenApiError::Transport(_)
        ));
        assert!(matches!(
            transport_error(ClientError::HttpStatus("502 Bad Gateway".into())),
            KrakenApiError::Transport(_)
        ));
    }

    #[test]
    fn canonical_symbol_only_strips_known_kraken_legacy_codes() {
        assert_eq!(canonical_symbol("XXBT"), "BTC");
        assert_eq!(canonical_symbol("ZUSD"), "USD");
        assert_eq!(canonical_symbol("XETH"), "ETH");
        assert_eq!(canonical_symbol("XRP"), "XRP");
        assert_eq!(canonical_symbol("XMR"), "XMR");
        assert_eq!(canonical_symbol("ZEC"), "ZEC");
    }

    #[test]
    fn debug_output_redacts_owned_credentials() {
        let provider = OwnedSecretsProvider {
            key: "public-key".to_string(),
            secret: "private-secret".to_string(),
        };
        let output = format!("{provider:?}");
        assert!(!output.contains("public-key"));
        assert!(!output.contains("private-secret"));
        assert!(output.contains("redacted"));
    }
}
