//! [`MexcClient`]: MEXC behind the venue-neutral [`CexBackend`].
//!
//! This file holds what every MEXC operation shares -- the client itself and
//! the mapping of the SDK's errors onto the shared classification -- and the
//! `CexBackend` impl, whose methods only delegate. The work is split by
//! concern into child modules so each can be audited on its own:
//! `mexc_symbols` (the venue's per-symbol limits and fees), `mexc_orders`
//! (sizing an order to those limits, submitting it, reading its fill),
//! `mexc_trading` (the quote, order book and swap the pipeline asks for) and
//! `mexc_funding` (deposit addresses, balances, withdrawals and their status).

use std::collections::HashMap;
use std::env;

use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CEX_VENUE_UNREACHABLE_PREFIX, CexBackend, CexSubmissionError, CexWithdrawError, DepositAddress,
    OrderBook, OrderBookLevel, SwapExecutionOptions, SwapFillReport, WithdrawStatus, WithdrawStatusSnapshot,
    WithdrawalReceipt,
};
use log::{debug, info, warn};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::Value;

use crate::swappers::model::BPS_PER_RATIO_UNIT;

use mexc_rs::spot::{
    MexcSpotApiClientWithAuthentication,
    v3::{
        self,
        account_information::{AccountBalance, AccountInformationEndpoint},
        deposit_address::DepositAddressEndpoint,
        depth::{DepthEndpoint, DepthParams},
        enums::{OrderSide, OrderStatus},
        get_order::{GetOrderEndpoint, GetOrderParams},
        order::{OrderEndpoint, OrderParams},
        withdraw::{WithdrawEndpoint, WithdrawHistoryRequest, WithdrawRequest},
    },
};

use num_traits::{FromPrimitive, ToPrimitive};

#[path = "mexc_funding.rs"]
mod mexc_funding;
#[path = "mexc_orders.rs"]
mod mexc_orders;
#[path = "mexc_symbols.rs"]
mod mexc_symbols;
#[path = "mexc_trading.rs"]
mod mexc_trading;

use self::mexc_symbols::SymbolFilters;

fn normalize_market_symbol(market: &str) -> String {
    market.replace(['/', '_', '-'], "").to_ascii_uppercase()
}

/// Whether the request behind this error never reached MEXC.
///
/// Only a failure to connect qualifies. A timeout is excluded: the request may
/// have arrived and been acted on, so it is ambiguous rather than absent.
fn is_unreachable_mexc_error(err: &v3::ApiError) -> bool {
    matches!(err, v3::ApiError::ReqwestError(inner) if inner.is_connect())
}

/// Renders a MEXC client error, marking one that never reached the exchange so
/// the decision survives the `Result<_, String>` boundary in `CexBackend`.
fn mexc_error_message(err: &v3::ApiError) -> String {
    if is_unreachable_mexc_error(err) {
        format!("{CEX_VENUE_UNREACHABLE_PREFIX}{err}")
    } else {
        err.to_string()
    }
}

/// The same marking for the calls this adapter makes outside the MEXC client.
fn http_error_message(err: &reqwest::Error) -> String {
    if err.is_connect() {
        format!("{CEX_VENUE_UNREACHABLE_PREFIX}{err}")
    } else {
        err.to_string()
    }
}

fn format_mexc_api_error(err: &v3::ApiError) -> String {
    let rendered = match err {
        v3::ApiError::ErrorResponse(resp) => match &resp._extend {
            Some(extra) => format!("code={:?} msg={} extend={}", resp.code, resp.msg, extra),
            None => format!("code={:?} msg={}", resp.code, resp.msg),
        },
        v3::ApiError::ReqwestError(err) => match err.status() {
            Some(status) => format!("status={} err={}", status, err),
            None => format!("err={}", err),
        },
        other => other.to_string(),
    };
    if is_unreachable_mexc_error(err) {
        format!("{CEX_VENUE_UNREACHABLE_PREFIX}{rendered}")
    } else {
        rendered
    }
}

fn is_bad_symbol(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::BadSymbol
    )
}

/// Reports whether MEXC rejected the order because the funds it already shows
/// in the account are not tradable yet.
///
/// `Oversold` is the sell-side form (base asset short) and `InsufficientPosition`
/// the buy-side one (quote asset short). Both appear while a credited deposit is
/// still settling into the matching engine, which is a wait, not a failure.
fn is_pending_settlement(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp)
            if resp.code == v3::ErrorCode::Oversold || resp.code == v3::ErrorCode::InsufficientPosition
    )
}

fn is_coin_missing(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::CurrencyDoesNotExist
    )
}

fn is_order_missing_lookup_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("order does not exist")
        || msg.contains("unknown order")
        || msg.contains("-2013")
        || msg.contains("code=-2013")
}

pub struct MexcClient {
    inner: tokio::sync::Mutex<MexcSpotApiClientWithAuthentication>,
    symbol_filters: tokio::sync::Mutex<HashMap<String, SymbolFilters>>,
    http: reqwest::Client,
}

impl MexcClient {
    pub fn new(api_key: &str, secret: &str) -> Self {
        let api = MexcSpotApiClientWithAuthentication::new(
            mexc_rs::spot::MexcSpotApiEndpoint::Base,
            api_key.to_string(),
            secret.to_string(),
        );
        Self {
            inner: tokio::sync::Mutex::new(api),
            symbol_filters: tokio::sync::Mutex::new(HashMap::new()),
            http: reqwest::Client::new(),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let api_key = env::var("CEX_MEXC_API_KEY").map_err(|_| "CEX_MEXC_API_KEY not set".to_string())?;
        let api_secret = env::var("CEX_MEXC_API_SECRET").map_err(|_| "CEX_MEXC_API_SECRET not set".to_string())?;

        Ok(Self::new(&api_key, &api_secret))
    }
}

#[async_trait]
impl CexBackend for MexcClient {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        self.quote_buy_cost(market, amount_in).await
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
    ) -> Result<SwapFillReport, CexSubmissionError> {
        self.execute_market_order(market, side, amount_in, options).await
    }

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, String> {
        self.read_orderbook(market, limit).await
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        self.deposit_address(asset, network).await
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        self.free_balance(asset).await
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

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        let snapshot = self.get_withdraw_status_snapshot_by_id(coin, withdraw_id).await?;
        Ok(snapshot.status)
    }

    async fn get_withdraw_status_snapshot_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        self.withdrawal_status(coin, withdraw_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liquidium_pipeline_connectors::backend::cex_backend::is_cex_venue_unreachable_error;

    /// The live ICP_USDT shape: `baseSizePrecision` 0.0001 against
    /// `baseAssetPrecision` 2. Treating that minimum as a step rounded to four
    /// decimals, which MEXC rejected as "quantity scale is invalid".
    /// Only a failure to connect is marked. A venue-side failure has an answer
    /// in it, so it must keep spending the retry budget.
    #[test]
    fn a_venue_side_failure_is_not_marked_unreachable() {
        for err in [v3::ApiError::InternalServerError, v3::ApiError::RateLimitExceeded] {
            assert!(!is_unreachable_mexc_error(&err));
            assert!(!is_cex_venue_unreachable_error(&mexc_error_message(&err)));
            assert!(!is_cex_venue_unreachable_error(&format_mexc_api_error(&err)));
        }
    }

    #[test]
    fn detects_order_missing_lookup_error_shapes() {
        assert!(is_order_missing_lookup_error(
            "400 Bad Request {\"msg\":\"Order does not exist.\",\"code\":-2013}"
        ));
        assert!(is_order_missing_lookup_error(
            "Get_order err: code=-2013 msg=Unknown order sent."
        ));
        assert!(!is_order_missing_lookup_error("order not executed, status: New"));
    }
}
