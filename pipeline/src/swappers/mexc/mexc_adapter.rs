use std::collections::HashMap;
use std::env;

use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackend, CexBackendError, DepositAddress, OrderBook, OrderBookLevel, PriceLimit,
    SwapExecutionOptions, SwapFillReport, WithdrawError, WithdrawStatus, WithdrawStatusSnapshot, WithdrawTerms,
    WithdrawalReceipt,
};
use log::{debug, info, warn};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::Value;

use crate::swappers::model::BPS_PER_RATIO_UNIT;

/// Default orderbook depth level used for quote-cost and preview estimations.
const DEFAULT_ORDERBOOK_DEPTH_LIMIT: u32 = 50;
/// Default record limit for withdrawal history lookups.
const DEFAULT_WITHDRAW_HISTORY_LIMIT: u32 = 50;
/// MEXC spot fee assumption used to convert gross fills into net-usable output.
const MEXC_SPOT_FEE_BPS: f64 = 5.01;
/// Tiny safety increment to avoid borderline balance/rounding rejections between legs.
const ROUND_HANDLER: f64 = 0.01;
/// Hard cap for fee inputs; values above this are treated as invalid.
const MAX_TAKER_FEE_BPS: f64 = 10_000.0;

use mexc_rs::spot::{
    MexcSpotApiClientWithAuthentication,
    v3::{
        self,
        account_information::{AccountBalance, AccountInformationEndpoint},
        cancel_order::{CancelOrderEndpoint, CancelOrderParams},
        deposit_address::DepositAddressEndpoint,
        depth::{DepthEndpoint, DepthParams},
        enums::{OrderSide, OrderStatus},
        get_order::{GetOrderEndpoint, GetOrderParams},
        order::{OrderEndpoint, OrderParams},
        withdraw::{WithdrawEndpoint, WithdrawHistoryRecord, WithdrawHistoryRequest, WithdrawRequest},
    },
};

use num_traits::{FromPrimitive, ToPrimitive};

#[path = "adapter/assets.rs"]
mod assets;
#[path = "adapter/backend.rs"]
mod backend;
#[path = "adapter/errors.rs"]
mod errors;
#[path = "adapter/filters.rs"]
mod filters;
#[path = "adapter/order_sizing.rs"]
mod order_sizing;
#[path = "adapter/orders.rs"]
mod orders;
#[cfg(test)]
#[path = "adapter/tests.rs"]
mod tests;
#[path = "adapter/withdraw_status.rs"]
mod withdraw_status;
#[path = "adapter/withdraw_terms.rs"]
mod withdraw_terms;

use assets::*;
use errors::*;
use filters::*;
use order_sizing::*;
pub use orders::LimitOrderReport;
use withdraw_status::*;
use withdraw_terms::*;

pub struct MexcClient {
    inner: tokio::sync::Mutex<MexcSpotApiClientWithAuthentication>,
    symbol_filters: tokio::sync::Mutex<HashMap<String, SymbolFilters>>,
    coin_config: CoinConfigCache,
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
            coin_config: CoinConfigCache::default(),
            http: reqwest::Client::new(),
        }
    }

    /// Finds a recent withdrawal by its MEXC id or by our order id.
    pub async fn find_withdraw_record(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<Option<WithdrawHistoryRecord>, String> {
        let ex = self.inner.lock().await;
        // Older durable rows retain the planned symbol, not the alias MEXC
        // accepted. Search the same candidates as submission, matching only the
        // durable withdrawal ID; absence under one alias is not non-delivery.
        for (index, candidate) in mexc_withdraw_coin_candidates(coin).into_iter().enumerate() {
            let records = match ex
                .withdraw_history(WithdrawHistoryRequest {
                    coin: Some(candidate),
                    status: None,
                    limit: Some(DEFAULT_WITHDRAW_HISTORY_LIMIT),
                    start_time: None,
                    end_time: None,
                })
                .await
            {
                Ok(records) => records,
                Err(error) if is_coin_missing(&error) => continue,
                // The planned coin must answer, so only an alias may be skipped.
                Err(error) if index > 0 && is_unlisted_alias(&error) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let found = records
                .into_iter()
                .find(|record| record.id == withdraw_id || record.withdraw_order_id.as_deref() == Some(withdraw_id));
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    pub fn from_env() -> Result<Self, String> {
        let api_key = env::var("CEX_MEXC_API_KEY").map_err(|_| "CEX_MEXC_API_KEY not set".to_string())?;
        let api_secret = env::var("CEX_MEXC_API_SECRET").map_err(|_| "CEX_MEXC_API_SECRET not set".to_string())?;

        Ok(Self::new(&api_key, &api_secret))
    }

}

