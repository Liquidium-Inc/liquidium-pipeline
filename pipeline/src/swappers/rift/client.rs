//! The Rift REST client: `POST /quote`, `POST /order`, `GET /order/{id}`.
//! No key is needed at one request per second; a key names the integrator.
use std::time::Duration;

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::rift_backend::{
    RiftBackend, RiftError, RiftOrder, RiftOrderState, RiftQuote, RiftQuoteRequest,
};
use serde_json::json;

use super::protocol::{classify_failure, parse_order, parse_order_state, parse_quote};

/// Rift answers quotes in one to ten seconds; this bounds a hung connection.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RiftClient {
    http: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
}

impl RiftClient {
    /// `base_url` without a trailing slash, for example `https://api.rift.trade`.
    pub fn new(base_url: &str, api_key: Option<String>) -> Result<Self, String> {
        let base_url = base_url.trim().trim_end_matches('/').to_string();
        reqwest::Url::parse(&base_url).map_err(|error| format!("RIFT_API_URL: {error}"))?;
        let http = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .map_err(|error| format!("Rift HTTP client: {error}"))?;
        Ok(Self {
            http,
            base_url,
            api_key: api_key.map(|key| key.trim().to_string()).filter(|key| !key.is_empty()),
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.http.request(method, format!("{}{path}", self.base_url));
        match &self.api_key {
            Some(key) => request.header("x-api-key", key),
            None => request,
        }
    }

    /// Sends the request and returns the body of a 2xx answer, or the typed failure.
    async fn send(&self, request: reqwest::RequestBuilder) -> Result<String, RiftError> {
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() || error.is_connect() || error.is_request() {
                RiftError::Unreachable(format!("Rift did not answer: {error}"))
            } else {
                RiftError::Malformed(format!("Rift request failed: {error}"))
            }
        })?;
        let status = response.status();
        let retry_after_secs = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok());
        let body = response
            .text()
            .await
            .map_err(|error| RiftError::Malformed(format!("Rift answer could not be read: {error}")))?;
        if status.is_success() {
            return Ok(body);
        }
        Err(classify_failure(status.as_u16(), retry_after_secs, &body))
    }
}

/// The quote request as Rift reads it. Empty venue lists are left out, so
/// Rift applies no restriction.
pub(super) fn quote_body(request: &RiftQuoteRequest) -> serde_json::Value {
    let mut body = json!({
        "from": request.from.as_str(),
        "to": request.to.as_str(),
        "amount_in": request.amount_in,
        "format": "formatted",
        "quote_mode": "optimal",
    });
    if !request.venues.allow.is_empty() {
        body["venue_allowlist"] = json!(request.venues.allow);
    }
    if !request.venues.block.is_empty() {
        body["venue_blocklist"] = json!(request.venues.block);
    }
    body
}

#[async_trait]
impl RiftBackend for RiftClient {
    async fn quote(&self, request: &RiftQuoteRequest) -> Result<RiftQuote, RiftError> {
        let answer = self
            .send(self.request(reqwest::Method::POST, "/quote").json(&quote_body(request)))
            .await?;
        parse_quote(&answer)
    }

    async fn create_order(
        &self,
        quote_id: &str,
        to_address: &str,
        refund_address: Option<String>,
    ) -> Result<RiftOrder, RiftError> {
        let mut body = json!({ "quote_id": quote_id, "to_address": to_address });
        if let Some(refund_address) = refund_address {
            body["refund_address"] = json!(refund_address);
        }
        let answer = self
            .send(self.request(reqwest::Method::POST, "/order").json(&body))
            .await?;
        parse_order(&answer)
    }

    async fn order_state(&self, order_id: &str) -> Result<RiftOrderState, RiftError> {
        let answer = self
            .send(self.request(reqwest::Method::GET, &format!("/order/{order_id}")))
            .await?;
        parse_order_state(&answer)
    }
}
