//! The Rift REST client: `POST /quote`, `POST /order`, `GET /order/{id}`.
//! No key is needed at one request per second; a key names the integrator.
use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::rift_backend::{
    RiftBackend, RiftError, RiftOrder, RiftOrderRequest, RiftOrderState, RiftQuote, RiftQuoteRequest,
};
use serde_json::json;

use super::protocol::{
    classify_failure, matched_order, matched_order_state, matched_quote, parse_order, parse_order_state, parse_quote,
};

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
        let url = reqwest::Url::parse(&base_url).map_err(|error| format!("RIFT_API_URL: {error}"))?;
        if !keeps_requests_private(&url) {
            return Err(format!(
                "RIFT_API_URL `{base_url}` must use https; plain http is accepted for a loopback address only"
            ));
        }
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
        let body = response.text().await;
        if status.is_success() {
            return body.map_err(|error| RiftError::Malformed(format!("Rift answer could not be read: {error}")));
        }
        // The status already decides a failed answer, so a lost body does not hide it.
        Err(classify_failure(
            status.as_u16(),
            retry_after_secs,
            &body.unwrap_or_default(),
        ))
    }
}

/// True when a request to the URL cannot be read on the network: HTTPS, or plain HTTP to this machine.
/// Each request carries the API key and the payout address, so any other URL is refused.
fn keeps_requests_private(url: &reqwest::Url) -> bool {
    let loopback = || {
        let host = url.host_str().unwrap_or_default();
        let host = host.trim_start_matches('[').trim_end_matches(']');
        host == "localhost" || host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
    };
    match url.scheme() {
        "https" => true,
        "http" => loopback(),
        _ => false,
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
        matched_quote(request, parse_quote(&answer)?)
    }

    async fn create_order(&self, request: &RiftOrderRequest) -> Result<RiftOrder, RiftError> {
        let body = json!({
            "quote_id": request.quote_id,
            "to_address": request.to_address,
            "refund_address": request.refund_address,
        });
        let answer = self
            .send(self.request(reqwest::Method::POST, "/order").json(&body))
            .await?;
        matched_order(request, parse_order(&answer)?)
    }

    async fn order_state(&self, order_id: &str) -> Result<RiftOrderState, RiftError> {
        let answer = self
            .send(self.request(reqwest::Method::GET, &format!("/order/{order_id}")))
            .await?;
        matched_order_state(order_id, parse_order_state(&answer)?)
    }
}
