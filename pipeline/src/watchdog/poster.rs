//! Posts alerts to one webhook, at most once per cooldown for each key. The
//! Slack and generic watchdogs differ only in their keys and payloads.
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use ic_agent::export::reqwest;
use log::warn;
use tokio::sync::Mutex;

use super::WATCHDOG_HTTP_TIMEOUT_SECS;

pub(super) struct WebhookPoster {
    url: String,
    cooldown: Duration,
    last: Mutex<HashMap<String, Instant>>,
    client: reqwest::Client,
}

impl WebhookPoster {
    pub(super) fn new(url: impl Into<String>, cooldown: Duration) -> Self {
        let timeout = Duration::from_secs(WATCHDOG_HTTP_TIMEOUT_SECS);
        let client = reqwest::Client::builder().timeout(timeout).build().unwrap_or_else(|e| {
            warn!("Failed to build webhook client with timeout: {}, using default", e);
            reqwest::Client::new()
        });
        Self {
            url: url.into(),
            cooldown,
            last: Mutex::new(HashMap::new()),
            client,
        }
    }

    /// Posts `payload` unless `key` was sent within the cooldown. A failed post
    /// frees the slot again: callers re-notify while a condition lasts, and a
    /// used slot would hide a stuck execution for the whole cooldown.
    pub(super) async fn send(&self, key: Option<&str>, payload: &serde_json::Value) -> Result<(), String> {
        let reserved = match key {
            Some(key) => match self.reserve_for_send(key).await {
                Some(displaced) => Some((key, displaced)),
                None => return Ok(()),
            },
            None => None,
        };
        let result = self.post(payload).await;
        if result.is_err()
            && let Some((key, displaced)) = reserved
        {
            self.release_reservation(key, displaced).await;
        }
        result
    }

    /// Webhook URLs carry their secret, so no error text here holds the URL.
    async fn post(&self, payload: &serde_json::Value) -> Result<(), String> {
        let response = self
            .client
            .post(&self.url)
            .json(payload)
            .send()
            .await
            .map_err(error_without_url)?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response
            .text()
            .await
            .unwrap_or_else(|err| format!("<failed to read response body: {}>", error_without_url(err)));
        Err(format!("status {status}: {body}"))
    }

    /// Claims the cooldown slot for `key`, returning the timestamp it displaced
    /// so a failed send can put it back. The check and the claim share one lock.
    async fn reserve_for_send(&self, key: &str) -> Option<Option<Instant>> {
        let mut m = self.last.lock().await;
        let now = Instant::now();
        m.retain(|_, ts| now.duration_since(*ts) < self.cooldown);
        if matches!(m.get(key), Some(&t) if now.duration_since(t) < self.cooldown) {
            return None;
        }
        Some(m.insert(key.to_string(), now))
    }

    async fn release_reservation(&self, key: &str, displaced: Option<Instant>) {
        let mut m = self.last.lock().await;
        match displaced {
            Some(previous) => {
                m.insert(key.to_string(), previous);
            }
            None => {
                m.remove(key);
            }
        }
    }
}

/// reqwest prints the request URL with timeout errors.
fn error_without_url(err: reqwest::Error) -> String {
    err.without_url().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_reservation_holds_its_key_for_the_cooldown() {
        let poster = WebhookPoster::new("http://localhost/webhook", Duration::from_secs(60));

        assert!(poster.reserve_for_send("low_balance:main:ckBTC").await.is_some());
        assert!(poster.reserve_for_send("low_balance:main:ckBTC").await.is_none());
        assert!(poster.reserve_for_send("low_balance:trader:ckBTC").await.is_some());
    }

    #[tokio::test]
    async fn an_undelivered_alert_does_not_use_its_cooldown() {
        // Port 0 is never connectable, so the post fails at the transport layer.
        let poster = WebhookPoster::new("http://127.0.0.1:0/webhook", Duration::from_secs(60));
        let payload = serde_json::json!({ "event": "heartbeat" });

        assert!(poster.send(Some("hb:Running"), &payload).await.is_err());

        // A used slot here would drop the next hour of alerts because one POST failed.
        assert!(
            poster.reserve_for_send("hb:Running").await.is_some(),
            "a failed send must leave the cooldown slot free to retry"
        );
    }

    #[tokio::test]
    async fn a_release_restores_a_displaced_reservation() {
        let poster = WebhookPoster::new("http://localhost/webhook", Duration::from_secs(60));

        let first = poster.reserve_for_send("hb:Running").await.expect("first reservation");
        assert!(first.is_none(), "nothing displaced on a fresh key");
        poster.release_reservation("hb:Running", first).await;

        assert!(
            poster.reserve_for_send("hb:Running").await.is_some(),
            "releasing a fresh reservation must clear the key entirely"
        );
    }

    #[tokio::test]
    async fn a_failed_webhook_send_logs_no_part_of_the_url() {
        // A server that accepts and never answers makes the send time out, and
        // reqwest attaches the URL to timeout errors.
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = server.local_addr().expect("addr").port();
        let _held = tokio::spawn(async move { server.accept().await });
        let err = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .expect("client")
            .post(format!("http://127.0.0.1:{port}/hooks/secret-path?token=hunter2"))
            .send()
            .await
            .expect_err("silent webhook");
        assert!(
            err.to_string().contains("hunter2"),
            "reqwest no longer prints the URL: {err}"
        );

        let logged = error_without_url(err);
        assert!(
            !logged.contains("hunter2") && !logged.contains("secret-path"),
            "{logged}"
        );
    }
}
