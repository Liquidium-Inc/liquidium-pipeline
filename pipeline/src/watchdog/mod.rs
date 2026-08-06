use async_trait::async_trait;
use candid::Principal;
use ic_agent::export::reqwest;
use log::warn;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

pub mod balance_monitor;
pub mod slack;

pub(crate) const WATCHDOG_HTTP_TIMEOUT_SECS: u64 = 5;

pub use slack::{slack_watchdog_from_env, slack_webhook_configured};

#[derive(Debug, Clone, Serialize)]
pub enum WatchdogEvent<'a> {
    Heartbeat {
        stage: &'a str,
    },
    BalanceMissing {
        asset: &'a str,
    },
    InsufficientFunds {
        asset: &'a str,
        available: String,
    },
    LowBalance {
        account: String,
        asset: String,
        asset_id: String,
        current: String,
        threshold: String,
    },
    Lifecycle {
        state: String,
        details: String,
    },
    OperatorRequired {
        execution_id: String,
        venue: String,
        pending_step: String,
        owner: String,
        details: String,
    },
    LiquidationFinalized {
        liquidation_id: String,
        borrower: String,
        debt_asset: String,
        collateral_asset: String,
        status: String,
        debt_repaid: String,
        collateral_received: String,
        swap_output: String,
        swapper: String,
        expected_profit: String,
        realized_profit: String,
        profit_delta: String,
        round_trip_secs: String,
    },
}

#[async_trait]
pub trait Watchdog: Send + Sync {
    async fn notify(&self, ev: WatchdogEvent<'_>);
}

pub struct NoopWatchdog;
#[async_trait]
impl Watchdog for NoopWatchdog {
    async fn notify(&self, _ev: WatchdogEvent<'_>) {}
}

pub struct WebhookWatchdog {
    url: String,
    cooldown: Duration,
    last: Mutex<HashMap<String, Instant>>,
    account: Option<Principal>,
    client: reqwest::Client,
}

impl WebhookWatchdog {
    pub fn new(url: impl Into<String>, cooldown: Duration, principal: Option<Principal>) -> Self {
        let timeout = Duration::from_secs(WATCHDOG_HTTP_TIMEOUT_SECS);
        let client = reqwest::Client::builder().timeout(timeout).build().unwrap_or_else(|e| {
            warn!("Failed to build reqwest client with timeout: {}, using default", e);
            reqwest::Client::new()
        });

        Self {
            url: url.into(),
            cooldown,
            account: principal,
            last: Mutex::new(HashMap::new()),
            client,
        }
    }

    /// Claims the cooldown slot for `key`, returning the timestamp it displaced
    /// so a failed send can put it back. Reserving up front keeps the check and
    /// the claim atomic under one lock; [`Self::release_reservation`] undoes it
    /// when the send fails, so a dropped alert is retried on the next attempt
    /// rather than silently consuming the whole cooldown window.
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

#[async_trait]
impl Watchdog for WebhookWatchdog {
    async fn notify(&self, ev: WatchdogEvent<'_>) {
        let key = match &ev {
            WatchdogEvent::Heartbeat { stage } => format!("hb:{stage}"),
            WatchdogEvent::BalanceMissing { asset } => format!("bal_missing:{asset}"),
            WatchdogEvent::InsufficientFunds { asset, .. } => format!("insuff:{asset}"),
            WatchdogEvent::LowBalance { account, asset_id, .. } => format!("low_balance:{account}:{asset_id}"),
            WatchdogEvent::Lifecycle { state, .. } => format!("lifecycle:{state}"),
            WatchdogEvent::OperatorRequired {
                execution_id,
                venue,
                pending_step,
                ..
            } => format!("operator_required:{venue}:{execution_id}:{pending_step}"),
            WatchdogEvent::LiquidationFinalized {
                liquidation_id, status, ..
            } => {
                format!("liquidation_finalized:{liquidation_id}:{status}")
            }
        };
        let Some(displaced) = self.reserve_for_send(&key).await else {
            return;
        };

        let payload = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "account": self.account,
            "event": ev,
        });

        // `WATCHDOG_WEBHOOK` commonly embeds a secret token in its path, so the
        // URL is never logged -- only the event key and the failure itself.
        let delivered = match self.client.post(&self.url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => true,
            Ok(resp) => {
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|err| format!("<failed to read response body: {err}>"));
                tracing::error!(
                    key = %key,
                    status = %status,
                    body = %body,
                    "Webhook notification failed with non-success status"
                );
                false
            }
            Err(err) => {
                tracing::error!(key = %key, error = %err, "Webhook notification transport failed");
                false
            }
        };

        // An undelivered alert must not burn its cooldown: callers re-notify while
        // a condition persists, and swallowing the failure here is what turns a
        // stuck execution into one nobody is ever told about.
        if !delivered {
            self.release_reservation(&key, displaced).await;
        }
    }
}

// helpers for wiring
pub fn noop_watchdog() -> Arc<dyn Watchdog> {
    Arc::new(NoopWatchdog)
}

fn normalize_webhook_url(raw: &str) -> Option<String> {
    let url = raw.trim();
    if url.is_empty() {
        return None;
    }
    match reqwest::Url::parse(url) {
        Ok(_) => Some(url.to_string()),
        Err(err) => {
            warn!("Ignoring invalid WATCHDOG_WEBHOOK URL: {}", err);
            None
        }
    }
}

pub fn webhook_watchdog_from_env(default_cooldown: Duration) -> Arc<dyn Watchdog> {
    if let Ok(raw_url) = std::env::var("WATCHDOG_WEBHOOK")
        && let Some(url) = normalize_webhook_url(&raw_url)
    {
        Arc::new(WebhookWatchdog::new(url, default_cooldown, None))
    } else {
        noop_watchdog()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn webhook_watchdog_attempt_reservation_consumes_cooldown() {
        let wd = WebhookWatchdog::new("http://localhost/webhook", Duration::from_secs(60), None);

        assert!(wd.reserve_for_send("hb:Running").await.is_some());
        assert!(wd.reserve_for_send("hb:Running").await.is_none());
    }

    #[tokio::test]
    async fn webhook_watchdog_undelivered_alert_does_not_burn_its_cooldown() {
        // Port 0 is never connectable, so `send()` fails at the transport layer.
        let wd = WebhookWatchdog::new("http://127.0.0.1:0/webhook", Duration::from_secs(60), None);

        wd.notify(WatchdogEvent::Heartbeat { stage: "Running" }).await;

        // A consumed cooldown here would mean the next hour of alerts is silently
        // dropped because one POST failed -- the failure mode F3 describes.
        assert!(
            wd.reserve_for_send("hb:Running").await.is_some(),
            "a failed send must leave the cooldown slot free to retry"
        );
    }

    #[tokio::test]
    async fn webhook_watchdog_release_restores_a_displaced_reservation() {
        let wd = WebhookWatchdog::new("http://localhost/webhook", Duration::from_secs(60), None);

        let first = wd.reserve_for_send("hb:Running").await.expect("first reservation");
        assert!(first.is_none(), "nothing displaced on a fresh key");
        wd.release_reservation("hb:Running", first).await;

        assert!(
            wd.reserve_for_send("hb:Running").await.is_some(),
            "releasing a fresh reservation must clear the key entirely"
        );
    }

    #[test]
    fn normalize_webhook_url_ignores_blank_values() {
        assert_eq!(normalize_webhook_url(""), None);
        assert_eq!(normalize_webhook_url("   "), None);
    }

    #[test]
    fn normalize_webhook_url_rejects_malformed_values() {
        assert_eq!(normalize_webhook_url("not a url"), None);
    }

    #[test]
    fn normalize_webhook_url_accepts_valid_values() {
        assert_eq!(
            normalize_webhook_url(" https://example.com/webhook "),
            Some("https://example.com/webhook".to_string())
        );
    }
}
