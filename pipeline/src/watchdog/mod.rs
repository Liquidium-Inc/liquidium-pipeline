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

    async fn reserve_for_send(&self, key: &str) -> Option<Instant> {
        let mut m = self.last.lock().await;
        let now = Instant::now();
        m.retain(|_, ts| now.duration_since(*ts) < self.cooldown);
        if matches!(m.get(key), Some(&t) if now.duration_since(t) < self.cooldown) {
            return None;
        }
        m.insert(key.to_string(), now);
        Some(now)
    }

    async fn release_reservation(&self, key: &str, reserved_at: Instant) {
        let mut m = self.last.lock().await;
        if m.get(key).is_some_and(|ts| *ts == reserved_at) {
            m.remove(key);
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
            WatchdogEvent::LiquidationFinalized {
                liquidation_id, status, ..
            } => {
                format!("liquidation_finalized:{liquidation_id}:{status}")
            }
        };
        let Some(reserved_at) = self.reserve_for_send(&key).await else {
            return;
        };

        let payload = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "account": self.account,
            "event": ev,
        });

        match self.client.post(&self.url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|err| format!("<failed to read response body: {err}>"));
                tracing::error!(
                    key = %key,
                    url = %self.url,
                    status = %status,
                    body = %body,
                    "Webhook notification failed with non-success status"
                );
                self.release_reservation(&key, reserved_at).await;
            }
            Err(err) => {
                tracing::error!(
                    key = %key,
                    url = %self.url,
                    error = %err,
                    "Webhook notification transport failed"
                );
                self.release_reservation(&key, reserved_at).await;
            }
        }
    }
}

// helpers for wiring
pub fn noop_watchdog() -> Arc<dyn Watchdog> {
    Arc::new(NoopWatchdog)
}

pub fn webhook_watchdog_from_env(default_cooldown: Duration) -> Arc<dyn Watchdog> {
    if let Ok(url) = std::env::var("WATCHDOG_WEBHOOK") {
        Arc::new(WebhookWatchdog::new(url, default_cooldown, None))
    } else {
        noop_watchdog()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn webhook_watchdog_cooldown_is_marked_only_after_success() {
        let wd = WebhookWatchdog::new("http://localhost/webhook", Duration::from_secs(60), None);

        let reserved_at = wd.reserve_for_send("hb:Running").await.expect("first reserve");
        assert!(wd.reserve_for_send("hb:Running").await.is_none());
        wd.release_reservation("hb:Running", reserved_at).await;
        assert!(wd.reserve_for_send("hb:Running").await.is_some());
    }
}
