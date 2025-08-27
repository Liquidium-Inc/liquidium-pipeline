use async_trait::async_trait;
use ic_agent::export::reqwest;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub enum WatchdogEvent<'a> {
    Heartbeat { stage: &'a str },
    BalanceMissing { asset: &'a str },
    InsufficientFunds { asset: &'a str, available: String },
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
    client: reqwest::Client,
}

impl WebhookWatchdog {
    pub fn new(url: impl Into<String>, cooldown: Duration) -> Self {
        Self {
            url: url.into(),
            cooldown,
            last: Mutex::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
        }
    }

    async fn should_send(&self, key: &str) -> bool {
        let mut m = self.last.lock().await;
        let now = Instant::now();
        match m.get(key) {
            Some(&t) if now.duration_since(t) < self.cooldown => false,
            _ => {
                m.insert(key.to_string(), now);
                true
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
        };
        if !self.should_send(&key).await {
            return;
        }

        let payload = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "event": ev,
        });

        // fire-and-forget; non-fatal on error
        let _ = self.client.post(&self.url).json(&payload).send().await;
    }
}

// helpers for wiring
pub fn noop_watchdog() -> Arc<dyn Watchdog> {
    Arc::new(NoopWatchdog)
}
pub fn webhook_watchdog_from_env(default_cooldown: Duration) -> Arc<dyn Watchdog> {
    if let Ok(url) = std::env::var("WATCHDOG_WEBHOOK") {
        Arc::new(WebhookWatchdog::new(url, default_cooldown))
    } else {
        noop_watchdog()
    }
}
