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
            WatchdogEvent::LowBalance { account, asset_id, .. } => format!("low_balance:{account}:{asset_id}"),
            WatchdogEvent::Lifecycle { state, .. } => format!("lifecycle:{state}"),
            WatchdogEvent::LiquidationFinalized { liquidation_id, status, .. } => {
                format!("liquidation_finalized:{liquidation_id}:{status}")
            }
        };
        if !self.should_send(&key).await {
            return;
        }

        let payload = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "account": self.account,
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
        Arc::new(WebhookWatchdog::new(url, default_cooldown, None))
    } else {
        noop_watchdog()
    }
}
