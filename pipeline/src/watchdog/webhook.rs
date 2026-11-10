//! The generic JSON webhook watchdog, set up from `WATCHDOG_WEBHOOK`.
use async_trait::async_trait;
use candid::Principal;
use ic_agent::export::reqwest;
use log::warn;
use std::{sync::Arc, time::Duration};

use super::{Watchdog, WatchdogEvent, noop_watchdog, poster::WebhookPoster};

pub struct WebhookWatchdog {
    poster: WebhookPoster,
    account: Option<Principal>,
}

impl WebhookWatchdog {
    pub fn new(url: impl Into<String>, cooldown: Duration, principal: Option<Principal>) -> Self {
        Self {
            poster: WebhookPoster::new(url, cooldown),
            account: principal,
        }
    }
}

/// One key per condition the operator should hear about once per cooldown.
/// An operator-required key carries the execution id, which for a multi-venue
/// leg includes the liquidation id, so two liquidations never share a slot.
fn cooldown_key(ev: &WatchdogEvent<'_>) -> String {
    match ev {
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
    }
}

#[async_trait]
impl Watchdog for WebhookWatchdog {
    async fn notify(&self, ev: WatchdogEvent<'_>) {
        let key = cooldown_key(&ev);
        let payload = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "account": self.account,
            "event": ev,
        });
        if let Err(error) = self.poster.send(Some(&key), &payload).await {
            tracing::error!(key = %key, error = %error, "Webhook notification failed");
        }
    }
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

    #[test]
    fn operator_required_keys_separate_liquidations_and_repeat_for_one_row() {
        let event = |execution_id: &str| WatchdogEvent::OperatorRequired {
            execution_id: execution_id.to_string(),
            venue: "mexc".to_string(),
            pending_step: "venue_reconciliation".to_string(),
            owner: "mexc".to_string(),
            details: String::new(),
        };
        let first = cooldown_key(&event("1721:mexc-0"));
        let second = cooldown_key(&event("1722:mexc-0"));
        assert_ne!(first, second);
        assert_eq!(first, cooldown_key(&event("1721:mexc-0")));
        assert_eq!(first, "operator_required:mexc:1721:mexc-0:venue_reconciliation");
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
