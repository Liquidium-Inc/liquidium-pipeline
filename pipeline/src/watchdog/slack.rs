use async_trait::async_trait;
use ic_agent::export::reqwest;
use log::warn;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use super::{WATCHDOG_HTTP_TIMEOUT_SECS, Watchdog, WatchdogEvent, noop_watchdog};

pub struct SlackWatchdog {
    url: String,
    cooldown: Duration,
    last: Mutex<HashMap<String, Instant>>,
    client: reqwest::Client,
}

impl SlackWatchdog {
    pub fn new(url: impl Into<String>, cooldown: Duration) -> Self {
        let timeout = Duration::from_secs(WATCHDOG_HTTP_TIMEOUT_SECS);
        let client = reqwest::Client::builder().timeout(timeout).build().unwrap_or_else(|e| {
            warn!(
                "Failed to build Slack reqwest client with timeout: {}, using default",
                e
            );
            reqwest::Client::new()
        });

        Self {
            url: url.into(),
            cooldown,
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
impl Watchdog for SlackWatchdog {
    async fn notify(&self, ev: WatchdogEvent<'_>) {
        let WatchdogEvent::LowBalance { account, asset_id, .. } = &ev else {
            return;
        };

        let key = format!("low_balance:{account}:{asset_id}");
        if !self.should_send(&key).await {
            return;
        }

        let Some(payload) = slack_payload_for_event(&ev) else {
            return;
        };

        match self.client.post(&self.url).json(&payload).send().await {
            Ok(resp) if !resp.status().is_success() => {
                warn!("Slack low-balance notification failed with status {}", resp.status());
            }
            Err(err) => {
                warn!("Slack low-balance notification failed: {}", err);
            }
            _ => {}
        }
    }
}

pub fn slack_watchdog_from_env(default_cooldown: Duration) -> Arc<dyn Watchdog> {
    match std::env::var("SLACK_WEBHOOK_URL") {
        Ok(url) if !url.trim().is_empty() => Arc::new(SlackWatchdog::new(url, default_cooldown)),
        _ => noop_watchdog(),
    }
}

pub fn slack_webhook_configured() -> bool {
    std::env::var("SLACK_WEBHOOK_URL")
        .map(|url| !url.trim().is_empty())
        .unwrap_or(false)
}

pub(crate) fn slack_payload_for_event(ev: &WatchdogEvent<'_>) -> Option<serde_json::Value> {
    let WatchdogEvent::LowBalance {
        account,
        asset,
        asset_id,
        current,
        threshold,
    } = ev
    else {
        return None;
    };

    let text = format!("Low balance: {account} {asset} is {current}, below {threshold}");
    Some(serde_json::json!({
        "text": text,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!("*Low balance*: `{account}` `{asset}`")
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": format!("*Account*\n{account}")
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Asset*\n{asset}")
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Current*\n{current}")
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Threshold*\n{threshold}")
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Asset ID*\n`{asset_id}`")
                    }
                ]
            }
        ]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slack_payload_formats_low_balance_event() {
        let ev = WatchdogEvent::LowBalance {
            account: "main".to_string(),
            asset: "ckBTC".to_string(),
            asset_id: "icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC".to_string(),
            current: "0 ckBTC".to_string(),
            threshold: "0.001 ckBTC".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("low balance should format");
        assert_eq!(payload["text"], "Low balance: main ckBTC is 0 ckBTC, below 0.001 ckBTC");
        assert_eq!(payload["blocks"][0]["type"], "section");
    }

    #[tokio::test]
    async fn slack_watchdog_cooldown_suppresses_repeated_keys() {
        let wd = SlackWatchdog::new("http://localhost/slack", Duration::from_secs(60));

        assert!(wd.should_send("low_balance:main:ckBTC").await);
        assert!(!wd.should_send("low_balance:main:ckBTC").await);
        assert!(wd.should_send("low_balance:trader:ckBTC").await);
    }
}
