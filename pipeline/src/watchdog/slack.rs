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
        if let Some(key) = slack_cooldown_key(&ev) {
            if !self.should_send(&key).await {
                return;
            }
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

fn slack_cooldown_key(ev: &WatchdogEvent<'_>) -> Option<String> {
    match ev {
        WatchdogEvent::LowBalance { account, asset_id, .. } => Some(format!("low_balance:{account}:{asset_id}")),
        _ => None,
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
    match ev {
        WatchdogEvent::LowBalance {
            account,
            asset,
            asset_id,
            current,
            threshold,
        } => {
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
        WatchdogEvent::Lifecycle { state, details } => {
            let text = format!("Liquidator {state}: {details}");
            Some(serde_json::json!({
                "text": text,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": format!("*Liquidator {state}*")
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": details
                        }
                    }
                ]
            }))
        }
        WatchdogEvent::LiquidationFinalized {
            liquidation_id,
            borrower,
            debt_asset,
            collateral_asset,
            status,
            debt_repaid,
            collateral_received,
            swap_output,
            swapper,
            expected_profit,
            realized_profit,
            profit_delta,
            round_trip_secs,
        } => {
            let text = format!("Liquidation finalized: {status} {liquidation_id}");
            Some(serde_json::json!({
                "text": text,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": format!("*Liquidation finalized*: `{status}`")
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": format!("*Liquidation ID*\n`{liquidation_id}`")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Borrower*\n`{borrower}`")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Debt repaid*\n{debt_repaid}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Collateral received*\n{collateral_received}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Assets*\n{debt_asset} / {collateral_asset}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Swap output*\n{swap_output}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Swapper*\n{swapper}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Expected profit*\n{expected_profit}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Realized profit*\n{realized_profit}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Profit delta*\n{profit_delta}")
                            },
                            {
                                "type": "mrkdwn",
                                "text": format!("*Round trip*\n{round_trip_secs}s")
                            }
                        ]
                    }
                ]
            }))
        }
        _ => None,
    }
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

    #[test]
    fn slack_payload_formats_lifecycle_event() {
        let ev = WatchdogEvent::Lifecycle {
            state: "started".to_string(),
            details: "Liquidator started on https://ic0.app.".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("lifecycle should format");
        assert_eq!(payload["text"], "Liquidator started: Liquidator started on https://ic0.app.");
        assert_eq!(payload["blocks"][0]["text"]["text"], "*Liquidator started*");
    }

    #[test]
    fn slack_payload_formats_successful_liquidation_event() {
        let ev = WatchdogEvent::LiquidationFinalized {
            liquidation_id: "42".to_string(),
            borrower: "aaaaa-aa".to_string(),
            debt_asset: "ckUSDC".to_string(),
            collateral_asset: "ckBTC".to_string(),
            status: "Success".to_string(),
            debt_repaid: "100 ckUSDC".to_string(),
            collateral_received: "0.002 ckBTC".to_string(),
            swap_output: "101 ckUSDC".to_string(),
            swapper: "mexc".to_string(),
            expected_profit: "1 ckUSDC".to_string(),
            realized_profit: "1.1 ckUSDC".to_string(),
            profit_delta: "+0.100".to_string(),
            round_trip_secs: "12".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("liquidation should format");
        assert_eq!(payload["text"], "Liquidation finalized: Success 42");
        assert_eq!(payload["blocks"][0]["text"]["text"], "*Liquidation finalized*: `Success`");
    }

    #[test]
    fn slack_payload_formats_failed_liquidation_event() {
        let ev = WatchdogEvent::LiquidationFinalized {
            liquidation_id: "n/a".to_string(),
            borrower: "aaaaa-aa".to_string(),
            debt_asset: "ICP".to_string(),
            collateral_asset: "ckBTC".to_string(),
            status: "SwapFailed: insufficient liquidity".to_string(),
            debt_repaid: "0 ICP".to_string(),
            collateral_received: "0 ckBTC".to_string(),
            swap_output: "0 ICP".to_string(),
            swapper: "unknown".to_string(),
            expected_profit: "0 ICP".to_string(),
            realized_profit: "0 ICP".to_string(),
            profit_delta: "+0.000".to_string(),
            round_trip_secs: "-".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("failed liquidation should format");
        assert_eq!(payload["text"], "Liquidation finalized: SwapFailed: insufficient liquidity n/a");
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            "*Liquidation finalized*: `SwapFailed: insufficient liquidity`"
        );
    }

    #[tokio::test]
    async fn slack_watchdog_cooldown_suppresses_repeated_keys() {
        let wd = SlackWatchdog::new("http://localhost/slack", Duration::from_secs(60));

        assert!(wd.should_send("low_balance:main:ckBTC").await);
        assert!(!wd.should_send("low_balance:main:ckBTC").await);
        assert!(wd.should_send("low_balance:trader:ckBTC").await);
    }

    #[test]
    fn lifecycle_and_liquidation_events_are_not_cooldown_keyed() {
        let lifecycle = WatchdogEvent::Lifecycle {
            state: "paused".to_string(),
            details: "Liquidation initiation suspended.".to_string(),
        };
        let liquidation = WatchdogEvent::LiquidationFinalized {
            liquidation_id: "7".to_string(),
            borrower: "aaaaa-aa".to_string(),
            debt_asset: "ICP".to_string(),
            collateral_asset: "ckBTC".to_string(),
            status: "Success".to_string(),
            debt_repaid: "1 ICP".to_string(),
            collateral_received: "0.001 ckBTC".to_string(),
            swap_output: "1.1 ICP".to_string(),
            swapper: "mexc".to_string(),
            expected_profit: "0.1 ICP".to_string(),
            realized_profit: "0.1 ICP".to_string(),
            profit_delta: "+0.000".to_string(),
            round_trip_secs: "3".to_string(),
        };

        assert!(slack_cooldown_key(&lifecycle).is_none());
        assert!(slack_cooldown_key(&liquidation).is_none());
    }
}
