use async_trait::async_trait;
use log::warn;
use std::{sync::Arc, time::Duration};

use super::{Watchdog, WatchdogEvent, noop_watchdog, poster::WebhookPoster};

const DEFAULT_BOT_NAME: &str = "liquidator";

pub struct SlackWatchdog {
    poster: WebhookPoster,
    bot_name: String,
}

impl SlackWatchdog {
    pub fn new(url: impl Into<String>, cooldown: Duration) -> Self {
        Self {
            poster: WebhookPoster::new(url, cooldown),
            bot_name: bot_name_from_env(),
        }
    }
}

#[async_trait]
impl Watchdog for SlackWatchdog {
    async fn notify(&self, ev: WatchdogEvent<'_>) {
        let Some(payload) = slack_payload_for_event_with_bot(&ev, &self.bot_name) else {
            return;
        };
        if let Err(error) = self.poster.send(slack_cooldown_key(&ev).as_deref(), &payload).await {
            warn!("Slack notification failed: {error}");
        }
    }
}

/// Slack refuses the whole message when one section text is longer than 3,000 characters.
const SECTION_TEXT_LIMIT: usize = 2_900;

/// Returns the text cut to what one Slack section accepts, with `…` at the cut.
fn within_section(text: &str) -> String {
    if text.chars().count() <= SECTION_TEXT_LIMIT {
        return text.to_string();
    }
    let mut cut: String = text.chars().take(SECTION_TEXT_LIMIT).collect();
    cut.push('…');
    cut
}

fn slack_cooldown_key(ev: &WatchdogEvent<'_>) -> Option<String> {
    match ev {
        WatchdogEvent::LowBalance { account, asset_id, .. } => Some(format!("low_balance:{account}:{asset_id}")),
        WatchdogEvent::LiquidationFinalized {
            liquidation_id,
            borrower,
            debt_asset,
            collateral_asset,
            status,
            ..
        } => Some(format!(
            "liquidation_finalized:{liquidation_id}:{borrower}:{debt_asset}:{collateral_asset}:{status}"
        )),
        WatchdogEvent::OperatorRequired {
            execution_id,
            venue,
            pending_step,
            ..
        } => Some(format!("operator_required:{venue}:{execution_id}:{pending_step}")),
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

#[cfg(test)]
fn slack_payload_for_event(ev: &WatchdogEvent<'_>) -> Option<serde_json::Value> {
    slack_payload_for_event_with_bot(ev, DEFAULT_BOT_NAME)
}

fn bot_name_from_env() -> String {
    std::env::var("BOT_NAME")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| DEFAULT_BOT_NAME.to_string())
}

fn slack_payload_for_event_with_bot(ev: &WatchdogEvent<'_>, bot_name: &str) -> Option<serde_json::Value> {
    match ev {
        WatchdogEvent::LowBalance {
            account,
            asset,
            asset_id,
            current,
            threshold,
        } => {
            let text = format!("[{bot_name}] Low balance: {account} {asset} is {current}, below {threshold}");
            Some(serde_json::json!({
                "text": text,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": format!("*{bot_name}* - Low balance: `{account}` `{asset}`")
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
            let text = format!("[{bot_name}] Liquidator {state}: {details}");
            let mut blocks = vec![serde_json::json!({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!("*{bot_name}* - Liquidator {state}")
                }
            })];
            // An empty section makes Slack refuse the whole alert, so the block is left out.
            if !details.trim().is_empty() {
                blocks.push(serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": within_section(details)
                    }
                }));
            }
            Some(serde_json::json!({
                "text": text,
                "blocks": blocks
            }))
        }
        WatchdogEvent::OperatorRequired {
            execution_id,
            venue,
            pending_step,
            owner,
            details,
        } => {
            let text = format!("[{bot_name}] Operator action required: {venue} {execution_id} ({pending_step})");
            let mut blocks = vec![
                serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("*{bot_name}* - Operator action required for `{venue}`")
                    }
                }),
                serde_json::json!({
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": format!("*Execution ID*\n`{execution_id}`")
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Pending step*\n`{pending_step}`")
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Owner*\n`{owner}`")
                        }
                    ]
                }),
            ];
            // Slack rejects a section block with empty text and drops the whole
            // message, so a blank detail string would silently swallow the very
            // alert that says a swap is stuck. Omit the block instead.
            if !details.trim().is_empty() {
                blocks.push(serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": within_section(details)
                    }
                }));
            }
            Some(serde_json::json!({
                "text": text,
                "blocks": blocks
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
            note,
        } => {
            let text = format!("[{bot_name}] Liquidation finalized: {status} {liquidation_id}");
            let mut blocks = serde_json::json!([
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": format!("*{bot_name}* - Liquidation finalized: `{status}`")
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
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
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
            ]);
            if let (Some(note), Some(list)) = (note, blocks.as_array_mut()) {
                list.push(serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": within_section(&format!("*Note*\n{note}"))
                    }
                }));
            }
            Some(serde_json::json!({
                "text": text,
                "blocks": blocks
            }))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A finished liquidation with this status and note; the other fields are fixed.
    fn finalized(status: &str, note: Option<&str>) -> WatchdogEvent<'static> {
        WatchdogEvent::LiquidationFinalized {
            liquidation_id: "955".to_string(),
            borrower: "aaaaa-aa".to_string(),
            debt_asset: "ckUSDC".to_string(),
            collateral_asset: "ICP".to_string(),
            status: status.to_string(),
            debt_repaid: "0.01 ckUSDC".to_string(),
            collateral_received: "0.005 ICP".to_string(),
            swap_output: "0 ckUSDC".to_string(),
            swapper: "recovery".to_string(),
            expected_profit: "0 ckUSDC".to_string(),
            realized_profit: "-0.01 ckUSDC".to_string(),
            profit_delta: "-0.010".to_string(),
            round_trip_secs: "60".to_string(),
            note: note.map(str::to_string),
        }
    }

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
        assert_eq!(
            payload["text"],
            "[liquidator] Low balance: main ckBTC is 0 ckBTC, below 0.001 ckBTC"
        );
        assert_eq!(payload["blocks"][0]["type"], "section");
    }

    #[test]
    fn slack_payload_includes_custom_bot_name() {
        let ev = WatchdogEvent::Lifecycle {
            state: "started".to_string(),
            details: "Liquidator started.".to_string(),
        };

        let payload = slack_payload_for_event_with_bot(&ev, "prod-liquidator").expect("payload");
        assert_eq!(
            payload["text"],
            "[prod-liquidator] Liquidator started: Liquidator started."
        );
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            "*prod-liquidator* - Liquidator started"
        );
    }

    #[test]
    fn slack_payload_formats_lifecycle_event() {
        let ev = WatchdogEvent::Lifecycle {
            state: "started".to_string(),
            details: "Liquidator started on https://ic0.app.".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("lifecycle should format");
        assert_eq!(
            payload["text"],
            "[liquidator] Liquidator started: Liquidator started on https://ic0.app."
        );
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            "*liquidator* - Liquidator started"
        );
    }

    #[test]
    fn slack_payload_formats_operator_required_event() {
        let ev = WatchdogEvent::OperatorRequired {
            execution_id: "42".to_string(),
            venue: "icpswap".to_string(),
            pending_step: "DepositPending".to_string(),
            owner: "aaaaa-aa".to_string(),
            details: "deposit outcome is ambiguous".to_string(),
        };

        let payload = slack_payload_for_event(&ev).expect("operator alert should format");
        assert_eq!(
            payload["text"],
            "[liquidator] Operator action required: icpswap 42 (DepositPending)"
        );
        assert_eq!(payload["blocks"][1]["fields"][0]["text"], "*Execution ID*\n`42`");
        assert_eq!(payload["blocks"][2]["text"]["text"], "deposit outcome is ambiguous");
        assert_eq!(
            slack_cooldown_key(&ev),
            Some("operator_required:icpswap:42:DepositPending".to_string())
        );
    }

    #[test]
    fn slack_payload_omits_empty_operator_details_block() {
        let ev = WatchdogEvent::OperatorRequired {
            execution_id: "42".to_string(),
            venue: "icpswap".to_string(),
            pending_step: "DepositPending".to_string(),
            owner: "aaaaa-aa".to_string(),
            details: String::new(),
        };

        let payload = slack_payload_for_event(&ev).expect("operator alert should format");
        let blocks = payload["blocks"].as_array().expect("blocks array");
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[1]["fields"][0]["text"], "*Execution ID*\n`42`");
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
            note: None,
        };

        let payload = slack_payload_for_event(&ev).expect("liquidation should format");
        assert_eq!(payload["text"], "[liquidator] Liquidation finalized: Success 42");
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            "*liquidator* - Liquidation finalized: `Success`"
        );
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
            note: None,
        };

        let payload = slack_payload_for_event(&ev).expect("failed liquidation should format");
        assert_eq!(
            payload["text"],
            "[liquidator] Liquidation finalized: SwapFailed: insufficient liquidity n/a"
        );
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            "*liquidator* - Liquidation finalized: `SwapFailed: insufficient liquidity`"
        );
    }

    #[test]
    fn lifecycle_events_are_not_cooldown_keyed_but_liquidations_are() {
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
            note: None,
        };

        assert!(slack_cooldown_key(&lifecycle).is_none());
        assert_eq!(
            slack_cooldown_key(&liquidation),
            Some("liquidation_finalized:7:aaaaa-aa:ICP:ckBTC:Success".to_string())
        );
    }

    #[test]
    fn a_note_is_the_last_block_of_a_finished_liquidation() {
        let note = "No venue can swap the collateral, so it went to the recovery account (tx 17).";
        let payload = slack_payload_for_event(&finalized("Recovered", Some(note))).expect("payload");

        assert_eq!(payload["text"], "[liquidator] Liquidation finalized: Recovered 955");
        let blocks = payload["blocks"].as_array().expect("blocks");
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[3]["text"]["text"], format!("*Note*\n{note}"));
    }

    #[test]
    fn a_finished_liquidation_without_a_note_has_no_note_block() {
        let payload = slack_payload_for_event(&finalized("Success", None)).expect("payload");

        assert_eq!(payload["blocks"].as_array().expect("blocks").len(), 3);
    }

    /// Slack drops the whole message when one section is too long, so a long text is cut instead.
    #[test]
    fn a_long_note_or_detail_is_cut_to_what_one_section_accepts() {
        let long = "x".repeat(5_000);
        let note = slack_payload_for_event(&finalized("Recovered", Some(&long))).expect("payload");
        let alert = slack_payload_for_event(&WatchdogEvent::OperatorRequired {
            execution_id: "955".to_string(),
            venue: "finalizer".to_string(),
            pending_step: "no_route_slow_retry".to_string(),
            owner: "aaaaa-aa".to_string(),
            details: long.clone(),
        })
        .expect("payload");

        for text in [&note["blocks"][3]["text"]["text"], &alert["blocks"][2]["text"]["text"]] {
            let text = text.as_str().expect("text");
            assert_eq!(
                text.chars().count(),
                SECTION_TEXT_LIMIT + 1,
                "the limit plus the cut mark"
            );
            assert!(text.ends_with('…'));
        }
        assert_eq!(within_section("short"), "short");
    }

    /// A stop alert that Slack refuses is the costliest alert to lose.
    #[test]
    fn a_stop_alert_with_a_long_or_empty_text_is_still_sent() {
        let long = slack_payload_for_event(&WatchdogEvent::Lifecycle {
            state: "stopped".to_string(),
            details: "y".repeat(5_000),
        })
        .expect("payload");
        let empty = slack_payload_for_event(&WatchdogEvent::Lifecycle {
            state: "stopped".to_string(),
            details: "  ".to_string(),
        })
        .expect("payload");

        let cut = long["blocks"][1]["text"]["text"].as_str().expect("text");
        assert_eq!(cut.chars().count(), SECTION_TEXT_LIMIT + 1);
        assert_eq!(empty["blocks"].as_array().expect("blocks").len(), 1);
    }
}
