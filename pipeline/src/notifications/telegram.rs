use async_trait::async_trait;
use reqwest::Client;
use std::{sync::Arc, time::Duration};
use tracing::warn;

use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::notifications::{LiquidationNotifier, noop_liquidation_notifier};
use crate::stages::executor::ExecutionStatus;

const TELEGRAM_HTTP_TIMEOUT_SECS: u64 = 5;
const TELEGRAM_API_BASE: &str = "https://api.telegram.org/bot";

pub struct TelegramNotifier {
    chat_id: String,
    endpoint: String,
    client: Client,
}

impl TelegramNotifier {
    pub fn new(bot_token: impl AsRef<str>, chat_id: impl Into<String>) -> Self {
        let timeout = Duration::from_secs(TELEGRAM_HTTP_TIMEOUT_SECS);
        let client = Client::builder().timeout(timeout).build().unwrap_or_else(|err| {
            warn!(
                "Failed to build Telegram reqwest client with timeout ({}); using default client",
                err
            );
            Client::new()
        });

        Self {
            chat_id: chat_id.into(),
            endpoint: format!("{TELEGRAM_API_BASE}{}/sendMessage", bot_token.as_ref()),
            client,
        }
    }

    fn render_message(outcome: &LiquidationOutcome) -> Option<String> {
        let liq = outcome.execution_receipt.liquidation_result.as_ref()?;
        let header = if matches!(&outcome.status, ExecutionStatus::Success) {
            "✅ *Liquidation Success*"
        } else {
            "⚠️ *Liquidation Finalized*"
        };

        let liquidation_id = escape_markdown_v2(&liq.id.to_string());
        let borrower = escape_markdown_v2(&outcome.request.liquidation.borrower.to_text());
        let status = escape_markdown_v2(&outcome.status.description());
        let debt_repaid = escape_markdown_v2(&outcome.formatted_debt_repaid());
        let collateral_received = escape_markdown_v2(&outcome.formatted_received_collateral());
        let swap_output = escape_markdown_v2(&outcome.formatted_swap_output());
        let swapper = escape_markdown_v2(&outcome.formatted_swapper());
        let expected_profit = escape_markdown_v2(&outcome.formatted_expected_profit());
        let realized_profit = escape_markdown_v2(&outcome.formatted_realized_profit());
        let profit_delta = escape_markdown_v2(&outcome.formatted_profit_delta());
        let round_trip = escape_markdown_v2(&outcome.formatted_round_trip_secs());

        Some(format!(
            "{header}\n\n🆔 *ID:* `{liquidation_id}`\n👤 *Borrower:* `{borrower}`\n📌 *Status:* {status}\n🧭 *Route:* {swapper}\n\n💸 *Debt Repaid:* {debt_repaid}\n🏦 *Collateral Received:* {collateral_received}\n🔄 *Swap Output:* {swap_output}\n\n📈 *Expected PnL:* {expected_profit}\n💰 *Realized PnL:* {realized_profit}\n📊 *PnL Delta:* {profit_delta}\n⏱ *Round Trip \\(s\\):* {round_trip}",
        ))
    }

    async fn send_message(&self, text: &str, parse_mode: Option<&str>) -> Result<(), String> {
        let mut payload = serde_json::json!({
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": true
        });
        if let Some(parse_mode) = parse_mode {
            payload["parse_mode"] = serde_json::Value::String(parse_mode.to_string());
        }

        let response = self
            .client
            .post(&self.endpoint)
            .json(&payload)
            .send()
            .await
            .map_err(|err| format!("request error: {err}"))?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "<unreadable body>".to_string());
        Err(format!("telegram API returned {status}: {body}"))
    }
}

fn escape_markdown_v2(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' | '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|' | '{'
            | '}' | '.' | '!' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

#[async_trait]
impl LiquidationNotifier for TelegramNotifier {
    async fn notify_startup(&self) {
        let message = "Liquidator started and is scanning for liquidation opportunities.";
        if let Err(err) = self.send_message(message, None).await {
            warn!("Failed Telegram startup notification: {}", err);
        }
    }

    async fn notify_shutdown(&self) {
        let message = "Liquidator is shutting down.";
        if let Err(err) = self.send_message(message, None).await {
            warn!("Failed Telegram shutdown notification: {}", err);
        }
    }

    async fn notify_liquidations(&self, outcomes: &[LiquidationOutcome]) {
        for outcome in outcomes {
            let Some(message) = Self::render_message(outcome) else {
                continue;
            };

            if let Err(err) = self.send_message(&message, Some("MarkdownV2")).await {
                warn!("Failed Telegram liquidation notification: {}", err);
            }
        }
    }
}

pub fn telegram_notifier_from_env() -> Arc<dyn LiquidationNotifier> {
    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let chat_id = std::env::var("TELEGRAM_CHAT_ID")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    match (bot_token, chat_id) {
        (Some(token), Some(chat_id)) => Arc::new(TelegramNotifier::new(token, chat_id)),
        (Some(_), None) => {
            warn!("TELEGRAM_BOT_TOKEN is set but TELEGRAM_CHAT_ID is missing; Telegram notifications disabled");
            noop_liquidation_notifier()
        }
        (None, Some(_)) => {
            warn!("TELEGRAM_CHAT_ID is set but TELEGRAM_BOT_TOKEN is missing; Telegram notifications disabled");
            noop_liquidation_notifier()
        }
        (None, None) => noop_liquidation_notifier(),
    }
}
