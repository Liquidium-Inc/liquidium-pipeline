use chrono::{Local, TimeZone};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::types::protocol_types::{LiquidationStatus, TransferStatus, TxStatus};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};
use serde_json::Value;

use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::persistance::{LiqMetaWrapper, ResultStatus, WalProfitSnapshot};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::wal::liq_id_from_receipt;

use super::super::app::{App, ExecutionRowData, UiFocus};

pub(super) fn draw_executions(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    if let Some(exec) = &app.executions {
        lines.push(Line::from(format!("Updated @ {}", exec.at.format("%H:%M:%S"))));
    } else {
        lines.push(Line::from("No executions yet (loading WAL)."));
    }
    lines.push(Line::from(
        "Focus: Down -> table, Enter/Right -> details, Left/Esc -> table, Esc -> tabs",
    ));

    if let Some(wal) = &app.wal {
        lines.push(Line::from(format!(
            "WAL: inflight={} wait={} ok={} fail={} total={}",
            wal.counts.inflight,
            wal.counts.waiting_collateral + wal.counts.waiting_profit,
            wal.counts.succeeded,
            wal.counts.failed_retryable + wal.counts.failed_permanent,
            wal.counts.total
        )));
    } else if let Some(err) = &app.wal_error {
        lines.push(Line::from(Span::styled(
            format!("WAL error: {}", err),
            Style::default().fg(Color::Red),
        )));
    }

    if let Some(err) = &app.executions_error {
        lines.push(Line::from(Span::styled(
            format!("Executions error: {}", err),
            Style::default().fg(Color::Red),
        )));
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(lines.len() as u16 + 2), Constraint::Min(0)])
        .split(area);

    let header = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Executions"))
        .wrap(Wrap { trim: false });
    f.render_widget(header, layout[0]);

    let Some(exec) = &app.executions else {
        return;
    };

    let now = Local::now().timestamp();

    let rows = exec.rows.iter().enumerate().map(|(idx, r)| {
        let status = status_label(r.status);
        let status_cell = Cell::new(status).style(status_style(r.status));

        let age_secs = now.saturating_sub(r.updated_at);
        let updated = format_age(age_secs);

        let profit_cell = profit_cell_for_row(r, app);

        let last_error = r
            .last_error
            .as_deref()
            .map(|e| truncate(e, 34))
            .unwrap_or_else(|| "-".to_string());

        let mut row = Row::new(vec![
            status_cell,
            Cell::new(truncate(&r.liq_id, 14)),
            Cell::new(r.attempt.to_string()),
            Cell::new(r.error_count.to_string()),
            Cell::new(updated),
            profit_cell,
            Cell::new(last_error),
        ]);

        if idx == app.executions_selected {
            row = row.style(Style::default().bg(Color::DarkGray));
        }
        row
    });

    let header = Row::new(vec![
        Cell::new("Status"),
        Cell::new("Liq ID"),
        Cell::new("Att"),
        Cell::new("Err"),
        Cell::new("Age"),
        Cell::new("Profit"),
        Cell::new("Last error"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let body_area = layout[1];
    let body_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(body_area);

    let selected = exec.rows.get(app.executions_selected).or_else(|| exec.rows.first());

    let details_block = {
        let title = if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            "Execution Details (j/k PgUp/PgDn Home/End)"
        } else {
            "Execution Details"
        };
        let mut block = Block::default().borders(Borders::ALL).title(title);
        if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            block = block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
        }
        block
    };

    if let Some(row) = selected {
        let lines = build_execution_details(row);
        let height = body_layout[1].height.saturating_sub(2) as usize;
        let content_width = body_layout[1].width.saturating_sub(2) as usize;
        let wrapped_lines = estimate_wrapped_lines(&lines, content_width);
        let max_scroll = wrapped_lines.saturating_sub(height) as u16;
        let scroll = app.executions_details_scroll.min(max_scroll);
        let details_widget = Paragraph::new(lines)
            .block(details_block)
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(details_widget, body_layout[1]);
    } else {
        let details_widget = Paragraph::new("No selection.")
            .block(details_block)
            .wrap(Wrap { trim: false });
        f.render_widget(details_widget, body_layout[1]);
    }

    let mut table_block = Block::default().borders(Borders::ALL).title("WAL");
    if matches!(app.ui_focus, UiFocus::ExecutionsTable) {
        table_block = table_block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    }

    if exec.rows.is_empty() {
        let w = Paragraph::new("No WAL entries yet.").block(table_block);
        f.render_widget(w, body_layout[0]);
        return;
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(16),
            Constraint::Min(0),
        ],
    )
    .header(header)
    .block(table_block);

    f.render_widget(table, body_layout[0]);
}

fn status_label(status: ResultStatus) -> &'static str {
    match status {
        ResultStatus::Enqueued => "enqueued",
        ResultStatus::InFlight => "in-flight",
        ResultStatus::Succeeded => "succeeded",
        ResultStatus::FailedRetryable => "retry",
        ResultStatus::FailedPermanent => "failed",
        ResultStatus::WaitingCollateral => "wait-col",
        ResultStatus::WaitingProfit => "wait-prof",
    }
}

fn status_style(status: ResultStatus) -> Style {
    match status {
        ResultStatus::Succeeded => Style::default().fg(Color::Green),
        ResultStatus::FailedPermanent => Style::default().fg(Color::Red),
        ResultStatus::FailedRetryable => Style::default().fg(Color::Yellow),
        ResultStatus::InFlight => Style::default().fg(Color::Cyan),
        ResultStatus::WaitingCollateral | ResultStatus::WaitingProfit => Style::default().fg(Color::Yellow),
        ResultStatus::Enqueued => Style::default().fg(Color::DarkGray),
    }
}

fn format_age(age_secs: i64) -> String {
    if age_secs < 0 {
        return "0s".to_string();
    }
    if age_secs < 60 {
        return format!("{}s", age_secs);
    }
    if age_secs < 3600 {
        return format!("{}m", age_secs / 60);
    }
    format!("{}h", age_secs / 3600)
}

fn truncate(s: &str, max: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max {
        return s.to_string();
    }
    if max <= 3 {
        return s.chars().take(max).collect();
    }
    let take = max - 3;
    let mut truncated: String = s.chars().take(take).collect();
    truncated.push_str("...");
    truncated
}

fn estimate_wrapped_lines(lines: &[Line<'_>], content_width: usize) -> usize {
    if content_width == 0 {
        return lines.len();
    }

    lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(content_width))
        .sum()
}

fn build_execution_details(row: &ExecutionRowData) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    push_section_title(&mut lines, "WAL Row");
    lines.push(Line::from(format!("Liq ID: {}", row.liq_id)));
    lines.push(Line::from(format!(
        "Status: {} | attempt: {} | errors: {}",
        status_label(row.status),
        row.attempt,
        row.error_count
    )));
    lines.push(Line::from(format!(
        "Created: {} | updated: {}",
        format_ts(row.created_at),
        format_ts(row.updated_at)
    )));

    if let Some(err) = row.last_error.as_deref() {
        lines.push(Line::from(format!("Last error: {}", err)));
    }

    append_receipt_from_meta(&mut lines, &row.meta_json);

    lines
}

fn push_section_title(lines: &mut Vec<Line<'static>>, title: &str) {
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(Span::styled(
        title.to_string(),
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
}

fn append_receipt_from_meta(lines: &mut Vec<Line<'static>>, raw: &str) {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        push_section_title(lines, "Execution");
        lines.push(Line::from("<no receipt metadata in WAL row>"));
        return;
    }

    match serde_json::from_str::<LiqMetaWrapper>(raw) {
        Ok(wrapper) => {
            append_receipt_lines(lines, &wrapper.receipt);
            append_finalizer_decision(lines, wrapper.finalizer_decision.as_ref());
            append_meta_summary(lines, &wrapper.meta);
        }
        Err(wrapper_err) => match serde_json::from_str::<ExecutionReceipt>(raw) {
            Ok(receipt) => {
                append_receipt_lines(lines, &receipt);
                push_section_title(lines, "Metadata");
                lines.push(Line::from(Span::styled(
                    "Legacy WAL payload: receipt present, wrapper/finalizer decision missing".to_string(),
                    Style::default().fg(Color::Yellow),
                )));
            }
            Err(receipt_err) => {
                if let Ok(value) = serde_json::from_str::<Value>(raw) {
                    push_section_title(lines, "Metadata");
                    lines.push(Line::from(Span::styled(
                        format!(
                            "meta_json unexpected shape (wrapper_err={}, receipt_err={})",
                            wrapper_err, receipt_err
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                    lines.push(Line::from(format!(
                        "JSON type: {}",
                        match value {
                            Value::Null => "null",
                            Value::Bool(_) => "bool",
                            Value::Number(_) => "number",
                            Value::String(_) => "string",
                            Value::Array(_) => "array",
                            Value::Object(_) => "object",
                        }
                    )));
                    return;
                }

                push_section_title(lines, "Metadata");
                lines.push(Line::from(Span::styled(
                    format!(
                        "meta_json parse error (wrapper_err={}, receipt_err={})",
                        wrapper_err, receipt_err
                    ),
                    Style::default().fg(Color::Red),
                )));
                lines.push(Line::from(format!("Raw preview: {}", truncate(raw, 220))));
            }
        },
    }
}

fn append_receipt_lines(lines: &mut Vec<Line<'static>>, receipt: &ExecutionReceipt) {
    push_section_title(lines, "Request");
    let req = &receipt.request;
    lines.push(Line::from(format!("Borrower: {}", req.liquidation.borrower.to_text())));
    lines.push(Line::from(format!(
        "Debt pool: {}",
        req.liquidation.debt_pool_id.to_text()
    )));
    lines.push(Line::from(format!(
        "Collateral pool: {}",
        req.liquidation.collateral_pool_id.to_text()
    )));
    lines.push(Line::from(format!(
        "Debt amount request: {}",
        ChainTokenAmount::from_raw(req.debt_asset.clone(), req.liquidation.debt_amount.clone()).formatted()
    )));
    lines.push(Line::from(format!("Bad debt mode: {}", req.liquidation.buy_bad_debt)));
    lines.push(Line::from(format!(
        "Swap requested: {}",
        if req.swap_args.is_some() { "yes" } else { "no" }
    )));
    if let Some(swap) = req.swap_args.as_ref() {
        lines.push(Line::from(format!(
            "Swap route: {}:{} -> {}:{}",
            swap.pay_asset.chain, swap.pay_asset.symbol, swap.receive_asset.chain, swap.receive_asset.symbol
        )));
    }
    lines.push(Line::from(format!(
        "Debt asset: {} ({})",
        req.debt_asset.symbol(),
        req.debt_asset.chain()
    )));
    lines.push(Line::from(format!(
        "Collateral asset: {} ({})",
        req.collateral_asset.symbol(),
        req.collateral_asset.chain()
    )));
    lines.push(Line::from(format!(
        "Expected profit: {}",
        format_profit(
            req.expected_profit,
            req.debt_asset.decimals(),
            req.debt_asset.symbol().as_str()
        )
    )));
    lines.push(Line::from(format!(
        "Debt approval needed: {}",
        req.debt_approval_needed
    )));

    push_section_title(lines, "Execution");
    lines.push(Line::from(format!(
        "Executor status: {}",
        format_execution_status(&receipt.status)
    )));
    lines.push(Line::from(format!("Change received: {}", receipt.change_received)));

    if let Some(liq) = &receipt.liquidation_result {
        push_section_title(lines, "Liquidation Result");
        lines.push(Line::from(format!("Liquidation ID: {}", liq.id)));
        lines.push(Line::from(format!("Timestamp: {}", format_ts(liq.timestamp as i64))));
        lines.push(Line::from(format!(
            "Liquidation status: {}",
            format_liquidation_status(&liq.status)
        )));
        lines.push(Line::from(format!(
            "Debt repaid: {}",
            ChainTokenAmount::from_raw(req.debt_asset.clone(), liq.amounts.debt_repaid.clone()).formatted()
        )));
        lines.push(Line::from(format!(
            "Collateral received: {}",
            ChainTokenAmount::from_raw(req.collateral_asset.clone(), liq.amounts.collateral_received.clone())
                .formatted()
        )));
        lines.push(Line::from(format!(
            "Change transfer: {}",
            format_tx_status(&liq.change_tx)
        )));
        lines.push(Line::from(format!(
            "Collateral transfer: {}",
            format_tx_status(&liq.collateral_tx)
        )));
    } else {
        push_section_title(lines, "Liquidation Result");
        lines.push(Line::from("<missing liquidation_result>"));
    }
}

fn append_finalizer_decision(
    lines: &mut Vec<Line<'static>>,
    decision: Option<&crate::persistance::FinalizerDecisionSnapshot>,
) {
    push_section_title(lines, "Hybrid Decision Snapshot");
    let Some(decision) = decision else {
        lines.push(Line::from("<not persisted yet>"));
        return;
    };

    lines.push(Line::from(format!(
        "Mode: {} | chosen: {}",
        decision.mode, decision.chosen
    )));
    lines.push(Line::from(format!("Reason: {}", decision.reason)));
    lines.push(Line::from(format!(
        "Min required edge: {:.2} bps",
        decision.min_required_bps
    )));
    lines.push(Line::from(format!(
        "DEX preview: gross={:?} net={:?}",
        decision.dex_preview_gross_bps, decision.dex_preview_net_bps
    )));
    lines.push(Line::from(format!(
        "CEX preview: gross={:?} net={:?}",
        decision.cex_preview_gross_bps, decision.cex_preview_net_bps
    )));
    lines.push(Line::from(format!("Decision time: {}", format_ts(decision.ts))));
}

fn append_meta_summary(lines: &mut Vec<Line<'static>>, meta: &[u8]) {
    push_section_title(lines, "Internal State");
    if meta.is_empty() {
        lines.push(Line::from("State bytes: <empty>"));
        return;
    }

    lines.push(Line::from(format!("State bytes length: {}", meta.len())));
    if let Ok(text) = std::str::from_utf8(meta) {
        lines.push(Line::from("Encoding: utf8"));
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            let mut emitted = false;
            if let Some(step) = value.get("step").and_then(Value::as_str) {
                lines.push(Line::from(format!("CEX step: {}", step)));
                emitted = true;
            }
            if let Some(last_error) = value.get("last_error").and_then(Value::as_str)
                && !last_error.trim().is_empty()
            {
                lines.push(Line::from(format!("CEX last error: {}", last_error)));
                emitted = true;
            }
            if let Some(withdraw_id) = value
                .get("withdraw_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                lines.push(Line::from(format!("CEX withdraw id: {}", withdraw_id)));
                emitted = true;
            }
            if !emitted {
                lines.push(Line::from(format!("State preview: {}", truncate(text, 180))));
            }
            return;
        }

        lines.push(Line::from(format!("State preview: {}", truncate(text, 180))));
        return;
    }

    lines.push(Line::from("Encoding: binary"));
}

fn format_execution_status(status: &ExecutionStatus) -> String {
    status.description()
}

fn format_liquidation_status(status: &LiquidationStatus) -> String {
    match status {
        LiquidationStatus::Success => "success".to_string(),
        LiquidationStatus::FailedLiquidation(err) => format!("failed liquidation ({err})"),
        LiquidationStatus::CollateralTransferFailed(err) => format!("collateral transfer failed ({err})"),
        LiquidationStatus::ChangeTransferFailed(err) => format!("change transfer failed ({err})"),
        LiquidationStatus::InflowProcessed => "inflow processed".to_string(),
        LiquidationStatus::CoreExecuted => "core executed".to_string(),
    }
}

fn format_transfer_status(status: &TransferStatus) -> String {
    match status {
        TransferStatus::Pending => "pending".to_string(),
        TransferStatus::Success => "success".to_string(),
        TransferStatus::Failed(err) => format!("failed ({err})"),
    }
}

fn format_tx_status(tx: &TxStatus) -> String {
    let status = format_transfer_status(&tx.status);
    let tx_id = tx.tx_id.as_deref().unwrap_or("-");
    format!("{status} | tx_id={tx_id}")
}

fn format_ts(secs: i64) -> String {
    if let Some(dt) = Local.timestamp_opt(secs, 0).single() {
        return dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }
    secs.to_string()
}

fn profit_cell_for_row(row: &ExecutionRowData, app: &App) -> Cell<'static> {
    if let Some(snapshot) = profit_snapshot_from_meta(&row.meta_json)
        && let Some((formatted, style)) = profit_display_from_snapshot(&snapshot)
    {
        return Cell::new(formatted).style(style);
    }

    if let Some(outcome) = latest_outcome_for(app, &row.liq_id) {
        let delta = outcome.realized_profit - outcome.expected_profit;
        let style = profit_style_from_delta(delta);
        return Cell::new(outcome.formatted_realized_profit()).style(style);
    }

    if let Some((expected, decimals, symbol)) = expected_profit_from_meta(&row.meta_json) {
        let style = profit_style_from_delta(expected);
        let formatted = format_profit(expected, decimals, &symbol);
        return Cell::new(formatted).style(style);
    }

    Cell::new("-").style(Style::default().fg(Color::DarkGray))
}

fn profit_snapshot_from_meta(raw: &str) -> Option<WalProfitSnapshot> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return None;
    }
    serde_json::from_str::<LiqMetaWrapper>(raw)
        .ok()
        .and_then(|wrapper| wrapper.profit_snapshot)
}

fn profit_display_from_snapshot(snapshot: &WalProfitSnapshot) -> Option<(String, Style)> {
    let expected = snapshot.expected_profit_raw.parse::<i128>().ok()?;
    let symbol = snapshot.debt_symbol.as_str();
    let decimals = snapshot.debt_decimals;

    if let Some(realized_raw) = snapshot.realized_profit_raw.as_deref()
        && let Ok(realized) = realized_raw.parse::<i128>()
    {
        let style = profit_style_from_delta(realized - expected);
        return Some((format_profit(realized, decimals, symbol), style));
    }

    let style = profit_style_from_delta(expected);
    Some((format_profit(expected, decimals, symbol), style))
}

fn latest_outcome_for<'a>(app: &'a App, liq_id: &str) -> Option<&'a LiquidationOutcome> {
    for r in app.recent_outcomes.iter().rev() {
        if let Ok(outcome_id) = liq_id_from_receipt(&r.outcome.execution_receipt) {
            if outcome_id == liq_id {
                return Some(&r.outcome);
            }
        }
    }
    None
}

fn expected_profit_from_meta(raw: &str) -> Option<(i128, u8, String)> {
    let receipt = extract_receipt(raw)?;
    let expected = receipt.request.expected_profit;
    let decimals = receipt.request.debt_asset.decimals();
    let symbol = receipt.request.debt_asset.symbol().to_string();
    Some((expected, decimals, symbol))
}

fn extract_receipt(raw: &str) -> Option<ExecutionReceipt> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return None;
    }
    if let Ok(wrapper) = serde_json::from_str::<LiqMetaWrapper>(raw) {
        return Some(wrapper.receipt);
    }
    if let Ok(receipt) = serde_json::from_str::<ExecutionReceipt>(raw) {
        return Some(receipt);
    }
    None
}

fn profit_style_from_delta(delta: i128) -> Style {
    match delta.cmp(&0) {
        std::cmp::Ordering::Greater => Style::default().fg(Color::Green),
        std::cmp::Ordering::Less => Style::default().fg(Color::Red),
        std::cmp::Ordering::Equal => Style::default().fg(Color::DarkGray),
    }
}

fn format_profit(amount: i128, decimals: u8, symbol: &str) -> String {
    let scaled = (amount as f64) / 10f64.powi(decimals as i32);
    format!("{scaled} {symbol}")
}

#[cfg(test)]
mod tests {
    use super::{WalProfitSnapshot, profit_display_from_snapshot, truncate};
    use ratatui::style::Color;

    #[test]
    fn truncate_no_change_when_within_limit() {
        assert_eq!(truncate("abcdef", 6), "abcdef");
        assert_eq!(truncate("abcdef", 7), "abcdef");
    }

    #[test]
    fn truncate_max_le_3_takes_chars_without_panic() {
        assert_eq!(truncate("é漢🧪abc", 0), "");
        assert_eq!(truncate("é漢🧪abc", 1), "é");
        assert_eq!(truncate("é漢🧪abc", 2), "é漢");
        assert_eq!(truncate("é漢🧪abc", 3), "é漢🧪");
    }

    #[test]
    fn truncate_max_gt_3_appends_three_dots() {
        assert_eq!(truncate("abcdef", 4), "a...");
        assert_eq!(truncate("abcdef", 5), "ab...");
    }

    #[test]
    fn truncate_handles_multibyte_utf8_without_panic() {
        assert_eq!(truncate("é漢🧪abc", 4), "é...");
        assert_eq!(truncate("é漢🧪abc", 5), "é漢...");
    }

    #[test]
    fn profit_snapshot_prefers_realized_and_colors_by_delta() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "1000".to_string(),
            realized_profit_raw: Some("1200".to_string()),
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        let (text, style) = profit_display_from_snapshot(&snapshot).expect("display should parse");
        assert_eq!(text, "12 ckUSDT");
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn profit_snapshot_falls_back_to_expected_when_realized_missing() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "-250".to_string(),
            realized_profit_raw: None,
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        let (text, style) = profit_display_from_snapshot(&snapshot).expect("display should parse");
        assert_eq!(text, "-2.5 ckUSDT");
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn malformed_profit_snapshot_returns_none() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "not-a-number".to_string(),
            realized_profit_raw: Some("1200".to_string()),
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        assert!(profit_display_from_snapshot(&snapshot).is_none());
    }
}
