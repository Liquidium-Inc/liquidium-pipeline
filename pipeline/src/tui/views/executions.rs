use chrono::{Local, TimeZone};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};
use serde_json::{Value, json};

use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::persistance::{LiqMetaWrapper, ResultStatus};
use crate::stages::executor::ExecutionReceipt;
use crate::wal::liq_id_from_receipt;

use super::super::app::{App, ExecutionRowData, UiFocus};

pub(super) fn draw_executions(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    if let Some(exec) = &app.executions {
        lines.push(Line::from(format!("Updated @ {}", exec.at.format("%H:%M:%S"))));
    } else {
        lines.push(Line::from("No executions yet (loading WAL)."));
    }

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

    let selected = exec
        .rows
        .get(app.executions_selected)
        .or_else(|| exec.rows.first());

    let details_block = {
        let mut block = Block::default().borders(Borders::ALL).title("Execution Details");
        if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            block = block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
        }
        block
    };

    if let Some(row) = selected {
        let lines = build_execution_details(row);
        let height = body_layout[1].height.saturating_sub(2) as usize;
        let max_scroll = lines.len().saturating_sub(height) as u16;
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
    if s.len() <= max {
        return s.to_string();
    }
    if max <= 3 {
        return s[..max].to_string();
    }
    let take = max - 3;
    format!("{}...", &s[..take])
}

fn build_execution_details(row: &ExecutionRowData) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    lines.push(Line::from(format!("liq_id: {}", row.liq_id)));
    lines.push(Line::from(format!(
        "status: {} | attempt: {} | errors: {}",
        status_label(row.status),
        row.attempt,
        row.error_count
    )));
    lines.push(Line::from(format!(
        "created: {} | updated: {}",
        format_ts(row.created_at),
        format_ts(row.updated_at)
    )));

    if let Some(err) = row.last_error.as_deref() {
        lines.push(Line::from(format!("last_error: {}", err)));
    }

    lines.push(Line::from("meta_json:"));

    let meta_lines = parse_meta_lines(&row.meta_json);
    lines.extend(meta_lines);

    lines
}

fn parse_meta_lines(raw: &str) -> Vec<Line<'static>> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return vec![Line::from("<empty>")];
    }

    match serde_json::from_str::<LiqMetaWrapper>(raw) {
        Ok(wrapper) => {
            let meta_val = decode_meta_value(&wrapper.meta);
            let val = json!({
                "receipt": wrapper.receipt,
                "meta": meta_val
            });
            return pretty_json_lines(&val);
        }
        Err(wrapper_err) => match serde_json::from_str::<ExecutionReceipt>(raw) {
            Ok(receipt) => {
                let val = json!({
                    "receipt": receipt,
                    "meta": Value::Null
                });
                return pretty_json_lines(&val);
            }
            Err(receipt_err) => {
                if let Ok(value) = serde_json::from_str::<Value>(raw) {
                    let mut lines = Vec::new();
                    lines.push(Line::from(Span::styled(
                        format!(
                            "meta_json unexpected shape (wrapper_err={}, receipt_err={})",
                            wrapper_err, receipt_err
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                    lines.extend(pretty_json_lines(&value));
                    return lines;
                }

                return vec![
                    Line::from(Span::styled(
                        format!(
                            "meta_json parse error (wrapper_err={}, receipt_err={})",
                            wrapper_err, receipt_err
                        ),
                        Style::default().fg(Color::Red),
                    )),
                    Line::from(truncate(raw, 220)),
                ];
            }
        },
    }
}

fn decode_meta_value(meta: &[u8]) -> Value {
    if meta.is_empty() {
        return Value::Null;
    }
    if let Ok(text) = std::str::from_utf8(meta) {
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            return value;
        }
        return Value::String(text.to_string());
    }
    json!({ "meta_bytes_len": meta.len() })
}

fn pretty_json_lines(value: &Value) -> Vec<Line<'static>> {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| "<failed to render json>".to_string());
    pretty.lines().map(|l| Line::from(l.to_string())).collect()
}

fn format_ts(secs: i64) -> String {
    if let Some(dt) = Local.timestamp_opt(secs, 0).single() {
        return dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }
    secs.to_string()
}

fn profit_cell_for_row(row: &ExecutionRowData, app: &App) -> Cell<'static> {
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
