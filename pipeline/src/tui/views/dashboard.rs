use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};
use unicode_width::UnicodeWidthStr;

use crate::persistance::{FinalizerMetaPayload, LiqMetaWrapper, ResultStatus, VenueLegState, VenueLegStatus};
use crate::stages::executor::ExecutionReceipt;

use super::super::app::{App, UiFocus};
use super::super::format::format_i128_amount;

pub(super) fn draw_dashboard(f: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(area);

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(67), Constraint::Percentage(33)])
        .split(chunks[0]);

    draw_recent_logs(f, top[0], app);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(12)])
        .split(top[1]);
    draw_balances_compact(f, right[0], app);
    draw_configuration(f, right[1], app);

    draw_profits_panel(f, chunks[1], app);

    if app.bad_debt_confirm_input.is_some() {
        draw_bad_debt_confirm(f, area, app);
    }
}

fn draw_recent_logs(f: &mut Frame<'_>, area: Rect, app: &App) {
    let lines: Vec<Line> = app.logs.iter().map(|l| super::logs::log_to_line(l)).collect();
    let height = area.height.saturating_sub(2) as usize;
    let content_width = area.width.saturating_sub(2) as usize;
    let wrapped_lines = super::logs::estimate_wrapped_log_lines(&app.logs, content_width);
    let max_scroll = wrapped_lines.saturating_sub(height) as u16;
    let scroll = if !app.dashboard_logs_scroll_active || app.dashboard_logs_follow {
        max_scroll
    } else {
        max_scroll.saturating_sub(app.dashboard_logs_scroll)
    };

    let title = if let Some(err) = &app.last_error {
        format!("Logs (last error: {})", truncate(err, 60))
    } else {
        "Logs".to_string()
    };

    let title = if !app.dashboard_logs_scroll_active {
        format!("{title} (view)")
    } else if app.dashboard_logs_follow {
        title
    } else {
        format!("{title} (scroll)")
    };

    let mut block = Block::default().borders(Borders::ALL).title(title);
    if matches!(app.ui_focus, UiFocus::Logs) {
        block = block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    }

    let w = Paragraph::new(lines)
        .block(block)
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
}

fn draw_balances_compact(f: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(b) = &app.balances else {
        let w = Paragraph::new("No balances yet (press 'b' to refresh).")
            .block(Block::default().borders(Borders::ALL).title("Balances"));
        f.render_widget(w, area);
        return;
    };

    if b.rows.is_empty() {
        let w = Paragraph::new("No assets.").block(Block::default().borders(Borders::ALL).title("Balances"));
        f.render_widget(w, area);
        return;
    }

    let title = format!("Balances (@ {})", b.at.format("%H:%M:%S"));
    let header = Row::new(vec![Cell::new("Asset"), Cell::new("Main"), Cell::new("Recovery")])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let max_rows = area.height.saturating_sub(3) as usize;
    let max_rows = max_rows.max(1).min(b.rows.len());
    let selected = app.balances_selected.min(b.rows.len().saturating_sub(1));
    let half = max_rows / 2;
    let start = selected.saturating_sub(half).min(b.rows.len().saturating_sub(max_rows));
    let end = (start + max_rows).min(b.rows.len());

    let rows = b.rows[start..end].iter().enumerate().map(|(offset, r)| {
        let idx = start + offset;
        let asset_label = format!("{}:{}", r.asset.chain, r.asset.symbol);
        let mut row = Row::new(vec![
            Cell::new(asset_label),
            Cell::new(r.main.clone()),
            Cell::new(r.recovery.clone()),
        ]);
        if idx == app.balances_selected {
            row = row.style(Style::default().bg(Color::DarkGray));
        }
        row
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Percentage(50),
            Constraint::Percentage(50),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(title));

    f.render_widget(table, area);
}

fn draw_configuration(f: &mut Frame<'_>, area: Rect, app: &App) {
    let buy_bad_debt = if app.config.buy_bad_debt {
        Span::styled("true", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("false", Style::default().fg(Color::Green))
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("Venues: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(app.config.enabled_swap_venues.clone()),
            Span::raw(" · "),
            Span::styled("DEX/CEX: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "{}/{} bps",
                app.config.max_dex_slippage_bps, app.config.max_cex_slippage_bps
            )),
            Span::raw(" · "),
            Span::styled("Bad Debt: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} bps", app.config.bad_debt_collateral_slippage_bps)),
        ]),
        Line::from(vec![
            Span::styled("BUY_BAD_DEBT: ", Style::default().add_modifier(Modifier::BOLD)),
            buy_bad_debt,
        ]),
        Line::from(format!("Control socket: {}", app.config.control_socket)),
        Line::from(app.config.log_source.clone()),
        Line::from(format!("Liq ICP: {}", truncate(&app.config.liquidator_principal, 44))),
        Line::from(format!("Trader ICP: {}", truncate(&app.config.trader_principal, 44))),
        Line::from(format!("Liq EVM: {}", truncate(&app.config.evm_address, 44))),
        Line::from(format!("IC: {}", truncate(&app.config.ic_url, 44))),
        Line::from(format!("DB: {}", truncate_start(&app.config.db_path, 44))),
        Line::from(format!("Export: {}", truncate_start(&app.config.export_path, 44))),
    ];

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Configuration"))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
}

fn draw_profits_panel(f: &mut Frame<'_>, area: Rect, app: &App) {
    let wal_hint = if let Some(err) = &app.wal_error {
        format!("WAL error: {}", truncate(err, 32))
    } else if let Some(wal) = &app.wal {
        format!(
            "WAL @ {} inflight={} wait={} operator={} unresumable={} ok={} fail={}",
            wal.at.format("%H:%M:%S"),
            wal.counts.inflight,
            wal.counts.waiting_collateral + wal.counts.waiting_profit,
            wal.counts.operator_required,
            wal.counts.unresumable,
            wal.counts.succeeded,
            wal.counts.failed_retryable + wal.counts.failed_permanent
        )
    } else {
        "WAL: loading…".to_string()
    };

    let profits_hint = match app.profits.as_ref() {
        Some(p) => format!("Profits @ {}", p.at.format("%H:%M:%S")),
        None => "Profits: loading…".to_string(),
    };
    let title = truncate(&format!("{profits_hint} · {wal_hint}"), 96);

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(inner);

    draw_profits_table(f, columns[0], app);
    draw_recent_outcomes_table(f, columns[1], app);
}

fn draw_profits_table(f: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(p) = &app.profits else {
        let w = Paragraph::new("No profit data yet (press 'p').").wrap(Wrap { trim: false });
        f.render_widget(w, area);
        return;
    };

    let header = Row::new(vec![Cell::new("Sym"), Cell::new("Realized"), Cell::new("Δ")])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let max_rows = area.height.saturating_sub(1) as usize;
    let rows = p.rows.iter().take(max_rows.max(1)).map(|r| {
        let delta = r.realized - r.expected;
        let delta_txt = if delta >= 0 {
            format!("+{}", format_i128_amount(delta, r.decimals))
        } else {
            format_i128_amount(delta, r.decimals)
        };
        let delta_style = match delta.cmp(&0) {
            std::cmp::Ordering::Greater => Style::default().fg(Color::Green),
            std::cmp::Ordering::Less => Style::default().fg(Color::Red),
            std::cmp::Ordering::Equal => Style::default(),
        };

        Row::new(vec![
            Cell::new(r.symbol.clone()),
            Cell::new(format_i128_amount(r.realized, r.decimals)),
            Cell::from(Span::styled(delta_txt, delta_style)),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Percentage(55),
            Constraint::Percentage(45),
        ],
    )
    .header(header);

    f.render_widget(table, area);
}

fn draw_recent_outcomes_table(f: &mut Frame<'_>, area: Rect, app: &App) {
    // WAL rows are the stable view for multi-venue progress and history. Keep
    // using them after completion so persisted venue branches never collapse
    // merely because the parent status changed.
    if app
        .executions
        .as_ref()
        .is_some_and(|executions| !executions.rows.is_empty())
    {
        if let Some(executions) = &app.executions
            && !executions.rows.is_empty()
        {
            let header = Row::new(vec![
                Cell::new("At"),
                Cell::new("Status"),
                Cell::new("Pair"),
                Cell::new("PnL"),
                Cell::new("Liq"),
                Cell::new("Try"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD));

            let max_rows = area.height.saturating_sub(1) as usize;
            let max_rows = max_rows.max(1);
            let mut rows = Vec::new();
            for r in &executions.rows {
                if rows.len() >= max_rows {
                    break;
                }

                // One decode per row: the fork rows and the compact context are
                // both projections of the same envelope, and this runs on the
                // render tick.
                let meta = parse_execution_meta(&r.meta_json);
                let venue_rows = multi_venue_fork_rows(meta.as_ref());
                // Keep a multi-venue execution together. It is easier to scan
                // one fewer liquidation than to show an orphaned first branch
                // without the remaining venue rows.
                if !rows.is_empty() && rows.len() + 1 + venue_rows.len() > max_rows {
                    break;
                }

                let at = chrono::DateTime::<chrono::Utc>::from_timestamp(r.updated_at, 0)
                    .map(|dt| dt.with_timezone(&chrono::Local).format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "-".to_string());
                let status = status_short(r.status);
                let status_style = status_style(r.status);
                let (pair, pnl, pnl_style) = compact_liq_context(meta.as_ref());

                rows.push(Row::new(vec![
                    Cell::new(at),
                    Cell::from(Span::styled(status, status_style)),
                    Cell::new(truncate(&pair, 18)),
                    Cell::from(Span::styled(truncate(&pnl, 20), pnl_style)),
                    Cell::new(truncate(&r.liq_id, 10)),
                    Cell::new(r.attempt.to_string()),
                ]));

                for venue in venue_rows.into_iter().take(max_rows.saturating_sub(rows.len())) {
                    rows.push(Row::new(vec![
                        Cell::new(""),
                        Cell::from(Span::styled(venue.branch_and_venue, venue.style)),
                        Cell::from(Span::styled(truncate(&venue.stage, 18), venue.style)),
                        Cell::new(truncate(&venue.amount, 20)),
                        Cell::new(""),
                        Cell::from(Span::styled(venue.alert, venue.alert_style)),
                    ]));
                }
            }

            let table = Table::new(
                rows,
                [
                    Constraint::Length(8),
                    Constraint::Length(14),
                    Constraint::Length(17),
                    Constraint::Length(21),
                    Constraint::Min(8),
                    Constraint::Length(4),
                ],
            )
            .header(header);

            f.render_widget(table, area);
            return;
        }

        let w = Paragraph::new(format!(
            "No executed liquidations yet.\n(last batch: {})",
            app.last_outcomes
        ))
        .wrap(Wrap { trim: false });
        f.render_widget(w, area);
        return;
    }

    let header = Row::new(vec![
        Cell::new("At"),
        Cell::new("Realized (Δ)"),
        Cell::new("Expected"),
        Cell::new("Status"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let max_rows = area.height.saturating_sub(1) as usize;
    let rows = app.recent_outcomes.iter().rev().take(max_rows.max(1)).map(|r| {
        let delta = r.outcome.realized_profit - r.outcome.expected_profit;
        let profit_style = match delta.cmp(&0) {
            std::cmp::Ordering::Greater => Style::default().fg(Color::Green),
            std::cmp::Ordering::Less => Style::default().fg(Color::Red),
            std::cmp::Ordering::Equal => Style::default(),
        };

        let realized = format!(
            "{} ({})",
            r.outcome.formatted_realized_profit(),
            r.outcome.formatted_profit_delta()
        );
        let expected = r.outcome.formatted_expected_profit();
        let status = truncate(&r.outcome.status.description(), 28);

        Row::new(vec![
            Cell::new(r.at.format("%H:%M:%S").to_string()),
            Cell::from(Span::styled(realized, profit_style)),
            Cell::new(expected),
            Cell::new(status),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Percentage(35),
            Constraint::Percentage(25),
            Constraint::Percentage(40),
        ],
    )
    .header(header);

    f.render_widget(table, area);
}

#[derive(Debug, PartialEq, Eq)]
struct VenueForkRow {
    branch_and_venue: String,
    stage: String,
    amount: String,
    alert: String,
    style: Style,
    alert_style: Style,
}

/// Expands every persisted multi-venue plan regardless of parent WAL status.
/// Before planning is committed an enqueued row has no legs to display yet.
fn multi_venue_fork_rows(meta: Option<&ParsedExecutionMeta>) -> Vec<VenueForkRow> {
    let Some(ParsedExecutionMeta::Wrapper(wrapper)) = meta else {
        return Vec::new();
    };
    let Some(meta_v2) = &wrapper.meta_v2 else {
        return Vec::new();
    };
    let FinalizerMetaPayload::MultiVenueSwap(state) = &meta_v2.payload;
    venue_fork_rows(&state.legs)
}

fn venue_fork_rows(legs: &[VenueLegState]) -> Vec<VenueForkRow> {
    if legs.is_empty() {
        return Vec::new();
    }

    let last = legs.len() - 1;
    legs.iter()
        .enumerate()
        .map(|(index, leg)| {
            let style = venue_leg_style(leg.status);
            let alert = if leg.last_error.is_some() { "!" } else { "" }.to_string();
            VenueForkRow {
                branch_and_venue: format!(
                    "{} {}",
                    if index == last { "└─" } else { "├─" },
                    venue_display_name(&leg.venue_id)
                ),
                stage: venue_leg_stage(leg),
                amount: venue_leg_amount(leg),
                alert,
                style,
                alert_style: if leg.last_error.is_some() {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                },
            }
        })
        .collect()
}

pub(super) fn venue_display_name(venue_id: &str) -> String {
    match venue_id {
        "icpswap" => "ICPSwap".to_string(),
        "mexc" => "MEXC".to_string(),
        other => other.to_string(),
    }
}

pub(super) fn venue_leg_stage(leg: &VenueLegState) -> String {
    match leg.status {
        VenueLegStatus::Planned => "planned".to_string(),
        VenueLegStatus::Completed => "succeeded".to_string(),
        VenueLegStatus::Recovered => "recovered".to_string(),
        VenueLegStatus::OperatorRequired => "operator required".to_string(),
        VenueLegStatus::FailedPermanent => "failed".to_string(),
        VenueLegStatus::Running => persisted_venue_step(leg).unwrap_or_else(|| "running".to_string()),
    }
}

fn persisted_venue_step(leg: &VenueLegState) -> Option<String> {
    let step = leg
        .execution
        .state
        .get("step")
        .or_else(|| leg.execution.state.pointer("/cex/step"))?
        .as_str()?;
    Some(humanize_step(step))
}

fn humanize_step(step: &str) -> String {
    let mut out = String::with_capacity(step.len() + 4);
    for (index, ch) in step.chars().enumerate() {
        if index > 0 && ch.is_uppercase() && !out.ends_with(' ') {
            out.push(' ');
        }
        if ch == '_' || ch == '-' {
            if !out.ends_with(' ') {
                out.push(' ');
            }
        } else {
            out.extend(ch.to_lowercase());
        }
    }
    out
}

fn venue_leg_amount(leg: &VenueLegState) -> String {
    if let Some(result) = &leg.result {
        let mut received = leg.quote.estimated_receive.clone();
        received.value = result.receive_amount.clone();
        return received.formatted();
    }
    format!("{} alloc", leg.request.pay_amount.formatted())
}

pub(super) fn venue_leg_style(status: VenueLegStatus) -> Style {
    match status {
        VenueLegStatus::Completed => Style::default().fg(Color::Green),
        VenueLegStatus::Recovered => Style::default().fg(Color::Cyan),
        VenueLegStatus::OperatorRequired => Style::default().fg(Color::Magenta),
        VenueLegStatus::FailedPermanent => Style::default().fg(Color::Red),
        VenueLegStatus::Running => Style::default().fg(Color::Yellow),
        VenueLegStatus::Planned => Style::default().fg(Color::DarkGray),
    }
}

fn truncate(s: &str, max: usize) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i >= max {
            out.push('…');
            break;
        }
        out.push(ch);
    }
    out
}

fn truncate_start(s: &str, max: usize) -> String {
    if max <= 1 {
        return "…".to_string();
    }

    let len = s.width();
    if len <= max {
        return s.to_string();
    }

    let tail: String = s
        .chars()
        .rev()
        .take(max - 1)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("…{}", tail)
}

fn status_short(status: ResultStatus) -> &'static str {
    match status {
        ResultStatus::Enqueued => "enqueued",
        ResultStatus::InFlight => "inflight",
        ResultStatus::Succeeded => "succeeded",
        ResultStatus::FailedRetryable => "failed(r)",
        ResultStatus::FailedPermanent => "failed(p)",
        ResultStatus::WaitingCollateral => "wait_collat",
        ResultStatus::WaitingProfit => "wait_profit",
        ResultStatus::OperatorRequired => "operator",
        ResultStatus::Unresumable => "unresumable",
    }
}

fn status_style(status: ResultStatus) -> Style {
    match status {
        ResultStatus::Succeeded => Style::default().fg(Color::Green),
        ResultStatus::FailedRetryable | ResultStatus::FailedPermanent => Style::default().fg(Color::Red),
        ResultStatus::InFlight => Style::default().fg(Color::Yellow),
        ResultStatus::WaitingCollateral | ResultStatus::WaitingProfit => Style::default().fg(Color::Cyan),
        ResultStatus::OperatorRequired => Style::default().fg(Color::Magenta),
        ResultStatus::Unresumable => Style::default().fg(Color::Red),
        ResultStatus::Enqueued => Style::default().fg(Color::DarkGray),
    }
}

fn compact_liq_context(meta: Option<&ParsedExecutionMeta>) -> (String, String, Style) {
    let default_profit = ("-".to_string(), Style::default().fg(Color::DarkGray));

    match meta {
        Some(ParsedExecutionMeta::Wrapper(wrapper)) => {
            let pair = format!(
                "{}→{}",
                wrapper.receipt.request.collateral_asset.symbol(),
                wrapper.receipt.request.debt_asset.symbol()
            );

            if let Some(snapshot) = &wrapper.profit_snapshot {
                let expected = snapshot.expected_profit_raw.parse::<i128>().ok();
                if let Some(realized) = snapshot
                    .realized_profit_raw
                    .as_deref()
                    .and_then(|raw| raw.parse::<i128>().ok())
                {
                    let delta_or_value = expected.map(|exp| realized - exp).unwrap_or(realized);
                    let style = compact_profit_style(delta_or_value);
                    let pnl = format!(
                        "{} {}",
                        format_i128_amount(realized, Some(snapshot.debt_decimals)),
                        snapshot.debt_symbol
                    );
                    return (pair, pnl, style);
                }

                if let Some(expected) = expected {
                    let style = compact_profit_style(expected);
                    let pnl = format!(
                        "{} {}",
                        format_i128_amount(expected, Some(snapshot.debt_decimals)),
                        snapshot.debt_symbol
                    );
                    return (pair, pnl, style);
                }
            }

            let expected = wrapper.receipt.request.expected_profit;
            let decimals = wrapper.receipt.request.debt_asset.decimals();
            let symbol = wrapper.receipt.request.debt_asset.symbol();
            let pnl = format!("{} {}", format_i128_amount(expected, Some(decimals)), symbol);
            (pair, pnl, compact_profit_style(expected))
        }
        Some(ParsedExecutionMeta::Receipt(receipt)) => {
            let pair = format!(
                "{}→{}",
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol()
            );
            let expected = receipt.request.expected_profit;
            let pnl = format!(
                "{} {}",
                format_i128_amount(expected, Some(receipt.request.debt_asset.decimals())),
                receipt.request.debt_asset.symbol()
            );
            (pair, pnl, compact_profit_style(expected))
        }
        None => ("-".to_string(), default_profit.0, default_profit.1),
    }
}

enum ParsedExecutionMeta {
    Wrapper(LiqMetaWrapper),
    Receipt(ExecutionReceipt),
}

fn parse_execution_meta(raw: &str) -> Option<ParsedExecutionMeta> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return None;
    }

    if let Ok(wrapper) = serde_json::from_str::<LiqMetaWrapper>(trimmed) {
        return Some(ParsedExecutionMeta::Wrapper(wrapper));
    }

    serde_json::from_str::<ExecutionReceipt>(trimmed)
        .ok()
        .map(ParsedExecutionMeta::Receipt)
}

fn compact_profit_style(delta: i128) -> Style {
    match delta.cmp(&0) {
        std::cmp::Ordering::Greater => Style::default().fg(Color::Green),
        std::cmp::Ordering::Less => Style::default().fg(Color::Red),
        std::cmp::Ordering::Equal => Style::default().fg(Color::DarkGray),
    }
}

fn draw_bad_debt_confirm(f: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(70, 35, area);
    f.render_widget(Clear, popup);

    let input = app.bad_debt_confirm_input.as_deref().unwrap_or("");

    let lines = vec![
        Line::from(Span::styled(
            "!!! BAD DEBT MODE !!!",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from("This bot WILL repay bad debt (you eat the loss)."),
        Line::from("Type 'yes' then Enter to start · Esc cancels"),
        Line::from(""),
        Line::from(vec![
            Span::styled("Input: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(input.to_string()),
        ]),
    ];

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Confirm BAD DEBT"))
        .wrap(Wrap { trim: false });
    f.render_widget(w, popup);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);

    horizontal[1]
}

#[cfg(test)]
mod tests {
    use candid::Nat;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use serde_json::json;

    use super::{humanize_step, venue_fork_rows};
    use crate::{
        persistance::{VenueExecutionState, VenueLegQuote, VenueLegState, VenueLegStatus},
        swappers::model::SwapRequest,
    };

    fn leg(venue: &str, status: VenueLegStatus, state: serde_json::Value, last_error: Option<&str>) -> VenueLegState {
        let pay_token = ChainToken::EvmNative {
            chain: "ICP".to_string(),
            symbol: "ICP".to_string(),
            decimals: 2,
            fee: Nat::from(1u8),
        };
        let receive_token = ChainToken::EvmNative {
            chain: "ICP".to_string(),
            symbol: "ckUSDC".to_string(),
            decimals: 2,
            fee: Nat::from(1u8),
        };
        let pay_amount = ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(720u64));
        let estimated_receive = ChainTokenAmount::from_raw(receive_token.clone(), Nat::from(810u64));

        VenueLegState {
            leg_id: format!("{venue}-0"),
            venue_id: venue.to_string(),
            request: SwapRequest {
                pay_asset: pay_token.asset_id(),
                pay_amount: pay_amount.clone(),
                receive_asset: receive_token.asset_id(),
                receive_address: None,
                max_slippage_bps: Some(100),
                venue_hint: Some(venue.to_string()),
            },
            quote: VenueLegQuote {
                pay_amount,
                estimated_receive: estimated_receive.clone(),
                conservative_receive: estimated_receive,
                estimated_price_impact_bps: 10.0,
                route_id: venue.to_string(),
            },
            execution: VenueExecutionState {
                venue: venue.to_string(),
                state,
            },
            status,
            result: None,
            last_error: last_error.map(str::to_string),
        }
    }

    #[test]
    fn venue_forks_show_every_branch_and_its_independent_step() {
        let legs = vec![
            leg(
                "icpswap",
                VenueLegStatus::Completed,
                json!({ "step": "Completed" }),
                None,
            ),
            leg(
                "mexc",
                VenueLegStatus::Running,
                json!({ "cex": { "step": "WithdrawPending" } }),
                Some("withdrawal pending"),
            ),
        ];

        let rows = venue_fork_rows(&legs);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].branch_and_venue, "├─ ICPSwap");
        assert_eq!(rows[0].stage, "succeeded");
        assert_eq!(rows[0].amount, "ICP: 7.20 alloc");
        assert_eq!(rows[1].branch_and_venue, "└─ MEXC");
        assert_eq!(rows[1].stage, "withdraw pending");
        assert_eq!(rows[1].alert, "!");
    }

    #[test]
    fn single_leg_multi_venue_plan_stays_expanded() {
        let rows = venue_fork_rows(&[leg(
            "icpswap",
            VenueLegStatus::Planned,
            json!({ "step": "Funding" }),
            None,
        )]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_and_venue, "└─ ICPSwap");
        assert_eq!(rows[0].stage, "planned");
    }

    #[test]
    fn step_names_are_human_readable() {
        assert_eq!(humanize_step("TradePending"), "trade pending");
        assert_eq!(humanize_step("operator_required"), "operator required");
    }
}
