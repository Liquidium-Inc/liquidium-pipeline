use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};
use unicode_width::UnicodeWidthStr;

use crate::persistance::ResultStatus;

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
            Span::styled("Swapper: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(app.config.swapper_mode.clone()),
            Span::raw(" · "),
            Span::styled("DEX/CEX: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "{}/{} bps",
                app.config.max_dex_slippage_bps, app.config.max_cex_slippage_bps
            )),
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
            "WAL @ {} inflight={} wait={} ok={} fail={}",
            wal.at.format("%H:%M:%S"),
            wal.counts.inflight,
            wal.counts.waiting_collateral + wal.counts.waiting_profit,
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
    if app.recent_outcomes.is_empty() {
        if let Some(executions) = &app.executions
            && !executions.rows.is_empty()
        {
            let header = Row::new(vec![
                Cell::new("At"),
                Cell::new("Status"),
                Cell::new("Liq ID"),
                Cell::new("Try"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD));

            let max_rows = area.height.saturating_sub(1) as usize;
            let rows = executions.rows.iter().take(max_rows.max(1)).map(|r| {
                let at = chrono::DateTime::<chrono::Utc>::from_timestamp(r.updated_at, 0)
                    .map(|dt| dt.with_timezone(&chrono::Local).format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "-".to_string());
                let status = status_short(r.status);
                let status_style = status_style(r.status);

                Row::new(vec![
                    Cell::new(at),
                    Cell::from(Span::styled(status, status_style)),
                    Cell::new(truncate(&r.liq_id, 24)),
                    Cell::new(r.attempt.to_string()),
                ])
            });

            let table = Table::new(
                rows,
                [
                    Constraint::Length(8),
                    Constraint::Length(12),
                    Constraint::Percentage(100),
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
    }
}

fn status_style(status: ResultStatus) -> Style {
    match status {
        ResultStatus::Succeeded => Style::default().fg(Color::Green),
        ResultStatus::FailedRetryable | ResultStatus::FailedPermanent => Style::default().fg(Color::Red),
        ResultStatus::InFlight => Style::default().fg(Color::Yellow),
        ResultStatus::WaitingCollateral | ResultStatus::WaitingProfit => Style::default().fg(Color::Cyan),
        ResultStatus::Enqueued => Style::default().fg(Color::DarkGray),
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
