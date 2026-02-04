use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};

use super::super::app::App;
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
    draw_balances_compact(f, top[1], app);

    draw_profits_panel(f, chunks[1], app);
}

fn draw_recent_logs(f: &mut Frame<'_>, area: Rect, app: &App) {
    let max_lines = area.height.saturating_sub(2) as usize;
    let start = app.logs.len().saturating_sub(max_lines);
    let lines: Vec<Line> = app.logs.iter().skip(start).map(|l| Line::from(l.as_str())).collect();

    let title = if let Some(err) = &app.last_error {
        format!("Logs (last error: {})", truncate(err, 60))
    } else {
        "Logs".to_string()
    };

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
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
