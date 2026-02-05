use chrono::Local;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};

use crate::persistance::ResultStatus;

use super::super::app::App;

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

    if exec.rows.is_empty() {
        let w = Paragraph::new("No WAL entries yet.")
            .block(Block::default().borders(Borders::ALL).title("WAL"));
        f.render_widget(w, layout[1]);
        return;
    }

    let now = Local::now().timestamp();

    let rows = exec.rows.iter().enumerate().map(|(idx, r)| {
        let status = status_label(r.status);
        let status_cell = Cell::new(status).style(status_style(r.status));

        let age_secs = now.saturating_sub(r.updated_at);
        let updated = format_age(age_secs);

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
        Cell::new("Last error"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Min(0),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title("WAL"));

    f.render_widget(table, layout[1]);
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
