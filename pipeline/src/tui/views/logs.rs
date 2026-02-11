use std::collections::VecDeque;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::super::app::{App, UiFocus};

pub(super) fn log_to_line(line: &str) -> Line<'static> {
    let line = line.trim_end_matches('\r');
    if line.is_empty() {
        return Line::from("");
    }

    // Typical tracing-subscriber fmt output:
    // 2026-02-04T10:35:14.778848Z INFO message...
    if let Some((ts, rest)) = split_once_ws(line)
        && looks_like_timestamp(ts)
    {
        let rest = rest.trim_start();
        if let Some((lvl, msg)) = split_once_ws(rest)
            && is_level(lvl)
        {
            let mut spans: Vec<Span<'static>> = Vec::new();
            spans.push(Span::styled(
                ts.to_string(),
                Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM),
            ));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                format!("{:<5}", lvl),
                level_style(lvl).add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::raw(" "));
            spans.extend(highlight_message(msg));
            return Line::from(spans);
        }
    }

    // Also support "LEVEL rest..." (for app-internal logs).
    if let Some((lvl, msg)) = split_once_ws(line)
        && is_level(lvl)
    {
        let mut spans: Vec<Span<'static>> = Vec::new();
        spans.push(Span::styled(
            format!("{:<5}", lvl),
            level_style(lvl).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" "));
        spans.extend(highlight_message(msg));
        return Line::from(spans);
    }

    // Fallback: color the whole line if it looks severe.
    Line::from(Span::styled(line.to_string(), fallback_style(line)))
}

fn fallback_style(line: &str) -> Style {
    let upper = line.to_ascii_uppercase();
    if line.starts_with("error:") || upper.contains("ERROR") || upper.contains("FAILED") || upper.contains("PANIC") {
        Style::default().fg(Color::Red)
    } else if upper.contains("WARN") {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    }
}

fn level_style(level: &str) -> Style {
    match level {
        "ERROR" => Style::default().fg(Color::Red),
        "WARN" => Style::default().fg(Color::Yellow),
        // Match "normal run": INFO is green.
        "INFO" => Style::default().fg(Color::Green),
        "DEBUG" => Style::default().fg(Color::Cyan),
        "TRACE" => Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM),
        _ => Style::default(),
    }
}

fn highlight_message(msg: &str) -> Vec<Span<'static>> {
    let patterns: [(&str, Style); 5] = [
        (
            "BAD DEBT MODE",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        (
            "BUY BAD DEBT",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        ("BAD DEBT", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        ("⚠️", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        ("🚨", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
    ];

    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut rest = msg;

    loop {
        let mut best_idx: Option<usize> = None;
        let mut best_pat: Option<&str> = None;
        let mut best_style: Style = Style::default();

        for (pat, style) in patterns.iter() {
            if let Some(idx) = rest.find(pat) {
                let is_better = match best_idx {
                    None => true,
                    Some(best) if idx < best => true,
                    Some(best) if idx == best => best_pat.is_none_or(|p| pat.len() > p.len()),
                    _ => false,
                };
                if is_better {
                    best_idx = Some(idx);
                    best_pat = Some(*pat);
                    best_style = *style;
                }
            }
        }

        let Some(idx) = best_idx else {
            spans.push(Span::raw(rest.to_string()));
            break;
        };
        let pat = best_pat.expect("pattern must exist when index exists");

        if idx > 0 {
            spans.push(Span::raw(rest[..idx].to_string()));
        }
        spans.push(Span::styled(pat.to_string(), best_style));
        rest = &rest[idx + pat.len()..];
    }

    spans
}

fn split_once_ws(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(|c: char| c.is_whitespace())?;
    let (a, b) = s.split_at(idx);
    Some((a, b.trim_start()))
}

fn looks_like_timestamp(token: &str) -> bool {
    // RFC3339-ish, e.g. 2026-02-04T10:35:14.778848Z
    token.len() >= 19
        && token.get(0..4).is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
        && token.contains(':')
        && (token.contains('T') || token.contains(' '))
}

fn is_level(token: &str) -> bool {
    matches!(token, "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE")
}

pub(super) fn estimate_wrapped_log_lines(logs: &VecDeque<String>, content_width: usize) -> usize {
    if content_width == 0 {
        return logs.len();
    }

    logs.iter()
        .map(|line| {
            // Keep empty lines visible as one visual row.
            let width = line.trim_end_matches('\r').chars().count().max(1);
            width.div_ceil(content_width)
        })
        .sum()
}

pub(super) fn draw_logs(f: &mut Frame<'_>, area: Rect, app: &App) {
    let lines: Vec<Line> = app.logs.iter().map(|l| log_to_line(l)).collect();
    let height = area.height.saturating_sub(2) as usize;
    let content_width = area.width.saturating_sub(2) as usize;
    let wrapped_lines = estimate_wrapped_log_lines(&app.logs, content_width);
    let max_scroll = wrapped_lines.saturating_sub(height) as u16;
    let (scroll, title) = if !app.logs_scroll_active {
        (max_scroll, "Logs (view)")
    } else if app.logs_follow {
        (max_scroll, "Logs (follow)")
    } else {
        (max_scroll.saturating_sub(app.logs_scroll), "Logs (scroll)")
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
