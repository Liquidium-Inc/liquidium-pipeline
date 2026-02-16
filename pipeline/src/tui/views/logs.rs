use std::collections::VecDeque;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::super::app::{App, UiFocus};

pub(super) fn log_to_line(line: &str) -> Line<'static> {
    let line = line.trim_end_matches('\r');
    let line = strip_duplicated_timestamp_level_prefix(line);
    if line.is_empty() {
        return Line::from("");
    }

    // Typical tracing-subscriber fmt output:
    // 2026-02-04T10:35:14.778848Z INFO message...
    if let Some((ts, rest)) = split_once_ws(line)
        && looks_like_timestamp(ts)
    {
        let rest = rest.trim_start();
        if let Some((lvl_token, msg)) = split_once_ws(rest)
            && let Some(lvl) = normalize_level(lvl_token)
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
    if let Some((lvl_token, msg)) = split_once_ws(line)
        && let Some(lvl) = normalize_level(lvl_token)
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
    if let Some(level) = detect_level_anywhere(line) {
        return level_style(level);
    }

    let upper = line.to_ascii_uppercase();
    if line.starts_with("error:") || upper.contains("ERROR") || upper.contains("FAILED") || upper.contains("PANIC") {
        return Style::default().fg(Color::Red);
    }
    if upper.contains("WARN") {
        return Style::default().fg(Color::Yellow);
    }
    Style::default()
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
    if looks_structured_kv_message(msg) {
        return highlight_structured_kv_message(msg);
    }

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

fn looks_structured_kv_message(msg: &str) -> bool {
    msg.matches('=').count() >= 3
}

fn highlight_structured_kv_message(msg: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();

    for (idx, token) in msg.split_whitespace().enumerate() {
        if idx > 0 {
            spans.push(Span::raw(" "));
        }

        if let Some((key, value)) = token.split_once('=') {
            spans.push(Span::styled(
                format!("{key}="),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                value.to_string(),
                style_structured_value(key, value),
            ));
            continue;
        }

        spans.push(Span::styled(token.to_string(), style_structured_word(token)));
    }

    spans
}

fn style_structured_value(key: &str, value: &str) -> Style {
    let key_l = key.to_ascii_lowercase();
    let normalized_value = normalize_token_for_match(value);

    if key_l.contains("status")
        && let Some(style) = status_word_style(normalized_value)
    {
        return style;
    }

    if key_l.contains("profit")
        && let Ok(n) = normalized_value.parse::<i128>()
    {
        return numeric_sign_style(n);
    }

    if key_l.ends_with("_id") || key_l == "liquidation_id" {
        return Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    }

    if looks_number_like(normalized_value) {
        return Style::default().fg(Color::LightCyan).add_modifier(Modifier::BOLD);
    }

    if let Some(style) = status_word_style(normalized_value) {
        return style;
    }

    Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
}

fn style_structured_word(word: &str) -> Style {
    let normalized = normalize_token_for_match(word);

    if let Some(style) = status_word_style(normalized) {
        return style;
    }

    if looks_number_like(normalized) {
        return Style::default().fg(Color::LightCyan).add_modifier(Modifier::BOLD);
    }

    if looks_asset_symbol(normalized) {
        return Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);
    }

    Style::default()
}

fn status_word_style(word: &str) -> Option<Style> {
    match word.to_ascii_lowercase().as_str() {
        "success" | "succeeded" | "ok" | "true" => {
            Some(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
        }
        "failed" | "fail" | "error" | "panic" | "false" => {
            Some(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
        }
        "pending" | "retry" | "waiting" => Some(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        _ => None,
    }
}

fn looks_number_like(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return false;
    }
    v.parse::<i128>().is_ok() || v.parse::<f64>().is_ok()
}

fn looks_asset_symbol(value: &str) -> bool {
    if value.len() < 2 || value.len() > 12 {
        return false;
    }
    let has_alpha = value.chars().any(|c| c.is_ascii_alphabetic());
    let has_upper = value.chars().any(|c| c.is_ascii_uppercase());
    has_alpha && has_upper && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn normalize_token_for_match(token: &str) -> &str {
    token.trim_matches(|c: char| matches!(c, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']'))
}

fn numeric_sign_style(value: i128) -> Style {
    match value.cmp(&0) {
        std::cmp::Ordering::Greater => Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        std::cmp::Ordering::Less => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        std::cmp::Ordering::Equal => Style::default().fg(Color::DarkGray).add_modifier(Modifier::BOLD),
    }
}

fn split_once_ws(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(|c: char| c.is_whitespace())?;
    let (a, b) = s.split_at(idx);
    Some((a, b.trim_start()))
}

fn strip_duplicated_timestamp_level_prefix(mut line: &str) -> &str {
    loop {
        let Some((first_ts, first_rest)) = split_once_ws(line) else {
            return line;
        };
        if !looks_like_timestamp(first_ts) {
            return line;
        }
        let Some((first_level, after_first_level)) = split_once_ws(first_rest) else {
            return line;
        };
        if normalize_level(first_level).is_none() {
            return line;
        }

        let Some((second_ts, second_rest)) = split_once_ws(after_first_level) else {
            return line;
        };
        if !looks_like_timestamp(second_ts) {
            return line;
        }
        let Some((second_level, _)) = split_once_ws(second_rest) else {
            return line;
        };
        if normalize_level(second_level).is_none() {
            return line;
        }

        // Drop the outer prefix and keep the inner structured message.
        line = after_first_level;
    }
}

fn looks_like_timestamp(token: &str) -> bool {
    // RFC3339-ish, e.g. 2026-02-04T10:35:14.778848Z
    token.len() >= 19
        && token.get(0..4).is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
        && token.contains(':')
        && (token.contains('T') || token.contains(' '))
}

fn normalize_level(token: &str) -> Option<&'static str> {
    let cleaned = token.trim_matches(|c: char| !c.is_ascii_alphabetic());
    if cleaned.is_empty() {
        return None;
    }

    match cleaned.to_ascii_uppercase().as_str() {
        "ERROR" => Some("ERROR"),
        "WARN" | "WARNING" => Some("WARN"),
        "INFO" => Some("INFO"),
        "DEBUG" => Some("DEBUG"),
        "TRACE" => Some("TRACE"),
        _ => None,
    }
}

fn detect_level_anywhere(line: &str) -> Option<&'static str> {
    line.split_whitespace().take(12).find_map(normalize_level)
}

pub(crate) fn wrapped_row_count_for_entry(line: &str, content_width: usize) -> usize {
    if content_width == 0 {
        return 1;
    }

    // Keep row estimation in lockstep with what ratatui actually renders.
    let width = log_to_line(line).width().max(1);
    width.div_ceil(content_width)
}

pub(super) fn estimate_wrapped_log_lines(logs: &VecDeque<String>, content_width: usize) -> usize {
    if content_width == 0 {
        return logs.len();
    }

    logs.iter()
        .map(|line| wrapped_row_count_for_entry(line, content_width))
        .sum()
}

fn clamped_scroll_rows(logs: &VecDeque<String>, content_width: usize, scroll: u16) -> usize {
    let total_rows = estimate_wrapped_log_lines(logs, content_width);
    usize::from(scroll).min(total_rows)
}

fn saturating_u16(value: usize) -> u16 {
    value.min(usize::from(u16::MAX)) as u16
}

pub(crate) fn scroll_up_by_entries(
    logs: &VecDeque<String>,
    content_width: usize,
    current_scroll: u16,
    entries: usize,
) -> u16 {
    let mut scroll = current_scroll;

    for _ in 0..entries {
        let target = clamped_scroll_rows(logs, content_width, scroll);
        let mut hidden_rows = 0usize;
        let mut step = 0usize;

        for entry in logs.iter().rev() {
            let entry_rows = wrapped_row_count_for_entry(entry, content_width);
            if target < hidden_rows + entry_rows {
                step = hidden_rows + entry_rows - target;
                break;
            }
            hidden_rows += entry_rows;
        }

        if step == 0 {
            break;
        }

        scroll = scroll.saturating_add(saturating_u16(step));
    }

    scroll
}

pub(crate) fn scroll_down_by_entries(
    logs: &VecDeque<String>,
    content_width: usize,
    current_scroll: u16,
    entries: usize,
) -> u16 {
    let mut scroll = current_scroll;

    for _ in 0..entries {
        let target = clamped_scroll_rows(logs, content_width, scroll);
        if target == 0 {
            break;
        }

        let mut hidden_rows = 0usize;
        let mut step = target;

        for entry in logs.iter().rev() {
            let entry_rows = wrapped_row_count_for_entry(entry, content_width);
            if target <= hidden_rows + entry_rows {
                step = target - hidden_rows;
                break;
            }
            hidden_rows += entry_rows;
        }

        if step == 0 {
            break;
        }

        scroll = scroll.saturating_sub(saturating_u16(step));
    }

    scroll
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use ratatui::style::Color;

    use super::{log_to_line, scroll_up_by_entries, wrapped_row_count_for_entry};

    #[test]
    fn highlights_lowercase_level_after_timestamp() {
        let line = log_to_line("2026-02-12T10:00:00.000000Z info started");
        assert_eq!(line.spans[2].content.as_ref(), "INFO ");
        assert_eq!(line.spans[2].style.fg, Some(Color::Green));
    }

    #[test]
    fn highlights_wrapped_warn_level() {
        let line = log_to_line("2026-02-12T10:00:00.000000Z [WARN] warning");
        assert_eq!(line.spans[2].content.as_ref(), "WARN ");
        assert_eq!(line.spans[2].style.fg, Some(Color::Yellow));
    }

    #[test]
    fn fallback_detects_embedded_level_tokens() {
        let line = log_to_line("Feb 12 host liquidator[123]: INFO daemon resumed");
        assert_eq!(line.spans[0].style.fg, Some(Color::Green));
    }

    #[test]
    fn collapses_double_timestamp_and_level_prefix() {
        let line = log_to_line(
            "2026-02-12T14:07:05.072810Z INFO  2026-02-12T14:07:05.071699Z INFO liquidation.init: Initializing liquidations stage",
        );
        let rendered: String = line.spans.iter().map(|span| span.content.as_ref()).collect();
        assert!(rendered.starts_with("2026-02-12T14:07:05.071699Z INFO"));
        assert!(!rendered.contains("2026-02-12T14:07:05.072810Z"));
    }

    #[test]
    fn structured_outcome_line_highlights_status_and_profit_values() {
        let line = log_to_line(
            "2026-02-12T14:06:12.802210Z INFO Liquidation outcome event=\"liquidation_outcome\" swap_status=Success status=Success expected_profit=-68999 realized_profit=6046660 profit_delta=6115659",
        );

        let has_success_green = line
            .spans
            .iter()
            .any(|span| span.content.as_ref().contains("Success") && span.style.fg == Some(Color::Green));
        let has_negative_profit_red = line
            .spans
            .iter()
            .any(|span| span.content.as_ref().contains("-68999") && span.style.fg == Some(Color::Red));
        let has_positive_profit_green = line
            .spans
            .iter()
            .any(|span| span.content.as_ref().contains("6115659") && span.style.fg == Some(Color::Green));

        assert!(has_success_green, "Success should be highlighted in green");
        assert!(has_negative_profit_red, "negative profit should be highlighted in red");
        assert!(has_positive_profit_green, "positive profit should be highlighted in green");
    }

    #[test]
    fn wrapped_row_count_uses_rendered_width_for_deduped_prefix_lines() {
        let raw = "2026-02-12T14:07:05.072810Z INFO  2026-02-12T14:07:05.071699Z INFO liquidation.init: Initializing liquidations stage";
        let rendered = log_to_line(raw);
        let rendered_width = rendered.width().max(1);

        assert_eq!(wrapped_row_count_for_entry(raw, rendered_width), 1);
    }

    #[test]
    fn scroll_up_moves_by_one_rendered_entry_even_when_raw_line_is_longer() {
        let raw = "2026-02-12T14:07:05.072810Z INFO  2026-02-12T14:07:05.071699Z INFO liquidation.init: Initializing liquidations stage";
        let rendered_width = log_to_line(raw).width().max(1);
        let mut logs = VecDeque::new();
        logs.push_back(raw.to_string());

        let next = scroll_up_by_entries(&logs, rendered_width, 0, 1);
        assert_eq!(next, 1);
    }
}
