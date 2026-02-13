use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use unicode_width::UnicodeWidthStr;

use super::super::app::App;

pub(super) fn draw_configuration(f: &mut Frame<'_>, area: Rect, app: &App) {
    let buy_bad_debt = if app.config.buy_bad_debt {
        Span::styled("true", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("false", Style::default().fg(Color::Green))
    };

    let mut lines = vec![
        Line::from(format!("Control socket: {}", app.config.control_socket)),
        Line::from(app.config.log_source.clone()),
        Line::from(""),
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
        Line::from(""),
        Line::from(format!("Network: {}", app.config.ic_url)),
        Line::from(format!("Liquidator principal: {}", app.config.liquidator_principal)),
        Line::from(format!("Trader principal: {}", app.config.trader_principal)),
        Line::from(format!("Liquidator EVM: {}", app.config.evm_address)),
        Line::from(format!(
            "DB: {}",
            truncate_start(&app.config.db_path, area.width.saturating_sub(6) as usize)
        )),
        Line::from(format!(
            "Export: {}",
            truncate_start(&app.config.export_path, area.width.saturating_sub(10) as usize)
        )),
    ];

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Opportunity filter:",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    if app.config.opportunity_filter.is_empty() {
        lines.push(Line::from("  none"));
    } else {
        for principal in &app.config.opportunity_filter {
            lines.push(Line::from(format!("  - {}", principal)));
        }
    }

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Configuration"))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
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
