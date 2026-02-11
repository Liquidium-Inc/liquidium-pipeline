use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use super::super::app::App;
use super::super::format::format_i128_amount;

pub(super) fn draw_profits(f: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(p) = &app.profits else {
        let w = Paragraph::new("No profit data yet (ensure EXPORT_PATH exists, or press 'p').")
            .block(Block::default().borders(Borders::ALL).title("Profits"));
        f.render_widget(w, area);
        return;
    };

    let title = format!("Profits (updated @ {})", p.at.format("%H:%M:%S"));

    let header = Row::new(vec![
        Cell::new("Symbol"),
        Cell::new("Count"),
        Cell::new("Realized"),
        Cell::new("Expected"),
        Cell::new("Δ"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = p.rows.iter().map(|r| {
        let delta = r.realized - r.expected;
        Row::new(vec![
            Cell::new(r.symbol.clone()),
            Cell::new(r.count.to_string()),
            Cell::new(format_i128_amount(r.realized, r.decimals)),
            Cell::new(format_i128_amount(r.expected, r.decimals)),
            Cell::new(format_i128_amount(delta, r.decimals)),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Percentage(26),
            Constraint::Percentage(26),
            Constraint::Percentage(26),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(title));

    f.render_widget(table, area);
}
