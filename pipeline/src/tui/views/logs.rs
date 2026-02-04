use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::super::app::App;

pub(super) fn draw_logs(f: &mut Frame<'_>, area: Rect, app: &App) {
    let lines: Vec<Line> = app
        .logs
        .iter()
        .rev()
        .take(200)
        .rev()
        .map(|l| Line::from(l.as_str()))
        .collect();

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Logs"))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
}
