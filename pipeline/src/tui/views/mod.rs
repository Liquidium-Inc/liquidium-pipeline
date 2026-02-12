use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs};

use crate::commands::liquidation_loop::LoopControl;

use super::app::{App, Tab};

mod balances;
mod dashboard;
mod executions;
pub(super) mod logs;
mod profits;

pub(super) fn draw_ui(f: &mut Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(3), Constraint::Min(0)])
        .split(f.size());

    draw_header(f, chunks[0], app);
    draw_tabs(f, chunks[1], app);
    draw_body(f, chunks[2], app);
}

fn draw_header(f: &mut Frame<'_>, area: Rect, app: &App) {
    let status = match app.engine {
        LoopControl::Running => Span::styled(
            "RUNNING",
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        ),
        LoopControl::Paused => Span::styled(
            "PAUSED",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ),
        LoopControl::Stopping => Span::styled("STOPPING", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
    };

    let line = Line::from(vec![
        Span::styled("Liquidium TUI", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("  |  engine="),
        status,
        Span::raw(
            "  |  keys: r start/pause · b balances · p profits · e executions · w withdraw · d deposit/refresh · tab switch · q quit",
        ),
    ]);

    f.render_widget(Paragraph::new(line), area);
}

fn draw_tabs(f: &mut Frame<'_>, area: Rect, app: &App) {
    let titles: Vec<Line> = Tab::all().iter().map(|t| Line::from(Span::raw(t.title()))).collect();

    let idx = Tab::all().iter().position(|t| *t == app.tab).unwrap_or(0);
    let tabs = Tabs::new(titles)
        .select(idx)
        .block(Block::default().borders(Borders::ALL).title("Views"))
        .highlight_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

    f.render_widget(tabs, area);
}

fn draw_body(f: &mut Frame<'_>, area: Rect, app: &App) {
    match app.tab {
        Tab::Dashboard => dashboard::draw_dashboard(f, area, app),
        Tab::Balances => balances::draw_balances(f, area, app),
        Tab::Profits => profits::draw_profits(f, area, app),
        Tab::Executions => executions::draw_executions(f, area, app),
        Tab::Logs => logs::draw_logs(f, area, app),
    }
}
