use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};

use super::super::app::{App, BalancesPanel, WithdrawAccountKind, WithdrawDestinationKind, WithdrawField};
use super::super::deposit_network_for_asset;

pub(super) fn draw_balances(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();
    if let Some(b) = &app.balances {
        lines.push(Line::from(format!("Updated @ {}", b.at.format("%H:%M:%S"))));
        lines.push(Line::from(
            "Keys: ↑/↓ select · w withdraw · d deposit · b refresh · Esc close panel",
        ));
    } else {
        lines.push(Line::from("No balances yet (press 'b' to refresh)"));
    }
    if let Some(err) = &app.balances_error {
        lines.push(Line::from(Span::styled(
            format!("Balances error: {}", err),
            Style::default().fg(Color::Red),
        )));
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(lines.len() as u16 + 2), Constraint::Min(0)])
        .split(area);

    let header = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Balances"))
        .wrap(Wrap { trim: false });
    f.render_widget(header, layout[0]);

    let Some(b) = &app.balances else {
        return;
    };

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(layout[1]);

    let header = Row::new(vec![
        Cell::new("Asset"),
        Cell::new("Main"),
        Cell::new("Trader"),
        Cell::new("Recovery"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = b.rows.iter().enumerate().map(|(idx, r)| {
        let asset_label = format!("{}:{}", r.asset.chain, r.asset.symbol);
        let mut row = Row::new(vec![
            Cell::new(asset_label),
            Cell::new(r.main.clone()),
            Cell::new(r.trader.clone()),
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
            Constraint::Length(18),
            Constraint::Percentage(27),
            Constraint::Percentage(27),
            Constraint::Percentage(27),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title("Assets"));

    f.render_widget(table, body[0]);

    match app.balances_panel {
        BalancesPanel::None => draw_balances_actions(f, body[1], app),
        BalancesPanel::Withdraw => draw_withdraw(f, body[1], app),
        BalancesPanel::Deposit => draw_deposit(f, body[1], app),
    }
}

fn draw_balances_actions(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from("Actions"));
    lines.push(Line::from("  w  withdraw selected (ICP only)"));
    lines.push(Line::from("  d  MEXC deposit address"));
    lines.push(Line::from("  Esc  close panel"));
    lines.push(Line::from(""));

    if let Some(balances) = &app.balances
        && let Some(selected) = balances.rows.get(app.balances_selected)
    {
        lines.push(Line::from(format!(
            "Selected: {}:{}",
            selected.asset.chain, selected.asset.symbol
        )));
        lines.push(Line::from(""));
    }

    if app.deposit.in_flight {
        lines.push(Line::from(Span::styled(
            "Deposit: fetching…",
            Style::default().fg(Color::Yellow),
        )));
    } else if let Some(res) = &app.deposit.last {
        match res {
            Ok(addr) => {
                lines.push(Line::from("Last deposit address:"));
                lines.push(Line::from(format!("  asset   : {}", addr.asset)));
                lines.push(Line::from(format!("  network : {}", addr.network)));
                lines.push(Line::from(format!("  address : {}", addr.address)));
                if let Some(tag) = addr.tag.as_ref()
                    && !tag.is_empty()
                {
                    lines.push(Line::from(format!("  tag     : {}", tag)));
                }
            }
            Err(err) => {
                lines.push(Line::from(Span::styled(
                    format!("Last deposit error: {}", err),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    }

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Panel"))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
}

fn draw_deposit(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    let asset = app
        .deposit
        .asset
        .as_ref()
        .map(|a| format!("{}:{}", a.chain, a.symbol))
        .unwrap_or_else(|| "-".to_string());
    let network = app.deposit.network.clone().unwrap_or_else(|| "-".to_string());
    lines.push(Line::from(format!("Asset   : {}", asset)));
    lines.push(Line::from(format!("Network : {}", network)));
    lines.push(Line::from(""));

    if app.deposit.in_flight {
        lines.push(Line::from(Span::styled(
            "Fetching deposit address…",
            Style::default().fg(Color::Yellow),
        )));
    } else if let Some(res) = &app.deposit.last {
        match res {
            Ok(addr) => {
                lines.push(Line::from(format!("Address : {}", addr.address)));
                if let Some(tag) = addr.tag.as_ref()
                    && !tag.is_empty()
                {
                    lines.push(Line::from(format!("Tag     : {}", tag)));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(format!("Resolved network: {}", addr.network)));
            }
            Err(err) => {
                lines.push(Line::from(Span::styled(
                    format!("Error: {}", err),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    } else {
        lines.push(Line::from(
            "Press 'd' to fetch the deposit address for the selected asset.",
        ));
    }

    lines.push(Line::from(""));
    lines.push(Line::from("Keys: d refresh · Esc close"));

    let title = match app.deposit.at.as_ref() {
        Some(ts) => format!("Deposit (updated @ {})", ts.format("%H:%M:%S")),
        None => "Deposit".to_string(),
    };

    let w = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false });
    f.render_widget(w, area);
}

fn draw_withdraw(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut rows: Vec<Row> = Vec::new();

    let source = match app.withdraw.source {
        WithdrawAccountKind::Main => "main",
        WithdrawAccountKind::Trader => "trader",
        WithdrawAccountKind::Recovery => "recovery",
    };
    let destination = match app.withdraw.destination {
        WithdrawDestinationKind::Main => "main",
        WithdrawDestinationKind::Trader => "trader",
        WithdrawDestinationKind::Recovery => "recovery",
        WithdrawDestinationKind::Manual => "manual",
    };

    let asset_label = app
        .withdraw_assets
        .get(app.withdraw.asset_idx)
        .map(|a| format!("{}:{}", a.chain, a.symbol))
        .unwrap_or_else(|| "-".to_string());

    let manual_value = if matches!(app.withdraw.destination, WithdrawDestinationKind::Manual) {
        app.withdraw.manual_destination.clone()
    } else {
        "-".to_string()
    };

    let fields: Vec<(WithdrawField, &str, String)> = vec![
        (WithdrawField::Source, "Source", source.to_string()),
        (WithdrawField::Destination, "Destination", destination.to_string()),
        (WithdrawField::ManualDestination, "Manual Dest", manual_value),
        (WithdrawField::Asset, "Asset", asset_label),
        (WithdrawField::Amount, "Amount", app.withdraw.amount.clone()),
        (
            WithdrawField::Submit,
            "Submit",
            if app.withdraw.in_flight {
                "sending…".to_string()
            } else {
                "press Enter".to_string()
            },
        ),
    ];

    for (field, label, value) in fields {
        let mut row = Row::new(vec![Cell::new(label), Cell::new(value)]);
        if field == app.withdraw.field {
            row = row.style(Style::default().bg(Color::DarkGray));
        }
        rows.push(row);
    }

    let table = Table::new(rows, [Constraint::Length(14), Constraint::Min(0)])
        .block(Block::default().borders(Borders::ALL).title("Withdraw (ICP-only)"))
        .header(
            Row::new(vec![Cell::new("Field"), Cell::new("Value")]).style(Style::default().add_modifier(Modifier::BOLD)),
        );

    let mut footer_lines: Vec<Line> = Vec::new();
    footer_lines.push(Line::from(
        "Controls: ↑/↓ select · ←/→ change · Enter edit/submit · d deposit · Esc close/cancel",
    ));

    // Show MEXC deposit address for the selected withdraw asset (if available).
    if let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx) {
        let expected_network = deposit_network_for_asset(asset);
        let matches = app.deposit.asset.as_ref() == Some(asset)
            && app.deposit.network.as_deref() == Some(expected_network.as_str());

        footer_lines.push(Line::from(""));
        footer_lines.push(Line::from("MEXC deposit address:"));

        if matches && app.deposit.in_flight {
            footer_lines.push(Line::from(Span::styled(
                "  fetching…",
                Style::default().fg(Color::Yellow),
            )));
        } else if matches {
            match app.deposit.last.as_ref() {
                Some(Ok(addr)) => {
                    footer_lines.push(Line::from(format!("  network : {}", addr.network)));
                    footer_lines.push(Line::from(format!("  address : {}", addr.address)));
                    if let Some(tag) = addr.tag.as_ref()
                        && !tag.is_empty()
                    {
                        footer_lines.push(Line::from(format!("  tag     : {}", tag)));
                    }
                }
                Some(Err(err)) => {
                    footer_lines.push(Line::from(Span::styled(
                        format!("  error: {}", err),
                        Style::default().fg(Color::Red),
                    )));
                    footer_lines.push(Line::from("  press 'd' to retry"));
                }
                None => {
                    footer_lines.push(Line::from("  (press 'd' to fetch)"));
                }
            }
        } else {
            footer_lines.push(Line::from("  (press 'd' to fetch)"));
        }
    }

    if app.withdraw.editing.is_some() {
        footer_lines.push(Line::from(Span::styled(
            "Editing… (Enter to save, Esc to cancel)",
            Style::default().fg(Color::Yellow),
        )));
    }
    if let Some(res) = &app.withdraw.last_result {
        match res {
            Ok(tx) => footer_lines.push(Line::from(Span::styled(
                format!("Last result: ok (tx={})", tx),
                Style::default().fg(Color::Green),
            ))),
            Err(e) => footer_lines.push(Line::from(Span::styled(
                format!("Last result: error ({})", e),
                Style::default().fg(Color::Red),
            ))),
        }
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(11)])
        .split(area);

    f.render_widget(table, layout[0]);
    f.render_widget(
        Paragraph::new(footer_lines)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .wrap(Wrap { trim: false }),
        layout[1],
    );
}
