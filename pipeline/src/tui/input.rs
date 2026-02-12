use std::sync::Arc;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::{mpsc, watch};

use crate::commands::liquidation_loop::LoopControl;

use super::app::{App, BalancesPanel, Tab, UiFocus, WithdrawField};
use super::events::UiEvent;
use super::snapshots::{compute_profits_snapshot, fetch_balances_snapshot};
use super::views::logs::{scroll_down_by_entries, scroll_up_by_entries};
use super::withdraw::{open_deposit_panel, open_withdraw_panel, refresh_withdraw_deposit_address, submit_withdraw};

const PAGE_SCROLL_ENTRIES: usize = 10;

fn reset_log_focus(app: &mut App) {
    app.ui_focus = UiFocus::Tabs;
    app.logs_scroll_active = false;
    app.dashboard_logs_scroll_active = false;
    app.logs_g_pending = false;
    app.dashboard_logs_g_pending = false;
    app.logs_follow = true;
    app.dashboard_logs_follow = true;
    app.logs_scroll = 0;
    app.dashboard_logs_scroll = 0;
    app.logs_scroll_x = 0;
    app.dashboard_logs_scroll_x = 0;
}

fn reset_executions_focus(app: &mut App) {
    app.ui_focus = UiFocus::Tabs;
    app.executions_details_scroll = 0;
}

fn scroll_logs_up(app: &mut App, entries: usize) {
    app.logs_follow = false;
    app.logs_scroll = scroll_up_by_entries(&app.logs, app.logs_content_width, app.logs_scroll, entries);
}

fn scroll_logs_down(app: &mut App, entries: usize) {
    app.logs_scroll = scroll_down_by_entries(&app.logs, app.logs_content_width, app.logs_scroll, entries);
    if app.logs_scroll == 0 {
        app.logs_follow = true;
    }
}

fn scroll_dashboard_logs_up(app: &mut App, entries: usize) {
    app.dashboard_logs_follow = false;
    app.dashboard_logs_scroll = scroll_up_by_entries(
        &app.logs,
        app.dashboard_logs_content_width,
        app.dashboard_logs_scroll,
        entries,
    );
}

fn scroll_dashboard_logs_down(app: &mut App, entries: usize) {
    app.dashboard_logs_scroll = scroll_down_by_entries(
        &app.logs,
        app.dashboard_logs_content_width,
        app.dashboard_logs_scroll,
        entries,
    );
    if app.dashboard_logs_scroll == 0 {
        app.dashboard_logs_follow = true;
    }
}

pub(super) async fn handle_key(
    app: &mut App,
    key: KeyEvent,
    engine_tx: &watch::Sender<LoopControl>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    ctx: &Arc<crate::context::PipelineContext>,
) -> Result<bool> {
    if let Some(input) = app.bad_debt_confirm_input.as_mut() {
        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), _) => {
                app.should_quit = true;
                return Ok(true);
            }
            (KeyCode::Esc, _) => {
                app.bad_debt_confirm_input = None;
                app.push_log("bad debt: start canceled");
                return Ok(false);
            }
            (KeyCode::Enter, _) => {
                if input.trim().eq_ignore_ascii_case("yes") {
                    app.bad_debt_confirmed = true;
                    app.bad_debt_confirm_input = None;
                    app.engine = LoopControl::Running;
                    let _ = engine_tx.send(LoopControl::Running);
                    app.push_log("bad debt: confirmed (engine started)");
                } else {
                    app.push_log("bad debt: type 'yes' and press Enter to start");
                }
                return Ok(false);
            }
            (KeyCode::Backspace, _) => {
                input.pop();
                return Ok(false);
            }
            (KeyCode::Char(c), KeyModifiers::NONE | KeyModifiers::SHIFT) => {
                if input.len() < 16 && !c.is_control() {
                    input.push(c);
                }
                return Ok(false);
            }
            _ => return Ok(false),
        }
    }

    if matches!(app.tab, Tab::Logs | Tab::Dashboard) {
        let scroll_active = matches!(app.tab, Tab::Logs) && app.logs_scroll_active
            || matches!(app.tab, Tab::Dashboard) && app.dashboard_logs_scroll_active;

        if matches!(key.code, KeyCode::Down) && matches!(app.ui_focus, UiFocus::Tabs) {
            app.ui_focus = UiFocus::Logs;
            return Ok(false);
        }

        if matches!(key.code, KeyCode::Up) && matches!(app.ui_focus, UiFocus::Logs) && !scroll_active {
            reset_log_focus(app);
            return Ok(false);
        }
    }

    if matches!(app.tab, Tab::Executions) {
        if matches!(key.code, KeyCode::Down) && matches!(app.ui_focus, UiFocus::Tabs) {
            app.ui_focus = UiFocus::ExecutionsTable;
            return Ok(false);
        }
        if matches!(key.code, KeyCode::Right) && matches!(app.ui_focus, UiFocus::ExecutionsTable) {
            app.ui_focus = UiFocus::ExecutionsDetails;
            return Ok(false);
        }
        if matches!(key.code, KeyCode::Left) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            app.ui_focus = UiFocus::ExecutionsTable;
            return Ok(false);
        }
    }

    if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) {
        if !matches!(key.code, KeyCode::Char('g')) {
            app.logs_g_pending = false;
        }
    } else {
        app.logs_g_pending = false;
    }

    if matches!(app.tab, Tab::Dashboard) && matches!(app.ui_focus, UiFocus::Logs) {
        if !matches!(key.code, KeyCode::Char('g')) {
            app.dashboard_logs_g_pending = false;
        }
    } else {
        app.dashboard_logs_g_pending = false;
    }

    match (key.code, key.modifiers) {
        (KeyCode::Char('q'), _) => {
            app.should_quit = true;
            return Ok(true);
        }
        (KeyCode::Esc, _) => {
            if app.withdraw.editing.is_some() {
                // Cancel edit and restore previous value.
                if let (Some(field), Some(backup)) = (app.withdraw.editing.take(), app.withdraw.edit_backup.take()) {
                    match field {
                        WithdrawField::Amount => app.withdraw.amount = backup,
                        WithdrawField::ManualDestination => app.withdraw.manual_destination = backup,
                        _ => {}
                    }
                }
                return Ok(false);
            }
            if matches!(app.tab, Tab::Executions) {
                if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
                    app.ui_focus = UiFocus::ExecutionsTable;
                    return Ok(false);
                }
                if matches!(app.ui_focus, UiFocus::ExecutionsTable) {
                    reset_executions_focus(app);
                    return Ok(false);
                }
            }
            if matches!(app.ui_focus, UiFocus::Logs) {
                reset_log_focus(app);
                return Ok(false);
            }
            if matches!(app.tab, Tab::Balances) && !matches!(app.balances_panel, BalancesPanel::None) {
                app.balances_panel = BalancesPanel::None;
                return Ok(false);
            }
            return Ok(false);
        }
        (KeyCode::Enter, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsTable) =>
        {
            app.ui_focus = UiFocus::ExecutionsDetails;
        }
        (KeyCode::Enter, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.ui_focus = UiFocus::ExecutionsTable;
        }
        (KeyCode::Enter, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) =>
        {
            app.logs_scroll_active = !app.logs_scroll_active;
            if !app.logs_scroll_active {
                app.logs_follow = true;
                app.logs_scroll = 0;
                app.logs_g_pending = false;
                app.logs_scroll_x = 0;
            }
        }
        (KeyCode::Enter, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard) && matches!(app.ui_focus, UiFocus::Logs) =>
        {
            app.dashboard_logs_scroll_active = !app.dashboard_logs_scroll_active;
            if !app.dashboard_logs_scroll_active {
                app.dashboard_logs_follow = true;
                app.dashboard_logs_scroll = 0;
                app.dashboard_logs_g_pending = false;
                app.dashboard_logs_scroll_x = 0;
            }
        }
        (KeyCode::Up, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_up(app, 1);
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, 1);
        }
        (KeyCode::Char('k'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_up(app, 1);
        }
        (KeyCode::Char('j'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, 1);
        }
        (KeyCode::PageUp, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_up(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::PageDown, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('u'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_up(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('d'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('g'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            if app.logs_g_pending {
                app.logs_g_pending = false;
                app.logs_follow = false;
                app.logs_scroll = u16::MAX;
            } else {
                app.logs_g_pending = true;
            }
        }
        (KeyCode::Char('G'), KeyModifiers::SHIFT)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            app.logs_g_pending = false;
            app.logs_scroll = 0;
            app.logs_follow = true;
        }
        (KeyCode::Home, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            app.logs_follow = false;
            app.logs_scroll = u16::MAX;
        }
        (KeyCode::End, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            app.logs_scroll = 0;
            app.logs_follow = true;
        }
        (KeyCode::Up, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_up(app, 1);
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_down(app, 1);
        }
        (KeyCode::Char('k'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_up(app, 1);
        }
        (KeyCode::Char('j'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_down(app, 1);
        }
        (KeyCode::PageUp, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_up(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::PageDown, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_down(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('u'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_up(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('d'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            scroll_dashboard_logs_down(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('g'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            if app.dashboard_logs_g_pending {
                app.dashboard_logs_g_pending = false;
                app.dashboard_logs_follow = false;
                app.dashboard_logs_scroll = u16::MAX;
            } else {
                app.dashboard_logs_g_pending = true;
            }
        }
        (KeyCode::Char('G'), KeyModifiers::SHIFT)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            app.dashboard_logs_g_pending = false;
            app.dashboard_logs_scroll = 0;
            app.dashboard_logs_follow = true;
        }
        (KeyCode::Left, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            app.logs_scroll_x = app.logs_scroll_x.saturating_sub(1);
        }
        (KeyCode::Right, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            app.logs_scroll_x = app.logs_scroll_x.saturating_add(1);
        }
        (KeyCode::Left, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            app.dashboard_logs_scroll_x = app.dashboard_logs_scroll_x.saturating_sub(1);
        }
        (KeyCode::Right, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Dashboard)
                && matches!(app.ui_focus, UiFocus::Logs)
                && app.dashboard_logs_scroll_active =>
        {
            app.dashboard_logs_scroll_x = app.dashboard_logs_scroll_x.saturating_add(1);
        }
        (KeyCode::Up, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_sub(1);
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_add(1);
        }
        (KeyCode::Char('k'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_sub(1);
        }
        (KeyCode::Char('j'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_add(1);
        }
        (KeyCode::PageUp, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_sub(10);
        }
        (KeyCode::PageDown, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_add(10);
        }
        (KeyCode::Home, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = u16::MAX;
        }
        (KeyCode::End, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = 0;
        }
        (KeyCode::Char('g'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = u16::MAX;
        }
        (KeyCode::Char('G'), KeyModifiers::SHIFT)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = 0;
        }
        (KeyCode::Char('u'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_sub(10);
        }
        (KeyCode::Char('d'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsDetails) =>
        {
            app.executions_details_scroll = app.executions_details_scroll.saturating_add(10);
        }
        (KeyCode::Tab, KeyModifiers::NONE) => app.next_tab(),
        (KeyCode::BackTab, KeyModifiers::SHIFT) => app.prev_tab(),
        (KeyCode::Left, KeyModifiers::NONE)
            if !matches!(app.tab, Tab::Balances) || matches!(app.balances_panel, BalancesPanel::None) =>
        {
            app.prev_tab();
        }
        (KeyCode::Right, KeyModifiers::NONE)
            if !matches!(app.tab, Tab::Balances) || matches!(app.balances_panel, BalancesPanel::None) =>
        {
            app.next_tab();
        }
        (KeyCode::Char('r'), KeyModifiers::NONE) => {
            let next = match app.engine {
                LoopControl::Running => LoopControl::Paused,
                LoopControl::Paused => LoopControl::Running,
                LoopControl::Stopping => LoopControl::Stopping,
            };

            if matches!(next, LoopControl::Running) && app.config.buy_bad_debt && !app.bad_debt_confirmed {
                app.bad_debt_confirm_input = Some(String::new());
                app.push_log("WARN BAD DEBT MODE ENABLED");
                app.push_log("Type 'yes' then Enter to start (Esc cancels).");
                return Ok(false);
            }

            app.engine = next;
            let _ = engine_tx.send(next);
            app.push_log(match next {
                LoopControl::Running => "engine: RUNNING".to_string(),
                LoopControl::Paused => "engine: PAUSED".to_string(),
                LoopControl::Stopping => "engine: STOPPING".to_string(),
            });
        }
        (KeyCode::Char('b'), KeyModifiers::NONE) => {
            let ui_tx = ui_tx.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let snapshot = fetch_balances_snapshot(ctx).await;
                let _ = ui_tx.send(UiEvent::Balances(snapshot));
            });
            app.push_log("balances: refreshing…");
        }
        (KeyCode::Char('p'), KeyModifiers::NONE) => {
            let export_path = ctx.config.export_path.clone();
            let registry = ctx.registry.clone();
            let ui_tx = ui_tx.clone();
            tokio::spawn(async move {
                let res = tokio::task::spawn_blocking(move || compute_profits_snapshot(&export_path, &registry)).await;
                let msg = match res {
                    Ok(v) => UiEvent::Profits(v),
                    Err(e) => UiEvent::Profits(Err(format!("profit task failed: {e}"))),
                };
                let _ = ui_tx.send(msg);
            });
            app.push_log("profits: refreshing…");
        }
        (KeyCode::Char('e'), KeyModifiers::NONE) => {
            reset_log_focus(app);
            reset_executions_focus(app);
            app.tab = Tab::Executions;
        }
        (KeyCode::Char('w'), KeyModifiers::NONE) => {
            reset_log_focus(app);
            app.tab = Tab::Balances;
            open_withdraw_panel(app, ui_tx);
        }
        (KeyCode::Char('d'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Balances) && matches!(app.balances_panel, BalancesPanel::Withdraw) =>
        {
            refresh_withdraw_deposit_address(app, ui_tx);
        }
        (KeyCode::Char('d'), KeyModifiers::NONE) => {
            reset_log_focus(app);
            app.tab = Tab::Balances;
            open_deposit_panel(app, ui_tx);
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Balances) && matches!(app.balances_panel, BalancesPanel::None) =>
        {
            let max = app.balances.as_ref().map(|b| b.rows.len()).unwrap_or(0);
            if max > 0 {
                app.balances_selected = (app.balances_selected + 1).min(max - 1);
            }
        }
        (KeyCode::Up, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Balances) && matches!(app.balances_panel, BalancesPanel::None) =>
        {
            app.balances_selected = app.balances_selected.saturating_sub(1);
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsTable) =>
        {
            let max = app.executions.as_ref().map(|e| e.rows.len()).unwrap_or(0);
            if max > 0 {
                app.executions_selected = (app.executions_selected + 1).min(max - 1);
                app.executions_details_scroll = 0;
            }
        }
        (KeyCode::Up, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions) && matches!(app.ui_focus, UiFocus::ExecutionsTable) =>
        {
            app.executions_selected = app.executions_selected.saturating_sub(1);
            app.executions_details_scroll = 0;
        }
        (code, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Balances) && matches!(app.balances_panel, BalancesPanel::Withdraw) =>
        {
            // Withdraw form key handling.
            if let Some(editing) = app.withdraw.editing {
                match code {
                    KeyCode::Enter => {
                        app.withdraw.editing = None;
                        app.withdraw.edit_backup = None;
                    }
                    KeyCode::Backspace => match editing {
                        WithdrawField::Amount => {
                            app.withdraw.amount.pop();
                        }
                        WithdrawField::ManualDestination => {
                            app.withdraw.manual_destination.pop();
                        }
                        _ => {}
                    },
                    KeyCode::Char(c) => match editing {
                        WithdrawField::Amount => app.withdraw.amount.push(c),
                        WithdrawField::ManualDestination => app.withdraw.manual_destination.push(c),
                        _ => {}
                    },
                    _ => {}
                }
                return Ok(false);
            }

            match code {
                KeyCode::Down => {
                    app.withdraw.field = match app.withdraw.field {
                        WithdrawField::Source => WithdrawField::Destination,
                        WithdrawField::Destination => WithdrawField::ManualDestination,
                        WithdrawField::ManualDestination => WithdrawField::Asset,
                        WithdrawField::Asset => WithdrawField::Amount,
                        WithdrawField::Amount => WithdrawField::Submit,
                        WithdrawField::Submit => WithdrawField::Source,
                    };
                }
                KeyCode::Up => {
                    app.withdraw.field = match app.withdraw.field {
                        WithdrawField::Source => WithdrawField::Submit,
                        WithdrawField::Destination => WithdrawField::Source,
                        WithdrawField::ManualDestination => WithdrawField::Destination,
                        WithdrawField::Asset => WithdrawField::ManualDestination,
                        WithdrawField::Amount => WithdrawField::Asset,
                        WithdrawField::Submit => WithdrawField::Amount,
                    };
                }
                KeyCode::Left => match app.withdraw.field {
                    WithdrawField::Source => {
                        app.withdraw.source = match app.withdraw.source {
                            super::app::WithdrawAccountKind::Main => super::app::WithdrawAccountKind::Recovery,
                            super::app::WithdrawAccountKind::Trader => super::app::WithdrawAccountKind::Main,
                            super::app::WithdrawAccountKind::Recovery => super::app::WithdrawAccountKind::Trader,
                        };
                    }
                    WithdrawField::Destination => {
                        app.withdraw.destination = match app.withdraw.destination {
                            super::app::WithdrawDestinationKind::Main => super::app::WithdrawDestinationKind::Manual,
                            super::app::WithdrawDestinationKind::Trader => super::app::WithdrawDestinationKind::Main,
                            super::app::WithdrawDestinationKind::Recovery => {
                                super::app::WithdrawDestinationKind::Trader
                            }
                            super::app::WithdrawDestinationKind::Manual => {
                                super::app::WithdrawDestinationKind::Recovery
                            }
                        };
                    }
                    WithdrawField::Asset => {
                        if !app.withdraw_assets.is_empty() {
                            app.withdraw.asset_idx =
                                (app.withdraw.asset_idx + app.withdraw_assets.len() - 1) % app.withdraw_assets.len();
                        }
                    }
                    _ => {}
                },
                KeyCode::Right => match app.withdraw.field {
                    WithdrawField::Source => {
                        app.withdraw.source = match app.withdraw.source {
                            super::app::WithdrawAccountKind::Main => super::app::WithdrawAccountKind::Trader,
                            super::app::WithdrawAccountKind::Trader => super::app::WithdrawAccountKind::Recovery,
                            super::app::WithdrawAccountKind::Recovery => super::app::WithdrawAccountKind::Main,
                        };
                    }
                    WithdrawField::Destination => {
                        app.withdraw.destination = match app.withdraw.destination {
                            super::app::WithdrawDestinationKind::Main => super::app::WithdrawDestinationKind::Trader,
                            super::app::WithdrawDestinationKind::Trader => {
                                super::app::WithdrawDestinationKind::Recovery
                            }
                            super::app::WithdrawDestinationKind::Recovery => {
                                super::app::WithdrawDestinationKind::Manual
                            }
                            super::app::WithdrawDestinationKind::Manual => super::app::WithdrawDestinationKind::Main,
                        };
                    }
                    WithdrawField::Asset => {
                        if !app.withdraw_assets.is_empty() {
                            app.withdraw.asset_idx = (app.withdraw.asset_idx + 1) % app.withdraw_assets.len();
                        }
                    }
                    _ => {}
                },
                KeyCode::Enter => match app.withdraw.field {
                    WithdrawField::Amount => {
                        app.withdraw.editing = Some(WithdrawField::Amount);
                        app.withdraw.edit_backup = Some(app.withdraw.amount.clone());
                    }
                    WithdrawField::ManualDestination => {
                        if matches!(app.withdraw.destination, super::app::WithdrawDestinationKind::Manual) {
                            app.withdraw.manual_destination.clear();
                            app.withdraw.editing = Some(WithdrawField::ManualDestination);
                            app.withdraw.edit_backup = Some(String::new());
                        }
                    }
                    WithdrawField::Submit => {
                        submit_withdraw(app, ui_tx, ctx);
                    }
                    _ => {}
                },
                KeyCode::Char('s') if matches!(app.withdraw.field, WithdrawField::Submit) => {
                    submit_withdraw(app, ui_tx, ctx);
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(false)
}

pub(super) fn handle_paste(app: &mut App, text: String) {
    let mut chars = text.chars().filter(|c| !c.is_control());

    if let Some(input) = app.bad_debt_confirm_input.as_mut() {
        while input.len() < 16 {
            let Some(c) = chars.next() else {
                break;
            };
            input.push(c);
        }
        return;
    }

    if matches!(app.tab, Tab::Balances)
        && matches!(app.balances_panel, BalancesPanel::Withdraw)
        && let Some(editing) = app.withdraw.editing
    {
        match editing {
            WithdrawField::Amount => {
                app.withdraw.amount.extend(chars);
            }
            WithdrawField::ManualDestination => {
                app.withdraw.manual_destination.extend(chars);
            }
            _ => {}
        }
    }
}
