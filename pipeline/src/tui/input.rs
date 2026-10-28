use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::commands::liquidation_loop::LoopControl;
use crate::control_plane::{ControlCommand, send_control_command};

use super::app::{App, BalancesPanel, Tab, UiFocus, WithdrawDestination, WithdrawField, WithdrawSource};
use super::events::UiEvent;
use super::log_source::LogControlHandle;
use super::reconcile::{handle_reconcile_key, open_reconcile};
use super::snapshots::{compute_profits_snapshot, fetch_balances_snapshot};
use super::views::logs::{scroll_down_by_entries, scroll_up_by_entries};
use super::withdraw::{open_deposit_panel, open_withdraw_panel, refresh_withdraw_deposit_address, submit_withdraw};

const PAGE_SCROLL_ENTRIES: usize = 10;
const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

enum ControlSendError {
    Timeout,
    Control(String),
}

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

fn scroll_logs_up(app: &mut App, entries: usize) -> bool {
    let before = app.logs_scroll;
    app.logs_follow = false;
    app.logs_scroll = scroll_up_by_entries(&app.logs, app.logs_content_width, app.logs_scroll, entries);
    app.logs_scroll == before
}

fn scroll_logs_down(app: &mut App, entries: usize) {
    app.logs_scroll = scroll_down_by_entries(&app.logs, app.logs_content_width, app.logs_scroll, entries);
    if app.logs_scroll == 0 {
        app.logs_follow = true;
    }
}

fn scroll_dashboard_logs_up(app: &mut App, entries: usize) -> bool {
    let before = app.dashboard_logs_scroll;
    app.dashboard_logs_follow = false;
    app.dashboard_logs_scroll = scroll_up_by_entries(
        &app.logs,
        app.dashboard_logs_content_width,
        app.dashboard_logs_scroll,
        entries,
    );
    app.dashboard_logs_scroll == before
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

const WITHDRAW_FIELDS: [WithdrawField; 6] = [
    WithdrawField::Source,
    WithdrawField::Destination,
    WithdrawField::ManualDestination,
    WithdrawField::Asset,
    WithdrawField::Amount,
    WithdrawField::Submit,
];
const WITHDRAW_SOURCES: [WithdrawSource; 4] = [
    WithdrawSource::Main,
    WithdrawSource::Trader,
    WithdrawSource::Recovery,
    WithdrawSource::Bridge,
];
const ICP_DESTINATIONS: [WithdrawDestination; 5] = [
    WithdrawDestination::Main,
    WithdrawDestination::Trader,
    WithdrawDestination::Recovery,
    WithdrawDestination::Bridge,
    WithdrawDestination::Manual,
];
// Trader and recovery hold ICP tokens only.
const OTHER_DESTINATIONS: [WithdrawDestination; 3] = [
    WithdrawDestination::Main,
    WithdrawDestination::Bridge,
    WithdrawDestination::Manual,
];

/// The option `step` places away from `current`, wrapping at both ends. A
/// value the list does not hold restarts at the first option.
fn cycle<T: Copy + PartialEq>(options: &[T], current: T, step: isize) -> T {
    let Some(index) = options.iter().position(|option| *option == current) else {
        return options[0];
    };
    options[(index as isize + step).rem_euclid(options.len() as isize) as usize]
}

fn destination_options(app: &App) -> &'static [WithdrawDestination] {
    let icp = app
        .withdraw_assets
        .get(app.withdraw.asset_idx)
        .is_none_or(|asset| asset.chain.eq_ignore_ascii_case("icp"));
    if icp { &ICP_DESTINATIONS } else { &OTHER_DESTINATIONS }
}

/// Moves the value of the selected withdraw field one option left or right.
fn step_withdraw_choice(app: &mut App, step: isize) {
    match app.withdraw.field {
        WithdrawField::Source => app.withdraw.source = cycle(&WITHDRAW_SOURCES, app.withdraw.source, step),
        WithdrawField::Destination => {
            app.withdraw.destination = cycle(destination_options(app), app.withdraw.destination, step);
        }
        WithdrawField::Asset if !app.withdraw_assets.is_empty() => {
            let count = app.withdraw_assets.len() as isize;
            app.withdraw.asset_idx = (app.withdraw.asset_idx as isize + step).rem_euclid(count) as usize;
            if !destination_options(app).contains(&app.withdraw.destination) {
                app.withdraw.destination = WithdrawDestination::Main;
            }
        }
        _ => {}
    }
}

async fn send_control_command_timed(
    sock_path: &Path,
    cmd: ControlCommand,
) -> std::result::Result<(), ControlSendError> {
    match timeout(CONTROL_COMMAND_TIMEOUT, send_control_command(sock_path, cmd)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(ControlSendError::Control(err)),
        Err(_) => Err(ControlSendError::Timeout),
    }
}

pub(super) async fn handle_key(
    app: &mut App,
    key: KeyEvent,
    sock_path: &Path,
    log_control: &LogControlHandle,
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
                    match send_control_command_timed(sock_path, ControlCommand::Resume).await {
                        Ok(()) => {
                            app.engine = LoopControl::Running;
                            app.push_log("bad debt: confirmed (engine resumed)");
                        }
                        Err(ControlSendError::Timeout) => {
                            app.push_log("control timeout: daemon did not respond within 2s");
                        }
                        Err(ControlSendError::Control(err)) => {
                            app.push_log(format!("control error: {}", err));
                        }
                    }
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

    if app.reconcile.is_some() {
        handle_reconcile_key(app, key, sock_path).await;
        return Ok(false);
    }

    if edit_withdraw_text(app, key) {
        return Ok(false);
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
            if scroll_logs_up(app, 1) {
                log_control.request_older();
            }
        }
        (KeyCode::Down, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, 1);
        }
        (KeyCode::Char('k'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            if scroll_logs_up(app, 1) {
                log_control.request_older();
            }
        }
        (KeyCode::Char('j'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, 1);
        }
        (KeyCode::PageUp, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            if scroll_logs_up(app, PAGE_SCROLL_ENTRIES) {
                log_control.request_older();
            }
        }
        (KeyCode::PageDown, KeyModifiers::NONE)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            scroll_logs_down(app, PAGE_SCROLL_ENTRIES);
        }
        (KeyCode::Char('u'), KeyModifiers::CONTROL)
            if matches!(app.tab, Tab::Logs) && matches!(app.ui_focus, UiFocus::Logs) && app.logs_scroll_active =>
        {
            if scroll_logs_up(app, PAGE_SCROLL_ENTRIES) {
                log_control.request_older();
            }
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
                log_control.request_older();
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
            log_control.request_older();
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
            if scroll_dashboard_logs_up(app, 1) {
                log_control.request_older();
            }
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
            if scroll_dashboard_logs_up(app, 1) {
                log_control.request_older();
            }
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
            if scroll_dashboard_logs_up(app, PAGE_SCROLL_ENTRIES) {
                log_control.request_older();
            }
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
            if scroll_dashboard_logs_up(app, PAGE_SCROLL_ENTRIES) {
                log_control.request_older();
            }
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
                log_control.request_older();
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

            let command = match next {
                LoopControl::Running => Some(ControlCommand::Resume),
                LoopControl::Paused => Some(ControlCommand::Pause),
                LoopControl::Stopping => None,
            };

            if let Some(command) = command {
                match send_control_command_timed(sock_path, command).await {
                    Ok(()) => {
                        app.engine = next;
                        app.push_log(match next {
                            LoopControl::Running => "engine: RUNNING".to_string(),
                            LoopControl::Paused => "engine: PAUSED".to_string(),
                            LoopControl::Stopping => "engine: STOPPING".to_string(),
                        });
                    }
                    Err(ControlSendError::Timeout) => {
                        app.push_log("control timeout: daemon did not respond within 2s");
                    }
                    Err(ControlSendError::Control(err)) => {
                        app.push_log(format!("control error: {}", err));
                    }
                }
            }
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
        (KeyCode::Char('o'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Executions)
                && matches!(app.ui_focus, UiFocus::ExecutionsTable | UiFocus::ExecutionsDetails) =>
        {
            if let Some(liq_id) = app.selected_execution().map(|row| row.liq_id.clone()) {
                open_reconcile(app, sock_path, liq_id).await;
            }
        }
        (KeyCode::Char('e'), KeyModifiers::NONE) => {
            reset_log_focus(app);
            reset_executions_focus(app);
            app.tab = Tab::Executions;
        }
        (KeyCode::Char('c'), KeyModifiers::NONE) => {
            reset_log_focus(app);
            reset_executions_focus(app);
            app.tab = Tab::Configuration;
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
            match code {
                KeyCode::Down => app.withdraw.field = cycle(&WITHDRAW_FIELDS, app.withdraw.field, 1),
                KeyCode::Up => app.withdraw.field = cycle(&WITHDRAW_FIELDS, app.withdraw.field, -1),
                KeyCode::Left => step_withdraw_choice(app, -1),
                KeyCode::Right => step_withdraw_choice(app, 1),
                KeyCode::Enter => match app.withdraw.field {
                    WithdrawField::Amount => {
                        app.withdraw.editing = Some(WithdrawField::Amount);
                        app.withdraw.edit_backup = Some(app.withdraw.amount.clone());
                    }
                    WithdrawField::ManualDestination => {
                        if matches!(app.withdraw.destination, WithdrawDestination::Manual) {
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

/// Gives a key to the open withdraw text field, if there is one. The field
/// takes every key, so typing an address cannot quit, pause or navigate.
fn edit_withdraw_text(app: &mut App, key: KeyEvent) -> bool {
    if !matches!(app.tab, Tab::Balances) || !matches!(app.balances_panel, BalancesPanel::Withdraw) {
        return false;
    }
    let withdraw = &mut app.withdraw;
    let text = match withdraw.editing {
        Some(WithdrawField::Amount) => &mut withdraw.amount,
        Some(WithdrawField::ManualDestination) => &mut withdraw.manual_destination,
        _ => return false,
    };
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => {
            if let Some(backup) = withdraw.edit_backup.take() {
                *text = backup;
            }
            withdraw.editing = None;
        }
        (KeyCode::Enter, _) => {
            withdraw.editing = None;
            withdraw.edit_backup = None;
        }
        (KeyCode::Backspace, _) => {
            text.pop();
        }
        (KeyCode::Char(c), KeyModifiers::NONE | KeyModifiers::SHIFT) if !c.is_control() => text.push(c),
        _ => {}
    }
    true
}

pub(super) fn handle_paste(app: &mut App, text: String) {
    let mut chars = text.chars().filter(|c| !c.is_control());

    if let Some(panel) = app.reconcile.as_mut() {
        panel.type_text(chars);
        return;
    }

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

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::{edit_withdraw_text, scroll_dashboard_logs_up, scroll_logs_up};
    use crate::commands::liquidation_loop::LoopControl;
    use crate::tui::app::{App, BalancesPanel, Tab, WithdrawField, test_app};

    #[test]
    fn logs_scroll_up_reports_top_edge() {
        let mut app = test_app();
        app.engine = LoopControl::Running;
        app.logs_scroll_active = true;
        app.logs_follow = false;
        app.logs_content_width = 120;
        app.logs.push_back("INFO line1".to_string());
        app.logs.push_back("INFO line2".to_string());

        app.logs_scroll = u16::MAX;
        assert!(scroll_logs_up(&mut app, 1));
    }

    #[test]
    fn dashboard_scroll_up_reports_top_edge() {
        let mut app = test_app();
        app.engine = LoopControl::Running;
        app.dashboard_logs_scroll_active = true;
        app.dashboard_logs_follow = false;
        app.dashboard_logs_content_width = 120;
        app.logs.push_back("INFO line1".to_string());
        app.logs.push_back("INFO line2".to_string());

        app.dashboard_logs_scroll = u16::MAX;
        assert!(scroll_dashboard_logs_up(&mut app, 1));
    }

    fn editing_app(field: WithdrawField) -> App {
        let mut app = test_app();
        app.tab = Tab::Balances;
        app.balances_panel = BalancesPanel::Withdraw;
        app.withdraw.editing = Some(field);
        app.withdraw.edit_backup = Some(String::new());
        app
    }

    fn typed(c: char) -> KeyEvent {
        let modifiers = if c.is_ascii_uppercase() {
            KeyModifiers::SHIFT
        } else {
            KeyModifiers::NONE
        };
        KeyEvent::new(KeyCode::Char(c), modifiers)
    }

    #[test]
    fn the_withdraw_field_takes_every_typed_character() {
        // Each address holds letters that are global shortcuts (q, r, b, c, e, w, d, p, o).
        for address in [
            "qrbcewdpo-2yaaa-aaaae-qddxa-cai",
            "0xAbCdEf0123456789abcdefABCDEF0123456789qw",
            "So11111111111111111111111111111111111111112",
            "bc1qrwdpe0",
        ] {
            let mut app = editing_app(WithdrawField::ManualDestination);
            for c in address.chars() {
                assert!(edit_withdraw_text(&mut app, typed(c)), "{c} left the field");
            }
            assert_eq!(app.withdraw.manual_destination, address);
            assert!(!app.should_quit);
            assert!(matches!(app.tab, Tab::Balances));
            assert!(matches!(app.balances_panel, BalancesPanel::Withdraw));
        }
    }

    #[test]
    fn the_withdraw_field_edits_confirms_and_cancels() {
        let mut app = editing_app(WithdrawField::Amount);
        app.withdraw.amount.clear();
        app.withdraw.edit_backup = Some("1.5".to_string());
        for c in "2.25x".chars() {
            edit_withdraw_text(&mut app, typed(c));
        }
        edit_withdraw_text(&mut app, KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        assert_eq!(app.withdraw.amount, "2.25");

        edit_withdraw_text(&mut app, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.withdraw.amount, "1.5");
        assert_eq!(app.withdraw.editing, None);

        // With no field open, keys go to the global shortcuts.
        assert!(!edit_withdraw_text(&mut app, typed('q')));
    }
}
