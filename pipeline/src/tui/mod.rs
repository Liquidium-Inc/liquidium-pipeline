mod app;
mod events;
mod format;
mod logging;
mod views;

use std::collections::HashMap;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use candid::Nat;
use chrono::Local;
use crossterm::event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers};
use icrc_ledger_types::icrc1::account::Account;
use num_traits::ToPrimitive;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{mpsc, watch};

use crate::commands::liquidation_loop::{LoopControl, LoopEvent, run_liquidation_loop_controlled};
use crate::config::ConfigTrait;
use crate::context::init_context;
use crate::persistance::sqlite::SqliteWalStore;
use crate::swappers::mexc::mexc_adapter::MexcClient;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;

use self::app::{
    App, BalanceRowData, BalancesPanel, BalancesSnapshot, ProfitBySymbol, ProfitsSnapshot, RecentOutcome, Tab,
    WalCounts, WalSnapshot, WithdrawAccountKind, WithdrawDestinationKind, WithdrawField,
};
use self::events::UiEvent;
use self::logging::{TerminalGuard, init_tui_tracing};
use self::views::draw_ui;

pub async fn run() -> anyhow::Result<()> {
    let ctx = init_context()
        .await
        .map_err(|e| anyhow::anyhow!("init context failed: {e}"))?;
    let ctx = Arc::new(ctx);

    let _terminal_guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    terminal.clear().ok();

    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    if let Err(err) = init_tui_tracing(ui_tx.clone()) {
        let _ = ui_tx.send(UiEvent::Engine(LoopEvent::Error(format!(
            "failed to init tracing: {err}"
        ))));
    }

    // Engine control + events
    let (engine_tx, engine_rx) = watch::channel::<LoopControl>(LoopControl::Paused);
    let (engine_event_tx, mut engine_event_rx) = mpsc::unbounded_channel::<LoopEvent>();
    let engine_handle = tokio::spawn(run_liquidation_loop_controlled(ctx.clone(), engine_rx, engine_event_tx));

    // Forward engine events into the main UI channel.
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = engine_event_rx.recv().await {
                let _ = ui_tx.send(UiEvent::Engine(ev));
            }
        });
    }

    // Ticks
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                if ui_tx.send(UiEvent::Tick).is_err() {
                    break;
                }
            }
        });
    }

    let stop = Arc::new(AtomicBool::new(false));

    // WAL polling
    {
        let ui_tx = ui_tx.clone();
        let stop = stop.clone();
        let db_path = ctx.config.db_path.clone();
        tokio::spawn(async move {
            let store = match SqliteWalStore::new_with_busy_timeout(&db_path, 30_000) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    let _ = ui_tx.send(UiEvent::Wal(Err(format!("db open failed: {e}"))));
                    return;
                }
            };

            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let store = store.clone();
                let res = tokio::task::spawn_blocking(move || store.status_counts()).await;
                match res {
                    Ok(Ok(map)) => {
                        let snapshot = WalSnapshot {
                            counts: WalCounts::from_map(&map),
                            at: Local::now(),
                        };
                        let _ = ui_tx.send(UiEvent::Wal(Ok(snapshot)));
                    }
                    Ok(Err(e)) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db query failed: {e}"))));
                    }
                    Err(e) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db task failed: {e}"))));
                    }
                }
            }
        });
    }

    // Profit polling (from executions.csv)
    {
        let ui_tx = ui_tx.clone();
        let stop = stop.clone();
        let export_path = ctx.config.export_path.clone();
        let registry = ctx.registry.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let export_path = export_path.clone();
                let registry = registry.clone();
                let res = tokio::task::spawn_blocking(move || compute_profits_snapshot(&export_path, &registry)).await;
                match res {
                    Ok(Ok(snapshot)) => {
                        let _ = ui_tx.send(UiEvent::Profits(Ok(snapshot)));
                    }
                    Ok(Err(e)) => {
                        let _ = ui_tx.send(UiEvent::Profits(Err(e)));
                    }
                    Err(e) => {
                        let _ = ui_tx.send(UiEvent::Profits(Err(format!("profit task failed: {e}"))));
                    }
                }
            }
        });
    }

    // Keyboard reader (blocking)
    {
        let ui_tx = ui_tx.clone();
        let stop = stop.clone();
        tokio::task::spawn_blocking(move || {
            while !stop.load(Ordering::Relaxed) {
                if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    match event::read() {
                        Ok(CrosstermEvent::Key(key)) => {
                            let _ = ui_tx.send(UiEvent::Input(key));
                        }
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        });
    }

    let withdraw_assets = build_withdrawable_assets(&ctx);
    let mut app = App::new(withdraw_assets);
    app.push_log("Press 'r' to start/pause, 'q' to quit, Tab to switch views.");
    {
        let ui_tx = ui_tx.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let snapshot = fetch_balances_snapshot(ctx).await;
            let _ = ui_tx.send(UiEvent::Balances(snapshot));
        });
    }

    loop {
        terminal.draw(|f| draw_ui(f, &app)).context("draw UI")?;

        let Some(ev) = ui_rx.recv().await else {
            break;
        };

        match ev {
            UiEvent::Tick => {
                app.last_tick = Instant::now();
            }
            UiEvent::Engine(engine_ev) => handle_engine_event(&mut app, engine_ev),
            UiEvent::Wal(res) => match res {
                Ok(snapshot) => {
                    app.wal = Some(snapshot);
                    app.wal_error = None;
                }
                Err(err) => {
                    app.wal_error = Some(err);
                }
            },
            UiEvent::Balances(res) => match res {
                Ok(snapshot) => {
                    app.balances = Some(snapshot);
                    app.balances_error = None;
                    app.balances_selected = app
                        .balances
                        .as_ref()
                        .map(|b| b.rows.len().saturating_sub(1).min(app.balances_selected))
                        .unwrap_or(0);
                }
                Err(err) => {
                    app.balances_error = Some(err);
                }
            },
            UiEvent::Profits(res) => match res {
                Ok(snapshot) => {
                    app.profits = Some(snapshot);
                    app.profits_error = None;
                }
                Err(err) => {
                    app.profits_error = Some(err);
                }
            },
            UiEvent::Withdraw(res) => {
                app.withdraw.in_flight = false;
                match &res {
                    Ok(tx) => app.push_log(format!("withdraw: sent (tx={})", tx)),
                    Err(err) => app.push_log(format!("withdraw: failed ({})", err)),
                }
                app.withdraw.last_result = Some(res);
            }
            UiEvent::Deposit(res) => {
                app.deposit.in_flight = false;
                app.deposit.at = Some(Local::now());
                match &res {
                    Ok(v) => {
                        app.push_log(format!("deposit: {} {} -> {}", v.asset, v.network, v.address));
                    }
                    Err(e) => {
                        app.push_log(format!("deposit: failed ({})", e));
                    }
                }
                app.deposit.last = Some(res);
            }
            UiEvent::Input(key) => {
                if handle_key(&mut app, key, &engine_tx, &ui_tx, &ctx).await? {
                    break;
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = engine_tx.send(LoopControl::Stopping);
    let _ = engine_handle.await;
    Ok(())
}

fn handle_engine_event(app: &mut App, ev: LoopEvent) {
    match ev {
        LoopEvent::Log(msg) => app.push_log(msg),
        LoopEvent::Error(err) => {
            app.last_error = Some(err.clone());
            app.push_log(format!("error: {}", err));
        }
        LoopEvent::Outcomes(outcomes) => {
            app.last_outcomes = outcomes.len();

            let at = Local::now();
            for outcome in outcomes {
                if app.recent_outcomes.len() >= 200 {
                    app.recent_outcomes.pop_front();
                }
                app.recent_outcomes.push_back(RecentOutcome { at, outcome });
            }

            app.push_log(format!("exported {} outcome(s)", app.last_outcomes));
        }
    }
}

async fn handle_key(
    app: &mut App,
    key: KeyEvent,
    engine_tx: &watch::Sender<LoopControl>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    ctx: &Arc<crate::context::PipelineContext>,
) -> anyhow::Result<bool> {
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
            if matches!(app.tab, Tab::Balances) && !matches!(app.balances_panel, BalancesPanel::None) {
                app.balances_panel = BalancesPanel::None;
                return Ok(false);
            }
            app.should_quit = true;
            return Ok(true);
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
        (KeyCode::Char('w'), KeyModifiers::NONE) => {
            app.tab = Tab::Balances;
            open_withdraw_panel(app, ui_tx);
        }
        (KeyCode::Char('d'), KeyModifiers::NONE)
            if matches!(app.tab, Tab::Balances) && matches!(app.balances_panel, BalancesPanel::Withdraw) =>
        {
            refresh_withdraw_deposit_address(app, ui_tx);
        }
        (KeyCode::Char('d'), KeyModifiers::NONE) => {
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
                            WithdrawAccountKind::Main => WithdrawAccountKind::Recovery,
                            WithdrawAccountKind::Trader => WithdrawAccountKind::Main,
                            WithdrawAccountKind::Recovery => WithdrawAccountKind::Trader,
                        };
                    }
                    WithdrawField::Destination => {
                        app.withdraw.destination = match app.withdraw.destination {
                            WithdrawDestinationKind::Main => WithdrawDestinationKind::Manual,
                            WithdrawDestinationKind::Trader => WithdrawDestinationKind::Main,
                            WithdrawDestinationKind::Recovery => WithdrawDestinationKind::Trader,
                            WithdrawDestinationKind::Manual => WithdrawDestinationKind::Recovery,
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
                            WithdrawAccountKind::Main => WithdrawAccountKind::Trader,
                            WithdrawAccountKind::Trader => WithdrawAccountKind::Recovery,
                            WithdrawAccountKind::Recovery => WithdrawAccountKind::Main,
                        };
                    }
                    WithdrawField::Destination => {
                        app.withdraw.destination = match app.withdraw.destination {
                            WithdrawDestinationKind::Main => WithdrawDestinationKind::Trader,
                            WithdrawDestinationKind::Trader => WithdrawDestinationKind::Recovery,
                            WithdrawDestinationKind::Recovery => WithdrawDestinationKind::Manual,
                            WithdrawDestinationKind::Manual => WithdrawDestinationKind::Main,
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
                        if matches!(app.withdraw.destination, WithdrawDestinationKind::Manual) {
                            app.withdraw.editing = Some(WithdrawField::ManualDestination);
                            app.withdraw.edit_backup = Some(app.withdraw.manual_destination.clone());
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

async fn fetch_balances_snapshot(ctx: Arc<crate::context::PipelineContext>) -> Result<BalancesSnapshot, String> {
    let mut asset_ids: Vec<AssetId> = ctx.registry.all().into_iter().map(|(id, _)| id).collect();
    asset_ids.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then(a.symbol.cmp(&b.symbol))
            .then(a.address.cmp(&b.address))
    });

    let (main_results, trader_results, recovery_results) = tokio::join!(
        ctx.main_service.sync_assets(&asset_ids),
        ctx.trader_service.sync_assets(&asset_ids),
        ctx.recovery_service.sync_assets(&asset_ids),
    );

    let mut rows = Vec::with_capacity(asset_ids.len());
    for (idx, asset_id) in asset_ids.iter().enumerate() {
        let main_cell = format::format_balance_result(main_results.get(idx));
        let trader_cell = format::format_balance_result(trader_results.get(idx));
        let recovery_cell = format::format_balance_result(recovery_results.get(idx));

        rows.push(BalanceRowData {
            asset: asset_id.clone(),
            main: main_cell,
            trader: trader_cell,
            recovery: recovery_cell,
        });
    }

    Ok(BalancesSnapshot { rows, at: Local::now() })
}

#[derive(serde::Deserialize)]
struct ExecutionCsvRow {
    #[serde(default)]
    receive_symbol: Option<String>,
    expected_profit: i128,
    realized_profit: i128,
    #[serde(default)]
    status: Option<String>,
}

fn compute_profits_snapshot(
    export_path: &str,
    registry: &liquidium_pipeline_core::tokens::token_registry::TokenRegistry,
) -> Result<ProfitsSnapshot, String> {
    let mut totals: HashMap<String, (usize, i128, i128)> = HashMap::new();

    let mut rdr = match csv::ReaderBuilder::new().from_path(export_path) {
        Ok(r) => r,
        Err(e) => {
            // Missing file is common on a fresh install.
            if e.is_io_error() {
                return Ok(ProfitsSnapshot {
                    rows: vec![],
                    at: Local::now(),
                });
            }
            return Err(format!("csv open failed: {e}"));
        }
    };

    for rec in rdr.deserialize::<ExecutionCsvRow>() {
        let row = rec.map_err(|e| format!("csv parse failed: {e}"))?;
        // Only count finalized rows; keep it simple.
        let status = row.status.unwrap_or_default();
        if status.is_empty() {
            // Backward compatibility: older exports might omit status; include them.
        }
        let sym = row.receive_symbol.unwrap_or_else(|| "unknown".to_string());
        let entry = totals.entry(sym).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += row.realized_profit;
        entry.2 += row.expected_profit;
    }

    let mut rows: Vec<ProfitBySymbol> = totals
        .into_iter()
        .map(|(symbol, (count, realized, expected))| ProfitBySymbol {
            decimals: registry
                .tokens
                .values()
                .find(|t| t.symbol() == symbol)
                .map(|t| t.decimals()),
            symbol,
            count,
            realized,
            expected,
        })
        .collect();

    rows.sort_by(|a, b| b.realized.cmp(&a.realized));

    Ok(ProfitsSnapshot { rows, at: Local::now() })
}

fn build_withdrawable_assets(ctx: &crate::context::PipelineContext) -> Vec<AssetId> {
    let mut ids: Vec<AssetId> = ctx
        .registry
        .tokens
        .iter()
        .filter_map(|(id, tok)| match tok {
            ChainToken::Icp { .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    ids.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.address.cmp(&b.address)));
    ids
}

fn deposit_network_for_asset(asset: &AssetId) -> String {
    if asset.chain.eq_ignore_ascii_case("icp") {
        "ICP".to_string()
    } else {
        asset.chain.to_ascii_uppercase()
    }
}

fn request_mexc_deposit_address(
    app: &mut App,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    asset: AssetId,
    network: String,
    force: bool,
    log: bool,
) {
    let same_asset = app.deposit.asset.as_ref() == Some(&asset) && app.deposit.network.as_deref() == Some(&network);
    if !force && same_asset && app.deposit.last.is_some() {
        return;
    }
    if app.deposit.in_flight && same_asset {
        return;
    }

    let replace = app.deposit.asset.as_ref() != Some(&asset) || app.deposit.network.as_deref() != Some(&network);
    if replace {
        app.deposit.last = None;
        app.deposit.at = None;
    }

    app.deposit.in_flight = true;
    app.deposit.asset = Some(asset.clone());
    app.deposit.network = Some(network.clone());

    if log {
        app.push_log(format!("deposit: fetching {} on {}", asset.symbol, network));
    }

    let ui_tx = ui_tx.clone();
    tokio::spawn(async move {
        let client = match MexcClient::from_env() {
            Ok(v) => v,
            Err(e) => {
                let _ = ui_tx.send(UiEvent::Deposit(Err(e)));
                return;
            }
        };

        let res = client.get_deposit_address(&asset.symbol, &network).await;
        let _ = ui_tx.send(UiEvent::Deposit(res));
    });
}

fn ensure_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        return;
    };
    let network = deposit_network_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, network, false, false);
}

fn refresh_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("deposit: no withdraw asset selected");
        return;
    };
    let network = deposit_network_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, network, true, true);
}

fn open_withdraw_panel(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    if let Some(balances) = &app.balances
        && let Some(selected) = balances.rows.get(app.balances_selected)
        && let Some(idx) = app.withdraw_assets.iter().position(|a| a == &selected.asset)
    {
        app.withdraw.asset_idx = idx;
    }

    app.withdraw.field = WithdrawField::Source;
    app.withdraw.editing = None;
    app.withdraw.edit_backup = None;
    app.withdraw.source = WithdrawAccountKind::Recovery;
    app.withdraw.destination = WithdrawDestinationKind::Main;
    app.withdraw.amount = "all".to_string();

    app.balances_panel = BalancesPanel::Withdraw;
    app.push_log("withdraw: opened");
    ensure_withdraw_deposit_address(app, ui_tx);
}

fn open_deposit_panel(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(balances) = &app.balances else {
        app.push_log("deposit: no balances yet (press 'b' first)");
        return;
    };
    let Some(selected) = balances.rows.get(app.balances_selected) else {
        app.push_log("deposit: invalid selection");
        return;
    };

    let asset = selected.asset.clone();
    let network = deposit_network_for_asset(&asset);

    app.balances_panel = BalancesPanel::Deposit;
    request_mexc_deposit_address(app, ui_tx, asset, network, true, true);
}

fn submit_withdraw(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>, ctx: &Arc<crate::context::PipelineContext>) {
    if app.withdraw.in_flight {
        app.push_log("withdraw: already in flight");
        return;
    }
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("withdraw: no asset selected");
        return;
    };

    if matches!(app.withdraw.destination, WithdrawDestinationKind::Manual)
        && app.withdraw.manual_destination.trim().is_empty()
    {
        app.push_log("withdraw: manual destination is empty");
        return;
    }

    app.withdraw.in_flight = true;
    app.push_log("withdraw: sending…");

    let source = app.withdraw.source;
    let destination = app.withdraw.destination;
    let manual_destination = app.withdraw.manual_destination.clone();
    let amount = app.withdraw.amount.clone();

    let ui_tx = ui_tx.clone();
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let res = execute_withdraw(ctx, source, destination, manual_destination, asset, amount).await;
        let _ = ui_tx.send(UiEvent::Withdraw(res));
    });
}

async fn execute_withdraw(
    ctx: Arc<crate::context::PipelineContext>,
    source: WithdrawAccountKind,
    destination: WithdrawDestinationKind,
    manual_destination: String,
    asset: AssetId,
    amount: String,
) -> Result<String, String> {
    let token = ctx
        .registry
        .get(&asset)
        .ok_or_else(|| format!("unknown asset: {}", asset))?;

    let ChainToken::Icp { decimals, fee, .. } = token else {
        return Err("TUI withdraw currently supports ICP tokens only".to_string());
    };

    let (transfers, balances) = match source {
        WithdrawAccountKind::Main => (ctx.main_transfers.clone(), ctx.main_service.clone()),
        WithdrawAccountKind::Trader => (ctx.trader_transfers.clone(), ctx.trader_service.clone()),
        WithdrawAccountKind::Recovery => (ctx.recovery_transfers.clone(), ctx.recovery_service.clone()),
    };

    let dst_account: Account = match destination {
        WithdrawDestinationKind::Main => ctx.config.liquidator_principal.into(),
        WithdrawDestinationKind::Trader => ctx.config.trader_principal.into(),
        WithdrawDestinationKind::Recovery => ctx.config.get_recovery_account(),
        WithdrawDestinationKind::Manual => {
            Account::from_str(manual_destination.trim()).map_err(|_| "invalid destination ICP account".to_string())?
        }
    };

    let amount_native: Nat = if amount.trim().eq_ignore_ascii_case("all") {
        let bal = balances.get_balance(&asset).await?;
        let bal_u128 = bal
            .value
            .0
            .to_u128()
            .ok_or_else(|| "balance too large for u128".to_string())?;
        let fee_u128 = fee.0.to_u128().unwrap_or(0);
        if bal_u128 <= fee_u128 {
            return Err("balance too low to cover fee".to_string());
        }
        Nat::from(bal_u128 - fee_u128)
    } else {
        let units = format::decimal_to_units(amount.trim(), decimals)
            .ok_or_else(|| format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')"))?;
        Nat::from(units)
    };

    transfers
        .transfer_by_asset_id(&asset, ChainAccount::Icp(dst_account), amount_native)
        .await
}
