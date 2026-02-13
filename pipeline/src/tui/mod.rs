mod app;
mod events;
mod format;
mod input;
mod log_sanitize;
mod log_source;
mod logging;
mod snapshots;
mod views;
mod withdraw;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use chrono::Local;
use crossterm::event::{self, Event as CrosstermEvent};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use crate::commands::liquidation_loop::LoopControl;
use crate::commands::tui::TuiOptions;
use crate::context::init_context_best_effort;
use crate::persistance::sqlite::SqliteWalStore;

use self::app::{App, ConfigSummary, ExecutionRowData, ExecutionsSnapshot, WalCounts, WalSnapshot};
use self::events::UiEvent;
use self::log_source::{describe_log_source, start_log_source};
use self::logging::TerminalGuard;
use self::snapshots::{compute_profits_snapshot, fetch_balances_snapshot};
use self::views::draw_ui;
use self::withdraw::build_withdrawable_assets;

pub async fn run(opts: TuiOptions) -> anyhow::Result<()> {
    let sock_path = opts.sock_path;
    let unit_name = opts.unit_name;
    let log_file = opts.log_file;

    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let log_control = start_log_source(ui_tx.clone(), unit_name.clone(), log_file.clone());

    let ctx = init_context_best_effort()
        .await
        .map_err(|e| anyhow::anyhow!("init context failed: {e}"))?;
    let ctx = Arc::new(ctx);

    let _terminal_guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    terminal.clear().ok();

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

    // WAL polling (read-only).
    {
        let ui_tx = ui_tx.clone();
        let stop = stop.clone();
        let db_path = ctx.config.db_path.clone();
        tokio::spawn(async move {
            let store = match SqliteWalStore::new_read_only_with_busy_timeout(&db_path, 30_000) {
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
                let res = tokio::task::spawn_blocking(move || {
                    let counts = store.status_counts().map_err(|e| e.to_string())?;
                    let rows = store.list_recent(200).map_err(|e| e.to_string())?;
                    let paused = store.daemon_paused().map_err(|e| e.to_string())?;
                    Ok::<_, String>((counts, rows, paused))
                })
                .await;
                match res {
                    Ok(Ok((map, rows, paused))) => {
                        let snapshot = WalSnapshot {
                            counts: WalCounts::from_map(&map),
                            at: Local::now(),
                        };
                        let _ = ui_tx.send(UiEvent::Wal(Ok(snapshot)));
                        let _ = ui_tx.send(UiEvent::DaemonPaused(Ok(paused)));

                        let rows = rows
                            .into_iter()
                            .map(|row| ExecutionRowData {
                                liq_id: row.id,
                                status: row.status,
                                attempt: row.attempt,
                                error_count: row.error_count,
                                last_error: row.last_error,
                                created_at: row.created_at,
                                updated_at: row.updated_at,
                                meta_json: row.meta_json,
                            })
                            .collect();
                        let exec_snapshot = ExecutionsSnapshot { rows, at: Local::now() };
                        let _ = ui_tx.send(UiEvent::Executions(Ok(exec_snapshot)));
                    }
                    Ok(Err(e)) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db query failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::Executions(Err(format!("db query failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::DaemonPaused(Err(format!("db query failed: {e}"))));
                    }
                    Err(e) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db task failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::Executions(Err(format!("db task failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::DaemonPaused(Err(format!("db task failed: {e}"))));
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
                        Ok(CrosstermEvent::Paste(text)) => {
                            let _ = ui_tx.send(UiEvent::Paste(text));
                        }
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        });
    }

    let withdraw_assets = build_withdrawable_assets(&ctx);
    let cfg = &ctx.config;
    let config = ConfigSummary {
        ic_url: cfg.ic_url.clone(),
        liquidator_principal: cfg.liquidator_principal.to_text(),
        trader_principal: cfg.trader_principal.to_text(),
        evm_address: ctx.evm_address.clone(),
        swapper_mode: format!("{:?}", cfg.swapper),
        max_dex_slippage_bps: cfg.max_allowed_dex_slippage,
        max_cex_slippage_bps: cfg.max_allowed_cex_slippage_bps,
        buy_bad_debt: cfg.buy_bad_debt,
        opportunity_filter: cfg.opportunity_account_filter.iter().map(|p| p.to_text()).collect(),
        control_socket: sock_path.display().to_string(),
        log_source: describe_log_source(&unit_name, log_file.as_deref()),
        db_path: cfg.db_path.clone(),
        export_path: cfg.export_path.clone(),
    };

    let mut app = App::new(withdraw_assets, config);
    if let Ok(size) = terminal.size() {
        app.update_log_viewport_widths(size);
    }
    {
        let ui_tx = ui_tx.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let snapshot = fetch_balances_snapshot(ctx).await;
            let _ = ui_tx.send(UiEvent::Balances(snapshot));
        });
    }

    loop {
        if let Ok(size) = terminal.size() {
            app.update_log_viewport_widths(size);
        }
        terminal.draw(|f| draw_ui(f, &app)).context("draw UI")?;

        let Some(ev) = ui_rx.recv().await else {
            break;
        };

        match ev {
            UiEvent::Tick => {
                app.last_tick = Instant::now();
            }
            UiEvent::AppendLogLines(lines) => app.append_logs(lines),
            UiEvent::PrependLogLines(lines) => app.prepend_logs(lines),
            UiEvent::DaemonPaused(res) => {
                if let Ok(is_paused) = res {
                    if !matches!(app.engine, LoopControl::Stopping) {
                        app.engine = if is_paused {
                            LoopControl::Paused
                        } else {
                            LoopControl::Running
                        };
                    }
                }
            }
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
            UiEvent::Executions(res) => match res {
                Ok(snapshot) => {
                    app.executions = Some(snapshot);
                    app.executions_error = None;
                    app.executions_selected = app
                        .executions
                        .as_ref()
                        .map(|e| e.rows.len().saturating_sub(1).min(app.executions_selected))
                        .unwrap_or(0);
                }
                Err(err) => {
                    app.executions_error = Some(err);
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
                if input::handle_key(&mut app, key, sock_path.as_path(), &log_control, &ui_tx, &ctx).await? {
                    break;
                }
            }
            UiEvent::Paste(text) => {
                input::handle_paste(&mut app, text);
            }
        }

        if app.should_quit {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    log_control.shutdown();
    Ok(())
}
