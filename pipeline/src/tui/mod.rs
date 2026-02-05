mod app;
mod events;
mod format;
mod input;
mod logging;
mod snapshots;
mod withdraw;
mod views;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use chrono::Local;
use crossterm::event::{self, Event as CrosstermEvent};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{mpsc, watch};

use crate::commands::liquidation_loop::{LoopControl, LoopEvent, run_liquidation_loop_controlled};
use crate::context::init_context_best_effort;
use crate::persistance::sqlite::SqliteWalStore;

use self::app::{App, ConfigSummary, ExecutionRowData, ExecutionsSnapshot, RecentOutcome, Tab, WalCounts, WalSnapshot};
use self::events::UiEvent;
use self::logging::{TerminalGuard, init_tui_tracing};
use self::snapshots::{compute_profits_snapshot, fetch_balances_snapshot};
use self::withdraw::build_withdrawable_assets;
use self::views::draw_ui;

pub(super) use self::withdraw::deposit_network_for_asset;

pub async fn run() -> anyhow::Result<()> {
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    if let Err(err) = init_tui_tracing(ui_tx.clone()) {
        let _ = ui_tx.send(UiEvent::Engine(LoopEvent::Error(format!(
            "failed to init tracing: {err}"
        ))));
    }

    let ctx = init_context_best_effort()
        .await
        .map_err(|e| anyhow::anyhow!("init context failed: {e}"))?;
    let ctx = Arc::new(ctx);

    let _terminal_guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    terminal.clear().ok();

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
                let res = tokio::task::spawn_blocking(move || {
                    let counts = store.status_counts().map_err(|e| e.to_string())?;
                    let rows = store.list_recent(200).map_err(|e| e.to_string())?;
                    Ok::<_, String>((counts, rows))
                })
                .await;
                match res {
                    Ok(Ok((map, rows))) => {
                        let snapshot = WalSnapshot {
                            counts: WalCounts::from_map(&map),
                            at: Local::now(),
                        };
                        let _ = ui_tx.send(UiEvent::Wal(Ok(snapshot)));

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
                            })
                            .collect();
                        let exec_snapshot = ExecutionsSnapshot {
                            rows,
                            at: Local::now(),
                        };
                        let _ = ui_tx.send(UiEvent::Executions(Ok(exec_snapshot)));
                    }
                    Ok(Err(e)) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db query failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::Executions(Err(format!("db query failed: {e}"))));
                    }
                    Err(e) => {
                        let _ = ui_tx.send(UiEvent::Wal(Err(format!("db task failed: {e}"))));
                        let _ = ui_tx.send(UiEvent::Executions(Err(format!("db task failed: {e}"))));
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
        db_path: cfg.db_path.clone(),
        export_path: cfg.export_path.clone(),
    };

    let mut app = App::new(withdraw_assets, config);
    push_startup_logs(&mut app, &ctx);
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
                if input::handle_key(&mut app, key, &engine_tx, &ui_tx, &ctx).await? {
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

fn push_startup_logs(app: &mut App, ctx: &Arc<crate::context::PipelineContext>) {
    let cfg = &ctx.config;
    app.push_log("=== Startup ===");
    app.push_log(format!("Network: {}", cfg.ic_url));
    app.push_log(format!("Liquidator principal: {}", cfg.liquidator_principal.to_text()));
    app.push_log(format!("Trader principal: {}", cfg.trader_principal.to_text()));
    app.push_log(format!(
        "Liquidator deposit (ICP): {}",
        cfg.liquidator_principal.to_text()
    ));
    app.push_log(format!("Liquidator deposit (EVM): {}", ctx.evm_address));
    app.push_log(format!("Swapper mode: {:?}", cfg.swapper));
    app.push_log(format!("Max DEX slippage (bps): {}", cfg.max_allowed_dex_slippage));
    app.push_log(format!("Max CEX slippage (bps): {}", cfg.max_allowed_cex_slippage_bps));
    app.push_log(format!("BUY_BAD_DEBT: {}", cfg.buy_bad_debt));
    if cfg.opportunity_account_filter.is_empty() {
        app.push_log("Opportunity filter: none");
    } else {
        app.push_log(format!(
            "Opportunity filter ({}):",
            cfg.opportunity_account_filter.len()
        ));
        for p in cfg.opportunity_account_filter.iter() {
            app.push_log(format!("  - {}", p.to_text()));
        }
    }
    app.push_log(format!("DB: {}", cfg.db_path));
    app.push_log(format!("EXPORT_PATH: {}", cfg.export_path));

    if cfg.buy_bad_debt {
        app.push_log("WARN BAD DEBT MODE ENABLED: liquidator will repay bad debt to restore solvency");
        app.push_log("WARN This bot WILL repay bad debt (you eat the loss).");
        app.push_log("WARN Press 'r' then type 'yes' + Enter to start.");
    } else {
        app.push_log("Bad debt mode disabled: only collateral-backed liquidations will run");
    }
}
