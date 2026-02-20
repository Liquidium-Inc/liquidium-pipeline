use icrc_ledger_types::icrc1::account::Account;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::instrument;
use tracing::{info, warn};

use crate::{
    commands::liquidation_loop_helpers::{
        bootstrap_control_plane, console_ui_enabled, debt_asset_principals, debt_assets_as_text,
        ensure_runtime_file_permissions, print_banner, run_daemon_cycle_loop,
    },
    config::{Config, ConfigTrait, SwapperMode},
    context::{PipelineContext, init_context},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::{
        cex_finalizer::CexFinalizerLogic, hybrid::hybrid_finalizer::HybridFinalizer,
        kong_swap::kong_swap_finalizer::KongSwapFinalizer, mexc::mexc_finalizer::MexcFinalizer,
        profit_calculator::SimpleProfitCalculator,
    },
    liquidation::collateral_service::CollateralService,
    notifications::telegram::telegram_notifier_from_env,
    persistance::sqlite::SqliteWalStore,
    price_oracle::price_oracle::LiquidationPriceOracle,
    stages::{
        export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
        settlement_watcher::SettlementWatcher, simple_strategy::SimpleLiquidationStrategy,
    },
    swappers::{mexc::mexc_adapter::MexcClient, router::SwapRouter},
    watchdog::{WatchdogEvent, account_monitor_watchdog, webhook_watchdog_from_env},
};
use ic_agent::Agent;

use liquidium_pipeline_core::tokens::token_registry::TokenRegistry;

#[instrument(name = "liquidation.init", skip_all, err)]
async fn init(
    ctx: Arc<PipelineContext>,
) -> Result<
    (
        OpportunityFinder<Agent>,
        SimpleLiquidationStrategy<SwapRouter, Config, TokenRegistry, CollateralService<LiquidationPriceOracle<Agent>>>,
        Arc<BasicExecutor<Agent, SqliteWalStore>>,
        Arc<ExportStage>,
        Arc<FinalizeStage<HybridFinalizer<Config>, SqliteWalStore, SimpleProfitCalculator, Agent>>,
    ),
    String,
> {
    let config = ctx.config.clone();
    let agent = ctx.agent.clone();
    let registry = ctx.registry.clone();
    let db = Arc::new(SqliteWalStore::new(&config.db_path).map_err(|e| format!("could not connect to db: {e}"))?);

    let tokens = debt_asset_principals(&registry);

    let mut executor = BasicExecutor::new(
        agent.clone(),
        Account {
            owner: config.liquidator_principal,
            subaccount: None,
        },
        config.lending_canister,
        db.clone(),
        ctx.approval_state.clone(),
    );

    executor
        .init(&tokens)
        .await
        .map_err(|e| format!("executor token init failed: {e}"))?;
    let executor = Arc::new(executor);

    if let Err(err) = ctx.swap_router.init().await {
        warn!("Swap router init failed: {}", err);
    }

    // Base DEX finalizer (Kong swapper)
    let kong_finalizer = Arc::new(KongSwapFinalizer::new(ctx.swap_router.clone()));

    let mexc_finalizer = match ctx.config.get_cex_credentials("mexc") {
        Ok((api_key, secret)) => {
            let mexc_client = Arc::new(MexcClient::new(&api_key, &secret));
            let mexc_finalizer = Arc::new(MexcFinalizer::new_with_tunables(
                mexc_client,
                ctx.trader_transfers.actions(),
                config.liquidator_principal,
                config.max_allowed_cex_slippage_bps as f64,
                config.cex_min_exec_usd,
                config.cex_slice_target_ratio,
                config.cex_buy_truncation_trigger_ratio,
                config.cex_buy_inverse_overspend_bps,
                config.cex_buy_inverse_max_retries,
                config.cex_buy_inverse_enabled,
            ));
            Some(mexc_finalizer)
        }
        Err(err) => {
            if config.swapper != SwapperMode::Dex {
                return Err(format!("Cex credentials not found: {err}"));
            }
            None
        }
    };

    // Hybrid finalizer composes DEX and CEX finalizers.
    // For now, CEX is wired to the same Kong finalizer; you can later swap in a dedicated CEX finalizer.
    let hybrid_finalizer = Arc::new(HybridFinalizer {
        config: config.clone(),
        trader_transfers: ctx.trader_transfers.actions(),
        dex_swapper: ctx.swap_router.clone(),
        dex_finalizer: kong_finalizer.clone(),
        cex_finalizer: mexc_finalizer.clone().map(|f| f as Arc<dyn CexFinalizerLogic>),
    });

    // Profit calculator for expected/realized PnL
    let profit_calc = Arc::new(SimpleProfitCalculator); //todo implement real profit calculator

    // FinalizeStage wires WAL + finalizer + profit calculation
    let finalizer = Arc::new(FinalizeStage::new(
        db.clone(),
        hybrid_finalizer,
        profit_calc,
        agent.clone(),
        config.lending_canister,
        config.cex_retry_base_secs,
        config.cex_retry_max_secs,
    ));

    info!("Initializing searcher stage ...");
    let finder = OpportunityFinder::new(
        agent.clone(),
        config.lending_canister,
        config.opportunity_account_filter.clone(),
    );

    info!("Initializing liquidations stage ...");
    let price_oracle = Arc::new(LiquidationPriceOracle::new(agent.clone(), config.lending_canister));

    let collateral_service = Arc::new(CollateralService::new(price_oracle));

    let wd = webhook_watchdog_from_env(Duration::from_secs(300));
    wd.notify(WatchdogEvent::Heartbeat { stage: "Init" }).await;

    let strategy = SimpleLiquidationStrategy::new(
        config.clone(),
        registry.clone(),
        ctx.swap_router.clone(),
        collateral_service.clone(),
        ctx.main_service.clone(),
        ctx.approval_state.clone(),
    )
    .with_watchdog(wd);

    let exporter = Arc::new(ExportStage {
        path: config.export_path.clone(),
    });

    Ok((finder, strategy, executor, exporter, finalizer))
}

pub async fn run_liquidation_loop(sock_path: PathBuf) {
    // Auditor note:
    // This function is the foreground daemon entrypoint. It does not fork/detach;
    // lifecycle is expected to be managed by an external supervisor (systemd).
    let ui_enabled = console_ui_enabled();
    if ui_enabled {
        print_banner();
    }

    let ctx = match init_context().await {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::error!("Failed to initialize pipeline context: {}", err);
            return;
        }
    };
    let ctx = Arc::new(ctx);
    let config = ctx.config.clone();

    if let Err(err) = ensure_runtime_file_permissions(&config.db_path, &config.export_path) {
        tracing::error!(
            db_path = %config.db_path,
            export_path = %config.export_path,
            "Startup filesystem preflight failed: {}",
            err
        );
        return;
    }

    if config.buy_bad_debt {
        info!(
            buy_bad_debt = true,
            "Bad debt mode enabled: liquidator may repay bad debt to restore solvency"
        );
        if ui_enabled {
            println!("====================================================================");
            println!("=                                                                  =");
            println!("=                   !!!  BAD DEBT MODE  !!!                        =");
            println!("=                                                                  =");
            println!("=  This bot WILL repay bad debt (you eat the loss).                =");
            println!("=  Use only if you intend to shore up protocol solvency.            =");
            println!("=                                                                  =");
            println!("====================================================================");
        }
        warn!("Continuing without interactive confirmation because BUY_BAD_DEBT is enabled.");
    } else {
        info!(
            buy_bad_debt = false,
            "Bad debt mode disabled: only collateral-backed liquidations will run"
        );
    }
    // Use main IC agent (liquidator identity) from context
    info!(
        liquidator_principal = %config.liquidator_principal.to_text(),
        "Agent initialized"
    );

    // Initialize components using shared pipeline context
    let (finder, strategy, executor, exporter, finalizer) = match init(ctx.clone()).await {
        Ok(stages) => stages,
        Err(err) => {
            tracing::error!("Failed to initialize pipeline stages: {}", err);
            return;
        }
    };

    let watcher_wal = match SqliteWalStore::new_with_busy_timeout(&config.db_path, 30_000) {
        Ok(wal) => Arc::new(wal),
        Err(err) => {
            tracing::error!("Failed to init watcher WAL: {}", err);
            return;
        }
    };
    // Settlement watcher is intentionally independent from pause/resume.
    // Even while paused, it can continue reconciling previously-started work.
    let watcher = SettlementWatcher::new(
        watcher_wal,
        ctx.agent.clone(),
        ctx.swap_router.clone(),
        config.lending_canister,
        Duration::from_secs(3),
        config.swapper,
    );
    tokio::spawn(async move { watcher.run().await });

    let debt_asset_principals = debt_asset_principals(&ctx.registry);
    let debt_assets = debt_assets_as_text(&debt_asset_principals);

    info!(
        network = %config.ic_url,
        liquidator_principal = %config.liquidator_principal.to_text(),
        swapper_mode = ?config.swapper,
        max_dex_slippage_bps = config.max_allowed_dex_slippage,
        max_cex_slippage_bps = config.max_allowed_cex_slippage_bps,
        buy_bad_debt = config.buy_bad_debt,
        "Startup configuration"
    );
    info!("Liquidator started; scanning for liquidation opportunities...");

    // Shared pause flag controlled by UDS commands.
    // `true` means: do not initiate new liquidations, but keep housekeeping alive.
    let paused = Arc::new(AtomicBool::new(false));
    // The helper encapsulates:
    // - persisted paused/running state bootstrap
    // - UDS bind + serve
    // - state persistence on pause/resume transitions
    if let Err(err) = bootstrap_control_plane(&sock_path, &config.db_path, paused.clone()) {
        tracing::error!("{}", err);
        return;
    }
    info!(sock_path = %sock_path.display(), "Control plane ready");

    // Setup liquidity monitor
    let liq_dog = account_monitor_watchdog(Duration::from_secs(5), ctx.config.liquidator_principal);
    let liquidation_notifier = telegram_notifier_from_env();
    let stopping = Arc::new(AtomicBool::new(false));
    register_shutdown_signal_listener(stopping.clone());
    liquidation_notifier.notify_startup().await;
    // Steady-state operation is delegated to a helper to keep this entrypoint
    // focused on bootstrap wiring and lifecycle boundaries.
    run_daemon_cycle_loop(
        &finder,
        &strategy,
        &executor,
        &exporter,
        &finalizer,
        &liq_dog,
        &liquidation_notifier,
        paused,
        stopping,
        &debt_assets,
        &debt_asset_principals,
        ui_enabled,
    )
    .await;
    liquidation_notifier.notify_shutdown().await;
}

fn register_shutdown_signal_listener(stopping: Arc<AtomicBool>) {
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("Shutdown signal received; draining current cycle and stopping");
        stopping.store(true, Ordering::Relaxed);
    });
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(err) => {
                warn!("Failed to install SIGTERM handler ({}); falling back to Ctrl+C only", err);
                if let Err(ctrl_c_err) = tokio::signal::ctrl_c().await {
                    warn!("Ctrl+C signal handling failed: {}", ctrl_c_err);
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("Ctrl+C signal handling failed: {}", err);
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopControl {
    Running,
    Paused,
    Stopping,
}

#[cfg(test)]
mod tests {
    use crate::commands::liquidation_loop_helpers::console_ui_enabled;
    use std::io::IsTerminal;

    fn calc_console_ui_enabled(human_output: bool, stdout_is_tty: bool, stderr_is_tty: bool) -> bool {
        human_output && stdout_is_tty && stderr_is_tty
    }

    #[test]
    fn disables_console_ui_when_plain_logs_feature_is_enabled() {
        assert!(!calc_console_ui_enabled(false, true, true));
    }

    #[test]
    fn enables_console_ui_when_feature_disabled_and_both_terminals_present() {
        assert!(calc_console_ui_enabled(true, true, true));
    }

    #[test]
    fn disables_console_ui_when_stdout_is_not_tty() {
        assert!(!calc_console_ui_enabled(true, false, true));
    }

    #[test]
    fn disables_console_ui_when_stderr_is_not_tty() {
        assert!(!calc_console_ui_enabled(true, true, false));
    }

    #[test]
    fn console_ui_enabled_matches_helper_contract() {
        let actual = console_ui_enabled();
        let expected = calc_console_ui_enabled(
            crate::output::human_output_enabled(),
            std::io::stdout().is_terminal(),
            std::io::stderr().is_terminal(),
        );
        assert_eq!(actual, expected);
    }
}
