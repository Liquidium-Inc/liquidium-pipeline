use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use indicatif::{ProgressBar, ProgressStyle};

use prettytable::{Cell, Row, Table, format};
use std::{
    io::{IsTerminal, stderr, stdout},
    sync::Arc,
    thread::sleep,
    time::Duration,
    time::Instant,
};
use tracing::{Instrument, info_span, instrument};
use tracing::{info, warn};

use crate::{
    config::{Config, ConfigTrait, SwapperMode},
    context::{PipelineContext, init_context},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::{
        cex_finalizer::CexFinalizerLogic, hybrid::hybrid_finalizer::HybridFinalizer,
        kong_swap::kong_swap_finalizer::KongSwapFinalizer, liquidation_outcome::LiquidationOutcome,
        mexc::mexc_finalizer::MexcFinalizer, profit_calculator::SimpleProfitCalculator,
    },
    liquidation::collateral_service::CollateralService,
    persistance::sqlite::SqliteWalStore,
    price_oracle::price_oracle::LiquidationPriceOracle,
    stage::PipelineStage,
    stages::{
        executor::ExecutionStatus, export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
        settlement_watcher::SettlementWatcher, simple_strategy::SimpleLiquidationStrategy,
    },
    swappers::{mexc::mexc_adapter::MexcClient, router::SwapRouter},
    watchdog::{WatchdogEvent, account_monitor_watchdog, webhook_watchdog_from_env},
};
use ic_agent::Agent;

use liquidium_pipeline_core::tokens::{
    chain_token::ChainToken,
    token_registry::{TokenRegistry, TokenRegistryTrait},
};

// Prints the startup banner.
fn print_banner() {
    println!(
        r#"
██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗
██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝
██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗  
██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝  
██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗
╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝
                                                        
          Liquidation Execution Engine
"#
    );
}

fn print_startup_table(config: &Config) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![Cell::new("Startup Configuration")]));

    table.add_row(Row::new(vec![Cell::new("Network"), Cell::new(&config.ic_url)]));
    table.add_row(Row::new(vec![
        Cell::new("Liquidator Principal"),
        Cell::new(&config.liquidator_principal.to_text()),
    ]));
    table.add_row(Row::new(vec![
        Cell::new("Swapper Mode"),
        Cell::new(&format!("{:?}", config.swapper)),
    ]));
    table.add_row(Row::new(vec![
        Cell::new("Max DEX Slippage (bps)"),
        Cell::new(&config.max_allowed_dex_slippage.to_string()),
    ]));
    table.add_row(Row::new(vec![
        Cell::new("Max CEX Slippage (bps)"),
        Cell::new(&config.max_allowed_cex_slippage_bps.to_string()),
    ]));
    table.add_row(Row::new(vec![
        Cell::new("Buy Bad Debt"),
        Cell::new(&config.buy_bad_debt.to_string()),
    ]));

    table.printstd();
}

fn console_ui_enabled() -> bool {
    is_console_ui_enabled(cfg!(feature = "plain-logs"), stdout().is_terminal(), stderr().is_terminal())
}

fn is_console_ui_enabled(plain_logs_feature_enabled: bool, stdout_is_tty: bool, stderr_is_tty: bool) -> bool {
    !plain_logs_feature_enabled && stdout_is_tty && stderr_is_tty
}

fn start_spinner(enabled: bool) -> Option<ProgressBar> {
    if !enabled {
        return None;
    }

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    spinner.enable_steady_tick(Duration::from_millis(100));
    Some(spinner)
}

fn log_execution_results(results: &[LiquidationOutcome]) {
    let success_count = results
        .iter()
        .filter(|r| matches!(r.status, ExecutionStatus::Success))
        .count();

    info!(
        outcome_count = results.len(),
        success_count,
        failed_count = results.len() - success_count,
        "Liquidation outcomes finalized"
    );

    for r in results {
        let liquidation_id = r
            .execution_receipt
            .liquidation_result
            .as_ref()
            .map(|v| v.id.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let swap_status = r
            .finalizer_result
            .swap_result
            .as_ref()
            .map(|v| v.status.clone())
            .unwrap_or_else(|| "none".to_string());
        let status = r.status.description();

        info!(
            event = "liquidation_outcome",
            liquidation_id = %liquidation_id,
            borrower = %r.request.liquidation.borrower.to_text(),
            debt_asset = %r.request.debt_asset.symbol(),
            collateral_asset = %r.request.collateral_asset.symbol(),
            debt_repaid = %r.formatted_debt_repaid(),
            collateral_received = %r.formatted_received_collateral(),
            swap_output = %r.formatted_swap_output(),
            swapper = %r.formatted_swapper(),
            swap_status = %swap_status,
            status = %status,
            expected_profit = r.expected_profit,
            realized_profit = r.realized_profit,
            profit_delta = r.realized_profit - r.expected_profit,
            round_trip_secs = r.round_trip_secs.unwrap_or(-1),
            "Liquidation outcome"
        );
    }
}

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

    let tokens: Vec<Principal> = registry
        .debt_assets()
        .iter()
        .filter_map(|(_, tok)| {
            if let ChainToken::Icp { ledger, .. } = tok {
                Some(*ledger)
            } else {
                None
            }
        })
        .collect();

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
            let mexc_finalizer = Arc::new(MexcFinalizer::new(
                mexc_client,
                ctx.trader_transfers.actions(),
                config.liquidator_principal,
                config.max_allowed_cex_slippage_bps as f64,
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

pub async fn run_liquidation_loop() {
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
    let watcher = SettlementWatcher::new(
        watcher_wal,
        ctx.agent.clone(),
        ctx.swap_router.clone(),
        config.lending_canister,
        Duration::from_secs(3),
    );
    tokio::spawn(async move { watcher.run().await });

    let debt_asset_principals: Vec<Principal> = ctx
        .registry
        .debt_assets()
        .iter()
        .filter_map(|(_, tok)| {
            if let ChainToken::Icp { ledger, .. } = tok {
                Some(*ledger)
            } else {
                None
            }
        })
        .collect();

    let debt_assets: Vec<String> = debt_asset_principals.iter().map(Principal::to_text).collect();

    if ui_enabled {
        print_startup_table(&config);
    }
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

    // Setup liquidity monitor
    let liq_dog = account_monitor_watchdog(Duration::from_secs(5), ctx.config.liquidator_principal);

    let mut spinner = start_spinner(ui_enabled);
    let mut last_alive_log = Instant::now();
    loop {
        if let Some(s) = spinner.as_ref() {
            s.set_message("Scanning for liquidation opportunities...");
        }
        if last_alive_log.elapsed() >= Duration::from_secs(300) {
            info!("Liquidator running; scanning for liquidation opportunities...");
            last_alive_log = Instant::now();
        }
        let opportunities = finder.process(&debt_assets).await.unwrap_or_else(|e| {
            warn!("Failed to find opportunities: {e}");
            vec![]
        });

        if !opportunities.is_empty() {
            sleep(Duration::from_secs(2));
            if let Some(s) = spinner.take() {
                s.finish_and_clear();
            }
            info!(
                opportunity_count = opportunities.len(),
                "Found liquidation opportunities"
            );

            let opp_count = opportunities.len();
            async {
                let executions = strategy.process(&opportunities).await.unwrap_or_else(|e| {
                    tracing::info!("Strategy processing failed: {e}");
                    vec![]
                });

                if executions.is_empty() {
                    info!(
                        "Found {} opportunities but none executable yet; likely waiting for funds or liquidity",
                        opportunities.len()
                    );
                }

                executor.process(&executions).await.unwrap_or_else(|e| {
                    tracing::error!("Executor failed: {e}");
                    vec![]
                });
            }
            .instrument(info_span!("liquidation.cycle", opportunities = opp_count))
            .await;
        }

        let outcomes = finalizer.process(&()).await.unwrap_or_else(|e| {
            tracing::error!("Finalizer failed: {e}");
            vec![]
        });

        if outcomes.is_empty() {
            spinner = start_spinner(ui_enabled);
            if let Some(s) = spinner.as_ref() {
                s.set_message("Scanning for liquidation opportunities...");
            }
            sleep(Duration::from_secs(2));
            continue;
        }

        if let Err(err) = exporter.process(&outcomes).await {
            warn!("Failed to export results: {}", err);
        }
        log_execution_results(&outcomes);
        if ui_enabled {
            print_execution_results(&outcomes);
        }
        spinner = start_spinner(ui_enabled);
        if let Some(s) = spinner.as_ref() {
            s.set_message("Scanning for liquidation opportunities...");
        }

        // Send liquidity monitor heart beat
        let _ = liq_dog.notify(WatchdogEvent::Heartbeat { stage: "Running" }).await;
        if let Err(err) = executor.refresh_allowances(&debt_asset_principals).await {
            warn!("Failed to refresh lending allowances: {}", err);
        }
        sleep(Duration::from_secs(5));
    }
}

pub fn print_execution_results(results: &[LiquidationOutcome]) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![
        Cell::new("Realized (Δ)"),
        Cell::new("Expected"),
        Cell::new("Debt Repaid"),
        Cell::new("Collateral"),
        Cell::new("Swap Output"),
        Cell::new("Swap Status"),
        Cell::new("Swapper"),
        Cell::new("Round Trip (s)"),
        Cell::new("Status"),
    ]));

    for r in results {
        let (debt, collat) = (r.formatted_debt_repaid(), r.formatted_received_collateral());

        let (recv_amt, swap_status) = match &r.finalizer_result.swap_result {
            Some(sr) => (r.formatted_swap_output(), sr.status.clone()),
            None => ("-".to_string(), "-".to_string()),
        };

        let delta = r.realized_profit - r.expected_profit;
        let delta_cell = {
            let txt = format!("{} ({})", r.formatted_realized_profit(), r.formatted_profit_delta());
            match delta.cmp(&0) {
                std::cmp::Ordering::Greater => Cell::new(&txt).style_spec("Fg"),
                std::cmp::Ordering::Less => Cell::new(&txt).style_spec("Fr"),
                std::cmp::Ordering::Equal => Cell::new(&txt),
            }
        };

        let status_text = r.status.description();
        let status_cell = match &r.status {
            ExecutionStatus::Success => Cell::new(&status_text).style_spec("Fg"),
            _ => Cell::new(&status_text).style_spec("Fr"),
        };

        table.add_row(Row::new(vec![
            delta_cell,
            Cell::new(&r.formatted_expected_profit()),
            Cell::new(&debt),
            Cell::new(&collat),
            Cell::new(&recv_amt),
            Cell::new(&swap_status),
            Cell::new(&r.formatted_swapper()),
            Cell::new(&r.formatted_round_trip_secs()),
            status_cell,
        ]));
    }

    table.printstd();
}

#[cfg(test)]
mod tests {
    use super::is_console_ui_enabled;

    #[test]
    fn disables_console_ui_when_plain_logs_feature_is_enabled() {
        assert!(!is_console_ui_enabled(true, true, true));
    }

    #[test]
    fn enables_console_ui_when_feature_disabled_and_both_terminals_present() {
        assert!(is_console_ui_enabled(false, true, true));
    }

    #[test]
    fn disables_console_ui_when_stdout_is_not_tty() {
        assert!(!is_console_ui_enabled(false, false, true));
    }

    #[test]
    fn disables_console_ui_when_stderr_is_not_tty() {
        assert!(!is_console_ui_enabled(false, true, false));
    }
}
