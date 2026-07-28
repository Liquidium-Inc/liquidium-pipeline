use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tracing::instrument;
use tracing::{info, warn};

use crate::{
    commands::liquidation_loop_helpers::{
        bootstrap_control_plane, console_ui_enabled, debt_asset_principals, debt_assets_as_text,
        ensure_runtime_file_permissions, print_banner, run_daemon_cycle_loop,
    },
    config::{Config, ConfigTrait},
    context::{PipelineContext, init_context},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::{
        mexc::runtime::build_mexc_finalizer,
        multi_venue::{
            ICPSWAP_VENUE_ID, IcpswapFirstPlannerConfig, MEXC_VENUE_ID, MultiVenueAdapter,
            MultiVenueFinalizer,
        },
        profit_calculator::SimpleProfitCalculator,
    },
    liquidation::collateral_service::CollateralService,
    persistance::{FinalizerMetaPayload, MultiVenueExecutionOutcome, sqlite::SqliteWalStore},
    price_oracle::price_oracle::LiquidationPriceOracle,
    stages::{
        export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
        settlement_watcher::SettlementWatcher, simple_strategy::SimpleLiquidationStrategy,
    },
    wal::decode_receipt_wrapper,
    watchdog::{
        WatchdogEvent,
        balance_monitor::{
            DEFAULT_LOW_BALANCE_ALERT_COOLDOWN, LowBalanceMonitor, MonitoredBalanceAccount, balance_check_exclude_from_env,
        },
        slack_watchdog_from_env, slack_webhook_configured, webhook_watchdog_from_env,
    },
};
use ic_agent::Agent;
use liquidium_pipeline_core::{
    balance_service::BalanceService,
    tokens::{chain_token::ChainToken, token_registry::TokenRegistry},
};

const BRIDGE_CKETH_LEDGER_ID: &str = "ss2fx-dyaaa-aaaar-qacoq-cai";

fn disabled_venue_ids<'a>(
    outcome: &MultiVenueExecutionOutcome,
    venue_ids: impl IntoIterator<Item = &'a str>,
    enabled: &HashSet<&str>,
) -> Vec<String> {
    if matches!(
        outcome,
        MultiVenueExecutionOutcome::Completed | MultiVenueExecutionOutcome::Recovered
    ) {
        return Vec::new();
    }

    let mut disabled = Vec::new();
    for venue_id in venue_ids {
        if !enabled.contains(venue_id) && !disabled.iter().any(|existing| existing == venue_id) {
            disabled.push(venue_id.to_string());
        }
    }
    disabled
}

fn ensure_committed_venues_are_enabled(db: &SqliteWalStore, enabled_venues: &[String]) -> Result<(), String> {
    let enabled: HashSet<&str> = enabled_venues.iter().map(String::as_str).collect();
    let mut conflicts = Vec::new();

    for row in db
        .list_unfinished_execution_rows()
        .map_err(|error| format!("failed to inspect unfinished venue executions: {error}"))?
    {
        let wrapper = match decode_receipt_wrapper(&row) {
            Ok(Some(wrapper)) => wrapper,
            Ok(None) => continue,
            Err(error) => {
                return Err(format!(
                    "cannot verify enabled venues for unfinished WAL row {} because its metadata is malformed: {error}",
                    row.id
                ));
            }
        };
        let Some(meta) = wrapper.meta_v2 else {
            continue;
        };
        let FinalizerMetaPayload::MultiVenueSwap(state) = meta.payload;
        let disabled = disabled_venue_ids(
            &state.outcome,
            state.legs.iter().map(|leg| leg.venue_id.as_str()),
            &enabled,
        );
        if !disabled.is_empty() {
            conflicts.push(format!("{} [{}]", row.id, disabled.join(",")));
        }
    }

    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unfinished multi-venue WAL rows reference disabled venues: {}; re-enable those venues or finish recovery before startup",
            conflicts.join("; ")
        ))
    }
}

#[instrument(name = "liquidation.init", skip_all, err)]
async fn init(
    ctx: Arc<PipelineContext>,
) -> Result<
    (
        OpportunityFinder<Agent>,
        SimpleLiquidationStrategy<Config, TokenRegistry, CollateralService<LiquidationPriceOracle<Agent>>>,
        Arc<BasicExecutor<Agent, SqliteWalStore>>,
        Arc<ExportStage>,
        Arc<FinalizeStage<MultiVenueFinalizer, SqliteWalStore, SimpleProfitCalculator, Agent>>,
    ),
    String,
> {
    let config = ctx.config.clone();
    let agent = ctx.agent.clone();
    let registry = ctx.registry.clone();
    let db = Arc::new(SqliteWalStore::new(&config.db_path).map_err(|e| format!("could not connect to db: {e}"))?);
    ensure_committed_venues_are_enabled(db.as_ref(), &config.enabled_swap_venues)?;

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

    let mut venue_adapters: Vec<Arc<dyn MultiVenueAdapter>> = Vec::new();
    for venue_id in config.get_enabled_swap_venues() {
        match venue_id.as_str() {
            ICPSWAP_VENUE_ID => venue_adapters.push(
                ctx.icpswap_finalizer
                    .clone()
                    .ok_or_else(|| "ICPSwap is enabled but its adapter was not initialized".to_string())?,
            ),
            MEXC_VENUE_ID => venue_adapters.push(build_mexc_finalizer(ctx.as_ref()).await?),
            _ => return Err(format!("unsupported enabled swap venue `{venue_id}`")),
        }
    }
    let multi_venue_finalizer = Arc::new(
        MultiVenueFinalizer::new(
            venue_adapters,
            IcpswapFirstPlannerConfig {
                max_price_impact_bps: config.get_icpswap_max_price_impact_bps(),
                max_search_iterations: config.get_icpswap_max_search_iterations(),
                cex_min_exec_usd: config.get_cex_min_exec_usd(),
                min_net_edge_bps: config.get_cex_min_net_edge_bps(),
            },
        )?
        .with_watchdog(slack_watchdog_from_env(DEFAULT_LOW_BALANCE_ALERT_COOLDOWN)),
    );

    // Profit calculator for expected/realized PnL
    let profit_calc = Arc::new(SimpleProfitCalculator); //todo implement real profit calculator

    // FinalizeStage wires WAL + finalizer + profit calculation
    let finalizer = Arc::new(FinalizeStage::new(
        db.clone(),
        multi_venue_finalizer,
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

fn bridge_low_balance_service(ctx: &PipelineContext) -> Arc<BalanceService> {
    let tokens = vec![
        ChainToken::EvmNative {
            chain: "eth".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        },
        ChainToken::Icp {
            ledger: Principal::from_text(BRIDGE_CKETH_LEDGER_ID).expect("valid ckETH ledger principal"),
            symbol: "ckETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        },
    ];
    let registry = TokenRegistry::new(
        tokens
            .into_iter()
            .map(|token| (token.asset_id(), token))
            .collect::<HashMap<_, _>>(),
    );

    Arc::new(BalanceService::new(Arc::new(registry), ctx.bridge_service.accounts()))
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
        config.lending_canister,
        Duration::from_secs(3),
    );

    tokio::spawn(async move { watcher.run().await });

    let debt_asset_principals = debt_asset_principals(&ctx.registry);
    let debt_assets = debt_assets_as_text(&debt_asset_principals);

    info!(
        network = %config.ic_url,
        liquidator_principal = %config.liquidator_principal.to_text(),
        enabled_swap_venues = ?config.enabled_swap_venues,
        max_dex_slippage_bps = config.max_allowed_dex_slippage,
        max_cex_slippage_bps = config.max_allowed_cex_slippage_bps,
        buy_bad_debt = config.buy_bad_debt,
        "Startup configuration"
    );
    info!("Liquidator started; scanning for liquidation opportunities...");

    // Shared pause flag controlled by UDS commands.
    // `true` means: do not initiate new liquidations, but keep housekeeping alive.
    let paused = Arc::new(AtomicBool::new(false));
    let slack_watchdog = if slack_webhook_configured() {
        Some(slack_watchdog_from_env(DEFAULT_LOW_BALANCE_ALERT_COOLDOWN))
    } else {
        None
    };
    // The helper encapsulates:
    // - persisted paused/running state bootstrap
    // - UDS bind + serve
    // - state persistence on pause/resume transitions
    if let Err(err) = bootstrap_control_plane(&sock_path, &config.db_path, paused.clone(), slack_watchdog.clone()) {
        tracing::error!("{}", err);
        return;
    }
    info!(sock_path = %sock_path.display(), "Control plane ready");

    let liq_dog = webhook_watchdog_from_env(Duration::from_secs(300));
    if let Some(slack) = slack_watchdog.as_ref() {
        slack.notify(WatchdogEvent::Lifecycle {
            state: "started".to_string(),
            details: format!(
                "Liquidator started on {}; scanning for liquidation opportunities.",
                config.ic_url
            ),
        })
        .await;
    }

    let low_balance_monitor = slack_watchdog.as_ref().map(|slack| Arc::new(LowBalanceMonitor::new(
            vec![
                MonitoredBalanceAccount {
                    label: "main",
                    service: ctx.main_service.clone(),
                    only_symbols: None,
                    exclude_symbols: balance_check_exclude_from_env(),
                },
                MonitoredBalanceAccount {
                    label: "bridge",
                    service: bridge_low_balance_service(&ctx),
                    only_symbols: None,
                    exclude_symbols: None,
                },
            ],
            slack.clone(),
        )));

    // Steady-state operation is delegated to a helper to keep this entrypoint
    // focused on bootstrap wiring and lifecycle boundaries.
    run_daemon_cycle_loop(
        &finder,
        &strategy,
        &executor,
        &exporter,
        &finalizer,
        &liq_dog,
        slack_watchdog,
        low_balance_monitor,
        paused,
        &debt_assets,
        &debt_asset_principals,
        ui_enabled,
    )
    .await;
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
    use super::disabled_venue_ids;
    use crate::commands::liquidation_loop_helpers::console_ui_enabled;
    use crate::persistance::MultiVenueExecutionOutcome;
    use std::collections::HashSet;
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

    #[test]
    fn unfinished_plans_report_disabled_venues_once_in_leg_order() {
        let enabled = HashSet::from(["icpswap"]);
        let disabled = disabled_venue_ids(
            &MultiVenueExecutionOutcome::Running,
            ["icpswap", "mexc", "mexc", "kraken"],
            &enabled,
        );
        assert_eq!(disabled, vec!["mexc".to_string(), "kraken".to_string()]);
    }

    #[test]
    fn completed_plans_do_not_block_disabling_a_venue() {
        let enabled = HashSet::from(["icpswap"]);
        let disabled = disabled_venue_ids(
            &MultiVenueExecutionOutcome::Completed,
            ["mexc"],
            &enabled,
        );
        assert!(disabled.is_empty());
    }
}
