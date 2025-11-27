use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use indicatif::{ProgressBar, ProgressStyle};

use log::{debug, info, warn};
use prettytable::{Cell, Row, Table, format};
use std::{sync::Arc, thread::sleep, time::Duration};

use crate::{
    config::Config,
    context::{PipelineContext, init_context},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::{
        kong_swap::kong_swap_finalizer::KongSwapFinalizer, liquidation_outcome::LiquidationOutcome,
        profit_calculator::PassthroughProfitCalculator,
    },
    liquidation::collateral_service::CollateralService,
    persistance::sqlite::SqliteWalStore,
    price_oracle::price_oracle::LiquidationPriceOracle,
    stage::PipelineStage,
    stages::{
        executor::ExecutionStatus, export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
        simple_strategy::SimpleLiquidationStrategy,
    },
    swappers::router::SwapRouter,
    watchdog::{WatchdogEvent, webhook_watchdog_from_env},
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

async fn init(
    ctx: Arc<PipelineContext>,
) -> (
    OpportunityFinder<Agent>,
    SimpleLiquidationStrategy<SwapRouter, Config, TokenRegistry, CollateralService<LiquidationPriceOracle<Agent>>>,
    Arc<BasicExecutor<Agent, SqliteWalStore>>,
    Arc<ExportStage>,
    Arc<FinalizeStage<KongSwapFinalizer<SwapRouter>, SqliteWalStore, PassthroughProfitCalculator>>,
) {
    let config = ctx.config.clone();
    let agent = ctx.agent.clone();
    let registry = ctx.registry.clone();
    let db = Arc::new(SqliteWalStore::new(&config.db_path).expect("could not connect to db"));

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
    );

    executor.init(&tokens).await.expect("could not approce executor tokens");
    let executor = Arc::new(executor);

    // Finalizer logic (Kong swapper)
    let kong_finalizer = Arc::new(KongSwapFinalizer::new(ctx.swap_router.clone()));

    // Profit calculator for expected/realized PnL
    let profit_calc = Arc::new(PassthroughProfitCalculator); //todo implement real profit calculator

    // FinalizeStage wires WAL + finalizer + profit calculation
    let finalizer = Arc::new(FinalizeStage::new(db.clone(), kong_finalizer, profit_calc));

    info!("Initializing searcher stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister);

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
    )
    .with_watchdog(wd);

    let exporter = Arc::new(ExportStage {
        path: config.export_path.clone(),
    });

    (finder, strategy, executor, exporter, finalizer)
}

pub async fn run_liquidation_loop() {
    print_banner();
    let ctx = init_context().await.expect("Failed to initialize pipeline context");
    let ctx = Arc::new(ctx);
    let config = ctx.config.clone();

    if config.buy_bad_debt {
        info!("🚨 BUYING BAD DEBT ENABLED: {} 🚨", config.buy_bad_debt);
        println!("⚠️  You are about to BUY BAD DEBT. Type 'yes' to continue:");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        if input.trim() != "yes" {
            panic!("Aborted by user.");
        }
    }
    info!("Config loaded for network: {}", config.ic_url);

    // Use main IC agent (liquidator identity) from context
    info!("Agent initialized with principal: {}", config.liquidator_principal);

    // Initialize components using shared pipeline context
    let (finder, strategy, executor, _exporter, finalizer) = init(ctx.clone()).await;

    let debt_assets: Vec<String> = ctx
        .registry
        .debt_assets()
        .iter()
        .filter_map(|(_, tok)| {
            if let ChainToken::Icp { ledger, .. } = tok {
                Some(ledger.to_text())
            } else {
                None
            }
        })
        .collect();

    let assets = ctx.registry.all();

    // Create the spinner for fancy UI
    let start_spinner = || {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::with_template("{spinner} {msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        spinner.enable_steady_tick(Duration::from_millis(100));
        spinner
    };

    let mut spinner = start_spinner();
    loop {
        spinner.set_message("Scanning for liquidation opportunities...");
        let opportunities = finder.process(&debt_assets).await.unwrap_or_else(|e| {
            warn!("Failed to find opportunities: {e}");
            vec![]
        });

        if opportunities.is_empty() {
            sleep(Duration::from_secs(2));
            spinner = start_spinner();
            continue;
        }

        spinner.finish_and_clear();
        info!("Found {} opportunities", opportunities.len());

        let executions = strategy.process(&opportunities).await.unwrap_or_else(|e| {
            log::info!("Strategy processing failed: {e}");
            vec![]
        });

        info!("Executing liquidations...");
        let receipts = executor.process(&executions).await.unwrap_or_else(|e| {
            log::error!("Executor failed: {e}");
            vec![]
        });

        info!("Finalizing liquidations...");
        let _outcomes = finalizer.process(&receipts).await.unwrap_or_else(|e| {
            log::error!("Executor failed: {e}");
            vec![]
        });

        // if outcomes.is_empty() {
        //     info!("No successful executions");
        //     spinner = start_spinner();
        //     spinner.set_message("Scanning for liquidation opportunities...");
        //     sleep(Duration::from_secs(30));
        //     continue;
        // }

        // exporter.process(&outcomes).await.expect("Failed to export results");
        // print_execution_results(outcomes);
        spinner = start_spinner();
        spinner.set_message("Scanning for liquidation opportunities...");
        sleep(Duration::from_secs(5));
    }
}

pub fn print_execution_results(results: Vec<LiquidationOutcome>) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![
        Cell::new("Realized (Δ)"),
        Cell::new("Expected"),
        Cell::new("Debt Repaid"),
        Cell::new("Collateral"),
        Cell::new("Swap Output"),
        Cell::new("Swap Status"),
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
            status_cell,
        ]));
    }

    table.printstd();
}
