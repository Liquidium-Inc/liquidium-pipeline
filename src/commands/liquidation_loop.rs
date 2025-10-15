use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use indicatif::{ProgressBar, ProgressStyle};

use log::{info, warn};
use prettytable::{Cell, Row, Table, format};
use std::{sync::Arc, thread::sleep, time::Duration};

use crate::{
    account::account::LiquidatorAccount,
    commands::funds::sync_balances,
    config::{Config, ConfigTrait},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::kong_swap::kong_swap_finalizer::KongSwapFinalizer,
    liquidation::collateral_service::CollateralService,
    persistance::sqlite::SqliteWalStore,
    price_oracle::price_oracle::LiquidationPriceOracle,
    stage::PipelineStage,
    stages::{
        executor::ExecutionStatus, export::ExportStage, finalize::LiquidationOutcome, opportunity::OpportunityFinder,
        simple_strategy::IcrcLiquidationStrategy,
    },
    swappers::kong_swap_swapper::KongSwapSwapper,
    watchdog::{WatchdogEvent, webhook_watchdog_from_env},
};
use ic_agent::Agent;

/// Prints the startup banner.
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
    config: Arc<Config>,
    agent: Arc<Agent>,
) -> (
    OpportunityFinder<Agent>,
    IcrcLiquidationStrategy<
        KongSwapSwapper<Agent>,
        Config,
        CollateralService<LiquidationPriceOracle<Agent>>,
        LiquidatorAccount<Agent>,
    >,
    Arc<BasicExecutor<Agent>>,
    Arc<LiquidatorAccount<Agent>>,
    Arc<ExportStage>,
    Arc<KongSwapFinalizer<SqliteWalStore, KongSwapSwapper<Agent>>>,
) {
    let swap_agent = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.trader_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .expect("Failed to initialize swap agent"),
    );

    let tokens: Vec<Principal> = config
        .collateral_assets
        .keys()
        .map(|item| Principal::from_text(item.clone()).unwrap())
        .collect();

    let mut swapper = KongSwapSwapper::new(
        swap_agent,
        Account {
            owner: config.trader_principal,
            subaccount: None,
        },
    );

    // Pre approve tokens
    swapper
        .init(&tokens)
        .await
        .expect("could not pre approve swapper tokens");

    let swapper = Arc::new(swapper);

    let mut executor = BasicExecutor::new(
        agent.clone(),
        Account {
            owner: config.liquidator_principal,
            subaccount: None,
        },
        config.lending_canister,
    );

    executor.init(&tokens).await.expect("could not approce executor tokens");
    let executor = Arc::new(executor);

    let db = Arc::new(SqliteWalStore::new(&config.db_path).expect("could not connect to db"));
    let finalizer = KongSwapFinalizer::new(db, swapper.clone());

    info!("Initializing searcher stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister);

    info!("Initializing liquidations stage ...");
    let price_oracle = Arc::new(LiquidationPriceOracle::new(agent.clone(), config.lending_canister));

    let collateral_service = Arc::new(CollateralService::new(price_oracle));
    let icrc_account_service = Arc::new(LiquidatorAccount::new(agent.clone()));

    let wd = webhook_watchdog_from_env(Duration::from_secs(300));
    wd.notify(WatchdogEvent::Heartbeat { stage: "Init" }).await;
    let strategy = IcrcLiquidationStrategy::new(
        config.clone(),
        swapper.clone(),
        collateral_service.clone(),
        icrc_account_service.clone(),
    )
    .with_watchdog(wd);

    let exporter = Arc::new(ExportStage {
        path: config.export_path.clone(),
    });

    (
        finder,
        strategy,
        executor,
        icrc_account_service,
        exporter,
        finalizer.into(),
    )
}

pub async fn run_liquidation_loop() {
    print_banner();
    // Load Config
    let config = Config::load().await.expect("Failed to load config");

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

    // Initialize IC Agent
    let agent = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .with_max_tcp_error_retries(3)
        .build()
        .expect("Failed to initialize IC agent");

    let agent = Arc::new(agent);
    info!("Agent initialized with principal: {}", config.liquidator_principal);

    // Initialize components from run_liquidation_loop module
    let (finder, strategy, executor, account_service, exporter, finalizer) = init(config.clone(), agent.clone()).await;
    info!("Components initialized");

    let debt_assets = config.get_debt_assets().keys().cloned().collect::<Vec<String>>();

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
        sync_balances(&config, account_service.clone(), &debt_assets).await;
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
            log::error!("Strategy processing failed: {e}");
            vec![]
        });

        let receipts = executor.process(&executions).await.unwrap_or_else(|e| {
            log::error!("Executor failed: {e}");
            vec![]
        });

        let outcomes = finalizer.process(&receipts).await.unwrap_or_else(|e| {
            log::error!("Executor failed: {e}");
            vec![]
        });

        if outcomes.is_empty() {
            info!("No successful executions");
            spinner = start_spinner();
            spinner.set_message("Scanning for liquidation opportunities...");
            sleep(Duration::from_secs(30));
            continue;
        }

        exporter.process(&outcomes).await.expect("Failed to export results");
        print_execution_results(outcomes);
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

        let (recv_amt, swap_status) = match &r.swap_result {
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
