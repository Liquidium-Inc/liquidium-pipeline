use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use log::{info, warn};
use std::{sync::Arc, thread::sleep, time::Duration};

use crate::{
    account::account::LiquidatorAccount,
    commands::funds::load_assets,
    config::{Config, ConfigTrait},
    executors::kong_swap::kong_swap::KongSwapExecutor,
    liquidation::collateral_service::CollateralService,
    price_oracle::price_oracle::LiquidationPriceOracle,
    stage::PipelineStage,
    stages::{export::ExportStage, opportunity::OpportunityFinder, simple_strategy::IcrcLiquidationStrategy},
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
        KongSwapExecutor<Agent>,
        Config,
        CollateralService<LiquidationPriceOracle<Agent>>,
        LiquidatorAccount<Agent>,
    >,
    Arc<KongSwapExecutor<Agent>>,
    Arc<LiquidatorAccount<Agent>>,
    Arc<ExportStage>,
) {
    let mut swapper = KongSwapExecutor::new(
        agent.clone(),
        Account {
            owner: config.liquidator_principal.clone(),
            subaccount: None,
        },
        config.lending_canister.clone(),
    );

    // Pre approve tokens
    swapper
        .init(
            config
                .collateral_assets
                .keys()
                .map(|item| Principal::from_text(item.clone()).unwrap())
                .collect(),
        )
        .await
        .expect("could not pre approve tokens");

    let executor = Arc::new(swapper);

    info!("Initializing find stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister.clone());

    info!("Initializing liquidations stage ...");
    let price_oracle = Arc::new(LiquidationPriceOracle::new(agent.clone(), config.lending_canister));

    let collateral_service = Arc::new(CollateralService::new(price_oracle));
    let icrc_account_service = Arc::new(LiquidatorAccount::new(agent.clone()));
    let strategy = IcrcLiquidationStrategy::new(
        config.clone(),
        executor.clone(),
        collateral_service.clone(),
        icrc_account_service.clone(),
    );

    let exporter = Arc::new(ExportStage {
        path: config.export_path.clone(),
    });

    (finder, strategy, executor, icrc_account_service, exporter)
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
        .build()
        .expect("Failed to initialize IC agent");

    let agent = Arc::new(agent);
    info!("Agent initialized with principal: {}", config.liquidator_principal);

    // Initialize components from run_liquidation_loop module
    let (finder, strategy, executor, account_service, exporter) = init(config.clone(), agent.clone()).await;
    info!("Components initialized");

    let debt_assets = config.get_debt_assets().keys().cloned().collect::<Vec<String>>();
    load_assets(&config, account_service, &debt_assets).await;

    loop {
        info!("Polling for liquidation opportunities...");

        let opportunities = finder.process(&debt_assets).await.unwrap_or_else(|e| {
            warn!("Failed to find opportunities: {e}");
            vec![]
        });

        if opportunities.is_empty() {
            info!("No opportunities found");
            sleep(Duration::from_secs(2));
            continue;
        }

        info!("Found {} opportunities", opportunities.len());

        let executions = strategy.process(&opportunities).await.unwrap_or_else(|e| {
            log::error!("Strategy processing failed: {e}");
            vec![]
        });

        let results = executor.process(&executions).await.unwrap_or_else(|e| {
            log::error!("Executor failed: {e}");
            vec![]
        });

        if results.is_empty() {
            info!("No successful executions");
            sleep(Duration::from_secs(30));
            continue;
        }

        exporter.process(&results).await.expect("Failed to export results");
        sleep(Duration::from_secs(30));
    }
}
