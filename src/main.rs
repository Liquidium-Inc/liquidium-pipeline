mod account;
mod config;
mod executors;
mod icrc_token;
mod liquidation;
mod pipeline_agent;
mod price_oracle;
mod stage;
mod stages;
mod utils;

use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use account::account::{IcrcAccountInfo, LiquidatorAccount};
use candid::Principal;
use config::{Config, ConfigTrait};
use executors::kong_swap::kong_swap::KongSwapExecutor;
use ic_agent::Agent;
use liquidation::collateral_service::CollateralService;

use icrc_ledger_types::icrc1::account::Account;
use log::{error, info, warn};
use price_oracle::price_oracle::LiquidationPriceOracle;
use stage::PipelineStage;
use stages::opportunity::OpportunityFinder;
use stages::simple_strategy::IcrcLiquidationStrategy;

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
) {
    info!("Initializing swap stage...");
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

    (finder, strategy, executor, icrc_account_service)
}

#[tokio::main]
async fn main() {
    // === Load Config ===
    let config = Config::load().await.expect("Failed to load config");
    info!("Config loaded for network: {}", config.ic_url);

    // === Initialize IC Agent ===
    let agent = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .build()
        .expect("Failed to initialize IC agent");
    let agent = Arc::new(agent);
    info!("Agent initialized with principal: {}", config.liquidator_principal);

    // === Initialize components ===
    let (finder, strategy, executor, account_service) = init(config.clone(), agent.clone()).await;
    info!("Components initialized");

    // === Sync balances for all debt assets ===
    let debt_assets = config.get_debt_assets().keys().cloned().collect::<Vec<String>>();
    for asset in &debt_assets {
        let principal = Principal::from_text(asset).unwrap_or_else(|_| panic!("Invalid asset principal: {asset}"));

        account_service
            .sync_balance(principal, config.liquidator_principal)
            .await
            .expect("Failed to sync balance");
        info!("Balance synced for asset {}", asset);
    }

    // === Main Loop ===
    loop {
        info!("Polling for liquidation opportunities...");

        let opportunities = finder.process(debt_assets.clone()).await.unwrap_or_else(|e| {
            warn!("Failed to find opportunities: {e}");
            vec![]
        });

        if opportunities.is_empty() {
            info!("No opportunities found");
            sleep(Duration::from_secs(2));
            continue;
        }

        info!("Found {} opportunities", opportunities.len());

        let executions = strategy.process(opportunities).await.unwrap_or_else(|e| {
            error!("Strategy processing failed: {e}");
            vec![]
        });

        let results = executor.process(executions).await.unwrap_or_else(|e| {
            error!("Executor failed: {e}");
            vec![]
        });

        info!("Completed {} executions", results.len());
        sleep(Duration::from_secs(2));
    }
}
