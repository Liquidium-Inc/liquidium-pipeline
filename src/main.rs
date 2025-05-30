mod account;
mod config;
mod executors;
mod icrc_token;
mod liquidation;
mod pipeline_agent;
mod price_oracle;
mod stage;
mod stages;
mod types;
mod utils;

use std::sync::Arc;

use account::account::{IcrcAccountInfo, LiquidatorAccount};
use candid::Principal;
use config::{Config, ConfigTrait};
use executors::kong_swap::kong_swap::KongSwapExecutor;
use ic_agent::Agent;
use liquidation::collateral_service::CollateralService;

use icrc_ledger_types::icrc1::account::Account;
use log::info;
use price_oracle::price_oracle::LiquidationPriceOracle;
use stages::opportunity::OpportunityFinder;
use stages::simple_strategy::IcrcLiquidationStrategy;

async fn init(
    config: Arc<Config>,
    agent: Arc<Agent>,
) -> (
    OpportunityFinder<Agent>,
    IcrcLiquidationStrategy<KongSwapExecutor<Agent>, Config, CollateralService<LiquidationPriceOracle<Agent>>, LiquidatorAccount<Agent>>,
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
        .init(config.collateral_assets.keys().map(|item| item.clone()).collect())
        .await
        .expect("could not pre approve tokens");

    let swapper = Arc::new(swapper);

    info!("Initializing find stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister.clone());

    info!("Initializing liquidations stage ...");
    let price_oracle = Arc::new(LiquidationPriceOracle::new(agent.clone(), config.lending_canister));

    let collateral_service = Arc::new(CollateralService::new(price_oracle));
    let icrc_account_service = Arc::new(LiquidatorAccount::new(agent.clone()));
    let executor = IcrcLiquidationStrategy::new(config.clone(), swapper.clone(), collateral_service.clone(), icrc_account_service.clone());

    (finder, executor, swapper, icrc_account_service)
}

#[tokio::main]
async fn main() {
    // Load our config
    let config = Config::load().await.unwrap();

    let agent = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .build()
        .expect("could not initialize client");

    let agent = Arc::new(agent);

    let (finder, executor, swapper, account_service) = init(config.clone(), agent.clone()).await;

    let debt_assets = config.get_debt_assets().keys().cloned().collect::<Vec<Principal>>();
    for asset in debt_assets {
        account_service
            .sync_balance(asset, config.liquidator_principal)
            .await
            .expect("could not sync balance")
    }
    // todo!()

    // loop {
    //     info!("Polling for opportunities...");
    //     let opportunities = finder.process(()).await.unwrap_or_default();

    //     for opp in opportunities {
    //         if let Ok(receipt) = executor.process(opp.clone()).await {
    //             if let Ok(swap) = swapper.process(receipt.clone()).await {
    //                 info!(
    //                     "Liquidation: loan={} swapped {} -> {} (amt={})",
    //                     opp.debt_pool_id, opp.debt_asset, swap.received_asset, swap.received_amount
    //                 );
    //             }
    //         }
    //     }

    //     sleep(Duration::from_secs(15)).await;
    // }
}
