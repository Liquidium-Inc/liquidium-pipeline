mod calculations;
mod config;
mod executors;
mod icrc_token;
mod pipeline_agent;
mod stage;
mod stages;
mod types;
mod utils;

use std::sync::Arc;

use calculations::collateral_service::{CollateralService};
use config::Config;
use executors::kong_swap::kong_swap::KongSwapExecutor;
use ic_agent::Agent;

use log::info;
use stages::opportunity::OpportunityFinder;
use stages::simple_strategy::IcrcLiquidationStrategy;

async fn init(
    config: Arc<Config>,
    agent: Arc<Agent>,
) -> (
    OpportunityFinder<Agent>,
    IcrcLiquidationStrategy<KongSwapExecutor<Agent>, Config, CollateralService>,
    Arc<KongSwapExecutor<Agent>>,
) {
    info!("Initializing swap stage...");
    let mut swapper = KongSwapExecutor::new(agent.clone(), config.clone());

    // Pre approve tokens
    swapper
        .init(
            config
                .collateral_assets
                .keys()
                .map(|item| item.clone())
                .collect(),
        )
        .await
        .expect("could not pre approve tokens");

    let swapper = Arc::new(swapper);

    info!("Initializing find stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister);

    info!("Initializing liquidations stage ...");
    let collateral_service = Arc::new(CollateralService::default());
    let executor =
        IcrcLiquidationStrategy::new(config.clone(), swapper.clone(), collateral_service);

    (finder, executor, swapper)
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

    let (finder, executor, swapper) = init(config.clone(), agent.clone()).await;

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
