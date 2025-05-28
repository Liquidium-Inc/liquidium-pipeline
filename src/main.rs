mod config;
mod executors;
mod icrc_token;
mod stage;
mod stages;
mod types;
mod utils;

use std::sync::Arc;

use config::Config;
use executors::kong_swap::kong_swap::KongSwapExecutor;
use ic_agent::Agent;

use log::info;
use stages::liquidation::LiquidationExecutor;
use stages::opportunity::OpportunityFinder;

async fn init(
    config: Arc<Config>,
    agent: Arc<Agent>,
) -> (OpportunityFinder, LiquidationExecutor, KongSwapExecutor) {
    info!("Initializing swap stage...");
    let mut swapper = KongSwapExecutor::new(agent.clone(), config.liquidator_principal);

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

    info!("Initializing find stage ...");
    let finder = OpportunityFinder::new(agent.clone(), config.lending_canister);

    info!("Initializing liquidations stage ...");
    let executor = LiquidationExecutor::new(
        agent.clone(),
        config.lending_canister,
        config.liquidator_principal,
    );

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

    // let token_in = IcrcToken::from_principal(
    //     Principal::from_text("cngnf-vqaaa-aaaar-qag4q-cai").unwrap(),
    //     agent.clone(),
    // )
    // .await;

    // let token_out = IcrcToken::from_principal(
    //     Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap(),
    //     agent.clone(),
    // )
    // .await;

    // let amount = IcrcTokenAmount::from_formatted(token_in.clone(), 1.0);
    // let swap_info = swapper
    //     .get_swap_info(token_in, token_out, amount.clone())
    //     .await
    //     .expect("failed to get swap info");

    // info!("Got swap info {:#?}", swap_info);

    // let swap_result = swapper
    //     .swap(SwapArgs {
    //         pay_token: swap_info.pay_symbol,
    //         pay_amount: amount.value,
    //         pay_tx_id: None,
    //         receive_token: swap_info.receive_symbol,
    //         receive_amount: None,
    //         receive_address: None,
    //         max_slippage: Some(swap_info.slippage),
    //         referred_by: None,
    //     })
    //     .await
    //     .expect("swap failed");

    // info!("Executed swap {:?}", swap_result);
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
