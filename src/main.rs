mod stage;
mod stages;
mod types;

use std::env;

use crate::stages::*;
use crate::types::*;
use candid::Principal;
use ic_agent::Agent;
use ic_agent::identity::BasicIdentity;
use stage::PipelineStage;
use stages::dex_swap::DexSwapExecutor;
use stages::liquidation::LiquidationExecutor;
use stages::opportunity::OpportunityFinder;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let ic_url = env::var("IC_URL").unwrap();
    let pem_path = env::var("IDENTITY_PEM").unwrap();
    let canister_id = env::var("CANISTER_ID").unwrap();

    let identity = BasicIdentity::from_pem_file(pem_path).unwrap();
    let agent = Agent::builder()
        .with_url(ic_url)
        .with_identity(identity)
        .build()
        .unwrap();

    let finder = OpportunityFinder {
        agent,
        canister_id: Principal::from_text(canister_id).unwrap(),
    };
    let executor = LiquidationExecutor;
    let swapper = DexSwapExecutor;

    loop {
        println!("Polling for opportunities...");
        let opportunities = finder.process(()).await.unwrap_or_default();

        // Iterate over all opportunities and execute them
        for opp in opportunities {
            if let Ok(receipt) = executor.process(opp.clone()).await {
                if let Ok(swap) = swapper.process(receipt.clone()).await {
                    println!(
                        "Liquidation: loan={} swapped {} -> {} (amt={})",
                        opp.debt_pool_id, opp.debt_asset, swap.received_asset, swap.received_amount
                    );
                }
            }
        }

        sleep(Duration::from_secs(15)).await;
    }
}
