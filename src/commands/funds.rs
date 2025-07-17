use std::sync::Arc;

use candid::Principal;
use futures::future::join_all;
use ic_agent::Agent;

use crate::{
    account::account::{IcrcAccountInfo, LiquidatorAccount},
    config::{Config, ConfigTrait},
};

pub async fn funds() {
    // Load Config
    let config = Config::load().await.expect("Failed to load config");
    // Initialize IC Agent
    let agent = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .build()
        .expect("Failed to initialize IC agent");

    let agent = Arc::new(agent);
    let account_service = Arc::new(LiquidatorAccount::new(agent.clone()));

    let debt_assets = config.get_debt_assets().keys().cloned().collect::<Vec<String>>();
    let results = load_assets(&config, account_service, &debt_assets).await;

    println!("Account: {}", config.liquidator_principal.to_text());
    for result in results {
        match result {
            Ok(formatted) => println!("{} ({})", formatted.0, formatted.1),
            Err(e) => eprintln!("Task failed: {}", e),
        }
    }
}

pub async fn load_assets(
    config: &Arc<Config>,
    account_service: Arc<LiquidatorAccount<Agent>>,
    assets: &Vec<String>,
) -> Vec<Result<(String, Principal), tokio::task::JoinError>> {
    let futures = assets.iter().map(|asset| {
        let account_service = account_service.clone();
        let principal = Principal::from_text(asset).unwrap_or_else(|_| panic!("Invalid asset principal: {asset}"));
        let liquidator_principal = config.liquidator_principal;

        tokio::spawn(async move {
            let balance = account_service
                .sync_balance(principal, liquidator_principal)
                .await
                .expect("Failed to sync balance");
            (balance.formatted(), principal)
        })
    });

    let results = join_all(futures).await;
    results
}
