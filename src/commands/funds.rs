use std::sync::Arc;

use candid::Principal;
use futures::future::join_all;
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use prettytable::{Cell, Row, Table, format};

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
        .with_max_tcp_error_retries(3)
        .build()
        .expect("Failed to initialize IC agent");

    let agent = Arc::new(agent);
    let account_service = Arc::new(LiquidatorAccount::new(agent.clone()));

    // Build union of debt + collateral asset principals (as strings)
    let mut asset_ids: Vec<String> = config.get_debt_assets().keys().cloned().collect::<Vec<String>>();
    for k in config.get_collateral_assets().keys() {
        if !asset_ids.contains(k) {
            asset_ids.push(k.clone());
        }
    }

    // MAIN (Liquidator) balances
    let results_main = sync_balances(account_service.clone(), &asset_ids, config.liquidator_principal.into()).await;

    // MAIN (Liquidator) balances table
    let mut table_main = Table::new();
    table_main.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_main.set_titles(Row::new(vec![
        Cell::new("Asset Balance"),
        Cell::new("Ledger Principal"),
    ]));

    println!("\n=== Account (Main / Liquidator) ===");
    println!("Principal: {}\n", config.liquidator_principal.to_text());
    for result in results_main {
        match result {
            Ok((balance, principal)) => {
                table_main.add_row(Row::new(vec![Cell::new(&balance), Cell::new(&principal.to_text())]))
            }
            Err(e) => table_main.add_row(Row::new(vec![Cell::new(&format!("Task failed: {}", e)), Cell::new("")])),
        };
    }
    table_main.printstd();

    // RECOVERY (Trader) balances
    let results_recovery = sync_balances(account_service.clone(), &asset_ids, config.get_recovery_account()).await;

    // RECOVERY (Trader) balances table
    let mut table_recovery = Table::new();
    table_recovery.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_recovery.set_titles(Row::new(vec![
        Cell::new("Asset Balance"),
        Cell::new("Ledger Principal"),
    ]));

    println!("\n=== Account (Recovery / Trader) ===");
    println!("Principal: {}\n", config.get_recovery_account());
    println!("\n⚠️  Recovery account holds seized collateral from liquidations where the swap failed.");
    println!("   These funds are safe but inactive. To reuse them, withdraw back to the Main account.");
    println!("   Run: `liquidium account withdraw` and choose 'Recovery (Trader)' as the source.");

    for result in results_recovery {
        match result {
            Ok((balance, principal)) => {
                table_recovery.add_row(Row::new(vec![Cell::new(&balance), Cell::new(&principal.to_text())]))
            }
            Err(e) => table_recovery.add_row(Row::new(vec![Cell::new(&format!("Task failed: {}", e)), Cell::new("")])),
        };
    }
    table_recovery.printstd();
}

pub async fn sync_balances(
    account_service: Arc<LiquidatorAccount<Agent>>,
    assets: &[String],
    owner: Account,
) -> Vec<Result<(String, Principal), tokio::task::JoinError>> {
    let futures = assets.iter().map(|asset| {
        let account_service = account_service.clone();
        let principal = Principal::from_text(asset).unwrap_or_else(|_| panic!("Invalid asset principal: {asset}"));
        let owner_principal = owner;

        tokio::spawn(async move {
            let balance = account_service
                .sync_balance(principal, owner_principal)
                .await
                .expect("Failed to sync balance");
            (balance.formatted(), principal)
        })
    });

    join_all(futures).await
}
