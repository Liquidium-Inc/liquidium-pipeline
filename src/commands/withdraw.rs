use std::sync::Arc;

use candid::Principal;
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;

use crate::{
    account::account::{IcrcAccountActions, LiquidatorAccount},
    config::Config,
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
};

pub async fn withdraw(asset: &String, amount: &str, to: &String) {
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

    let principal = Principal::from_text(asset).expect("Invalid asset principal");
    let token = IcrcToken::from_principal(principal, agent.clone()).await;
    let amount = IcrcTokenAmount::from_formatted(token.clone(), amount.parse().unwrap());

    println!("Withdrawing {} to {}", amount.value, to);

    let txid = account_service
        .transfer(
            amount,
            Account {
                owner: Principal::from_text(to.clone()).unwrap(),
                subaccount: None,
            },
        )
        .await
        .expect("Failed to withdraw funds");

    println!("Success. Withdraw transaction {}", txid);
}
