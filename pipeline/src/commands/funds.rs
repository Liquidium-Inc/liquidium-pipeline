use std::sync::Arc;

use candid::Principal;
use futures::future::join_all;
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use prettytable::{Cell, Row, Table, format};

use liquidium_pipeline_core::account::actions::AccountInfo;
use liquidium_pipeline_core::account::model::ChainBalance;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistry;

use liquidium_pipeline_connectors::{account::router::MultiChainAccountInfoRouter, backend::icp_backend::{IcpBackend, IcpBackendImpl}, token_registry_loader::load_token_registry};

use crate::config::{Config, ConfigTrait};

pub async fn funds() -> Result<(), String> {
    let config = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;

    // ICP agents for main and recovery
    let agent_main = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .with_max_tcp_error_retries(3)
        .build()
        .map_err(|e| format!("ic agent(main) build: {e}"))?;

    let agent_recovery = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config..clone())
        .with_max_tcp_error_retries(3)
        .build()
        .map_err(|e| format!("ic agent(recovery) build: {e}"))?;

    let agent_main = Arc::new(agent_main);
    let agent_recovery = Arc::new(agent_recovery);

    // ICP backends
    let icp_backend_main: Arc<dyn IcpBackend> = Arc::new(IcpBackendImpl::new(agent_main.clone()));
    let icp_backend_recovery: Arc<dyn IcpBackend> = Arc::new(IcpBackendImpl::new(agent_recovery.clone()));

    // EVM backends (adapt to your constructors)
    let evm_backend_main: Arc<dyn EvmBackend> =
        Arc::new(EvmBackendImpl::new_main(&config).map_err(|e| format!("evm main: {e}"))?);
    let evm_backend_recovery: Arc<dyn EvmBackend> =
        Arc::new(EvmBackendImpl::new_recovery(&config).map_err(|e| format!("evm recovery: {e}"))?);

    // Token registry (decimals resolved via backends)
    let registry = Arc::new(load_token_registry(icp_backend_main.clone(), evm_backend_main.clone()).await?);

    // ICP account descriptors
    let main_icp_account = Account {
        owner: config.liquidator_principal,
        subaccount: None,
    };
    let recovery_icp_account = Account {
        owner: Principal::from_text(config.get_recovery_account())
            .map_err(|e| format!("invalid recovery principal in config: {e}"))?,
        subaccount: None,
    };

    // AccountInfo routers for main and recovery
    let icp_info_main = Arc::new(IcpAccountInfoAdapter::new(icp_backend_main.clone(), main_icp_account));
    let evm_info_main = Arc::new(EvmAccountInfoAdapter::new(evm_backend_main.clone()));
    let main_accounts: Arc<dyn AccountInfo + Send + Sync> =
        Arc::new(MultiChainAccountInfoRouter::new(icp_info_main, evm_info_main));

    let icp_info_recovery = Arc::new(IcpAccountInfoAdapter::new(
        icp_backend_recovery.clone(),
        recovery_icp_account,
    ));
    let evm_info_recovery = Arc::new(EvmAccountInfoAdapter::new(evm_backend_recovery.clone()));
    let recovery_accounts: Arc<dyn AccountInfo + Send + Sync> =
        Arc::new(MultiChainAccountInfoRouter::new(icp_info_recovery, evm_info_recovery));

    // All assets from registry (all chains)
    let asset_ids: Vec<AssetId> = registry.tokens.keys().cloned().collect();

    // Main balances
    let main_results = sync_balances(&registry, main_accounts.clone(), &asset_ids).await;

    let mut table_main = Table::new();
    table_main.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_main.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Balance"),
    ]));

    println!("\n=== Account (Main / Liquidator) ===");
    println!("ICP principal: {}\n", config.liquidator_principal.to_text());

    for r in main_results {
        match r {
            Ok((id, bal)) => table_main.add_row(Row::new(vec![
                Cell::new(&id.to_string()),
                Cell::new(&format_chain_balance(&bal)),
            ])),
            Err(e) => table_main.add_row(Row::new(vec![Cell::new("error"), Cell::new(&e)])),
        }
    }
    table_main.printstd();

    // Recovery balances
    let recovery_results = sync_balances(&registry, recovery_accounts.clone(), &asset_ids).await;

    let mut table_recovery = Table::new();
    table_recovery.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_recovery.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Balance"),
    ]));

    println!("\n=== Account (Recovery / Trader) ===");
    println!("ICP principal: {}\n", config.get_recovery_account());
    println!("Recovery account holds seized collateral from failed swaps.\n");

    for r in recovery_results {
        match r {
            Ok((id, bal)) => table_recovery.add_row(Row::new(vec![
                Cell::new(&id.to_string()),
                Cell::new(&format_chain_balance(&bal)),
            ])),
            Err(e) => table_recovery.add_row(Row::new(vec![Cell::new("error"), Cell::new(&e)])),
        }
    }
    table_recovery.printstd();

    Ok(())
}

async fn sync_balances(
    registry: &TokenRegistry,
    accounts: Arc<dyn AccountInfo + Send + Sync>,
    assets: &[AssetId],
) -> Vec<Result<(AssetId, ChainBalance), String>> {
    let futs = assets.iter().cloned().map(|asset_id| {
        let accounts = accounts.clone();
        let token = registry
            .get(&asset_id)
            .expect("TokenRegistry out of sync with AssetId list")
            .clone();

        tokio::spawn(async move {
            let bal = accounts
                .sync_balance(&token)
                .await
                .map_err(|e| format!("sync_balance failed for {}: {}", asset_id, e))?;
            Ok::<_, String>((asset_id, bal))
        })
    });

    join_all(futs)
        .await
        .into_iter()
        .map(|j| j.map_err(|e| e.to_string()).and_then(|x| x))
        .collect()
}

fn format_chain_balance(bal: &ChainBalance) -> String {
    let raw = bal.amount_native;
    let decimals = bal.decimals as u32;

    let int_part = raw / 10u128.pow(decimals);
    let frac_part = raw % 10u128.pow(decimals);

    if decimals > 0 {
        format!(
            "{}.{:0>width$} {}",
            int_part,
            frac_part,
            bal.symbol,
            width = decimals as usize
        )
    } else {
        format!("{} {}", int_part, bal.symbol)
    }
}
