use prettytable::{Cell, Row, Table, format};
use tracing::info;

use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};

use crate::config::ConfigTrait;
use crate::context::init_context;
use crate::output::plain_logs_enabled;
pub async fn funds() -> Result<(), String> {
    let ctx = init_context().await?;

    const HEADER_LABEL_WIDTH: usize = 22;

    let recovery_account = ctx.config.get_recovery_account();
    let plain_logs = plain_logs_enabled();

    if plain_logs {
        info!(
            main_icp_principal = %ctx.config.liquidator_principal.to_text(),
            trader_icp_principal = %ctx.config.trader_principal.to_text(),
            recovery_account = %recovery_account,
            evm_address = %ctx.evm_address,
            "Balances header"
        );
        info!("Recovery account holds seized collateral from failed swaps.");
    } else {
        println!("\n=== Balances (Main | Trader | Recovery) ===");
        println!(
            "{: <HEADER_LABEL_WIDTH$}: {}",
            "Main ICP principal",
            ctx.config.liquidator_principal.to_text()
        );
        println!(
            "{: <HEADER_LABEL_WIDTH$}: {}",
            "Trader ICP principal",
            ctx.config.trader_principal.to_text()
        );
        if let Some((principal, subaccount)) = recovery_account.to_string().split_once('.') {
            println!("{: <HEADER_LABEL_WIDTH$}: {}.", "Recovery account", principal);
            println!("{: <HEADER_LABEL_WIDTH$}  {}", "", subaccount);
        } else {
            println!("{: <HEADER_LABEL_WIDTH$}: {}", "Recovery account", recovery_account);
        }
        println!("{: <HEADER_LABEL_WIDTH$}: {}\n", "EVM address", ctx.evm_address);
        const ANSI_YELLOW: &str = "\x1b[33m";
        const ANSI_RESET: &str = "\x1b[0m";
        println!(
            "{}Recovery account holds seized collateral from failed swaps.{}\n",
            ANSI_YELLOW, ANSI_RESET
        );
    }

    let asset_ids: Vec<AssetId> = ctx.registry.all().into_iter().map(|(id, _)| id).collect();

    let (main_results, trader_results, recovery_results) = tokio::join!(
        ctx.main_service.sync_assets(&asset_ids),
        ctx.trader_service.sync_assets(&asset_ids),
        ctx.recovery_service.sync_assets(&asset_ids),
    );

    if plain_logs {
        for (idx, asset_id) in asset_ids.iter().enumerate() {
            let main_cell = format_balance_result(main_results.get(idx));
            let trader_cell = format_balance_result(trader_results.get(idx));
            let recovery_cell = format_balance_result(recovery_results.get(idx));

            info!(
                asset = %asset_id,
                main = %main_cell,
                trader = %trader_cell,
                recovery = %recovery_cell,
                "Balance row"
            );
        }

        return Ok(());
    }

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Main"),
        Cell::new("Trader"),
        Cell::new("Recovery"),
    ]));

    for (idx, asset_id) in asset_ids.iter().enumerate() {
        let main_cell = format_balance_result(main_results.get(idx));
        let trader_cell = format_balance_result(trader_results.get(idx));
        let recovery_cell = format_balance_result(recovery_results.get(idx));

        table.add_row(Row::new(vec![
            Cell::new(&asset_id.to_string()),
            Cell::new(&main_cell),
            Cell::new(&trader_cell),
            Cell::new(&recovery_cell),
        ]));
    }

    table.printstd();

    Ok(())
}

fn format_balance_result(res: Option<&Result<(AssetId, ChainTokenAmount), String>>) -> String {
    match res {
        Some(Ok((_, bal))) => format_chain_balance(bal),
        Some(Err(e)) => format!("error: {}", e),
        None => "n/a".to_string(),
    }
}

fn format_chain_balance(bal: &ChainTokenAmount) -> String {
    let raw = bal.value.clone();
    let decimals = bal.token.decimals() as u32;

    if decimals == 0 {
        let int_str = raw.to_string().replace('_', "");
        return format!("{} {}", int_str, bal.token.symbol());
    }

    // clamp to max 6 displayed decimals
    let display_decimals = decimals.min(6);
    let scale = 10u128.pow(decimals - display_decimals);
    let scaled = raw / scale;

    let int_part = scaled.clone() / 10u128.pow(display_decimals);
    let frac_part = scaled % 10u128.pow(display_decimals);

    let int_str = int_part.to_string().replace('_', "");

    let mut frac_clean = frac_part.to_string();
    frac_clean = frac_clean.replace('_', "");

    let frac_str = format!("{:0>width$}", frac_clean, width = display_decimals as usize);

    format!("{}.{} {}", int_str, frac_str, bal.token.symbol())
}
