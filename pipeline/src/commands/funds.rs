use liquidium_pipeline::{config::ConfigTrait, context::init_context};
use prettytable::{Cell, Row, Table, format};

use liquidium_pipeline_core::account::model::ChainBalance;

pub async fn funds() -> Result<(), String> {
    let ctx = init_context().await?;

    // Main balances
    let main_results = ctx.main_service.sync_all().await;

    let mut table_main = Table::new();
    table_main.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_main.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Balance"),
    ]));

    println!("\n=== Account (Main / Liquidator) ===");
    println!("ICP principal: {}\n", ctx.config.liquidator_principal.to_text());

    for r in main_results {
        match r {
            Ok((id, bal)) => table_main.add_row(Row::new(vec![
                Cell::new(&id.to_string()),
                Cell::new(&format_chain_balance(&bal)),
            ])),
            Err(e) => table_main.add_row(Row::new(vec![Cell::new("error"), Cell::new(&e)])),
        };
    }
    table_main.printstd();

    // Recovery balances
    let recovery_results = ctx.recovery_service.sync_all().await;

    let mut table_recovery = Table::new();
    table_recovery.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_recovery.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Balance"),
    ]));

    println!("\n=== Account (Recovery / Trader) ===");
    println!("ICP principal: {}\n", ctx.config.get_recovery_account());
    println!("Recovery account holds seized collateral from failed swaps.\n");

    for r in recovery_results {
        match r {
            Ok((id, bal)) => table_recovery.add_row(Row::new(vec![
                Cell::new(&id.to_string()),
                Cell::new(&format_chain_balance(&bal)),
            ])),
            Err(e) => table_recovery.add_row(Row::new(vec![Cell::new("error"), Cell::new(&e)])),
        };
    }
    table_recovery.printstd();

    Ok(())
}

fn format_chain_balance(bal: &ChainBalance) -> String {
    let raw = bal.amount_native.clone();
    let decimals = bal.decimals as u32;

    let int_part = raw.clone() / 10u128.pow(decimals);
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
