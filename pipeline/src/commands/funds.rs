use liquidium_pipeline::{config::ConfigTrait, context::init_context};
use prettytable::{Cell, Row, Table, format};

use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

pub async fn funds() -> Result<(), String> {
    let ctx = init_context().await?;

    println!("\n=== Account (Main / Liquidator) ===");
    // Main balances
    let main_results = ctx.main_service.sync_all().await;

    let mut table_main = Table::new();
    table_main.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table_main.set_titles(Row::new(vec![
        Cell::new("Asset (chain:address:symbol)"),
        Cell::new("Balance"),
    ]));

    println!("\n=== Account (Main / Liquidator) ===");
    println!("ICP principal: {}", ctx.config.liquidator_principal.to_text());
    println!("EVM address: {}\n", ctx.evm_address);

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
