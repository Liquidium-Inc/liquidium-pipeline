use liquidium_pipeline::{config::ConfigTrait, context::init_context};
use prettytable::{format, Cell, Row, Table};

use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;
pub async fn funds() -> Result<(), String> {
    let ctx = init_context().await?;

    const HEADER_LABEL_WIDTH: usize = 22;

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
    println!(
        "{: <HEADER_LABEL_WIDTH$}: {}",
        "Recovery account",
        ctx.config.get_recovery_account()
    );
    println!("{: <HEADER_LABEL_WIDTH$}: {}\n", "EVM address", ctx.evm_address);
    println!("Recovery account holds seized collateral from failed swaps.\n");

    let asset_ids: Vec<AssetId> = ctx.registry.all().into_iter().map(|(id, _)| id).collect();

    let main_results = ctx.main_service.sync_assets(&asset_ids).await;
    let trader_results = ctx.trader_service.sync_assets(&asset_ids).await;
    let recovery_results = ctx.recovery_service.sync_assets(&asset_ids).await;

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
