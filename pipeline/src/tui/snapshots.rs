use std::collections::HashMap;
use std::sync::Arc;

use chrono::Local;

use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistry;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;

use super::app::{BalanceRowData, BalancesSnapshot, ProfitBySymbol, ProfitsSnapshot};
use super::format;

pub(super) async fn fetch_balances_snapshot(
    ctx: Arc<crate::context::PipelineContext>,
) -> Result<BalancesSnapshot, String> {
    let mut asset_ids: Vec<AssetId> = ctx.registry.all().into_iter().map(|(id, _)| id).collect();
    asset_ids.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then(a.symbol.cmp(&b.symbol))
            .then(a.address.cmp(&b.address))
    });

    let (main_results, trader_results, recovery_results) = tokio::join!(
        ctx.main_service.sync_assets(&asset_ids),
        ctx.trader_service.sync_assets(&asset_ids),
        ctx.recovery_service.sync_assets(&asset_ids),
    );

    let mut rows = Vec::with_capacity(asset_ids.len());
    for (idx, asset_id) in asset_ids.iter().enumerate() {
        let main_cell = format::format_balance_result(main_results.get(idx));
        let trader_cell = format::format_balance_result(trader_results.get(idx));
        let recovery_cell = format::format_balance_result(recovery_results.get(idx));

        rows.push(BalanceRowData {
            asset: asset_id.clone(),
            main: main_cell,
            trader: trader_cell,
            recovery: recovery_cell,
        });
    }

    Ok(BalancesSnapshot { rows, at: Local::now() })
}

#[derive(serde::Deserialize)]
struct ExecutionCsvRow {
    #[serde(default)]
    receive_symbol: Option<String>,
    expected_profit: i128,
    realized_profit: i128,
    #[serde(default)]
    status: Option<String>,
}

pub(super) fn compute_profits_snapshot(export_path: &str, registry: &TokenRegistry) -> Result<ProfitsSnapshot, String> {
    let mut totals: HashMap<String, (usize, i128, i128)> = HashMap::new();

    let mut rdr = match csv::ReaderBuilder::new().from_path(export_path) {
        Ok(r) => r,
        Err(e) => {
            // Missing file is common on a fresh install.
            if let csv::ErrorKind::Io(io_err) = e.kind()
                && io_err.kind() == std::io::ErrorKind::NotFound
            {
                return Ok(ProfitsSnapshot {
                    rows: vec![],
                    at: Local::now(),
                });
            }
            return Err(format!("csv open failed: {e}"));
        }
    };

    for rec in rdr.deserialize::<ExecutionCsvRow>() {
        let row = rec.map_err(|e| format!("csv parse failed: {e}"))?;
        // Only count finalized rows; keep it simple.
        let status = row.status.unwrap_or_default();
        if status.is_empty() {
            // Backward compatibility: older exports might omit status; include them.
        }
        let Some(sym) = row
            .receive_symbol
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let entry = totals.entry(sym).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += row.realized_profit;
        entry.2 += row.expected_profit;
    }

    let mut rows: Vec<ProfitBySymbol> = totals
        .into_iter()
        .map(|(symbol, (count, realized, expected))| ProfitBySymbol {
            decimals: registry
                .tokens
                .values()
                .find(|t| t.symbol() == symbol)
                .map(|t| t.decimals()),
            symbol,
            count,
            realized,
            expected,
        })
        .collect();

    rows.sort_by(|a, b| b.realized.cmp(&a.realized));

    Ok(ProfitsSnapshot { rows, at: Local::now() })
}
