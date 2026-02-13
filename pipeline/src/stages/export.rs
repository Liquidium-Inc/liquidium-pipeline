use std::fs::{OpenOptions, metadata};

use async_trait::async_trait;
use csv::WriterBuilder;
use serde::Serialize;

use crate::error::AppResult;
use crate::{finalizers::liquidation_outcome::LiquidationOutcome, stage::PipelineStage};

pub struct ExportStage {
    pub path: String,
}

#[derive(Serialize)]
struct ExecutionAnalyticsRow {
    status: String,
    swapper: String,
    expected_profit: i128,
    realized_profit: i128,
    liquidation_tx_id: Option<String>,
    swap_tx_id: Option<u64>,
    pay_chain: Option<String>,
    pay_symbol: Option<String>,
    pay_amount: Option<String>,
    receive_chain: Option<String>,
    receive_symbol: Option<String>,
    receive_amount: Option<String>,
    price: Option<f64>,
    slippage: Option<f64>,
    swap_ts: Option<u64>,
}

#[async_trait]
impl<'a> PipelineStage<'a, Vec<LiquidationOutcome>, ()> for ExportStage {
    async fn process(&self, input: &'a Vec<LiquidationOutcome>) -> AppResult<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("File error: {}", e))?;

        let is_empty = metadata(&self.path).map(|m| m.len() == 0).unwrap_or(true);
        let mut wtr = WriterBuilder::new();
        if !is_empty {
            wtr.has_headers(false);
        }

        let mut wtr = wtr.from_writer(file);

        for r in input {
            let row = ExecutionAnalyticsRow {
                status: r.status.description(),
                swapper: r.formatted_swapper(),
                expected_profit: r.request.expected_profit,
                realized_profit: r.realized_profit,
                liquidation_tx_id: r
                    .execution_receipt
                    .liquidation_result
                    .as_ref()
                    .and_then(|lr| lr.collateral_tx.tx_id.clone()),
                swap_tx_id: r.finalizer_result.swap_result.as_ref().map(|s| s.swap_id),
                pay_chain: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.pay_asset.chain.clone()),
                pay_symbol: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.pay_asset.symbol.clone()),
                pay_amount: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.pay_amount.to_string()),
                receive_chain: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.receive_asset.chain.clone()),
                receive_symbol: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.receive_asset.symbol.clone()),
                receive_amount: r
                    .finalizer_result
                    .swap_result
                    .as_ref()
                    .map(|s| s.receive_amount.to_string()),
                price: r.finalizer_result.swap_result.as_ref().map(|s| s.exec_price),
                slippage: r.finalizer_result.swap_result.as_ref().map(|s| s.slippage),
                swap_ts: r.finalizer_result.swap_result.as_ref().map(|s| s.ts),
            };
            wtr.serialize(row).map_err(|e| format!("CSV serialize error: {}", e))?;
        }

        wtr.flush().map_err(|e| format!("CSV flush error: {}", e))?;
        Ok(())
    }
}
