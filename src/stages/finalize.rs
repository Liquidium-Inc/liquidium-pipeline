use crate::{
    executors::executor::ExecutorRequest,
    finalizers::kong_swap::kong_swap_finalizer::KongSwapFinalizer,
    persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs},
    stage::PipelineStage,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{kong_types::SwapReply, swap_interface::IcrcSwapInterface},
    types::protocol_types::LiquidationResult,
};
use async_trait::async_trait;
use num_traits::ToPrimitive;
use std::time::Duration;

const MAX_WAL_ATTEMPTS: i32 = 8;

#[derive(Debug, Clone)]
pub struct LiquidationOutcome {
    pub request: ExecutorRequest,
    pub liquidation_result: LiquidationResult,
    pub swap_result: Option<SwapReply>,
    pub status: ExecutionStatus,
    pub expected_profit: i128,
    pub realized_profit: i128,
}

impl LiquidationOutcome {
    pub fn formatted_debt_repaid(&self) -> String {
        let amount = self.liquidation_result.amounts.debt_repaid.0.to_f64().unwrap()
            / 10f64.powi(self.request.debt_asset.decimals as i32);
        format!("{amount} {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_received_collateral(&self) -> String {
        let amount = self.liquidation_result.amounts.collateral_received.0.to_f64().unwrap()
            / 10f64.powi(self.request.collateral_asset.decimals as i32);
        format!("{amount} {}", self.request.collateral_asset.symbol)
    }

    pub fn formatted_swap_output(&self) -> String {
        if let Some(result) = &self.swap_result {
            let amount =
                result.receive_amount.0.to_f64().unwrap() / 10f64.powi(self.request.debt_asset.decimals as i32);
            return format!("{amount} {}", self.request.debt_asset.symbol);
        }
        format!("0 {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_realized_profit(&self) -> String {
        let amount = self.realized_profit as f64 / 10f64.powi(self.request.debt_asset.decimals as i32);
        format!("{amount} {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_expected_profit(&self) -> String {
        let amount = self.expected_profit as f64 / 10f64.powi(self.request.debt_asset.decimals as i32);
        format!("{amount} {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_profit_delta(&self) -> String {
        let delta = self.realized_profit - self.expected_profit;
        let decimals = self.request.debt_asset.decimals as i32;
        let abs_amount = (delta.abs() as f64) / 10f64.powi(decimals);
        let prefix = if delta >= 0 { "+" } else { "-" };
        format!("{prefix}{:.3}", abs_amount)
    }
}

#[async_trait]
impl<'a, D: WalStore, S: IcrcSwapInterface> PipelineStage<'a, Vec<ExecutionReceipt>, Vec<LiquidationOutcome>>
    for KongSwapFinalizer<D, S>
{
    async fn process(&self, executor_receipts: &'a Vec<ExecutionReceipt>) -> Result<Vec<LiquidationOutcome>, String> {
        let mut outcomes = Vec::with_capacity(executor_receipts.len());

        for (i, receipt) in executor_receipts.iter().enumerate() {
            let exec_status = receipt.status.clone();

            let liq_id = format!("liq:{}:{}", i, now_secs());

            // Persist to WAL first
            self.persist_receipt_to_wal(&liq_id, i as i32, 0, receipt).await?;

            // If eligible, execute swap with retries
            let mut effective_status = exec_status.clone();
            let mut swap_result: Option<SwapReply> = None;
            if Self::needs_swap(receipt) && matches!(effective_status, ExecutionStatus::Success) {
                match self.execute_swap_with_retries(0, receipt).await {
                    Ok(Some(sr)) => {
                        swap_result = Some(sr);
                        // Remove WAL entry on success
                        if let Err(e) = self.db.delete(&liq_id, i as i32).await {
                            eprintln!("Failed to delete WAL entry after successful swap: {}", e);
                        }
                    }
                    Ok(None) => {
                        if let Err(e) = self.db.delete(&liq_id, i as i32).await {
                            eprintln!("Failed to delete WAL entry after successful swap: {}", e);
                        }
                    }
                    Err(status_override) => {
                        effective_status = status_override;
                        // Decide next status based on attempt count
                        let next_status = match self.db.get_result(&liq_id, i as i32).await {
                            Ok(Some(r)) if r.attempt + 1 >= MAX_WAL_ATTEMPTS => ResultStatus::FailedPermanent,
                            _ => ResultStatus::FailedRetryable,
                        };
                        if let Err(e) = self.db.update_status(&liq_id, i as i32, next_status, true).await {
                            eprintln!("Failed to update WAL after swap failure: {}", e);
                        }
                    }
                }
            } else if matches!(effective_status, ExecutionStatus::Success) {
                // No swap needed; clean up WAL row eagerly
                if let Err(e) = self.db.delete(&liq_id, i as i32).await {
                    eprintln!("Failed to delete WAL entry (no-swap case): {}", e);
                }
            }

            // If there's no liquidation result, skip producing an outcome.
            let Some(liq_res) = receipt.liquidation_result.clone() else {
                continue;
            };

            let realized_profit = match (&effective_status, &swap_result) {
                (ExecutionStatus::Success, Some(sr)) => sr.receive_amount.0.to_i128().unwrap(),
                _ => 0,
            };

            outcomes.push(LiquidationOutcome {
                request: receipt.request.clone(),
                liquidation_result: liq_res,
                swap_result,
                status: effective_status,
                expected_profit: receipt.request.expected_profit,
                realized_profit,
            });
        }

        // Finalize: retry any swap rows left as FailedRetryable in WAL and append their outcomes
        let mut retried = self.retry_failed_swaps(None).await?;
        outcomes.append(&mut retried);

        Ok(outcomes)
    }
}

impl<D: WalStore, S: IcrcSwapInterface> KongSwapFinalizer<D, S> {
    async fn persist_receipt_to_wal(
        &self,
        liq_id: &str,
        idx: i32,
        attempt: i32,
        receipt: &ExecutionReceipt,
    ) -> Result<(), String> {
        let exec_status = receipt.status.clone();
        let now = now_secs();
        let meta_json = serde_json::to_string(&serde_json::json!({
            "v": 1,
            "status": exec_status.description(),
            "receipt": receipt,
        }))
        .map_err(|e| format!("serialize receipt for wal failed: {}", e))?;
        let status = match exec_status {
            ExecutionStatus::Success => ResultStatus::Succeeded,
            ExecutionStatus::LiquidationCallFailed(_) => ResultStatus::FailedPermanent,
            ExecutionStatus::FailedLiquidation(_) => ResultStatus::FailedPermanent,
            ExecutionStatus::CollateralTransferFailed(_) => ResultStatus::FailedRetryable,
            ExecutionStatus::ChangeTransferFailed(_) => ResultStatus::FailedRetryable,
            ExecutionStatus::SwapFailed(_) => ResultStatus::FailedRetryable,
        };
        let wal_row = LiqResultRecord {
            liq_id: liq_id.to_string(),
            idx,
            status,
            attempt,
            created_at: now,
            updated_at: now,
            meta_json,
        };
        self.db
            .upsert_result(wal_row)
            .await
            .map_err(|e| format!("wal upsert failed: {}", e))?;
        Ok(())
    }
}

impl<D: WalStore, S: IcrcSwapInterface> KongSwapFinalizer<D, S> {
    fn needs_swap(receipt: &ExecutionReceipt) -> bool {
        receipt.request.expected_profit > 0
            && receipt.request.swap_args.is_some()
            && receipt.request.debt_asset != receipt.request.collateral_asset
            && receipt.liquidation_result.is_some()
    }

    async fn execute_swap_with_retries(
        &self,
        mut attempt: i32,
        receipt: &ExecutionReceipt,
    ) -> Result<Option<SwapReply>, ExecutionStatus> {
        let liq = match &receipt.liquidation_result {
            Some(l) => l.clone(),
            None => return Ok(None), // no result, nothing to swap
        };

        // Prepare swap args from receipt
        let mut swap_args = match receipt.request.swap_args.clone() {
            Some(sa) => sa,
            None => return Ok(None),
        };
        swap_args.pay_amount = liq.amounts.collateral_received.clone();

        let max_retries = 3;
        loop {
            match self.swapper.swap(swap_args.clone()).await {
                Ok(res) => {
                    return Ok(Some(res));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        return Err(ExecutionStatus::SwapFailed(format!(
                            "Swap failed after {} attempts: {}",
                            max_retries, e
                        )));
                    }

                    // Increase slippage step; default 0.5, cap 5.0
                    let prev = swap_args.max_slippage.unwrap_or(0.5);
                    let next = (prev + 0.25).min(5.0);
                    swap_args.max_slippage = Some(next);

                    // Reduce pay amount by collateral fee; abort if not enough
                    if swap_args.pay_amount > receipt.request.collateral_asset.fee.clone() {
                        swap_args.pay_amount -= receipt.request.collateral_asset.fee.clone();
                    } else {
                        return Err(ExecutionStatus::SwapFailed(
                            "Swap retry aborted: insufficient pay amount".to_string(),
                        ));
                    }

                    // Exponential backoff
                    tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt as u32))).await;
                }
            }
        }
    }
}

impl<D: WalStore, S: IcrcSwapInterface> KongSwapFinalizer<D, S> {
    pub async fn retry_failed_swaps(&self, max: Option<usize>) -> Result<Vec<LiquidationOutcome>, String> {
        let limit: usize = max.unwrap_or(100);

        let rows = self
            .db
            .list_by_status(ResultStatus::FailedRetryable, limit)
            .await
            .map_err(|e| format!("wal list_by_status failed: {}", e))?;

        let mut outcomes = Vec::with_capacity(rows.len());

        for row in rows {
            // Hard cap attempts at MAX_WAL_ATTEMPTS
            if row.attempt >= MAX_WAL_ATTEMPTS {
                if let Err(e) = self
                    .db
                    .update_status(&row.liq_id, row.idx, ResultStatus::FailedPermanent, false)
                    .await
                {
                    eprintln!(
                        "Failed to mark WAL entry as permanent {}[{}]: {}",
                        row.liq_id, row.idx, e
                    );
                }
                continue;
            }

            let receipt: ExecutionReceipt = match serde_json::from_str::<serde_json::Value>(&row.meta_json)
                .ok()
                .and_then(|v| serde_json::from_value(v.get("receipt").cloned().unwrap_or(serde_json::Value::Null)).ok())
            {
                Some(r) => r,
                None => {
                    eprintln!(
                        "Failed to deserialize receipt from WAL meta for {}[{}]. Skipping.",
                        row.liq_id, row.idx
                    );
                    continue;
                }
            };

            if !Self::needs_swap(&receipt) {
                if let Err(e) = self.db.delete(&row.liq_id, row.idx).await {
                    eprintln!(
                        "Failed to delete WAL entry for {}[{}] (no longer needs swap): {}",
                        row.liq_id, row.idx, e
                    );
                }
                continue;
            }

            match self.execute_swap_with_retries(row.attempt, &receipt).await {
                Ok(Some(sr)) => {
                    if let Err(e) = self.db.delete(&row.liq_id, row.idx).await {
                        eprintln!(
                            "Failed to delete WAL entry after successful retry {}[{}]: {}",
                            row.liq_id, row.idx, e
                        );
                    }

                    if let Some(liq_res) = receipt.liquidation_result.clone() {
                        outcomes.push(LiquidationOutcome {
                            request: receipt.request.clone(),
                            liquidation_result: liq_res,
                            swap_result: Some(sr.clone()),
                            status: ExecutionStatus::Success,
                            expected_profit: receipt.request.expected_profit,
                            realized_profit: sr.receive_amount.0.to_i128().unwrap(),
                        });
                    }
                }
                Ok(None) => {
                    if let Err(e) = self.db.delete(&row.liq_id, row.idx).await {
                        eprintln!(
                            "Failed to delete WAL entry after no-op retry {}[{}]: {}",
                            row.liq_id, row.idx, e
                        );
                    }
                }
                Err(status_override) => {
                    let next_status = if row.attempt + 1 >= MAX_WAL_ATTEMPTS {
                        ResultStatus::FailedPermanent
                    } else {
                        ResultStatus::FailedRetryable
                    };
                    if let Err(e) = self.db.update_status(&row.liq_id, row.idx, next_status, true).await {
                        eprintln!(
                            "Failed to update WAL after failed retry {}[{}]: {}",
                            row.liq_id, row.idx, e
                        );
                    }

                    if let Some(liq_res) = receipt.liquidation_result.clone() {
                        outcomes.push(LiquidationOutcome {
                            request: receipt.request.clone(),
                            liquidation_result: liq_res,
                            swap_result: None,
                            status: status_override,
                            expected_profit: receipt.request.expected_profit,
                            realized_profit: 0,
                        });
                    }
                }
            }
        }

        Ok(outcomes)
    }
}
