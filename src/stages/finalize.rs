use crate::{
    account::account::{IcrcAccountActions, RECOVERY_ACCOUNT},
    config::ConfigTrait,
    executors::executor::ExecutorRequest,
    finalizers::kong_swap::kong_swap_finalizer::KongSwapFinalizer,
    icrc_token::icrc_token_amount::IcrcTokenAmount,
    persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs},
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        kong_types::{SwapAmountsReply, SwapReply},
        swap_interface::IcrcSwapInterface,
    },
    types::protocol_types::{LiquidationResult, TransferStatus},
};
use async_trait::async_trait;
use candid::Encode;
use icrc_ledger_types::icrc1::account::Account;
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
impl<'a, D: WalStore, S: IcrcSwapInterface, A: IcrcAccountActions, C: ConfigTrait, P: PipelineAgent>
    PipelineStage<'a, Vec<ExecutionReceipt>, Vec<LiquidationOutcome>> for KongSwapFinalizer<D, S, A, C, P>
{
    async fn process(&self, executor_receipts: &'a Vec<ExecutionReceipt>) -> Result<Vec<LiquidationOutcome>, String> {
        let mut outcomes = Vec::with_capacity(executor_receipts.len());

        for (i, receipt) in executor_receipts.iter().enumerate() {
            let exec_status = receipt.status.clone();

            let liq_id = format!("liq:{}:{}", i, now_secs());

            // Persist to WAL first
            self.persist_receipt_to_wal(&liq_id, i as i32, 0, receipt).await?;

            let mut effective_status = exec_status.clone();
            let mut swap_result: Option<SwapReply> = None;

            // Swap only if still Success and we have a fresh liquidation
            if matches!(effective_status, ExecutionStatus::Success) && Self::needs_swap(receipt) {
                match self.execute_swap_with_retries(receipt).await {
                    Ok(Some(sr)) => {
                        swap_result = Some(sr);
                        self.delete_wal_silent(&liq_id, i as i32).await;
                    }
                    Ok(None) => {
                        self.delete_wal_silent(&liq_id, i as i32).await;
                    }
                    Err(status_override) => {
                        effective_status = status_override;
                        let attempt = self
                            .db
                            .get_result(&liq_id, i as i32)
                            .await
                            .ok()
                            .flatten()
                            .map(|r| r.attempt)
                            .unwrap_or(0);
                        let next = Self::next_retry_status(attempt);
                        if let Err(e) = self.db.update_status(&liq_id, i as i32, next, true).await {
                            eprintln!("Failed to update WAL after swap failure: {}", e);
                        }
                    }
                }
            } else if matches!(effective_status, ExecutionStatus::Success) {
                // Success but no swap needed -> remove now
                self.delete_wal_silent(&liq_id, i as i32).await;
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

impl<D: WalStore, S: IcrcSwapInterface, A: IcrcAccountActions, C: ConfigTrait, P: PipelineAgent>
    KongSwapFinalizer<D, S, A, C, P>
{
    // Helper: extract ExecutionReceipt from a WAL row's meta_json
    fn receipt_from_meta(meta_json: &str) -> Option<ExecutionReceipt> {
        serde_json::from_str::<serde_json::Value>(meta_json).ok().and_then(|v| {
            serde_json::from_value::<ExecutionReceipt>(v.get("receipt").cloned().unwrap_or(serde_json::Value::Null))
                .ok()
        })
    }

    fn receipt_from_row(row: &LiqResultRecord) -> Option<ExecutionReceipt> {
        Self::receipt_from_meta(&row.meta_json)
    }


    async fn delete_wal_silent(&self, liq_id: &str, idx: i32) {
        if let Err(e) = self.db.delete(liq_id, idx).await {
            eprintln!("Failed to delete WAL entry {}[{}]: {}", liq_id, idx, e);
        }
    }

    #[inline]
    fn next_retry_status(current_attempt: i32) -> ResultStatus {
        if current_attempt + 1 >= MAX_WAL_ATTEMPTS {
            ResultStatus::FailedPermanent
        } else {
            ResultStatus::FailedRetryable
        }
    }

    // Build swap args from receipt and liquidation, then fetch a fresh quote.
    // Returns SwapInfo if quoting succeeds; Err(String) otherwise.
    async fn preflight_quote(
        &self,
        receipt: &ExecutionReceipt,
        liq: &LiquidationResult,
    ) -> Result<SwapAmountsReply, String> {
        self.swapper
            .get_swap_info(
                &receipt.request.collateral_asset,
                &receipt.request.debt_asset,
                &IcrcTokenAmount {
                    token: receipt.request.collateral_asset.clone(),
                    value: liq.amounts.collateral_received.clone(),
                },
            )
            .await
            .map_err(|e| e.to_string())
    }

    // Preflight quote and check against expected profit. Returns the quote if acceptable,
    // or Err(ResultStatus) indicating the next WAL status to set when unprofitable/unavailable.
    async fn preflight_quote_checked(
        &self,
        receipt: &ExecutionReceipt,
        liq: &LiquidationResult,
        attempt: i32,
    ) -> Result<SwapAmountsReply, ResultStatus> {
        match self.preflight_quote(receipt, liq).await {
            Ok(info) => {
                let quoted = info.receive_amount.0.to_i128().unwrap_or(0);
                if quoted < receipt.request.expected_profit {
                    Err(Self::next_retry_status(attempt))
                } else {
                    Ok(info)
                }
            }
            Err(_err) => Err(Self::next_retry_status(attempt)),
        }
    }

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

    // Query the canister for a liquidation by id using the pipeline agent.
    pub async fn get_liquidation(&self, liquidation_id: u128) -> Result<LiquidationResult, String> {
        let args = Encode!(&liquidation_id).map_err(|e| e.to_string())?;

        // Call canister get_liquidation
        match self
            .agent
            .call_query::<Result<LiquidationResult, String>>(
                &self.config.get_lending_canister(),
                "get_liquidation",
                args,
            )
            .await
        {
            Ok(v) => v,
            Err(err) => {
                eprintln!("get_liquidation call failed: {}", err);
                Err(err)
            }
        }
    }
}

impl<D: WalStore, S: IcrcSwapInterface, A: IcrcAccountActions, C: ConfigTrait, P: PipelineAgent>
    KongSwapFinalizer<D, S, A, C, P>
{
    fn needs_swap(receipt: &ExecutionReceipt) -> bool {
        receipt.request.expected_profit > 0
            && receipt.request.swap_args.is_some()
            && receipt.request.debt_asset != receipt.request.collateral_asset
            && receipt.liquidation_result.is_some()
    }

    async fn execute_swap_with_retries(
        &self,
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

        let mut attempt = 0;
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
                    tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempt as u32))).await;
                }
            }
        }
    }
}

impl<D: WalStore, S: IcrcSwapInterface, A: IcrcAccountActions, C: ConfigTrait, P: PipelineAgent>
    KongSwapFinalizer<D, S, A, C, P>
{
    async fn move_funds_to_recovery(&self, receipt: &ExecutionReceipt) {
        // Move seized collateral to recovery account when we cannot complete the swap
        let Some(liq) = &receipt.liquidation_result else {
            return;
        };

        // Destination account is configured on the finalizer (assumed present)
        let trasfer = self.account.transfer(
            IcrcTokenAmount {
                token: receipt.request.collateral_asset.clone(),
                value: liq.amounts.collateral_received.clone(),
            },
            Account {
                owner: self.config.get_liquidator_principal(),
                subaccount: Some(*RECOVERY_ACCOUNT),
            },
        );

        if let Err(e) = trasfer.await {
            eprintln!("Recovery transfer failed: {}", e);
        }
    }

    pub async fn retry_failed_swaps(&self, max: Option<usize>) -> Result<Vec<LiquidationOutcome>, String> {
        let limit: usize = max.unwrap_or(100);

        let rows = self
            .db
            .list_by_status(ResultStatus::FailedRetryable, limit)
            .await
            .map_err(|e| format!("wal list_by_status failed: {}", e))?;

        let mut outcomes = Vec::with_capacity(rows.len());

        for row in rows {
            // Load stored receipt
            let mut receipt: ExecutionReceipt = match Self::receipt_from_row(&row) {
                Some(r) => r,
                None => {
                    eprintln!(
                        "Failed to deserialize receipt from WAL meta for {}[{}]. Skipping.",
                        row.liq_id, row.idx
                    );
                    continue;
                }
            };

            // Skip rows that no longer need swap
            if !Self::needs_swap(&receipt) {
                self.delete_wal_silent(&row.liq_id, row.idx).await;
                continue;
            }

            // Ensure we have a liquidation id recorded in the receipt
            let Some(id) = receipt.liquidation_result.as_ref().and_then(|l| l.id) else {
                self.delete_wal_silent(&row.liq_id, row.idx).await;
                continue;
            };

            // Fetch latest liquidation from canister
            let liq_from_chain = match self.get_liquidation(id).await {
                Ok(liq) => liq,
                Err(err) => {
                    let next = Self::next_retry_status(row.attempt);
                    if let Err(e) = self.db.update_status(&row.liq_id, row.idx, next, true).await {
                        eprintln!("WAL update failed {}[{}]: {}", row.liq_id, row.idx, e);
                    }
                    eprintln!("get_liquidation failed for {}[{}]: {}", row.liq_id, row.idx, err);
                    continue;
                }
            };

            // Do not swap until collateral transfer is confirmed
            if matches!(
                liq_from_chain.collateral_tx.status,
                TransferStatus::Failed(_) | TransferStatus::Pending
            ) {
                continue;
            }

            // Use refreshed liquidation for swap
            receipt.liquidation_result = Some(liq_from_chain.clone());

            // Preflight: re-quote and check profitability in one step
            match self
                .preflight_quote_checked(&receipt, &liq_from_chain, row.attempt)
                .await
            {
                Ok(_) => {}
                Err(next) => {
                    if let Err(e) = self.db.update_status(&row.liq_id, row.idx, next, true).await {
                        eprintln!("WAL update failed {}[{}]: {}", row.liq_id, row.idx, e);
                    }
                    if matches!(next, ResultStatus::FailedPermanent) {
                        self.move_funds_to_recovery(&receipt).await;
                    }
                    continue;
                }
            };

            match self.execute_swap_with_retries(&receipt).await {
                Ok(Some(sr)) => {
                    self.delete_wal_silent(&row.liq_id, row.idx).await;
                    outcomes.push(LiquidationOutcome {
                        request: receipt.request.clone(),
                        liquidation_result: liq_from_chain.clone(),
                        swap_result: Some(sr.clone()),
                        status: ExecutionStatus::Success,
                        expected_profit: receipt.request.expected_profit,
                        realized_profit: sr.receive_amount.0.to_i128().unwrap(),
                    });
                }
                Ok(None) => {
                    self.delete_wal_silent(&row.liq_id, row.idx).await;
                }
                Err(status_override) => {
                    let next = Self::next_retry_status(row.attempt);
                    if let Err(e) = self.db.update_status(&row.liq_id, row.idx, next, true).await {
                        eprintln!("WAL update failed {}[{}]: {}", row.liq_id, row.idx, e);
                    }
                    if matches!(next, ResultStatus::FailedPermanent) {
                        self.move_funds_to_recovery(&receipt).await;
                    }
                    outcomes.push(LiquidationOutcome {
                        request: receipt.request.clone(),
                        liquidation_result: liq_from_chain.clone(),
                        swap_result: None,
                        status: status_override,
                        expected_profit: receipt.request.expected_profit,
                        realized_profit: 0,
                    });
                }
            }
        }

        Ok(outcomes)
    }
}
