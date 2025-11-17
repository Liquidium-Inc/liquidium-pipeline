use crate::{
    config::ConfigTrait,
    executors::executor::ExecutorRequest,
    finalizers::kong_swap::kong_swap_finalizer::KongSwapFinalizer,
    persistance::{now_secs, LiqResultRecord, ResultStatus, WalStore},
    stage::PipelineStage,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        model::{SwapExecution, SwapQuote},
        swap_interface::SwapInterface,
    },
};
use async_trait::async_trait;
use candid::Encode;
use liquidium_pipeline_core::account::actions::AccountActions;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, TransferStatus};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use num_traits::ToPrimitive;
use std::time::Duration;

const MAX_WAL_ATTEMPTS: i32 = 8;

#[derive(Debug, Clone)]
pub struct LiquidationOutcome {
    pub request: ExecutorRequest,
    pub liquidation_result: LiquidationResult,
    pub swap_result: Option<SwapExecution>,
    pub status: ExecutionStatus,
    pub expected_profit: i128,
    pub realized_profit: i128,
}

impl LiquidationOutcome {
    pub fn formatted_debt_repaid(&self) -> String {
        let amount = self.liquidation_result.amounts.debt_repaid.0.to_f64().unwrap()
            / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_received_collateral(&self) -> String {
        let amount = self.liquidation_result.amounts.collateral_received.0.to_f64().unwrap()
            / 10f64.powi(self.request.collateral_asset.decimals() as i32);
        format!("{amount} {}", self.request.collateral_asset.symbol())
    }

    pub fn formatted_swap_output(&self) -> String {
        if let Some(result) = &self.swap_result {
            let amount =
                result.receive_amount.0.to_f64().unwrap() / 10f64.powi(self.request.debt_asset.decimals() as i32);
            return format!("{amount} {}", self.request.debt_asset.symbol());
        }
        format!("0 {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_realized_profit(&self) -> String {
        let amount = self.realized_profit as f64 / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_expected_profit(&self) -> String {
        let amount = self.expected_profit as f64 / 10f64.powi(self.request.debt_asset.decimals() as i32);
        format!("{amount} {}", self.request.debt_asset.symbol())
    }

    pub fn formatted_profit_delta(&self) -> String {
        let delta = self.realized_profit - self.expected_profit;
        let decimals = self.request.debt_asset.decimals() as i32;
        let abs_amount = (delta.abs() as f64) / 10f64.powi(decimals);
        let prefix = if delta >= 0 { "+" } else { "-" };
        format!("{prefix}{:.3}", abs_amount)
    }
}

#[async_trait]
impl<'a, D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent>
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
            let mut swap_result: Option<SwapExecution> = None;

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

impl<D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent>
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
    async fn preflight_quote(&self, _receipt: &ExecutionReceipt, _liq: &LiquidationResult) -> Result<SwapQuote, String> {
        todo!()
    }

    // Preflight quote and check against expected profit. Returns the quote if acceptable,
    // or Err(ResultStatus) indicating the next WAL status to set when unprofitable/unavailable.
    async fn preflight_quote_checked(
        &self,
        receipt: &ExecutionReceipt,
        liq: &LiquidationResult,
        attempt: i32,
    ) -> Result<SwapQuote, ResultStatus> {
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

impl<D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent>
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
    ) -> Result<Option<SwapExecution>, ExecutionStatus> {
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
            match self.swapper.execute(&swap_args).await {
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
                    let prev = swap_args.max_slippage_bps.unwrap_or(50);
                    let next = (prev + 25).min(500);
                    swap_args.max_slippage_bps = Some(next);

                    let fee = 0u8; // TODO: get icrc fee
                    // Reduce pay amount by collateral fee; abort if not enough
                    if swap_args.pay_amount > fee {
                        swap_args.pay_amount -= fee;
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

impl<D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent>
    KongSwapFinalizer<D, S, A, C, P>
{
    async fn move_funds_to_recovery(&self, receipt: &ExecutionReceipt) {
        // Move seized collateral to recovery account when we cannot complete the swap
        let Some(_liq) = &receipt.liquidation_result else {
            return;
        };

        // // Destination account is configured on the finalizer (assumed present)
        // let trasfer = self.account.transfer(
        //     IcrcTokenAmount {
        //         token: receipt.request.collateral_asset.clone(),
        //         value: liq.amounts.collateral_received.clone(),
        //     },
        //     Account {
        //         owner: self.config.get_liquidator_principal(),
        //         subaccount: Some(*RECOVERY_ACCOUNT),
        //     },
        // );

        todo!();
        // if let Err(e) = trasfer.await {
        //     eprintln!("Recovery transfer failed: {}", e);
        // }
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
            let Some(id) = receipt.liquidation_result.as_ref().map(|l| l.id) else {
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::MockConfigTrait;
//     use crate::persistance::MockWalStore;
//     use crate::pipeline_agent::MockPipelineAgent;

//     use candid::{Nat, Principal};
//     use mockall::predicate::*;
//     use std::sync::Arc;

//     // ------------------------
//     // Test helpers
//     // ------------------------
//     // Minimal constructors for tokens and receipts used across tests.
//     // These helpers intentionally avoid touching production logic.

//     fn tok(decimals: u8, symbol: &str) -> IcrcToken {
//         IcrcToken {
//             ledger: candid::Principal::anonymous(),
//             decimals,
//             name: symbol.to_string(),
//             symbol: symbol.to_string(),
//             fee: 0_u128.into(),
//         }
//     }

//     #[tokio::test]
//     async fn slippage_increases_across_retries() {
//         // Verify max_slippage increments 0.5 -> 0.75 -> 1.0 across failures.
//         let rec = receipt_ok_with_swap(100);

//         // WAL: upsert ok, no delete (process will fail)
//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(()));
//         wal.expect_get_result().returning(|_, _| {
//             Ok(Some(LiqResultRecord {
//                 liq_id: "x".into(),
//                 idx: 0,
//                 status: ResultStatus::FailedRetryable,
//                 attempt: 0,
//                 created_at: 0,
//                 updated_at: 0,
//                 meta_json: "{}".into(),
//             }))
//         });
//         wal.expect_update_status().returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         // Swapper: three calls; first two fail, third fails too to produce SwapFailed
//         use mockall::Sequence;
//         let mut seq = Sequence::new();
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         // 1st attempt: expect slippage ~0.5
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert_eq!(args.max_slippage.unwrap_or(0.5), 0.5);
//             Err("fail1".into())
//         });
//         // 2nd attempt: expect slippage bumped to 0.75
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert!((args.max_slippage.unwrap() - 0.75).abs() < 1e-9);
//             Err("fail2".into())
//         });
//         // 3rd attempt: expect slippage bumped to 1.0, then still fail -> SwapFailed
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert!((args.max_slippage.unwrap() - 1.0).abs() < 1e-9);
//             Err("fail3".into())
//         });

//         // Config/Agent/Account
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::SwapFailed(_)));
//     }

//     #[tokio::test]
//     async fn insufficient_pay_amount_aborts() {
//         // When pay_amount <= fee on retry, we abort with SwapFailed.
//         let mut rec = receipt_ok_with_swap(100);
//         // Make collateral fee large so first retry immediately aborts
//         rec.request.collateral_asset.fee = Nat::from(10u128);
//         // And set small collateral_received
//         if let Some(liq) = rec.liquidation_result.as_mut() {
//             liq.amounts.collateral_received = Nat::from(10u128);
//         }

//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(()));
//         wal.expect_get_result().returning(|_, _| {
//             Ok(Some(LiqResultRecord {
//                 liq_id: "x".into(),
//                 idx: 0,
//                 status: ResultStatus::FailedRetryable,
//                 attempt: 0,
//                 created_at: 0,
//                 updated_at: 0,
//                 meta_json: "{}".into(),
//             }))
//         });
//         wal.expect_update_status().returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         // First attempt fails; after which pay_amount (10) <= fee (10) -> aborts without second call
//         swap.expect_swap().returning(|args| {
//             assert_eq!(args.pay_amount, Nat::from(10u128));
//             Err("boom".into())
//         });
//         // No more swap attempts

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::SwapFailed(_)));
//     }

//     #[tokio::test]
//     async fn eventual_success_after_n_failures() {
//         // Fail twice then succeed.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(()));
//         wal.expect_delete().returning(|_, _| Ok(())).times(1); // deleted on success
//         wal.expect_get_result().times(0);
//         wal.expect_update_status().times(0);
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         use mockall::Sequence;
//         let mut seq = Sequence::new();
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         // 1st fail @ 0.5
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert_eq!(args.max_slippage.unwrap_or(0.5), 0.5);
//             Err("e1".into())
//         });
//         // 2nd fail @ 0.75
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert!((args.max_slippage.unwrap() - 0.75).abs() < 1e-9);
//             Err("e2".into())
//         });
//         // 3rd success @ 1.0
//         swap.expect_swap().times(1).in_sequence(&mut seq).returning(|args| {
//             assert!((args.max_slippage.unwrap() - 1.0).abs() < 1e-9);
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(777u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap(),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::Success));
//         assert_eq!(out[0].swap_result.as_ref().unwrap().receive_amount, Nat::from(777u128));
//     }

//     // Build a successful ExecutionReceipt with swap args and a default on-chain
//     // liquidation result. The exact amounts are small and deterministic for tests.
//     fn receipt_ok_with_swap(expected_profit: i128) -> ExecutionReceipt {
//         let debt = tok(8, "DEBT");
//         let coll = tok(8, "COLL");
//         let liq_req = crate::types::protocol_types::LiquidationRequest {
//             borrower: Principal::anonymous(),
//             debt_pool_id: Principal::anonymous(),
//             collateral_pool_id: Principal::anonymous(),
//             debt_amount: Nat::from(1000_u128),
//             receiver_address: Principal::anonymous(),
//         };
//         ExecutionReceipt {
//             request: ExecutorRequest {
//                 liquidation: liq_req,
//                 debt_asset: debt.clone(),
//                 collateral_asset: coll.clone(),
//                 expected_profit,
//                 swap_args: Some(crate::swappers::kong_types::SwapArgs {
//                     pay_token: coll.name.clone(),
//                     receive_token: debt.name.clone(),
//                     pay_amount: 0_u128.into(),
//                     max_slippage: Some(0.5),
//                     ..Default::default()
//                 }),
//             },
//             liquidation_result: Some(LiquidationResult {
//                 id: 42,
//                 amounts: crate::types::protocol_types::LiquidationAmounts {
//                     debt_repaid: Nat::from(1000_u128),
//                     collateral_received: Nat::from(2000_u128),
//                 },
//                 collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                 debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                 status: crate::types::protocol_types::LiquidationStatus::Success,
//                 change_tx: crate::types::protocol_types::TxStatus {
//                     tx_id: None,
//                     status: crate::types::protocol_types::TransferStatus::Success,
//                 },
//                 collateral_tx: crate::types::protocol_types::TxStatus {
//                     tx_id: None,
//                     status: crate::types::protocol_types::TransferStatus::Success,
//                 },
//             }),
//             status: ExecutionStatus::Success,
//             change_received: false,
//         }
//     }

//     // Create a WAL row snapshot that embeds the serialized receipt.
//     // This mirrors the shape produced by persist_receipt_to_wal, but keeps timestamps at 0.
//     fn wal_row_for_receipt(
//         liq_id: &str,
//         idx: i32,
//         status: ResultStatus,
//         attempt: i32,
//         receipt: &ExecutionReceipt,
//     ) -> LiqResultRecord {
//         LiqResultRecord {
//             liq_id: liq_id.to_string(),
//             idx,
//             status,
//             attempt,
//             created_at: 0,
//             updated_at: 0,
//             meta_json: serde_json::to_string(&serde_json::json!({
//                 "v": 1,
//                 "status": receipt.status.description(),
//                 "receipt": receipt,
//             }))
//             .unwrap(),
//         }
//     }

//     #[tokio::test]
//     async fn process_happy_path_swaps_and_deletes_wal() {
//         // Arrange: make WAL a no-op (insert/delete/list are fine)
//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(()));
//         wal.expect_delete().returning(|_, _| Ok(()));
//         wal.expect_get_result().returning(|_, _| Ok(None));
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         // Quote is profitable; swap succeeds
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|args| {
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(123u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap_or(0.5),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });

//         // Config/Agent/Account not relevant for happy path
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         // Act
//         let rec = receipt_ok_with_swap(100);
//         let out = fz.process(&vec![rec]).await.unwrap();
//         // Assert
//         assert_eq!(out.len(), 1);
//         assert!(out[0].swap_result.is_some());
//     }

//     #[tokio::test]
//     async fn retry_marks_unprofitable_quote_and_no_swap() {
//         // If the refreshed quote is below expected_profit, we back off and do not swap.
//         let rec = receipt_ok_with_swap(5_000);

//         let mut wal = MockWalStore::new();
//         // Provide one retryable WAL row
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:unprofitable", 0, status, 0, &rec)])
//         });
//         // Expect attempt bump
//         wal.expect_update_status()
//             .with(eq("liq:unprofitable"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         // No deletion since row remains retryable
//         wal.expect_delete().times(0);

//         // Liquidation exists and collateral is confirmed
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 42,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         // Low quote triggers backoff
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(100u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         // Swap should not be called
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         // No outcomes produced when backing off
//         let out = fz.retry_failed_swaps(Some(10)).await.unwrap();
//         assert!(out.is_empty(), "no outcomes should be produced on backoff");
//     }

//     #[tokio::test]
//     async fn process_sets_retryable_on_swap_error() {
//         // Swap fails during process(); we mark retryable and produce a failed outcome.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         // Persist, read attempt=0, then bump
//         wal.expect_upsert_result().returning(|_| Ok(()));
//         wal.expect_get_result().returning(|liq_id, idx| {
//             Ok(Some(LiqResultRecord {
//                 liq_id: liq_id.to_string(),
//                 idx,
//                 status: ResultStatus::FailedRetryable,
//                 attempt: 0,
//                 created_at: 0,
//                 updated_at: 0,
//                 meta_json: "{}".into(),
//             }))
//         });
//         wal.expect_update_status()
//             .with(always(), always(), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);
//         // No retry pass here
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         // Quote OK, but swap returns error
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|_| Err("boom".into()));

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         // We report SwapFailed and no swap_result
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::SwapFailed(_)));
//         assert!(out[0].swap_result.is_none());
//     }

//     #[tokio::test]
//     async fn retry_succeeds_after_profitable_quote() {
//         // Retry path: good quote + swap success -> row deleted and outcome success.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         // One retryable row; expect delete after success
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:good", 0, status, 1, &rec)])
//         });
//         wal.expect_delete().with(eq("liq:good"), eq(0)).returning(|_, _| Ok(()));
//         wal.expect_update_status().times(0);

//         // Collateral confirmed
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 42,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         // Profitable quote and successful swap
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(5000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|args| {
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(123u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap_or(0.5),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         // Outcome success with realized_profit == 123
//         let out = fz.retry_failed_swaps(Some(10)).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::Success));
//         assert!(out[0].swap_result.is_some());
//         assert_eq!(out[0].realized_profit, 123);
//     }

//     #[tokio::test]
//     async fn retry_skips_when_collateral_pending() {
//         // Edge case: collateral transfer is still pending -> we should not swap.
//         let rec = receipt_ok_with_swap(100);

//         // WAL returns one retryable row
//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:pending", 0, status, 0, &rec)])
//         });
//         // No status update or delete in current behavior
//         wal.expect_update_status().times(0);
//         wal.expect_delete().times(0);

//         // Agent says collateral is PENDING
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 42,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Pending,
//                     },
//                 }))
//             });

//         // Swapper must not be invoked
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         // Config + Account (unused)
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         // Should produce no outcomes and make no WAL changes
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn retry_backs_off_when_get_liquidation_errors() {
//         // Edge case: network/transient error fetching liquidation -> back off.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:neterr", 0, status, 0, &rec)])
//         });
//         // Expect status bump on error
//         wal.expect_update_status()
//             .with(eq("liq:neterr"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);

//         // Agent returns an error
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| Err("network".into()));

//         // Swapper must not be invoked
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn retry_moves_to_recovery_on_permanent_failure() {
//         // Edge case: attempts exhausted and quote remains unprofitable -> mark permanent and move funds to recovery.
//         let rec = receipt_ok_with_swap(10_000);
//         let last_attempt = (super::MAX_WAL_ATTEMPTS - 1).max(0);

//         // WAL row at max-1 attempts
//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:perm", 0, status, last_attempt, &rec)])
//         });
//         // Expect status set to FailedPermanent (bump attempt = true)
//         wal.expect_update_status()
//             .with(eq("liq:perm"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);

//         // Agent returns confirmed collateral
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 42,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         // Preflight quote is unprofitable -> triggers permanent failure at max-1
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(1u128), // far below expected_profit
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().times(0);

//         // Expect a recovery transfer when it becomes permanent
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into())).times(1);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };

//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn persist_receipt_writes_expected_status() {
//         use ExecutionStatus as ES;
//         use ResultStatus as RS;
//         let check = |status: ExecutionStatus, expected: RS| async move {
//             let rec = ExecutionReceipt {
//                 status: status.clone(),
//                 ..receipt_ok_with_swap(100)
//             };
//             let mut wal = MockWalStore::new();
//             // upsert_result should receive a row with the expected ResultStatus and valid meta_json
//             wal.expect_upsert_result().returning(move |row: LiqResultRecord| {
//                 assert_eq!(row.status, expected, "status mapping mismatch");
//                 // meta_json sanity
//                 let v: serde_json::Value = serde_json::from_str(&row.meta_json).unwrap();
//                 assert_eq!(v["v"].as_u64().unwrap(), 1);
//                 assert_eq!(v["status"].as_str().unwrap(), status.description());
//                 assert!(v["receipt"].is_object());
//                 Ok(())
//             });
//             // other WAL calls not used
//             wal.expect_delete().times(0);
//             wal.expect_get_result().times(0);
//             wal.expect_list_by_status().times(0);
//             wal.expect_update_status().times(0);

//             let mut cfg = MockConfigTrait::new();
//             cfg.expect_get_lending_canister().returning(Principal::anonymous);
//             cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//             cfg.expect_get_trader_principal().returning(Principal::anonymous);
//             cfg.expect_get_collateral_assets().returning(Default::default);
//             cfg.expect_get_debt_assets().returning(Default::default);
//             cfg.expect_should_buy_bad_debt().returning(|| false);

//             let agent = MockPipelineAgent::new();
//             let mut swap = MockIcrcSwapInterface::new();
//             // ensure process() doesn't attempt to swap; we won't call process here
//             swap.expect_get_swap_info().times(0);
//             swap.expect_swap().times(0);
//             let mut acct = MockIcrcAccountActions::new();
//             acct.expect_transfer().times(0);

//             let fz = KongSwapFinalizer {
//                 db: Arc::new(wal),
//                 swapper: Arc::new(swap),
//                 account: Arc::new(acct),
//                 config: Arc::new(cfg),
//                 agent: Arc::new(agent),
//             };
//             // Call the private helper directly (same module)
//             fz.persist_receipt_to_wal("liq:test", 0, 0, &rec)
//                 .await
//                 .expect("wal upsert should succeed");
//         };

//         // Success -> Succeeded
//         check(ES::Success, RS::Succeeded).await;
//         // Liquidation failures -> FailedPermanent
//         check(ES::LiquidationCallFailed("x".into()), RS::FailedPermanent).await;
//         check(ES::FailedLiquidation("x".into()), RS::FailedPermanent).await;
//         // Transfer / Swap failures -> FailedRetryable
//         check(ES::CollateralTransferFailed("x".into()), RS::FailedRetryable).await;
//         check(ES::ChangeTransferFailed("x".into()), RS::FailedRetryable).await;
//         check(ES::SwapFailed("x".into()), RS::FailedRetryable).await;
//     }

//     #[tokio::test]
//     async fn retry_limits_enforced() {
//         // Row at MAX_WAL_ATTEMPTS-1 with unprofitable quote -> FailedPermanent and recovery transfer.
//         let rec = receipt_ok_with_swap(9_999_999); // very high expectation to force unprofitable
//         let last_attempt = (super::MAX_WAL_ATTEMPTS - 1).max(0);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:limit", 0, status, last_attempt, &rec)])
//         });
//         // Expect bump to permanent
//         wal.expect_update_status()
//             .with(eq("liq:limit"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);

//         // Agent -> confirmed collateral
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 1,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         // Swapper -> always low quote
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(1u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().times(0);

//         // Recovery expected on permanent
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into())).times(1);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(10)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn retry_skips_when_collateral_failed() {
//         // Collateral transfer failed on-chain -> we should not quote/swap in this pass.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:collfail", 0, status, 0, &rec)])
//         });
//         wal.expect_update_status().times(0);
//         wal.expect_delete().times(0);

//         // Agent: collateral failed
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 7,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Failed("x".into()),
//                     },
//                 }))
//             });

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn get_liquidation_missing_id_in_receipt() {
//         // If receipt lacks liquidation id, WAL row should be deleted and no swap attempted.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:noid", 0, status, 0, &rec)])
//         });
//         wal.expect_delete()
//             .with(eq("liq:noid"), eq(0))
//             .returning(|_, _| Ok(()))
//             .times(1);
//         wal.expect_update_status().times(0);

//         let mut agent = MockPipelineAgent::new();
//         agent.expect_call_query::<Result<LiquidationResult, String>>().times(0);

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn agent_returns_ok_err_string_treated_as_retryable() {
//         // Agent returns Ok(Err(msg)) -> should backoff (FailedRetryable)
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:okerr", 0, status, 0, &rec)])
//         });
//         wal.expect_update_status()
//             .with(eq("liq:okerr"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);

//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| Ok(Err("logic fail".into())));

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn preflight_error_backs_off() {
//         // If get_swap_info errors, preflight_quote_checked returns retryable and no swap occurs.
//         let rec = receipt_ok_with_swap(100);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:preflighterr", 0, status, 0, &rec)])
//         });
//         wal.expect_update_status()
//             .with(eq("liq:preflighterr"), eq(0), always(), eq(true))
//             .returning(|_, _, _, _| Ok(()));
//         wal.expect_delete().times(0);

//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 1,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info()
//             .returning(|_, _, _| Err("quote failed".into()));
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn needs_swap_false_cases() {
//         // 1) expected_profit <= 0
//         let rec1 = receipt_ok_with_swap(0);
//         // 2) swap_args = None
//         let mut rec2 = receipt_ok_with_swap(100);
//         rec2.request.swap_args = None;
//         // 3) same asset for debt & collateral
//         let tok = tok(8, "SAME");
//         let mut rec3 = receipt_ok_with_swap(100);
//         rec3.request.debt_asset = tok.clone();
//         rec3.request.collateral_asset = tok.clone();

//         let mut wal = MockWalStore::new();
//         // Upserts for each receipt
//         wal.expect_upsert_result().returning(|_| Ok(())).times(3);
//         // delete calls for each (no swap needed path triggers delete)
//         wal.expect_delete().returning(|_, _| Ok(())).times(3);
//         wal.expect_get_result().returning(|_, _| Ok(None));
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec1, rec2, rec3]).await.unwrap();
//         assert_eq!(out.len(), 3);
//         assert!(out.iter().all(|o| o.swap_result.is_none()));
//     }

//     #[tokio::test]
//     async fn process_mixed_receipts_partial_success() {
//         // First: performs swap successfully; Second: no swap needed
//         let rec_swap = receipt_ok_with_swap(100);
//         let rec_noswap = receipt_ok_with_swap(0);

//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(())).times(2);
//         wal.expect_delete().returning(|_, _| Ok(())).times(2);
//         wal.expect_get_result().returning(|_, _| Ok(None));
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(2000u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|args| {
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(321u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap_or(0.5),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec_swap, rec_noswap]).await.unwrap();
//         assert_eq!(out.len(), 2);
//         assert!(out[0].swap_result.is_some());
//         assert!(out[1].swap_result.is_none());
//     }

//     #[tokio::test]
//     async fn profit_boundary_condition_allows_equal_quote() {
//         // Retry path: quote equals expected_profit should be acceptable (>=)
//         let rec = receipt_ok_with_swap(1234);

//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             let rec = rec.clone();
//             move |status, _| Ok(vec![wal_row_for_receipt("liq:eq", 0, status, 0, &rec)])
//         });
//         wal.expect_delete().with(eq("liq:eq"), eq(0)).returning(|_, _| Ok(()));
//         wal.expect_update_status().times(0);

//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 1,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1000_u128),
//                         collateral_received: Nat::from(2000_u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });

//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(1234u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|args| {
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(1234u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap_or(0.5),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });

//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);

//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));

//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(matches!(out[0].status, ExecutionStatus::Success));
//         assert_eq!(out[0].swap_result.as_ref().unwrap().receive_amount, Nat::from(1234u128));
//     }

//     #[tokio::test]
//     async fn receipt_from_meta_tolerates_garbage() {
//         // A corrupt meta_json row is skipped without panic or outcome.
//         let _rec = receipt_ok_with_swap(100);
//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status().returning({
//             move |status, _| {
//                 Ok(vec![LiqResultRecord {
//                     liq_id: "bad".into(),
//                     idx: 0,
//                     status,
//                     attempt: 0,
//                     created_at: 0,
//                     updated_at: 0,
//                     meta_json: "{ this is not json }".into(),
//                 }])
//             }
//         });
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);
//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);
//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(5)).await.unwrap();
//         assert!(out.is_empty());
//     }

//     #[tokio::test]
//     async fn delete_wal_silent_does_not_panic() {
//         // When delete fails, process continues and returns outcome.
//         let rec = receipt_ok_with_swap(0); // no swap path
//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(())).times(1);
//         wal.expect_delete()
//             .returning(|_, _| Err(anyhow::anyhow!("boom")))
//             .times(1);
//         wal.expect_get_result().returning(|_, _| Ok(None)).times(0);
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);
//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);
//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//         assert!(out[0].swap_result.is_none());
//     }

//     #[tokio::test]
//     async fn list_by_status_pagination_respected() {
//         // Ensure we request the specified limit and handle returned rows.
//         let rec = receipt_ok_with_swap(100);
//         let mut wal = MockWalStore::new();
//         wal.expect_list_by_status()
//             .with(eq(ResultStatus::FailedRetryable), eq(1usize))
//             .returning({
//                 let rec = rec.clone();
//                 move |_, _| {
//                     Ok(vec![wal_row_for_receipt(
//                         "liq:limit1",
//                         0,
//                         ResultStatus::FailedRetryable,
//                         0,
//                         &rec,
//                     )])
//                 }
//             });
//         wal.expect_delete().returning(|_, _| Ok(())).times(1);
//         let mut agent = MockPipelineAgent::new();
//         agent
//             .expect_call_query::<Result<LiquidationResult, String>>()
//             .returning(|_, _, _| {
//                 Ok(Ok(LiquidationResult {
//                     id: 1,
//                     amounts: crate::types::protocol_types::LiquidationAmounts {
//                         debt_repaid: Nat::from(1u128),
//                         collateral_received: Nat::from(2u128),
//                     },
//                     collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//                     status: crate::types::protocol_types::LiquidationStatus::Success,
//                     change_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                     collateral_tx: crate::types::protocol_types::TxStatus {
//                         tx_id: None,
//                         status: crate::types::protocol_types::TransferStatus::Success,
//                     },
//                 }))
//             });
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().returning(|_tin, _tout, amount| {
//             Ok(SwapAmountsReply {
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_address: "test".into(),
//                 pay_amount: amount.value.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_address: "test".into(),
//                 receive_amount: Nat::from(100u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: 0.0,
//                 txs: vec![],
//             })
//         });
//         swap.expect_swap().returning(|args| {
//             Ok(SwapReply {
//                 tx_id: 1,
//                 request_id: 1,
//                 status: "ok".into(),
//                 pay_chain: "IC".into(),
//                 pay_symbol: "COLL".into(),
//                 pay_amount: args.pay_amount.clone(),
//                 receive_chain: "IC".into(),
//                 receive_symbol: "DEBT".into(),
//                 receive_amount: Nat::from(100u128),
//                 mid_price: 0.0,
//                 price: 0.0,
//                 slippage: args.max_slippage.unwrap_or(0.5),
//                 txs: vec![],
//                 transfer_ids: vec![],
//                 claim_ids: vec![],
//                 ts: 0,
//             })
//         });
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().returning(|_, _| Ok("ok".into()));
//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.retry_failed_swaps(Some(1)).await.unwrap();
//         assert_eq!(out.len(), 1);
//     }

//     #[tokio::test]
//     async fn process_idempotent_wal_write() {
//         // upsert_result should be called exactly once per receipt
//         let rec = receipt_ok_with_swap(0); // no swap path
//         let mut wal = MockWalStore::new();
//         wal.expect_upsert_result().returning(|_| Ok(())).times(1);
//         wal.expect_delete().returning(|_, _| Ok(())).times(1);
//         wal.expect_list_by_status().returning(|_, _| Ok(vec![]));
//         let mut swap = MockIcrcSwapInterface::new();
//         swap.expect_get_swap_info().times(0);
//         swap.expect_swap().times(0);
//         let mut cfg = MockConfigTrait::new();
//         cfg.expect_get_lending_canister().returning(Principal::anonymous);
//         cfg.expect_get_liquidator_principal().returning(Principal::anonymous);
//         cfg.expect_get_trader_principal().returning(Principal::anonymous);
//         cfg.expect_get_collateral_assets().returning(Default::default);
//         cfg.expect_get_debt_assets().returning(Default::default);
//         cfg.expect_should_buy_bad_debt().returning(|| false);
//         let agent = MockPipelineAgent::new();
//         let mut acct = MockIcrcAccountActions::new();
//         acct.expect_transfer().times(0);
//         let fz = KongSwapFinalizer {
//             db: Arc::new(wal),
//             swapper: Arc::new(swap),
//             account: Arc::new(acct),
//             config: Arc::new(cfg),
//             agent: Arc::new(agent),
//         };
//         let out = fz.process(&vec![rec]).await.unwrap();
//         assert_eq!(out.len(), 1);
//     }

//     #[test]
//     fn formatted_values_rounding_and_sign() {
//         // Construct a LiquidationOutcome inline and verify formatted strings.
//         let debt = tok(2, "USD");
//         let coll = tok(2, "COLL");
//         let receipt_req = ExecutorRequest {
//             liquidation: crate::types::protocol_types::LiquidationRequest {
//                 borrower: Principal::anonymous(),
//                 debt_pool_id: Principal::anonymous(),
//                 collateral_pool_id: Principal::anonymous(),
//                 debt_amount: Nat::from(0u128),
//                 receiver_address: Principal::anonymous(),
//             },
//             swap_args: None,
//             debt_asset: debt.clone(),
//             collateral_asset: coll.clone(),
//             expected_profit: 100, // 1.00 USD
//         };
//         let liq_res = LiquidationResult {
//             id: 1,
//             amounts: crate::types::protocol_types::LiquidationAmounts {
//                 debt_repaid: Nat::from(1234u128),         // 12.34
//                 collateral_received: Nat::from(5678u128), // 56.78
//             },
//             collateral_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//             debt_asset: crate::types::protocol_types::AssetType::CkAsset(Principal::anonymous()),
//             status: crate::types::protocol_types::LiquidationStatus::Success,
//             change_tx: crate::types::protocol_types::TxStatus {
//                 tx_id: None,
//                 status: crate::types::protocol_types::TransferStatus::Success,
//             },
//             collateral_tx: crate::types::protocol_types::TxStatus {
//                 tx_id: None,
//                 status: crate::types::protocol_types::TransferStatus::Success,
//             },
//         };
//         let out = LiquidationOutcome {
//             request: receipt_req,
//             liquidation_result: liq_res,
//             swap_result: None,
//             status: ExecutionStatus::Success,
//             expected_profit: 100,
//             realized_profit: 90,
//         };
//         assert_eq!(out.formatted_debt_repaid(), "12.34 USD");
//         assert_eq!(out.formatted_received_collateral(), "56.78 COLL");
//         assert_eq!(out.formatted_swap_output(), "0 USD");
//         assert_eq!(out.formatted_realized_profit(), "0.9 USD");
//         assert_eq!(out.formatted_expected_profit(), "1 USD");
//         assert_eq!(out.formatted_profit_delta(), "-0.100");
//     }
// }
