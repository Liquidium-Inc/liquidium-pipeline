use async_trait::async_trait;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json;

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    finalizers::liquidation_outcome::LiquidationOutcome,
    persistance::{LiqResultRecord, ResultStatus, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::SwapExecution,
    wal::{liq_id_from_receipt, wal_load, wal_mark_inflight, wal_mark_permanent_failed, wal_mark_retryable_failed},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CexStep {
    Deposit,
    DepositPending,
    Trade,
    Withdraw,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexState {
    pub liq_id: String,
    pub step: CexStep,
    pub last_error: Option<String>,

    // deposit leg
    pub deposit_asset: ChainToken,
    pub deposit_txid: Option<String>,
    pub deposit_balance_before: Option<f64>,

    // trade leg
    pub market: String,
    pub side: String,
    pub size_in: ChainTokenAmount,

    // withdraw leg
    pub withdraw_asset: ChainToken,
    pub withdraw_address: String,
    pub withdraw_id: Option<String>,
    pub withdraw_txid: Option<String>,
    pub size_out: Option<ChainTokenAmount>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct CexLiqMetaWrapper {
    pub outcome: LiquidationOutcome,
    pub meta: Option<CexState>,
}

pub fn decode_cex_meta(row: &LiqResultRecord) -> Result<Option<CexLiqMetaWrapper>, String> {
    if row.meta_json.is_empty() || row.meta_json == "{}" {
        return Ok(None);
    }

    if let Ok(meta) = serde_json::from_str::<CexLiqMetaWrapper>(&row.meta_json) {
        return Ok(Some(meta));
    }

    // Backward compat: old rows with only LiquidationOutcome
    match serde_json::from_str::<LiquidationOutcome>(&row.meta_json) {
        Ok(outcome) => Ok(Some(CexLiqMetaWrapper { outcome, meta: None })),
        Err(e) => Err(format!("invalid meta_json for {}: {}", row.liq_id, e)),
    }
}

pub fn encode_cex_meta(row: &mut LiqResultRecord, meta: &CexLiqMetaWrapper) -> Result<(), String> {
    row.meta_json =
        serde_json::to_string(meta).map_err(|e| format!("failed to serialize meta_json for {}: {}", row.liq_id, e))?;
    Ok(())
}

#[async_trait]
pub trait CexFinalizerLogic: Send + Sync {
    // Build the initial CEX state for this liquidation from the receipt
    async fn prepare(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<CexState, String>;

    // On chain: collateral -> CEX deposit asset, send to deposit address
    async fn deposit(&self, state: &mut CexState) -> Result<(), String>;

    // On CEX: deposit asset -> withdraw asset
    async fn trade(&self, state: &mut CexState) -> Result<(), String>;

    // On CEX: withdraw to chain
    async fn withdraw(&self, state: &mut CexState) -> Result<(), String>;

    // Build final SwapExecution to hand back to pipeline when Completed
    async fn finish(&self, receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String>;
}

const MAX_CEX_FINALIZER_ATTEMPTS: i32 = 5;

#[async_trait]
impl Finalizer for dyn CexFinalizerLogic {
    async fn finalize(
        &self,
        wal: &dyn WalStore,
        receipts: Vec<ExecutionReceipt>,
    ) -> Result<Vec<FinalizerResult>, String> {
        let mut out = Vec::with_capacity(receipts.len());

        for receipt in receipts {
            // Only finalize successful executions
            if !matches!(receipt.status, ExecutionStatus::Success) {
                out.push(FinalizerResult::noop());
                continue;
            }

            // If no swap args, nothing to do
            if receipt.request.swap_args.is_none() {
                out.push(FinalizerResult::noop());
                continue;
            }

            let liq_id = liq_id_from_receipt(&receipt)?;

            // Load WAL row and wrapper meta
            let (row, mut meta): (LiqResultRecord, CexLiqMetaWrapper) = match wal_load(wal, &liq_id).await? {
                Some(row) => {
                    let row_checked = match row.status {
                        ResultStatus::Succeeded => {
                            debug!("[cex] liq_id={} already succeeded, skipping", liq_id);
                            out.push(FinalizerResult::noop());
                            continue;
                        }
                        ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable => row,
                        ResultStatus::FailedPermanent => {
                            return Err(format!("invalid WAL state {:?} for liq_id {}", row.status, row.liq_id));
                        }
                    };

                    let meta = decode_cex_meta(&row_checked)?
                        .ok_or_else(|| format!("missing CexLiqMetaWrapper for liq_id {}", row_checked.liq_id))?;

                    (row_checked, meta)
                }
                None => {
                    debug!("[cex] liq_id={} not found in WAL, skipping CEX", liq_id);
                    out.push(FinalizerResult::noop());
                    continue;
                }
            };

            wal_mark_inflight(wal, &row.liq_id).await?;

            // Ensure we have a CEX state
            let mut cex_state = if let Some(s) = meta.meta.clone() {
                s
            } else {
                self.prepare(&liq_id, &receipt).await?
            };

            // Run as many steps as possible in one finalize call.
            // We rely on WAL for crash recovery, but in the happy path we
            // want to progress Deposit -> Trade -> Withdraw -> Completed
            // within a single invocation if each leg succeeds.
            let mut step_res: Result<(), String> = Ok(());

            loop {
                step_res = match cex_state.step {
                    CexStep::Deposit => {
                        debug!("[cex] liq_id={} step=Deposit", cex_state.liq_id);
                        self.deposit(&mut cex_state).await
                    }
                    CexStep::DepositPending => {
                        // Deposit did not arrive, we cannot break yet, so break;
                        break;
                    }
                    CexStep::Trade => {
                        debug!("[cex] liq_id={} step=Trade", cex_state.liq_id);
                        self.trade(&mut cex_state).await
                    }
                    CexStep::Withdraw => {
                        debug!("[cex] liq_id={} step=Withdraw", cex_state.liq_id);
                        self.withdraw(&mut cex_state).await
                    }
                    CexStep::Completed => {
                        debug!("[cex] liq_id={} step=Completed (noop)", cex_state.liq_id);
                        break;
                    }
                    CexStep::Failed => {
                        step_res = Err("CEX finalizer in Failed state".to_string());
                        break;
                    }
                };

                // If the current leg failed, stop and handle retry / permanent fail below.
                if step_res.is_err() {
                    break;
                }
            }

            if let Err(err) = step_res {
                let next_attempt = row.attempt + 1;
                if next_attempt >= MAX_CEX_FINALIZER_ATTEMPTS {
                    let _ = wal_mark_permanent_failed(wal, &row.liq_id).await;
                } else {
                    let _ = wal_mark_retryable_failed(wal, &row.liq_id).await;
                }
                return Err(err);
            }

            // Persist updated CexLiqMetaWrapper with new cex_state
            let mut row_after = row;
            meta.meta = Some(cex_state.clone());
            encode_cex_meta(&mut row_after, &meta)?;

            // If completed, mark as succeeded and build final SwapExecution; otherwise just persist state
            if matches!(cex_state.step, CexStep::Completed) {
                row_after.status = ResultStatus::Succeeded;
                wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;

                let swap_exec = self.finish(&receipt, &cex_state).await?;
                out.push(FinalizerResult {
                    swap_result: Some(swap_exec),
                });
            } else {
                // Keep it InFlight or re-enqueue, depending on how your poller works.
                // Minimal change: just persist the updated state.
                wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;
                out.push(FinalizerResult::noop());
            }
        }

        Ok(out)
    }
}
