use async_trait::async_trait;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use serde::{Deserialize, Serialize};

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::SwapExecution,
    wal::{decode_meta, encode_meta, liq_id_from_receipt, wal_load},
};

use log::debug;

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

#[async_trait]
impl Finalizer for dyn CexFinalizerLogic {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Only finalize successful executions
        if !matches!(receipt.status, ExecutionStatus::Success) {
            return Ok(FinalizerResult::noop());
        }

        // If no swap args, nothing to do
        if receipt.request.swap_args.is_none() {
            return Ok(FinalizerResult::noop());
        }

        let liq_id = liq_id_from_receipt(&receipt)?;

        // Load WAL row and wrapper meta
        let (row, mut meta): (LiqResultRecord, LiqMetaWrapper) = match wal_load(wal, &liq_id).await? {
            Some(row) => {
                let row_checked = match row.status {
                    ResultStatus::Succeeded => return Ok(FinalizerResult::noop()),
                    ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable => row,
                    ResultStatus::FailedPermanent => {
                        return Err(format!("invalid WAL state {:?} for liq_id {}", row.status, row.id));
                    }
                };

                let meta = decode_meta(&row_checked)?
                    .ok_or_else(|| format!("missing CexLiqMetaWrapper for liq_id {}", row_checked.id))?;

                (row_checked, meta)
            }
            None => {
                debug!("[cex] liq_id={} not found in WAL, skipping CEX", liq_id);
                return Ok(FinalizerResult::noop());
            }
        };

        // Ensure we have a CEX state
        let mut cex_state = if !meta.meta.is_empty() {
            serde_json::from_slice(&meta.meta).map_err(|e| e.to_string())?
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

        // Persist updated CexLiqMetaWrapper with new cex_state
        let mut row_after = row;
        meta.meta = serde_json::to_vec(&cex_state).map_err(|e| e.to_string())?;
        encode_meta(&mut row_after, &meta)?;

        // If completed, mark as succeeded and build final SwapExecution; otherwise just persist state
        let res = if matches!(cex_state.step, CexStep::Completed) {
            row_after.status = ResultStatus::Succeeded;
            wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;

            let swap_exec = self.finish(&receipt, &cex_state).await?;
            FinalizerResult {
                swap_result: Some(swap_exec),
                finalized: true,
            }
        } else {
            // Minimal change: just persist the updated state.
            wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;
            FinalizerResult::noop()
        };

        Ok(res)
    }
}
