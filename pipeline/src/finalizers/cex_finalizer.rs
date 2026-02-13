use async_trait::async_trait;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use serde::{Deserialize, Serialize};

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::SwapExecution,
    utils::now_ts,
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
};

use log::{debug, error, info};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CexStep {
    Deposit,
    DepositPending,
    Trade,
    TradePending,
    Withdraw,
    Completed,
    Failed,
}

/// Route-level CEX feasibility and cost preview used by hybrid routing decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexRoutePreview {
    /// Whether the route is executable against current orderbook depth.
    pub is_executable: bool,
    /// Estimated output amount in final receive-asset native units.
    pub estimated_receive_amount: f64,
    /// Estimated end-to-end route slippage in basis points.
    pub estimated_slippage_bps: f64,
    /// Optional reason when preview cannot be executed.
    pub reason: Option<String>,
}

/// One executed CEX slice for observability and export analytics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexTradeSlice {
    pub leg_index: u32,
    pub market: String,
    pub side: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub mid_price: f64,
    pub exec_price: f64,
    pub slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexDepositState {
    pub deposit_asset: ChainToken,
    pub deposit_txid: Option<String>,
    pub deposit_balance_before: Option<f64>,
    #[serde(default)]
    pub approval_bump_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexTradeState {
    /// One-based index of the current trade leg being executed.
    /// `None` means no leg has started yet.
    pub trade_leg_index: Option<u32>,
    /// Total number of legs in the resolved route.
    pub trade_leg_total: Option<u32>,
    /// Market used by the most recently attempted leg/slice.
    pub trade_last_market: Option<String>,
    /// Side (`buy`/`sell`) used by the most recently attempted leg/slice.
    pub trade_last_side: Option<String>,
    /// Input amount requested/consumed for the most recent leg update.
    pub trade_last_amount_in: Option<f64>,
    /// Output amount produced by the most recent leg update.
    pub trade_last_amount_out: Option<f64>,
    /// Amount to carry into the next leg after a completed leg.
    pub trade_next_amount_in: Option<f64>,
    /// Weighted average route slippage across all executed slices, in bps.
    pub trade_weighted_slippage_bps: Option<f64>,
    /// Sum(amount_in * mid_price) across slices.
    pub trade_mid_notional_sum: Option<f64>,
    /// Sum(amount_in * exec_price) across slices.
    pub trade_exec_notional_sum: Option<f64>,
    /// Recorded slice-level executions for telemetry.
    #[serde(default)]
    pub trade_slices: Vec<CexTradeSlice>,
    /// Whether residual was skipped as dust (< min execution USD).
    #[serde(default)]
    pub trade_dust_skipped: bool,
    /// Residual notional skipped as dust, in USD.
    #[serde(default)]
    pub trade_dust_usd: Option<f64>,
    /// Remaining input for the current leg persisted for retry-safe resume.
    #[serde(default)]
    pub trade_progress_remaining_in: Option<f64>,
    /// Accumulated output for the current leg persisted for retry-safe resume.
    #[serde(default)]
    pub trade_progress_total_out: Option<f64>,
    /// Deterministic client order id for the in-flight slice, if any.
    #[serde(default)]
    pub trade_pending_client_order_id: Option<String>,
    /// Market of the in-flight slice.
    #[serde(default)]
    pub trade_pending_market: Option<String>,
    /// Side of the in-flight slice.
    #[serde(default)]
    pub trade_pending_side: Option<String>,
    /// Requested input of the in-flight slice.
    #[serde(default)]
    pub trade_pending_requested_in: Option<f64>,
    /// Buy mode selected for the in-flight slice / next buy hint.
    #[serde(default)]
    pub trade_pending_buy_mode: Option<String>,
    /// Count of inverse buy retries executed in the current leg.
    #[serde(default)]
    pub trade_inverse_retry_count: u32,
    /// Residual input that could not be executed due to constraints.
    #[serde(default)]
    pub trade_unexecutable_residual_in: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexWithdrawState {
    pub withdraw_asset: ChainToken,
    pub withdraw_address: String,
    pub withdraw_id: Option<String>,
    pub withdraw_txid: Option<String>,
    pub size_out: Option<ChainTokenAmount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexState {
    pub liq_id: String,
    pub step: CexStep,
    pub last_error: Option<String>,
    pub market: String,
    pub side: String,
    pub size_in: ChainTokenAmount,
    #[serde(flatten)]
    pub deposit: CexDepositState,
    #[serde(flatten)]
    pub trade: CexTradeState,
    #[serde(flatten)]
    pub withdraw: CexWithdrawState,
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

    // Preview route feasibility and slippage using current orderbook depth.
    async fn preview_route(&self, receipt: &ExecutionReceipt) -> Result<CexRoutePreview, String>;
}

#[async_trait]
impl Finalizer for dyn CexFinalizerLogic {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Only finalize successful executions
        if !matches!(receipt.status, ExecutionStatus::Success) {
            debug!("[cex] ⏭️ skip: execution status not successful: {:?}", receipt.status);
            return Ok(FinalizerResult::noop());
        }

        // If no swap args, nothing to do
        if receipt.request.swap_args.is_none() {
            debug!("[cex] ⏭️ skip: no swap_args for receipt");
            return Ok(FinalizerResult::noop());
        }

        let liq_id = liq_id_from_receipt(&receipt)?;
        info!("[cex] 🚀 liq_id={} finalize start", liq_id);

        // Load WAL row and wrapper meta
        let (row, mut meta): (LiqResultRecord, LiqMetaWrapper) = match wal_load(wal, &liq_id).await? {
            Some(row) => {
                let row_checked = match row.status {
                    ResultStatus::Succeeded => return Ok(FinalizerResult::noop()),
                    ResultStatus::Enqueued | ResultStatus::InFlight | ResultStatus::FailedRetryable => row,
                    ResultStatus::WaitingCollateral | ResultStatus::WaitingProfit => {
                        return Ok(FinalizerResult::noop());
                    }
                    ResultStatus::FailedPermanent => {
                        return Err(format!("invalid WAL state {:?} for liq_id {}", row.status, row.id));
                    }
                };
                debug!(
                    "[cex] 📓 liq_id={} wal_status={:?} attempt={}",
                    row_checked.id, row_checked.status, row_checked.attempt
                );

                let meta = decode_receipt_wrapper(&row_checked)?
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

        debug!(
            "[cex] ✅ liq_id={} state loaded: step={:?}",
            cex_state.liq_id, cex_state.step
        );

        // Legacy WAL bootstrap: older rows do not include per-leg progress/pending fields.
        if matches!(cex_state.step, CexStep::Trade | CexStep::TradePending) {
            if cex_state.trade.trade_progress_remaining_in.is_none() {
                let bootstrap_in = cex_state
                    .trade
                    .trade_next_amount_in
                    .unwrap_or_else(|| cex_state.size_in.to_f64());
                cex_state.trade.trade_progress_remaining_in = Some(bootstrap_in);
            }

            if cex_state.trade.trade_progress_total_out.is_none() {
                cex_state.trade.trade_progress_total_out = Some(0.0);
            }
        }

        let mut row_after = row;
        loop {
            let step_res = match cex_state.step {
                CexStep::Deposit => {
                    debug!("[cex] 💰 liq_id={} step=Deposit", cex_state.liq_id);
                    self.deposit(&mut cex_state).await
                }
                CexStep::DepositPending => {
                    info!("[cex] ⏳ liq_id={} step=DepositPending", cex_state.liq_id);
                    // Re-check deposit arrival on the CEX.
                    self.deposit(&mut cex_state).await
                }
                CexStep::Trade => {
                    debug!("[cex] 🔁 liq_id={} step=Trade", cex_state.liq_id);
                    self.trade(&mut cex_state).await
                }
                CexStep::TradePending => {
                    debug!("[cex] ⏳ liq_id={} step=TradePending", cex_state.liq_id);
                    self.trade(&mut cex_state).await
                }
                CexStep::Withdraw => {
                    debug!("[cex] 🏦 liq_id={} step=Withdraw", cex_state.liq_id);
                    self.withdraw(&mut cex_state).await
                }
                CexStep::Completed => {
                    info!("[cex] 🎉 liq_id={} step=Completed", cex_state.liq_id);
                    debug!("[cex] liq_id={} step=Completed (noop)", cex_state.liq_id);
                    break;
                }
                CexStep::Failed => {
                    error!(
                        "[cex] ❌ liq_id={} step=Failed last_error={:?}",
                        cex_state.liq_id, cex_state.last_error
                    );
                    break;
                }
            };

            // If the current leg failed, stop and handle retry / permanent fail below.
            if let Err(err) = step_res {
                error!("[cex] ❗ liq_id={} step error: {}", cex_state.liq_id, err);
                cex_state.last_error = Some(err.to_string());
                row_after.status = ResultStatus::FailedRetryable;
                row_after.updated_at = now_ts();
                // Persist last known state for journaling before exiting.
                meta.meta = serde_json::to_vec(&cex_state).map_err(|e| e.to_string())?;
                encode_meta(&mut row_after, &meta)?;
                wal.upsert_result(row_after.clone()).await.map_err(|e| e.to_string())?;
                return Err(err.to_string());
            }

            debug!(
                "[cex] ✅ liq_id={} step advanced to {:?}",
                cex_state.liq_id, cex_state.step
            );

            // Persist state after each successful step so we can resume safely on crash.
            meta.meta = serde_json::to_vec(&cex_state).map_err(|e| e.to_string())?;
            encode_meta(&mut row_after, &meta)?;
            wal.upsert_result(row_after.clone()).await.map_err(|e| e.to_string())?;

            if matches!(cex_state.step, CexStep::DepositPending) {
                break;
            }
        }

        // If completed, mark as succeeded and build final SwapExecution; otherwise just persist state
        let res = if matches!(cex_state.step, CexStep::Completed) {
            info!("[cex] 🎯 liq_id={} finalize completed", cex_state.liq_id);
            row_after.status = ResultStatus::Succeeded;
            meta.meta = serde_json::to_vec(&cex_state).map_err(|e| e.to_string())?;
            encode_meta(&mut row_after, &meta)?;
            wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;

            let swap_exec = self.finish(&receipt, &cex_state).await?;
            FinalizerResult {
                swap_result: Some(swap_exec),
                finalized: true,
                swapper: None,
            }
        } else {
            // Minimal change: just persist the updated state (already done in-loop).
            wal.upsert_result(row_after).await.map_err(|e| e.to_string())?;
            info!(
                "[cex] ⏸️ liq_id={} finalize paused at step={:?}",
                cex_state.liq_id, cex_state.step
            );
            FinalizerResult::noop()
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };
    use std::sync::{Arc, Mutex};

    use crate::executors::executor::ExecutorRequest;
    use crate::persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore};
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
    use crate::swappers::model::{SwapExecution, SwapRequest};
    use crate::wal::encode_meta;

    #[derive(Default)]
    struct TestWal {
        row: Mutex<Option<LiqResultRecord>>,
    }

    impl TestWal {
        fn new(row: LiqResultRecord) -> Self {
            Self {
                row: Mutex::new(Some(row)),
            }
        }

        fn snapshot(&self) -> Option<LiqResultRecord> {
            self.row.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WalStore for TestWal {
        async fn upsert_result(&self, row: LiqResultRecord) -> anyhow::Result<()> {
            *self.row.lock().unwrap() = Some(row);
            Ok(())
        }

        async fn get_result(&self, _liq_id: &str) -> anyhow::Result<Option<LiqResultRecord>> {
            Ok(self.row.lock().unwrap().clone())
        }

        async fn list_by_status(&self, _status: ResultStatus, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
            Ok(vec![])
        }

        async fn get_pending(&self, _limit: usize) -> anyhow::Result<Vec<LiqResultRecord>> {
            Ok(vec![])
        }

        async fn update_status(&self, _liq_id: &str, _next: ResultStatus, _bump_attempt: bool) -> anyhow::Result<()> {
            Ok(())
        }

        async fn update_failure(
            &self,
            _liq_id: &str,
            _next: ResultStatus,
            _last_error: String,
            _bump_attempt: bool,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn delete(&self, _liq_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct DummyCexFinalizer {
        trade_calls: Arc<Mutex<u32>>,
        pending_once: bool,
    }

    #[async_trait]
    impl CexFinalizerLogic for DummyCexFinalizer {
        async fn prepare(&self, liq_id: &str, _receipt: &ExecutionReceipt) -> Result<CexState, String> {
            let pay = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckBTC".to_string(),
                decimals: 8,
                fee: Nat::from(10u64),
            };
            let recv = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckUSDT".to_string(),
                decimals: 6,
                fee: Nat::from(10_000u64),
            };

            Ok(CexState {
                liq_id: liq_id.to_string(),
                step: CexStep::Trade,
                last_error: None,
                market: "CKBTC_BTC".to_string(),
                side: "sell".to_string(),
                size_in: ChainTokenAmount::from_raw(pay.clone(), Nat::from(1_000u64)),
                deposit: CexDepositState {
                    deposit_asset: pay.clone(),
                    deposit_txid: None,
                    deposit_balance_before: None,
                    approval_bump_count: None,
                },
                trade: CexTradeState {
                    trade_leg_index: None,
                    trade_leg_total: None,
                    trade_last_market: None,
                    trade_last_side: None,
                    trade_last_amount_in: None,
                    trade_last_amount_out: None,
                    trade_next_amount_in: None,
                    trade_weighted_slippage_bps: None,
                    trade_mid_notional_sum: None,
                    trade_exec_notional_sum: None,
                    trade_slices: Vec::new(),
                    trade_dust_skipped: false,
                    trade_dust_usd: None,
                    trade_progress_remaining_in: None,
                    trade_progress_total_out: None,
                    trade_pending_client_order_id: None,
                    trade_pending_market: None,
                    trade_pending_side: None,
                    trade_pending_requested_in: None,
                    trade_pending_buy_mode: None,
                    trade_inverse_retry_count: 0,
                    trade_unexecutable_residual_in: None,
                },
                withdraw: CexWithdrawState {
                    withdraw_asset: recv,
                    withdraw_address: "dest".to_string(),
                    withdraw_id: None,
                    withdraw_txid: None,
                    size_out: None,
                },
            })
        }

        async fn deposit(&self, _state: &mut CexState) -> Result<(), String> {
            Ok(())
        }

        async fn trade(&self, state: &mut CexState) -> Result<(), String> {
            let mut calls = self.trade_calls.lock().unwrap();
            *calls += 1;

            if self.pending_once && *calls == 1 {
                state.trade.trade_leg_index = Some(1);
                state.trade.trade_leg_total = Some(2);
                state.step = CexStep::TradePending;
                return Ok(());
            }

            state.trade.trade_leg_index = Some(2);
            state.trade.trade_leg_total = Some(2);
            state.step = CexStep::Completed;
            Ok(())
        }

        async fn withdraw(&self, _state: &mut CexState) -> Result<(), String> {
            Ok(())
        }

        async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {
            let pay = state.deposit.deposit_asset.asset_id();
            let recv = state.withdraw.withdraw_asset.asset_id();
            Ok(SwapExecution {
                swap_id: 0,
                request_id: 0,
                status: "ok".to_string(),
                pay_asset: pay,
                pay_amount: Nat::from(0u8),
                receive_asset: recv,
                receive_amount: Nat::from(0u8),
                mid_price: 0.0,
                exec_price: 0.0,
                slippage: 0.0,
                legs: vec![],
                approval_count: None,
                ts: 0,
            })
        }

        async fn preview_route(&self, _receipt: &ExecutionReceipt) -> Result<CexRoutePreview, String> {
            Ok(CexRoutePreview {
                is_executable: true,
                estimated_receive_amount: 0.0,
                estimated_slippage_bps: 0.0,
                reason: None,
            })
        }
    }

    struct FailThenResumeFinalizer {
        prepare_calls: Arc<Mutex<u32>>,
        trade_calls: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl CexFinalizerLogic for FailThenResumeFinalizer {
        async fn prepare(&self, liq_id: &str, _receipt: &ExecutionReceipt) -> Result<CexState, String> {
            *self.prepare_calls.lock().unwrap() += 1;

            let pay = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckBTC".to_string(),
                decimals: 8,
                fee: Nat::from(10u64),
            };
            let recv = ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckUSDT".to_string(),
                decimals: 6,
                fee: Nat::from(10_000u64),
            };

            Ok(CexState {
                liq_id: liq_id.to_string(),
                step: CexStep::Trade,
                last_error: None,
                market: "CKBTC_BTC".to_string(),
                side: "sell".to_string(),
                size_in: ChainTokenAmount::from_raw(pay.clone(), Nat::from(1_000u64)),
                deposit: CexDepositState {
                    deposit_asset: pay.clone(),
                    deposit_txid: None,
                    deposit_balance_before: None,
                    approval_bump_count: None,
                },
                trade: CexTradeState {
                    trade_leg_index: None,
                    trade_leg_total: None,
                    trade_last_market: None,
                    trade_last_side: None,
                    trade_last_amount_in: None,
                    trade_last_amount_out: None,
                    trade_next_amount_in: None,
                    trade_weighted_slippage_bps: None,
                    trade_mid_notional_sum: None,
                    trade_exec_notional_sum: None,
                    trade_slices: Vec::new(),
                    trade_dust_skipped: false,
                    trade_dust_usd: None,
                    trade_progress_remaining_in: None,
                    trade_progress_total_out: None,
                    trade_pending_client_order_id: None,
                    trade_pending_market: None,
                    trade_pending_side: None,
                    trade_pending_requested_in: None,
                    trade_pending_buy_mode: None,
                    trade_inverse_retry_count: 0,
                    trade_unexecutable_residual_in: None,
                },
                withdraw: CexWithdrawState {
                    withdraw_asset: recv,
                    withdraw_address: "dest".to_string(),
                    withdraw_id: None,
                    withdraw_txid: None,
                    size_out: None,
                },
            })
        }

        async fn deposit(&self, _state: &mut CexState) -> Result<(), String> {
            Ok(())
        }

        async fn trade(&self, state: &mut CexState) -> Result<(), String> {
            let mut calls = self.trade_calls.lock().unwrap();
            *calls += 1;

            if *calls == 1 {
                state.trade.trade_leg_index = Some(1);
                state.trade.trade_leg_total = Some(2);
                state.trade.trade_next_amount_in = Some(0.1234);
                return Err("trade exploded once".to_string());
            }

            if state.trade.trade_leg_index != Some(1) {
                return Err("expected persisted trade_leg_index=1 on retry".to_string());
            }
            if (state.trade.trade_next_amount_in.unwrap_or_default() - 0.1234).abs() > 1e-12 {
                return Err("expected persisted trade_next_amount_in on retry".to_string());
            }

            state.trade.trade_leg_index = Some(2);
            state.trade.trade_leg_total = Some(2);
            state.step = CexStep::Completed;
            Ok(())
        }

        async fn withdraw(&self, _state: &mut CexState) -> Result<(), String> {
            Ok(())
        }

        async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {
            Ok(SwapExecution {
                swap_id: 0,
                request_id: 0,
                status: "ok".to_string(),
                pay_asset: state.deposit.deposit_asset.asset_id(),
                pay_amount: Nat::from(0u8),
                receive_asset: state.withdraw.withdraw_asset.asset_id(),
                receive_amount: Nat::from(0u8),
                mid_price: 0.0,
                exec_price: 0.0,
                slippage: 0.0,
                legs: vec![],
                approval_count: None,
                ts: 0,
            })
        }

        async fn preview_route(&self, _receipt: &ExecutionReceipt) -> Result<CexRoutePreview, String> {
            Ok(CexRoutePreview {
                is_executable: true,
                estimated_receive_amount: 0.0,
                estimated_slippage_bps: 0.0,
                reason: None,
            })
        }
    }

    fn make_receipt(liq_id: u128) -> ExecutionReceipt {
        let pay_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        };
        let recv_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        };

        let liquidation = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            receiver_address: Principal::anonymous(),
            buy_bad_debt: false,
        };

        let swap_req = SwapRequest {
            pay_asset: pay_token.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(1_000u64)),
            receive_asset: recv_token.asset_id(),
            receive_address: Some("dest".to_string()),
            max_slippage_bps: Some(100),
            venue_hint: Some("mexc".to_string()),
        };

        let exec_req = ExecutorRequest {
            liquidation,
            swap_args: Some(swap_req),
            debt_asset: recv_token.clone(),
            collateral_asset: pay_token.clone(),
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_approval_needed: false,
        };

        let liq_result = LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(1_000u64),
                debt_repaid: Nat::from(1_000u64),
            },
            collateral_asset: AssetType::CkAsset(Principal::anonymous()),
            debt_asset: AssetType::CkAsset(Principal::anonymous()),
            status: LiquidationStatus::Success,
            timestamp: 0,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            id: liq_id,
        };

        ExecutionReceipt {
            request: exec_req,
            liquidation_result: Some(liq_result),
            status: ExecutionStatus::Success,
            change_received: true,
        }
    }

    #[tokio::test]
    async fn cex_finalize_runs_trade_pending_in_same_call() {
        let receipt = make_receipt(42);
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
        };

        let mut row = LiqResultRecord {
            id: "42".to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: "{}".to_string(),
        };
        encode_meta(&mut row, &wrapper).expect("encode meta should succeed");

        let wal = TestWal::new(row);

        let trade_calls = Arc::new(Mutex::new(0u32));
        let finalizer = DummyCexFinalizer {
            trade_calls: trade_calls.clone(),
            pending_once: true,
        };

        let res = (&finalizer as &dyn CexFinalizerLogic)
            .finalize(&wal, receipt)
            .await
            .expect("finalize should succeed");

        assert!(res.finalized);
        assert_eq!(*trade_calls.lock().unwrap(), 2);

        let row_after = wal.snapshot().expect("row should exist");
        assert_eq!(row_after.status, ResultStatus::Succeeded);
    }

    #[tokio::test]
    async fn cex_finalize_persists_failed_retryable_and_resumes_from_serialized_state() {
        let receipt = make_receipt(777);
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
        };

        let mut row = LiqResultRecord {
            id: "777".to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: "{}".to_string(),
        };
        encode_meta(&mut row, &wrapper).expect("encode meta should succeed");

        let wal = TestWal::new(row);

        let prepare_calls = Arc::new(Mutex::new(0u32));
        let trade_calls = Arc::new(Mutex::new(0u32));
        let finalizer = FailThenResumeFinalizer {
            prepare_calls: prepare_calls.clone(),
            trade_calls: trade_calls.clone(),
        };

        let first_err = (&finalizer as &dyn CexFinalizerLogic)
            .finalize(&wal, receipt.clone())
            .await
            .expect_err("first finalize should fail");
        assert!(first_err.contains("trade exploded once"));

        assert_eq!(*prepare_calls.lock().unwrap(), 1);
        assert_eq!(*trade_calls.lock().unwrap(), 1);

        let row_after_first = wal.snapshot().expect("row should exist");
        assert_eq!(row_after_first.status, ResultStatus::FailedRetryable);

        let wrapper_after_first = decode_receipt_wrapper(&row_after_first)
            .expect("decode wrapper should succeed")
            .expect("wrapper should exist");
        let state_after_first: CexState =
            serde_json::from_slice(&wrapper_after_first.meta).expect("state should decode");
        assert!(matches!(state_after_first.step, CexStep::Trade));
        assert_eq!(state_after_first.trade.trade_leg_index, Some(1));
        assert_eq!(state_after_first.trade.trade_leg_total, Some(2));
        assert!((state_after_first.trade.trade_next_amount_in.unwrap_or_default() - 0.1234).abs() < 1e-12);
        assert!(
            state_after_first
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("trade exploded once")
        );

        let second = (&finalizer as &dyn CexFinalizerLogic)
            .finalize(&wal, receipt)
            .await
            .expect("second finalize should resume and succeed");
        assert!(second.finalized);

        // prepare() runs only on the first invocation; second call must load persisted state.
        assert_eq!(*prepare_calls.lock().unwrap(), 1);
        assert_eq!(*trade_calls.lock().unwrap(), 2);

        let row_after_second = wal.snapshot().expect("row should exist");
        assert_eq!(row_after_second.status, ResultStatus::Succeeded);
        let wrapper_after_second = decode_receipt_wrapper(&row_after_second)
            .expect("decode wrapper should succeed")
            .expect("wrapper should exist");
        let state_after_second: CexState =
            serde_json::from_slice(&wrapper_after_second.meta).expect("state should decode");
        assert!(matches!(state_after_second.step, CexStep::Completed));
        assert_eq!(state_after_second.trade.trade_leg_index, Some(2));
    }

    #[tokio::test]
    async fn cex_state_deserializes_when_new_trade_progress_fields_are_missing() {
        let receipt = make_receipt(888);
        let finalizer = DummyCexFinalizer {
            trade_calls: Arc::new(Mutex::new(0)),
            pending_once: false,
        };
        let state = finalizer
            .prepare("888", &receipt)
            .await
            .expect("prepare should succeed");

        let mut value = serde_json::to_value(state).expect("serialize state");
        let map = value.as_object_mut().expect("state should serialize to object");
        map.remove("trade_progress_remaining_in");
        map.remove("trade_progress_total_out");
        map.remove("trade_pending_client_order_id");
        map.remove("trade_pending_market");
        map.remove("trade_pending_side");
        map.remove("trade_pending_requested_in");
        map.remove("trade_pending_buy_mode");
        map.remove("trade_inverse_retry_count");
        map.remove("trade_unexecutable_residual_in");

        let decoded: CexState = serde_json::from_value(value).expect("legacy deserialize should succeed");
        assert_eq!(decoded.trade.trade_progress_remaining_in, None);
        assert_eq!(decoded.trade.trade_progress_total_out, None);
        assert_eq!(decoded.trade.trade_pending_client_order_id, None);
        assert_eq!(decoded.trade.trade_pending_market, None);
        assert_eq!(decoded.trade.trade_pending_side, None);
        assert_eq!(decoded.trade.trade_pending_requested_in, None);
        assert_eq!(decoded.trade.trade_pending_buy_mode, None);
        assert_eq!(decoded.trade.trade_inverse_retry_count, 0);
        assert_eq!(decoded.trade.trade_unexecutable_residual_in, None);
    }

    #[tokio::test]
    async fn cex_finalize_bootstraps_legacy_trade_progress_from_wal_state() {
        let receipt = make_receipt(889);
        let trade_calls = Arc::new(Mutex::new(0u32));
        let finalizer = DummyCexFinalizer {
            trade_calls: trade_calls.clone(),
            pending_once: false,
        };

        let mut state = finalizer
            .prepare("889", &receipt)
            .await
            .expect("prepare should succeed");
        state.step = CexStep::Trade;
        state.trade.trade_next_amount_in = Some(0.1234);
        state.trade.trade_progress_remaining_in = Some(0.1234);
        state.trade.trade_progress_total_out = Some(0.0);
        state.trade.trade_pending_client_order_id = None;
        state.trade.trade_pending_market = None;
        state.trade.trade_pending_side = None;
        state.trade.trade_pending_requested_in = None;
        state.trade.trade_pending_buy_mode = None;
        state.trade.trade_inverse_retry_count = 0;
        state.trade.trade_unexecutable_residual_in = None;

        let mut legacy_value = serde_json::to_value(state).expect("serialize state");
        let map = legacy_value.as_object_mut().expect("state should serialize to object");
        map.remove("trade_progress_remaining_in");
        map.remove("trade_progress_total_out");
        map.remove("trade_pending_client_order_id");
        map.remove("trade_pending_market");
        map.remove("trade_pending_side");
        map.remove("trade_pending_requested_in");
        map.remove("trade_pending_buy_mode");
        map.remove("trade_inverse_retry_count");
        map.remove("trade_unexecutable_residual_in");

        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: serde_json::to_vec(&legacy_value).expect("legacy meta encoding"),
            finalizer_decision: None,
            profit_snapshot: None,
        };

        let mut row = LiqResultRecord {
            id: "889".to_string(),
            status: ResultStatus::FailedRetryable,
            attempt: 1,
            error_count: 1,
            last_error: Some("prior failure".to_string()),
            created_at: 0,
            updated_at: 0,
            meta_json: "{}".to_string(),
        };
        encode_meta(&mut row, &wrapper).expect("encode meta should succeed");
        let wal = TestWal::new(row);

        let res = (&finalizer as &dyn CexFinalizerLogic)
            .finalize(&wal, receipt)
            .await
            .expect("finalize should succeed");
        assert!(res.finalized);
        assert_eq!(*trade_calls.lock().unwrap(), 1);
    }
}
