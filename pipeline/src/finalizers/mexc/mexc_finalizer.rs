use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, OrderBookLevel};
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::TransferActions,
};
use log::{debug, info, warn};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use super::mexc_utils::{
    LIQUIDITY_EPS, SlicePreview, TradeLeg, f64_to_nat, is_usd_stable_symbol, mexc_special_trade_legs,
    parse_market_symbols, simulate_buy_from_asks, simulate_sell_from_bids,
};
use crate::{
    finalizers::cex_finalizer::{CexFinalizerLogic, CexRoutePreview, CexState, CexStep, CexTradeSlice},
    stages::executor::ExecutionReceipt,
    swappers::model::{SwapExecution, SwapQuoteLeg},
    utils::now_ts,
};

// MEXC-specific implementation of the generic CEX finalizer logic.
//
// This is a thin state machine wrapper around the generic `CexState`:
// - `prepare` initializes the state from a liquidation id / receipt plus static config
// - `deposit` transitions Deposit -> Trade
// - `trade` transitions Trade -> Withdraw
// - `withdraw` transitions Withdraw -> Completed
pub struct MexcFinalizer<C>
where
    C: CexBackend,
{
    pub backend: Arc<C>,
    pub transfer_service: Arc<dyn TransferActions>,
    pub liquidator_principal: Principal,
    pub max_sell_slippage_bps: f64,
    /// Minimum USD notional allowed for any CEX execution slice.
    pub cex_min_exec_usd: f64,
    /// Target fraction of slippage cap used for per-slice sizing.
    pub cex_slice_target_ratio: f64,
    approve_bumps: Mutex<HashMap<String, u8>>,
    market_locks: TokioMutex<HashMap<String, Arc<TokioMutex<()>>>>,
}

/// Number of orderbook levels used for impact simulation.
/// Higher = better estimate, slower API calls.
const DEFAULT_ORDERBOOK_LIMIT: u32 = 50;
/// Number of binary-search iterations for max-slice estimation.
const SLICE_SEARCH_STEPS: usize = 24;
pub const APPROVE_BUMP_MAX_COUNT: u8 = 6;
const APPROVE_BUMP_BATCH_SIZE: u8 = 3;
const APPROVE_BUMP_DELAY_MS: u64 = 300;
const APPROVE_BUMP_BATCH_DELAY_SECS: u64 = 3;
pub const MEXC_DEPOSIT_FEE_MULTIPLIER: u8 = 7;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    pub fn new(
        backend: Arc<C>,
        transfer_service: Arc<dyn TransferActions>,
        liquidator_principal: Principal,
        max_sell_slippage_bps: f64,
        cex_min_exec_usd: f64,
        cex_slice_target_ratio: f64,
    ) -> Self {
        Self {
            backend,
            transfer_service,
            liquidator_principal,
            max_sell_slippage_bps,
            cex_min_exec_usd,
            cex_slice_target_ratio,
            approve_bumps: Mutex::new(HashMap::new()),
            market_locks: TokioMutex::new(HashMap::new()),
        }
    }
}

#[path = "helper_methods.rs"]
mod helper_methods;

#[async_trait]
impl<B> CexFinalizerLogic for MexcFinalizer<B>
where
    B: CexBackend,
{
    async fn prepare(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<CexState, String> {
        let amount = &receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| "missing liquidation result".to_string())?
            .amounts
            .collateral_received;

        let size_in = ChainTokenAmount {
            token: receipt.request.collateral_asset.clone(),
            value: amount.clone(),
        };

        Ok(CexState {
            liq_id: liq_id.to_string(),
            step: CexStep::Deposit,
            last_error: None,

            // deposit leg
            deposit_asset: receipt.request.collateral_asset.clone(),
            deposit_txid: None,
            deposit_balance_before: None,
            approval_bump_count: None,

            // trade leg
            market: format!(
                "{}_{}",
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol()
            ),
            side: "sell".to_string(),
            size_in,
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

            // withdraw leg
            withdraw_asset: receipt.request.debt_asset.clone(),
            withdraw_address: self.liquidator_principal.to_text(),
            withdraw_id: None,
            withdraw_txid: None,
            size_out: None,
        })
    }

    async fn deposit(&self, state: &mut CexState) -> Result<(), String> {
        debug!(
            "[mexc] liq_id={} step=Deposit asset={} network={}",
            state.liq_id,
            state.deposit_asset,
            state.deposit_asset.chain()
        );

        // Phase A: no transfer yet -> snapshot balance and send once, then stay in Deposit.
        if state.deposit_txid.is_none() {
            let symbol = state.deposit_asset.symbol();
            let baseline = match self.backend.get_balance(&symbol).await {
                Ok(bal) => bal,
                Err(e) => {
                    debug!(
                        "[mexc] liq_id={} could not get baseline balance before deposit: {} (using 0.0)",
                        state.liq_id, e
                    );
                    0.0
                }
            };

            debug!(
                "[mexc] liq_id={} baseline balance before deposit: {}",
                state.liq_id, baseline
            );

            state.deposit_balance_before = Some(baseline);

            let addr = self
                .backend
                .get_deposit_address(&state.deposit_asset.symbol(), &state.deposit_asset.chain())
                .await?;

            debug!(
                "[mexc] liq_id={} got deposit address={} tag={:?}",
                state.liq_id, addr.address, addr.tag
            );

            let amount = &state.size_in;

            let fee = state.deposit_asset.fee() * MEXC_DEPOSIT_FEE_MULTIPLIER; // TODO: Remove after mexc handles deposit confirations
            let fee_amount = ChainTokenAmount::from_raw(state.deposit_asset.clone(), fee.clone());

            let zero = Nat::from(0u8);
            let transfer_value = if fee > zero {
                if amount.value.clone() <= fee {
                    return Err(format!(
                        "deposit amount {} too small to cover fee {} for {}",
                        amount.formatted(),
                        fee_amount.formatted(),
                        state.deposit_asset.symbol()
                    ));
                }
                amount.value.clone() - fee.clone()
            } else {
                amount.value.clone()
            };
            let transfer_amount = ChainTokenAmount::from_raw(state.deposit_asset.clone(), transfer_value.clone());
            let total_amount =
                ChainTokenAmount::from_raw(state.deposit_asset.clone(), transfer_value.clone() + fee.clone());

            debug!(
                "[mexc] liq_id={} deposit transfer requested={} fee={} net_transfer={} total_debit={}",
                state.liq_id,
                amount.formatted(),
                fee_amount.formatted(),
                transfer_amount.formatted(),
                total_amount.formatted()
            );

            let actions = &self.transfer_service;

            debug!(
                "[mexc] liq_id={} transferring {} address={}",
                state.liq_id, transfer_value, addr.address
            );

            let tx_id = actions
                .transfer(
                    &state.deposit_asset,
                    &ChainAccount::Icp(Account {
                        owner: Principal::from_text(addr.address)
                            .map_err(|e| format!("invalid deposit address: {e}"))?,
                        subaccount: None,
                    }),
                    transfer_value.clone(),
                )
                .await?;

            state.trade_next_amount_in = Some(transfer_amount.to_f64());

            debug!("[mexc] liq_id={} sent deposit txid={}", state.liq_id, tx_id);

            if matches!(state.deposit_asset, ChainToken::Icp { .. }) {
                let approved = self.maybe_bump_mexc_approval(&state.liq_id, &state.deposit_asset).await;
                if approved > 0 {
                    state.approval_bump_count = Some(approved);
                }
            } else {
                debug!(
                    "[mexc] liq_id={} deposit transfer: approve bump skipped (non-ICP asset={})",
                    state.liq_id, state.deposit_asset
                );
            }

            state.deposit_txid = Some(tx_id);
            state.step = CexStep::DepositPending;

            // Keep step as Deposit. WAL will persist, and a later finalize run
            // will call check_deposit to see if funds landed on the CEX.
            return Ok(());
        }

        // Phase B: transfer already sent -> just check whether it is now credited.
        self.check_deposit(state).await
    }

    /// Execute one resumable trade invocation for the current CEX route leg.
    ///
    /// This method orchestrates leg selection and state transitions, while delegating
    /// slice-by-slice execution to `execute_trade_leg_slices`.
    ///
    /// Behavior summary:
    /// - resolves direct vs multi-hop route for `deposit_asset -> withdraw_asset`
    /// - resumes from `trade_leg_index` and `trade_next_amount_in` when present
    /// - serializes execution per market via a market lock
    /// - enforces slippage and dust controls during sliced execution
    /// - updates `state` so next invocation continues at the correct leg/step
    ///
    /// Slice execution algorithm (implemented in `execute_trade_leg_slices`):
    /// 1. Compute a preferred chunk under the soft target (`target_bps`), using
    ///    orderbook simulation + binary search.
    /// 2. If no soft-target chunk exists, run a one-shot fallback check on the full
    ///    remainder against the hard cap (`max_sell_slippage_bps`).
    /// 3. Skip execution when chunk/remainder notional is below `cex_min_exec_usd`
    ///    (dust), and carry forward the partial result.
    /// 4. Execute one market slice, derive realized execution price from fills, and
    ///    compute realized slippage versus preview midpoint.
    /// 5. Abort the leg when realized slice slippage exceeds the hard cap.
    /// 6. Record per-slice telemetry and update weighted route-level notional/slippage.
    /// 7. Repeat until remainder is consumed or marked dust, then return `(remaining_in, total_out)`.
    ///
    /// Success path:
    /// - moves to `TradePending` for next leg, or `Withdraw` when route is complete
    ///
    /// Error path:
    /// - returns `Err` and stores a descriptive `state.last_error` where applicable
    async fn trade(&self, state: &mut CexState) -> Result<(), String> {
        // 1) Trace function entry with the currently persisted trade snapshot.
        debug!(
            "[mexc] liq_id={} step=Trade market={} side={} size_in={}",
            state.liq_id,
            state.market,
            state.side,
            state.size_in.formatted(),
        );

        // 2) Resolve the full conversion route (single leg or multi-hop).
        let legs = self.resolve_trade_legs(state).await?;

        // 3) Persist total route length for telemetry / resumability.
        state.trade_leg_total = Some(legs.len() as u32);

        // 4) Log explicit multi-hop route details when applicable.
        if legs.len() > 1 {
            debug!("[mexc] liq_id={} multi-hop route: {:?}", state.liq_id, legs);
        }

        // 5) For one-leg routes, normalize state market/side to the resolved leg.
        if legs.len() == 1 {
            state.market = legs[0].market.clone();
            state.side = legs[0].side.clone();
        }

        // 6) Figure out which leg to process now (resumable index).
        let idx = state.trade_leg_index.unwrap_or(0) as usize;

        // 7) If index is already past route end, go straight to withdraw step.
        if idx >= legs.len() {
            state.step = CexStep::Withdraw;
            // 8) Carry forward already computed output amount if present.
            if let Some(out_amt) = state.trade_next_amount_in {
                state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), out_amt));
            }
            return Ok(());
        }

        // 9) Input for this leg is either previous leg output or initial deposit size.
        let amount_in = state.trade_next_amount_in.unwrap_or_else(|| state.size_in.to_f64());

        // 10) Nothing to trade (or dust below epsilon) -> finish trading phase.
        if amount_in <= LIQUIDITY_EPS {
            debug!(
                "[mexc] liq_id={} trade skipped: non-positive amount_in={}",
                state.liq_id, amount_in
            );
            state.step = CexStep::Withdraw;
            return Ok(());
        }

        // 11) Pick current leg by index.
        let leg = &legs[idx];

        // 12) Store current-leg context fields (`trade_last_*`, clear `last_error`).
        Self::set_trade_leg_context(state, leg, amount_in);

        // 13) Log leg-level execution start.
        debug!(
            "[mexc] liq_id={} trade leg {}/{} market={} side={} amount_in={}",
            state.liq_id,
            idx + 1,
            legs.len(),
            leg.market,
            leg.side,
            amount_in
        );

        // Per-market queue to avoid self-impact by concurrent liquidations on the same book.
        let _market_guard = self.acquire_market_lock(&leg.market).await;

        // Target impact per slice = configured fraction of hard cap.
        let target_bps = self.target_slice_bps();

        // 14) Execute this leg via shared slice runner.
        //     The helper repeatedly:
        //     - previews a safe chunk from live orderbook depth,
        //     - skips tiny residual chunks below `cex_min_exec_usd` as dust,
        //     - executes market orders and validates realized slippage,
        //     - records per-slice telemetry, then returns `(remaining_in, total_out)`.
        let (remaining_in, total_out) = self
            .execute_trade_leg_slices(state, leg, idx, legs.len(), amount_in, target_bps)
            .await?;

        // 15) Emit end-of-leg summary including residual and dust info.
        info!(
            "[mexc] liq_id={} trade leg {}/{} completed market={} side={} amount_in={} amount_out={} residual_in={} dust_skipped={} dust_usd={:?}",
            state.liq_id,
            idx + 1,
            legs.len(),
            leg.market,
            leg.side,
            amount_in,
            total_out,
            remaining_in,
            state.trade_dust_skipped,
            state.trade_dust_usd
        );

        // 16) Move to next leg or to withdraw (and set `size_out` when route is done).
        Self::advance_after_trade_leg(state, idx, legs.len(), total_out);
        // 17) Successful completion of this trade invocation.
        Ok(())
    }

    async fn withdraw(&self, state: &mut CexState) -> Result<(), String> {
        let expected_address = self.liquidator_principal.to_text();
        if state.withdraw_address != expected_address {
            debug!(
                "[mexc] liq_id={} override withdraw_address {} -> {}",
                state.liq_id, state.withdraw_address, expected_address
            );
            state.withdraw_address = expected_address;
        }

        debug!(
            "[mexc] liq_id={} step=Withdraw asset={} network={} address={}",
            state.liq_id,
            state.withdraw_asset,
            state.withdraw_asset.chain(),
            state.withdraw_address,
        );

        // Idempotency: if we already have a withdraw id or txid, just advance.
        if state.withdraw_id.is_some() || state.withdraw_txid.is_some() {
            debug!(
                "[mexc] liq_id={} withdraw already recorded (id={:?}, txid={:?}), skipping",
                state.liq_id, state.withdraw_id, state.withdraw_txid,
            );
            state.step = CexStep::Completed;
            return Ok(());
        }

        // Amount to withdraw: prefer post-trade output when available.
        let amount = state
            .size_out
            .as_ref()
            .map(|out| out.to_f64())
            .unwrap_or_else(|| state.size_in.to_f64());

        if amount <= 0.0 {
            return Err(format!(
                "withdrawal amount is zero or negative for liq_id {} (amount={})",
                state.liq_id, amount
            ));
        }

        // Execute withdrawal on MEXC via the backend.
        let receipt = self
            .backend
            .withdraw(
                &state.withdraw_asset.symbol(),
                &state.withdraw_asset.chain(),
                &state.withdraw_address,
                amount,
            )
            .await?;

        debug!(
            "[mexc] liq_id={} withdraw executed: asset={} network={} amount={} txid={:?} internal_id={:?}",
            state.liq_id,
            state.withdraw_asset.symbol(),
            state.withdraw_asset.chain(),
            amount,
            receipt.txid,
            receipt.internal_id,
        );

        // Persist identifiers for idempotency.
        state.withdraw_id = receipt.internal_id.clone();
        state.withdraw_txid = receipt.txid.clone();

        // Mark the CEX leg as completed.
        state.step = CexStep::Completed;
        Ok(())
    }

    async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {
        let receive_amount = state
            .size_out
            .clone()
            .ok_or_else(|| "receive amount missing".to_string())?;

        // Pay side: seized collateral we sent in, as recorded on the CEX state.
        let pay_amount = state.size_in.clone();

        // Compute an effective execution price in native units: receive / pay.
        let pay_f = pay_amount.to_f64();
        let recv_f = receive_amount.to_f64();

        let exec_price = if pay_f > 0.0 { recv_f / pay_f } else { 0.0 };
        let mid_price =
            if let (Some(mid_sum), Some(exec_sum)) = (state.trade_mid_notional_sum, state.trade_exec_notional_sum) {
                if exec_sum > LIQUIDITY_EPS {
                    (mid_sum / exec_sum) * exec_price
                } else {
                    exec_price
                }
            } else {
                exec_price
            };

        let slippage = state.trade_weighted_slippage_bps.unwrap_or(0.0);
        let legs: Vec<SwapQuoteLeg> = state
            .trade_slices
            .iter()
            .map(|slice| {
                let (base, quote) = parse_market_symbols(&slice.market).unwrap_or_else(|| {
                    (
                        state.deposit_asset.symbol().to_ascii_uppercase(),
                        state.withdraw_asset.symbol().to_ascii_uppercase(),
                    )
                });
                let (pay_symbol, recv_symbol, pay_amount, recv_amount) = if slice.side.eq_ignore_ascii_case("sell") {
                    (base, quote, slice.amount_in, slice.amount_out)
                } else {
                    (quote, base, slice.amount_in, slice.amount_out)
                };

                SwapQuoteLeg {
                    venue: "mexc".to_string(),
                    route_id: slice.market.clone(),
                    pay_chain: state.deposit_asset.chain(),
                    pay_symbol,
                    pay_amount: f64_to_nat(pay_amount),
                    receive_chain: state.withdraw_asset.chain(),
                    receive_symbol: recv_symbol,
                    receive_amount: f64_to_nat(recv_amount),
                    price: slice.exec_price,
                    lp_fee: Nat::from(0u8),
                    gas_fee: Nat::from(0u8),
                }
            })
            .collect();

        let exec = SwapExecution {
            swap_id: 0,
            request_id: 0,
            status: "completed".to_string(),
            pay_asset: state.deposit_asset.asset_id(),
            pay_amount: pay_amount.value,
            receive_asset: state.withdraw_asset.asset_id(),
            receive_amount: receive_amount.value,
            mid_price,
            exec_price,
            slippage,
            legs,
            approval_count: state.approval_bump_count,
            ts: now_ts().max(0) as u64,
        };

        Ok(exec)
    }

    async fn preview_route(&self, receipt: &ExecutionReceipt) -> Result<CexRoutePreview, String> {
        let state = self.prepare("preview", receipt).await?;
        let legs = self.resolve_trade_legs(&state).await?;
        let mut amount_in = state.size_in.to_f64();
        if amount_in <= LIQUIDITY_EPS {
            return Ok(CexRoutePreview {
                is_executable: false,
                estimated_receive_amount: 0.0,
                estimated_slippage_bps: 0.0,
                reason: Some("non-positive amount_in".to_string()),
            });
        }

        let mut weighted_slippage_sum = 0.0;
        let mut weighted_notional_usd = 0.0;
        for leg in &legs {
            let (out, _avg_price, impact_bps) = self.preview_leg(&leg.market, &leg.side, amount_in).await?;
            let leg_notional_usd = self
                .input_slice_usd(&leg.market, &leg.side, amount_in)
                .await
                .unwrap_or(0.0);
            weighted_slippage_sum += impact_bps * leg_notional_usd;
            weighted_notional_usd += leg_notional_usd;
            amount_in = out;
        }

        let route_slippage_bps = if weighted_notional_usd > LIQUIDITY_EPS {
            weighted_slippage_sum / weighted_notional_usd
        } else {
            0.0
        };

        Ok(CexRoutePreview {
            is_executable: true,
            estimated_receive_amount: amount_in,
            estimated_slippage_bps: route_slippage_bps,
            reason: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use candid::{Nat, Principal};
    use liquidium_pipeline_connectors::backend::cex_backend::{
        DepositAddress, MockCexBackend, OrderBook, OrderBookLevel,
    };
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::transfer::actions::MockTransferActions;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };

    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::cex_finalizer::{CexState, CexStep};
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
    use proptest::prelude::*;

    const TEST_MAX_SELL_SLIPPAGE_BPS: f64 = 200.0;
    /// Minimum USD chunk used by test finalizer instances.
    /// Keep tiny so tests do not trigger dust skipping unless explicitly intended.
    const TEST_CEX_MIN_EXEC_USD: f64 = 0.0001;
    /// Slice target ratio used by test finalizer instances.
    const TEST_CEX_SLICE_TARGET_RATIO: f64 = 0.7;

    fn make_execution_receipt(liq_id: u128) -> ExecutionReceipt {
        let collateral_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let debt_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };

        let liquidation = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u32),
            receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
            buy_bad_debt: false,
        };

        let liq_result = LiquidationResult {
            id: liq_id,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(1_000_000u64),
                debt_repaid: Nat::from(2_000_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
        };

        let req = ExecutorRequest {
            liquidation,
            swap_args: None,
            debt_asset: debt_token.clone(),
            collateral_asset: collateral_token.clone(),
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_approval_needed: false,
        };

        ExecutionReceipt {
            request: req,
            liquidation_result: Some(liq_result),
            status: ExecutionStatus::Success,
            change_received: false,
        }
    }

    #[tokio::test]
    async fn mexc_prepare_builds_initial_cex_state() {
        let backend = Arc::new(MockCexBackend::new());
        let transfer_service = Arc::new(MockTransferActions::new());
        let liquidator = Principal::anonymous();

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            liquidator,
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let state: CexState = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        assert_eq!(state.liq_id, "42");
        assert!(matches!(state.step, CexStep::Deposit));

        // deposit leg
        assert_eq!(state.deposit_asset, receipt.request.collateral_asset);
        assert!(state.deposit_txid.is_none());
        assert!(state.deposit_balance_before.is_none());

        // trade leg
        let expected_market = format!(
            "{}_{}",
            receipt.request.collateral_asset.symbol(),
            receipt.request.debt_asset.symbol()
        );
        assert_eq!(state.market, expected_market);
        assert_eq!(state.side, "sell");
        assert_eq!(state.size_in.token, receipt.request.collateral_asset);
        assert_eq!(
            state.size_in.value,
            receipt.liquidation_result.as_ref().unwrap().amounts.collateral_received
        );

        // withdraw leg
        assert_eq!(state.withdraw_asset, receipt.request.debt_asset);
        assert_eq!(state.withdraw_address, liquidator.to_text());
        assert!(state.withdraw_id.is_none());
        assert!(state.withdraw_txid.is_none());
        assert!(state.size_out.is_none());
    }

    #[tokio::test]
    async fn mexc_deposit_phase_a_snapshots_baseline_and_sends_transfer() {
        let mut backend = MockCexBackend::new();
        let mut transfers = MockTransferActions::new();

        backend.expect_get_balance().returning(|_symbol| Ok(10.0));

        backend.expect_get_deposit_address().returning(|_symbol, _chain| {
            Ok(DepositAddress {
                asset: "CkBTC".to_string(),
                network: "ICP".to_string(),
                address: "aaaaa-aa".to_string(),
                tag: None,
            })
        });

        transfers
            .expect_transfer()
            .returning(|_token, _to, _amount| Ok("tx-123".to_string()));
        transfers
            .expect_approve()
            .times(6)
            .returning(|_token, _spender, _amount| Ok("approve-1".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Pre-conditions: no deposit has been sent yet
        assert!(state.deposit_txid.is_none());
        assert!(state.deposit_balance_before.is_none());
        assert!(matches!(state.step, CexStep::Deposit));

        // Phase A: snapshot baseline and send transfer
        finalizer.deposit(&mut state).await.expect("deposit should succeed");

        assert_eq!(state.deposit_balance_before, Some(10.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_without_baseline_sets_baseline_and_keeps_step() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend.expect_get_balance().returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Simulate that Phase A already ran and sent a tx, but baseline was never recorded.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = None;
        state.step = CexStep::DepositPending;

        // Phase B: deposit() delegates to check_deposit, which should set the baseline
        // and keep the step unchanged.
        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_moves_to_trade_when_balance_increased() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend.expect_get_balance().returning(|_symbol| Ok(5.1));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Simulate that Phase A already ran, we have a baseline, and we are now in DepositPending.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = Some(5.0);
        state.step = CexStep::DepositPending;

        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        // Baseline should remain unchanged, and we should advance to Trade when balance increased.
        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_stays_in_deposit_when_balance_unchanged() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Balance stays the same as baseline.
        backend.expect_get_balance().returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Simulate Phase A done, baseline recorded, and we are waiting in DepositPending.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = Some(5.0);
        state.step = CexStep::DepositPending;

        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        // Since balance did not increase enough, we should still be in DepositPending.
        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_trade_skips_when_amount_in_zero_and_moves_to_withdraw() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // When amount_in <= 0, execute_swap must never be called.
        backend.expect_execute_swap().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Force amount_in to zero and move state into Trade step.
        state.size_in.value = Nat::from(0u32);
        state.step = CexStep::Trade;

        finalizer
            .trade(&mut state)
            .await
            .expect("trade should succeed even when skipped");

        // No size_out set and step advanced to Withdraw.
        assert!(state.size_out.is_none());
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_executes_swap_and_sets_size_out_and_step_withdraw() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let orderbook = OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 1_000.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 1_000.0,
            }],
        };
        backend
            .expect_get_orderbook()
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let calls = std::sync::Arc::new(std::sync::Mutex::new(0usize));
        let calls_handle = calls.clone();
        backend
            .expect_execute_swap()
            .times(4)
            .returning(move |market, side, amount_in| {
                let mut idx = calls_handle.lock().unwrap();
                let cur = *idx;
                *idx += 1;

                match cur {
                    0 => {
                        assert_eq!(market, "CKBTC_BTC");
                        assert_eq!(side, "sell");
                        assert!(amount_in > 0.0);
                        Ok(amount_in * 0.9999)
                    }
                    1 => {
                        assert_eq!(market, "BTC_USDC");
                        assert_eq!(side, "sell");
                        assert!(amount_in > 0.0);
                        Ok(amount_in * 0.9998)
                    }
                    2 => {
                        assert_eq!(market, "USDC_USDT");
                        assert_eq!(side, "sell");
                        assert!(amount_in > 0.0);
                        Ok(amount_in * 0.9997)
                    }
                    3 => {
                        assert_eq!(market, "CKUSDT_USDT");
                        assert_eq!(side, "buy");
                        assert!(amount_in > 0.0);
                        Ok(amount_in / 1.0012)
                    }
                    _ => unreachable!("unexpected execute_swap call"),
                }
            });

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Move directly into Trade step; size_in is taken from receipt and should be > 0.
        state.step = CexStep::Trade;

        finalizer.trade(&mut state).await.expect("trade leg 1 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 2 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 3 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 4 should succeed");

        let out = state.size_out.as_ref().expect("size_out should be set");
        assert_eq!(out.token, state.withdraw_asset);
        assert!(out.to_f64() > 0.0);
        assert_eq!(state.trade_slices.len(), 4);
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_propagates_backend_errors() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let orderbook = OrderBook {
            bids: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1_000.0,
            }],
            asks: vec![OrderBookLevel {
                price: 101.0,
                quantity: 1_000.0,
            }],
        };
        backend
            .expect_get_orderbook()
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        backend
            .expect_execute_swap()
            .times(1)
            .returning(|_market, _side, _amount_in| Err("boom".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        state.step = CexStep::Trade;

        let err = finalizer.trade(&mut state).await.expect_err("trade should fail");

        assert_eq!(err, "boom");
        // On error we expect the step to remain Trade.
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_trade_marks_dust_and_skips_execution_for_small_residual() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let ckbtc_btc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 100.0,
            }],
        };
        let btc_usdc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 69_000.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 69_010.0,
                quantity: 100.0,
            }],
        };

        backend
            .expect_get_orderbook()
            .returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
                "BTC_USDC" => Ok(btc_usdc.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

        // Dust path should break before any market execution.
        backend.expect_execute_swap().times(0);

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            // 0.01 ckBTC ~= 690 USD at mocked conversion, so this forces dust skip.
            1_000.0,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        // Force single-leg route: CKBTC -> BTC.
        state.withdraw_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        finalizer
            .trade(&mut state)
            .await
            .expect("trade should succeed with dust skip");

        assert!(state.trade_dust_skipped);
        assert!(state.trade_dust_usd.unwrap_or_default() > 0.0);
        assert!(state.trade_slices.is_empty());
        assert!(matches!(state.step, CexStep::Withdraw));
        let out = state.size_out.as_ref().expect("size_out should be set");
        assert_eq!(out.to_f64(), 0.0);
    }

    #[tokio::test]
    async fn mexc_trade_fails_when_realized_slice_slippage_exceeds_cap() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let ckbtc_btc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 100.0,
            }],
        };
        let btc_usdc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 69_000.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 69_010.0,
                quantity: 100.0,
            }],
        };

        backend
            .expect_get_orderbook()
            .returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
                "BTC_USDC" => Ok(btc_usdc.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

        backend
            .expect_execute_swap()
            .times(1)
            .returning(|_market, _side, amount_in| Ok(amount_in * 0.9));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            // Keep cap tight so the 10% execution drift fails loudly.
            50.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        state.withdraw_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let err = finalizer
            .trade(&mut state)
            .await
            .expect_err("trade should fail when realized slippage exceeds cap");

        assert!(err.contains("slice slippage too high"));
        assert!(
            state
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("slice slippage too high")
        );
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_trade_retries_current_leg_from_original_amount_after_mid_leg_error() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let ckbtc_btc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 100.0,
            }],
        };
        let btc_usdc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 69_000.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 69_010.0,
                quantity: 100.0,
            }],
        };

        backend
            .expect_get_orderbook()
            .returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
                "BTC_USDC" => Ok(btc_usdc.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

        let call_count = Arc::new(std::sync::Mutex::new(0usize));
        let seen_amounts = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
        let call_count_handle = call_count.clone();
        let seen_amounts_handle = seen_amounts.clone();
        backend
            .expect_execute_swap()
            .times(2)
            .returning(move |_market, _side, amount_in| {
                let mut idx = call_count_handle.lock().unwrap();
                *idx += 1;
                seen_amounts_handle.lock().unwrap().push(amount_in);
                if *idx == 1 {
                    // First attempt intentionally fails post-trade slippage check.
                    Ok(amount_in * 0.95)
                } else {
                    Ok(amount_in * 0.999)
                }
            });

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            200.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        state.withdraw_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let first_err = finalizer
            .trade(&mut state)
            .await
            .expect_err("first trade attempt should fail on slippage");
        assert!(first_err.contains("slice slippage too high"));
        assert!(matches!(state.step, CexStep::Trade));

        // Retry same state; this should replay from the original leg amount.
        finalizer
            .trade(&mut state)
            .await
            .expect("second trade attempt should succeed");

        let amounts = seen_amounts.lock().unwrap();
        assert_eq!(amounts.len(), 2);
        assert!((amounts[0] - 0.01).abs() < 1e-12);
        assert!((amounts[1] - 0.01).abs() < 1e-12);
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_uses_resume_amount_when_trade_next_amount_in_is_present() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let ckbtc_btc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 1.001,
                quantity: 100.0,
            }],
        };
        let btc_usdc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 69_000.0,
                quantity: 100.0,
            }],
            asks: vec![OrderBookLevel {
                price: 69_010.0,
                quantity: 100.0,
            }],
        };

        backend
            .expect_get_orderbook()
            .returning(move |market, _limit| match market {
                "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
                "BTC_USDC" => Ok(btc_usdc.clone()),
                _ => Err(format!("unexpected market {}", market)),
            });

        let seen = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
        let seen_handle = seen.clone();
        backend
            .expect_execute_swap()
            .times(1)
            .returning(move |_market, _side, amount_in| {
                seen_handle.lock().unwrap().push(amount_in);
                Ok(amount_in * 0.999)
            });

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        // Force a direct single-leg route: CKBTC -> BTC.
        state.withdraw_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        state.trade_next_amount_in = Some(0.0042);

        finalizer.trade(&mut state).await.expect("trade should succeed");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!((seen[0] - 0.0042).abs() < 1e-12);
        assert!((state.trade_last_amount_in.unwrap_or_default() - 0.0042).abs() < 1e-12);
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_when_leg_index_is_past_route_moves_to_withdraw_and_sets_size_out() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend.expect_get_orderbook().times(0);
        backend.expect_execute_swap().times(0);

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        // Route for CKBTC -> CKUSDT has 4 legs, so this index is out-of-range.
        state.trade_leg_index = Some(99);
        state.trade_next_amount_in = Some(12.345678);

        finalizer.trade(&mut state).await.expect("trade should short-circuit");

        assert!(matches!(state.step, CexStep::Withdraw));
        let out = state.size_out.as_ref().expect("size_out should be carried forward");
        assert_eq!(out.token, state.withdraw_asset);
        assert!((out.to_f64() - 12.345678).abs() < 1e-12);
    }

    #[tokio::test]
    async fn mexc_trade_errors_when_direct_market_cannot_be_resolved() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let empty_book = OrderBook {
            bids: vec![],
            asks: vec![],
        };

        backend.expect_get_orderbook().times(2).returning(move |_market, _limit| Ok(empty_book.clone()));
        backend.expect_execute_swap().times(0);

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
        state.step = CexStep::Trade;
        // Force non-special pair so resolver must probe direct books.
        state.deposit_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        state.withdraw_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ETH".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        state.size_in = ChainTokenAmount::from_formatted(state.deposit_asset.clone(), 0.01);

        let err = finalizer
            .trade(&mut state)
            .await
            .expect_err("trade should fail when no direct leg can be resolved");

        assert!(err.contains("could not resolve direct market"));
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_sell_binary_search_finds_largest_chunk_under_target() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Threshold shape:
        // - up to 1.0 base filled at 100
        // - remainder filled at 90
        // For target=500 bps, largest valid chunk is ~2.0.
        let orderbook = OrderBook {
            bids: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 1.0,
                },
                OrderBookLevel {
                    price: 90.0,
                    quantity: 10.0,
                },
            ],
            asks: vec![],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            2_000.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "BTC_USDC".to_string(),
            side: "sell".to_string(),
        };

        let preview = finalizer
            .preview_trade_slice(&leg, 3.0, 500.0)
            .await
            .expect("sell preview should succeed");

        assert!((preview.chunk_in - 2.0).abs() < 0.01);
        assert!(preview.preview_impact_bps <= 500.0 + 0.01);
        assert!(preview.preview_impact_bps > 450.0);
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_buy_binary_search_finds_largest_chunk_under_target() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Threshold shape:
        // - first 100 quote buys at 100
        // - remaining quote buys at 120
        // For target=500 bps, largest valid chunk is ~140 quote.
        let orderbook = OrderBook {
            bids: vec![],
            asks: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 1.0,
                },
                OrderBookLevel {
                    price: 120.0,
                    quantity: 10.0,
                },
            ],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            2_000.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "CKUSDT_USDT".to_string(),
            side: "buy".to_string(),
        };

        let preview = finalizer
            .preview_trade_slice(&leg, 240.0, 500.0)
            .await
            .expect("buy preview should succeed");

        assert!((preview.chunk_in - 140.0).abs() < 0.05);
        assert!(preview.preview_impact_bps <= 500.0 + 0.01);
        assert!(preview.preview_impact_bps > 450.0);
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_sell_fallback_uses_full_remaining_when_hard_cap_allows() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // First level has zero quantity at better price, so any positive amount fills at 90
        // while best bid is still 100, forcing 1000 bps impact for all >0 chunks.
        let orderbook = OrderBook {
            bids: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 0.0,
                },
                OrderBookLevel {
                    price: 90.0,
                    quantity: 10.0,
                },
            ],
            asks: vec![],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            1_500.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "BTC_USDC".to_string(),
            side: "sell".to_string(),
        };

        // target=0 makes binary search return ~0 chunk; fallback should allow full remaining
        // because hard cap (1500 bps) is above realized impact (1000 bps).
        let preview = finalizer
            .preview_trade_slice(&leg, 1.0, 0.0)
            .await
            .expect("fallback should allow full remaining");

        assert!((preview.chunk_in - 1.0).abs() < 1e-12);
        assert!((preview.preview_impact_bps - 1_000.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_sell_fallback_errors_when_hard_cap_rejects() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let orderbook = OrderBook {
            bids: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 0.0,
                },
                OrderBookLevel {
                    price: 90.0,
                    quantity: 10.0,
                },
            ],
            asks: vec![],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            500.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "BTC_USDC".to_string(),
            side: "sell".to_string(),
        };

        let err = finalizer
            .preview_trade_slice(&leg, 1.0, 0.0)
            .await
            .expect_err("fallback should reject when hard cap is breached");

        assert!(err.contains("cannot find sell chunk under impact target"));
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_buy_fallback_uses_full_remaining_when_hard_cap_allows() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Mirror of sell fallback shape on ask side: best ask at 100 with zero quantity,
        // actual fills at 120 -> 2000 bps impact for any positive buy chunk.
        let orderbook = OrderBook {
            bids: vec![],
            asks: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 0.0,
                },
                OrderBookLevel {
                    price: 120.0,
                    quantity: 10.0,
                },
            ],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            2_500.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "CKUSDT_USDT".to_string(),
            side: "buy".to_string(),
        };

        let preview = finalizer
            .preview_trade_slice(&leg, 120.0, 0.0)
            .await
            .expect("buy fallback should allow full remaining");

        assert!((preview.chunk_in - 120.0).abs() < 1e-12);
        assert!((preview.preview_impact_bps - 2_000.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn mexc_preview_trade_slice_buy_fallback_errors_when_hard_cap_rejects() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let orderbook = OrderBook {
            bids: vec![],
            asks: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 0.0,
                },
                OrderBookLevel {
                    price: 120.0,
                    quantity: 10.0,
                },
            ],
        };

        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            500.0,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let leg = TradeLeg {
            market: "CKUSDT_USDT".to_string(),
            side: "buy".to_string(),
        };

        let err = finalizer
            .preview_trade_slice(&leg, 120.0, 0.0)
            .await
            .expect_err("buy fallback should reject when hard cap is breached");

        assert!(err.contains("cannot find buy chunk under impact target"));
    }

    #[tokio::test]
    async fn mexc_withdraw_is_idempotent_when_already_recorded() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // No withdraw should be executed if we already have identifiers.
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        state.step = CexStep::Withdraw;
        state.withdraw_id = Some("internal-1".to_string());
        state.withdraw_txid = Some("tx-1".to_string());

        finalizer
            .withdraw(&mut state)
            .await
            .expect("withdraw should succeed idempotently");

        assert!(matches!(state.step, CexStep::Completed));
    }

    #[tokio::test]
    async fn mexc_withdraw_fails_on_non_positive_amount() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Backend must not be called when amount <= 0.
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        state.step = CexStep::Withdraw;
        state.size_in.value = Nat::from(0u32);

        let err = finalizer
            .withdraw(&mut state)
            .await
            .expect_err("withdraw should fail for non-positive amount");

        assert!(err.contains("withdrawal amount is zero or negative"));
    }

    #[tokio::test]
    async fn mexc_finish_builds_synthetic_swap_execution_from_state() {
        let backend = Arc::new(MockCexBackend::new());
        let transfers = Arc::new(MockTransferActions::new());

        let finalizer = MexcFinalizer::new(
            backend,
            transfers,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Simulate that trade/withdraw legs have populated size_out.
        // Use a nice round native amount so we can reason about the price.
        state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), 2.0));

        let swap = finalizer.finish(&receipt, &state).await.expect("finish should succeed");

        // Pay leg comes from seized collateral (size_in).
        assert_eq!(swap.pay_asset, state.deposit_asset.asset_id());
        assert_eq!(swap.pay_amount, state.size_in.value);

        // Receive leg comes from size_out.
        let expected_out = state.size_out.as_ref().unwrap();
        assert_eq!(swap.receive_asset, state.withdraw_asset.asset_id());
        assert_eq!(swap.receive_amount, expected_out.value);

        // Price is computed as receive / pay in native units.
        let pay_native = state.size_in.to_f64();
        let recv_native = expected_out.to_f64();
        let expected_price = if pay_native > 0.0 {
            recv_native / pay_native
        } else {
            0.0
        };

        assert!((swap.exec_price - expected_price).abs() < 1e-9);
        assert!((swap.mid_price - expected_price).abs() < 1e-9);

        // Status and legs should reflect a single synthetic CEX hop.
        assert_eq!(swap.status, "completed".to_string());
        assert!(swap.legs.is_empty());
    }

    #[tokio::test]
    async fn mexc_preview_route_returns_non_executable_for_zero_amount() {
        let backend = Arc::new(MockCexBackend::new());
        let transfers = Arc::new(MockTransferActions::new());
        let finalizer = MexcFinalizer::new(
            backend,
            transfers,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let mut receipt = make_execution_receipt(42);
        if let Some(liq) = receipt.liquidation_result.as_mut() {
            liq.amounts.collateral_received = Nat::from(0u8);
        }

        let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
        assert!(!preview.is_executable);
        assert_eq!(preview.estimated_receive_amount, 0.0);
        assert_eq!(preview.estimated_slippage_bps, 0.0);
        assert_eq!(preview.reason.as_deref(), Some("non-positive amount_in"));
    }

    #[tokio::test]
    async fn mexc_preview_route_resolves_direct_buy_leg_when_sell_book_empty() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Direct sell market BTC_CKBTC has no bids -> resolver should choose buy market CKBTC_BTC.
        let btc_ckbtc = OrderBook {
            bids: vec![],
            asks: vec![],
        };
        let ckbtc_btc = OrderBook {
            bids: vec![],
            asks: vec![OrderBookLevel {
                price: 1.0,
                quantity: 10.0,
            }],
        };
        let btc_usdc = OrderBook {
            bids: vec![OrderBookLevel {
                price: 69_000.0,
                quantity: 10.0,
            }],
            asks: vec![OrderBookLevel {
                price: 69_010.0,
                quantity: 10.0,
            }],
        };

        backend.expect_get_orderbook().returning(move |market, _limit| match market {
            "BTC_CKBTC" => Ok(btc_ckbtc.clone()),
            "CKBTC_BTC" => Ok(ckbtc_btc.clone()),
            "BTC_USDC" => Ok(btc_usdc.clone()),
            _ => Err(format!("unexpected market {}", market)),
        });

        let finalizer = MexcFinalizer::new(
            Arc::new(backend),
            Arc::new(transfers),
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
            TEST_CEX_MIN_EXEC_USD,
            TEST_CEX_SLICE_TARGET_RATIO,
        );

        let mut receipt = make_execution_receipt(42);
        receipt.request.collateral_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "BTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        receipt.request.debt_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let preview = finalizer.preview_route(&receipt).await.expect("preview should succeed");
        assert!(preview.is_executable);
        assert!(preview.estimated_receive_amount > 0.0);
        assert!(preview.estimated_slippage_bps >= 0.0);
    }

    mod fuzz {
        use super::*;

        proptest! {
            #[test]
            fn prop_trade_single_leg_success_invariants(
                amount_sats in 1_000u64..=2_000_000u64,
                btc_usd in 20_000u64..=120_000u64,
                exec_loss_bps in 0u64..=80u64
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = MockCexBackend::new();
                    let transfers = MockTransferActions::new();

                    let ckbtc_btc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                        asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                    };
                    let btc_usdc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: btc_usd as f64, quantity: 1000.0 }],
                        asks: vec![OrderBookLevel { price: btc_usd as f64 + 1.0, quantity: 1000.0 }],
                    };

                    backend.expect_get_orderbook().returning(move |market, _limit| match market {
                        "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                        "BTC_USDC" => Ok(btc_usdc_book.clone()),
                        _ => Err(format!("unexpected market {}", market)),
                    });

                    let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
                    backend.expect_execute_swap().times(1).returning(move |_market, _side, amount_in| {
                        Ok(amount_in * factor)
                    });

                    let finalizer = MexcFinalizer::new(
                        Arc::new(backend),
                        Arc::new(transfers),
                        Principal::anonymous(),
                        200.0,
                        TEST_CEX_MIN_EXEC_USD,
                        TEST_CEX_SLICE_TARGET_RATIO,
                    );

                    let mut receipt = make_execution_receipt(42);
                    if let Some(liq) = receipt.liquidation_result.as_mut() {
                        liq.amounts.collateral_received = Nat::from(amount_sats);
                    }

                    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
                    state.step = CexStep::Trade;
                    // Force single leg CKBTC->BTC for deterministic fuzz invariants.
                    state.withdraw_asset = ChainToken::Icp {
                        ledger: Principal::anonymous(),
                        symbol: "BTC".to_string(),
                        decimals: 8,
                        fee: Nat::from(1_000u64),
                    };

                    finalizer.trade(&mut state).await.expect("trade should succeed");

                    let input = amount_sats as f64 / 100_000_000.0;
                    let output = state.size_out.as_ref().expect("size_out must exist").to_f64();

                    assert!(matches!(state.step, CexStep::Withdraw));
                    assert_eq!(state.trade_slices.len(), 1);
                    assert!(output > 0.0);
                    assert!(output <= input + 1e-12);
                    assert!(state.last_error.is_none());
                });
            }

            #[test]
            fn prop_trade_single_leg_slippage_breach_sets_error(
                amount_sats in 1_000u64..=2_000_000u64,
                exec_loss_bps in 250u64..=1500u64
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = MockCexBackend::new();
                    let transfers = MockTransferActions::new();

                    let ckbtc_btc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                        asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                    };
                    let btc_usdc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                        asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                    };

                    backend.expect_get_orderbook().returning(move |market, _limit| match market {
                        "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                        "BTC_USDC" => Ok(btc_usdc_book.clone()),
                        _ => Err(format!("unexpected market {}", market)),
                    });

                    let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
                    backend.expect_execute_swap().times(1).returning(move |_market, _side, amount_in| {
                        Ok(amount_in * factor)
                    });

                    let finalizer = MexcFinalizer::new(
                        Arc::new(backend),
                        Arc::new(transfers),
                        Principal::anonymous(),
                        200.0,
                        TEST_CEX_MIN_EXEC_USD,
                        TEST_CEX_SLICE_TARGET_RATIO,
                    );

                    let mut receipt = make_execution_receipt(42);
                    if let Some(liq) = receipt.liquidation_result.as_mut() {
                        liq.amounts.collateral_received = Nat::from(amount_sats);
                    }

                    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
                    state.step = CexStep::Trade;
                    state.withdraw_asset = ChainToken::Icp {
                        ledger: Principal::anonymous(),
                        symbol: "BTC".to_string(),
                        decimals: 8,
                        fee: Nat::from(1_000u64),
                    };

                    let err = finalizer.trade(&mut state).await.expect_err("trade should fail");
                    assert!(err.contains("slice slippage too high"));
                    assert!(matches!(state.step, CexStep::Trade));
                    assert!(state
                        .last_error
                        .as_deref()
                        .unwrap_or_default()
                        .contains("slice slippage too high"));
                });
            }

            #[test]
            fn prop_trade_single_leg_multi_slice_progresses(
                first_level_sat in 50_000u64..=300_000u64,
                multiplier in 3u64..=8u64,
                exec_loss_bps in 0u64..=20u64
            ) {
                // Remaining amount is 3x..8x first-level depth, which should require multiple
                // slices under the configured target.
                let total_sats = first_level_sat * multiplier;
                let q1 = first_level_sat as f64 / 100_000_000.0;
                let total_btc = total_sats as f64 / 100_000_000.0;

                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = MockCexBackend::new();
                    let transfers = MockTransferActions::new();

                    let ckbtc_btc_book = OrderBook {
                        bids: vec![
                            OrderBookLevel { price: 1.0, quantity: q1 },
                            OrderBookLevel { price: 0.99, quantity: 10.0 },
                        ],
                        asks: vec![OrderBookLevel { price: 1.001, quantity: 10.0 }],
                    };
                    let btc_usdc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                        asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                    };

                    backend.expect_get_orderbook().returning(move |market, _limit| match market {
                        "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                        "BTC_USDC" => Ok(btc_usdc_book.clone()),
                        _ => Err(format!("unexpected market {}", market)),
                    });

                    let calls = Arc::new(std::sync::Mutex::new(0usize));
                    let calls_handle = calls.clone();
                    let factor = 1.0 - (exec_loss_bps as f64 / 10_000.0);
                    backend.expect_execute_swap().returning(move |_market, _side, amount_in| {
                        *calls_handle.lock().unwrap() += 1;
                        Ok(amount_in * factor)
                    });

                    let finalizer = MexcFinalizer::new(
                        Arc::new(backend),
                        Arc::new(transfers),
                        Principal::anonymous(),
                        200.0,
                        TEST_CEX_MIN_EXEC_USD,
                        // target=50 bps when max=200 bps, forcing chunking on this two-level book.
                        0.25,
                    );

                    let mut receipt = make_execution_receipt(42);
                    if let Some(liq) = receipt.liquidation_result.as_mut() {
                        liq.amounts.collateral_received = Nat::from(total_sats);
                    }

                    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
                    state.step = CexStep::Trade;
                    state.withdraw_asset = ChainToken::Icp {
                        ledger: Principal::anonymous(),
                        symbol: "BTC".to_string(),
                        decimals: 8,
                        fee: Nat::from(1_000u64),
                    };

                    finalizer.trade(&mut state).await.expect("trade should succeed");

                    let call_count = *calls.lock().unwrap();
                    assert!(call_count > 1);
                    assert!(state.trade_slices.len() > 1);
                    assert!(matches!(state.step, CexStep::Withdraw));
                    assert!(state.size_out.as_ref().unwrap().to_f64() > 0.0);
                    assert!(state.size_out.as_ref().unwrap().to_f64() <= total_btc + 1e-12);
                });
            }

            #[test]
            fn prop_trade_retry_replays_same_leg_input_after_first_failure(
                amount_sats in 1_000u64..=2_000_000u64,
                failing_loss_bps in 250u64..=2_000u64,
                recovery_loss_bps in 0u64..=80u64
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = MockCexBackend::new();
                    let transfers = MockTransferActions::new();

                    let ckbtc_btc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 1.0, quantity: 100.0 }],
                        asks: vec![OrderBookLevel { price: 1.001, quantity: 100.0 }],
                    };
                    let btc_usdc_book = OrderBook {
                        bids: vec![OrderBookLevel { price: 69_000.0, quantity: 1000.0 }],
                        asks: vec![OrderBookLevel { price: 69_010.0, quantity: 1000.0 }],
                    };

                    backend.expect_get_orderbook().returning(move |market, _limit| match market {
                        "CKBTC_BTC" => Ok(ckbtc_btc_book.clone()),
                        "BTC_USDC" => Ok(btc_usdc_book.clone()),
                        _ => Err(format!("unexpected market {}", market)),
                    });

                    let call_count = Arc::new(std::sync::Mutex::new(0usize));
                    let seen_amounts = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
                    let call_count_handle = call_count.clone();
                    let seen_amounts_handle = seen_amounts.clone();
                    let fail_factor = 1.0 - (failing_loss_bps as f64 / 10_000.0);
                    let recovery_factor = 1.0 - (recovery_loss_bps as f64 / 10_000.0);
                    backend.expect_execute_swap().times(2).returning(move |_market, _side, amount_in| {
                        let mut idx = call_count_handle.lock().unwrap();
                        seen_amounts_handle.lock().unwrap().push(amount_in);
                        let out = if *idx == 0 {
                            amount_in * fail_factor
                        } else {
                            amount_in * recovery_factor
                        };
                        *idx += 1;
                        Ok(out)
                    });

                    let finalizer = MexcFinalizer::new(
                        Arc::new(backend),
                        Arc::new(transfers),
                        Principal::anonymous(),
                        200.0,
                        TEST_CEX_MIN_EXEC_USD,
                        TEST_CEX_SLICE_TARGET_RATIO,
                    );

                    let mut receipt = make_execution_receipt(42);
                    if let Some(liq) = receipt.liquidation_result.as_mut() {
                        liq.amounts.collateral_received = Nat::from(amount_sats);
                    }

                    let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");
                    state.step = CexStep::Trade;
                    // Force single-leg route for deterministic retry assertions.
                    state.withdraw_asset = ChainToken::Icp {
                        ledger: Principal::anonymous(),
                        symbol: "BTC".to_string(),
                        decimals: 8,
                        fee: Nat::from(1_000u64),
                    };

                    let first = finalizer.trade(&mut state).await.expect_err("first attempt should fail");
                    assert!(first.contains("slice slippage too high"));
                    assert!(matches!(state.step, CexStep::Trade));

                    finalizer.trade(&mut state).await.expect("second attempt should recover");

                    let seen = seen_amounts.lock().unwrap();
                    assert_eq!(seen.len(), 2);
                    assert!((seen[0] - seen[1]).abs() < 1e-12);
                    assert!(matches!(state.step, CexStep::Withdraw));
                    assert!(state.size_out.as_ref().unwrap().to_f64() > 0.0);
                });
            }
        }
    }
}
