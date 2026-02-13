use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackend, OrderBookLevel, SwapExecutionOptions,
};
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
    error::AppResult,
    finalizers::cex_finalizer::{
        CexDepositState, CexFinalizerLogic, CexRoutePreview, CexState, CexStep, CexTradeSlice, CexTradeState,
        CexWithdrawState,
    },
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
    /// Buy truncation trigger ratio for quote->inverse fallback.
    pub cex_buy_truncation_trigger_ratio: f64,
    /// Overspend cap for inverse/base buy fallback, in bps.
    pub cex_buy_inverse_overspend_bps: u32,
    /// Maximum inverse fallback retries per leg.
    pub cex_buy_inverse_max_retries: u32,
    /// Enables inverse buy fallback after truncation.
    pub cex_buy_inverse_enabled: bool,
    /// `approve_bumps` is only touched in short synchronous sections.
    approve_bumps: Mutex<HashMap<String, u8>>,
    /// `market_locks` is acquired/held in async trade flow, so it uses Tokio's async mutex.
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
#[allow(dead_code)]
const DEFAULT_BUY_TRUNCATION_TRIGGER_RATIO: f64 = 0.25;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_OVERSPEND_BPS: u32 = 10;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_MAX_RETRIES: u32 = 1;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_ENABLED: bool = true;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    // used in tests
    #[allow(unused)]
    pub fn new(
        backend: Arc<C>,
        transfer_service: Arc<dyn TransferActions>,
        liquidator_principal: Principal,
        max_sell_slippage_bps: f64,
        cex_min_exec_usd: f64,
        cex_slice_target_ratio: f64,
    ) -> Self {
        Self::new_with_tunables(
            backend,
            transfer_service,
            liquidator_principal,
            max_sell_slippage_bps,
            cex_min_exec_usd,
            cex_slice_target_ratio,
            DEFAULT_BUY_TRUNCATION_TRIGGER_RATIO,
            DEFAULT_BUY_INVERSE_OVERSPEND_BPS,
            DEFAULT_BUY_INVERSE_MAX_RETRIES,
            DEFAULT_BUY_INVERSE_ENABLED,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tunables(
        backend: Arc<C>,
        transfer_service: Arc<dyn TransferActions>,
        liquidator_principal: Principal,
        max_sell_slippage_bps: f64,
        cex_min_exec_usd: f64,
        cex_slice_target_ratio: f64,
        cex_buy_truncation_trigger_ratio: f64,
        cex_buy_inverse_overspend_bps: u32,
        cex_buy_inverse_max_retries: u32,
        cex_buy_inverse_enabled: bool,
    ) -> Self {
        Self {
            backend,
            transfer_service,
            liquidator_principal,
            max_sell_slippage_bps,
            cex_min_exec_usd,
            cex_slice_target_ratio,
            cex_buy_truncation_trigger_ratio,
            cex_buy_inverse_overspend_bps,
            cex_buy_inverse_max_retries,
            cex_buy_inverse_enabled,
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
    async fn prepare(&self, liq_id: &str, receipt: &ExecutionReceipt) -> AppResult<CexState> {
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
            market: format!(
                "{}_{}",
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol()
            ),
            side: "sell".to_string(),
            size_in,
            deposit: CexDepositState {
                deposit_asset: receipt.request.collateral_asset.clone(),
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
                withdraw_asset: receipt.request.debt_asset.clone(),
                withdraw_address: self.liquidator_principal.to_text(),
                withdraw_id: None,
                withdraw_txid: None,
                size_out: None,
            },
        })
    }

    async fn deposit(&self, state: &mut CexState) -> AppResult<()> {
        debug!(
            "[mexc] liq_id={} step=Deposit asset={} network={}",
            state.liq_id,
            state.deposit.deposit_asset,
            state.deposit.deposit_asset.chain()
        );

        // Phase A: no transfer yet -> snapshot balance and send once, then stay in Deposit.
        if state.deposit.deposit_txid.is_none() {
            let symbol = state.deposit.deposit_asset.symbol();
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

            state.deposit.deposit_balance_before = Some(baseline);

            let addr = self
                .backend
                .get_deposit_address(
                    &state.deposit.deposit_asset.symbol(),
                    &state.deposit.deposit_asset.chain(),
                )
                .await?;

            debug!(
                "[mexc] liq_id={} got deposit address={} tag={:?}",
                state.liq_id, addr.address, addr.tag
            );

            let amount = &state.size_in;

            let fee = state.deposit.deposit_asset.fee() * MEXC_DEPOSIT_FEE_MULTIPLIER; // TODO: Remove after mexc handles deposit confirations
            let fee_amount = ChainTokenAmount::from_raw(state.deposit.deposit_asset.clone(), fee.clone());

            let zero = Nat::from(0u8);
            let transfer_value = if fee > zero {
                if amount.value.clone() <= fee {
                    return Err(format!(
                        "deposit amount {} too small to cover fee {} for {}",
                        amount.formatted(),
                        fee_amount.formatted(),
                        state.deposit.deposit_asset.symbol()
                    )
                    .into());
                }
                amount.value.clone() - fee.clone()
            } else {
                amount.value.clone()
            };
            let transfer_amount =
                ChainTokenAmount::from_raw(state.deposit.deposit_asset.clone(), transfer_value.clone());
            let total_amount = ChainTokenAmount::from_raw(
                state.deposit.deposit_asset.clone(),
                transfer_value.clone() + fee.clone(),
            );

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
                    &state.deposit.deposit_asset,
                    &ChainAccount::Icp(Account {
                        owner: Principal::from_text(addr.address)
                            .map_err(|e| format!("invalid deposit address: {e}"))?,
                        subaccount: None,
                    }),
                    transfer_value.clone(),
                )
                .await?;

            state.trade.trade_next_amount_in = Some(transfer_amount.to_f64());

            debug!("[mexc] liq_id={} sent deposit txid={}", state.liq_id, tx_id);

            if matches!(state.deposit.deposit_asset, ChainToken::Icp { .. }) {
                let approved = self
                    .maybe_bump_mexc_approval(&state.liq_id, &state.deposit.deposit_asset)
                    .await;
                if approved > 0 {
                    state.deposit.approval_bump_count = Some(approved);
                }
            } else {
                debug!(
                    "[mexc] liq_id={} deposit transfer: approve bump skipped (non-ICP asset={})",
                    state.liq_id, state.deposit.deposit_asset
                );
            }

            state.deposit.deposit_txid = Some(tx_id);
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
    async fn trade(&self, state: &mut CexState) -> AppResult<()> {
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
        state.trade.trade_leg_total = Some(legs.len() as u32);

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
        let idx = state.trade.trade_leg_index.unwrap_or(0) as usize;

        // 7) If index is already past route end, go straight to withdraw step.
        if idx >= legs.len() {
            state.step = CexStep::Withdraw;
            // 8) Carry forward already computed output amount if present.
            if let Some(out_amt) = state.trade.trade_next_amount_in {
                state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
                    state.withdraw.withdraw_asset.clone(),
                    out_amt,
                ));
            }
            return Ok(());
        }

        // 9) Input for this leg is either resumed residual, previous leg output, or initial deposit size.
        let amount_in = state.trade.trade_progress_remaining_in.unwrap_or_else(|| {
            state
                .trade
                .trade_next_amount_in
                .unwrap_or_else(|| state.size_in.to_f64())
        });

        // 10) Nothing to trade (or dust below epsilon) -> finish trading phase.
        if amount_in <= LIQUIDITY_EPS {
            debug!(
                "[mexc] liq_id={} trade skipped: non-positive amount_in={}",
                state.liq_id, amount_in
            );
            state.step = CexStep::Withdraw;
            if let Some(out_amt) = state
                .trade
                .trade_progress_total_out
                .or(state.trade.trade_next_amount_in)
            {
                state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
                    state.withdraw.withdraw_asset.clone(),
                    out_amt.max(0.0),
                ));
            }
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
            state.trade.trade_dust_skipped,
            state.trade.trade_dust_usd
        );

        // 16) Move to next leg or to withdraw (and set `size_out` when route is done).
        Self::advance_after_trade_leg(state, idx, legs.len(), total_out);
        // 17) Successful completion of this trade invocation.
        Ok(())
    }

    async fn withdraw(&self, state: &mut CexState) -> AppResult<()> {
        let expected_address = self.liquidator_principal.to_text();
        if state.withdraw.withdraw_address != expected_address {
            debug!(
                "[mexc] liq_id={} override withdraw_address {} -> {}",
                state.liq_id, state.withdraw.withdraw_address, expected_address
            );
            state.withdraw.withdraw_address = expected_address;
        }

        debug!(
            "[mexc] liq_id={} step=Withdraw asset={} network={} address={}",
            state.liq_id,
            state.withdraw.withdraw_asset,
            state.withdraw.withdraw_asset.chain(),
            state.withdraw.withdraw_address,
        );

        // Idempotency: if we already have a withdraw id or txid, just advance.
        if state.withdraw.withdraw_id.is_some() || state.withdraw.withdraw_txid.is_some() {
            debug!(
                "[mexc] liq_id={} withdraw already recorded (id={:?}, txid={:?}), skipping",
                state.liq_id, state.withdraw.withdraw_id, state.withdraw.withdraw_txid,
            );
            state.step = CexStep::Completed;
            return Ok(());
        }

        // Amount to withdraw: prefer post-trade output when available.
        let amount = state
            .withdraw
            .size_out
            .as_ref()
            .map(|out| out.to_f64())
            .unwrap_or_else(|| state.size_in.to_f64());

        if amount <= 0.0 {
            return Err(format!(
                "withdrawal amount is zero or negative for liq_id {} (amount={})",
                state.liq_id, amount
            )
            .into());
        }

        // Execute withdrawal on MEXC via the backend.
        let receipt = self
            .backend
            .withdraw(
                &state.withdraw.withdraw_asset.symbol(),
                &state.withdraw.withdraw_asset.chain(),
                &state.withdraw.withdraw_address,
                amount,
            )
            .await?;

        debug!(
            "[mexc] liq_id={} withdraw executed: asset={} network={} amount={} txid={:?} internal_id={:?}",
            state.liq_id,
            state.withdraw.withdraw_asset.symbol(),
            state.withdraw.withdraw_asset.chain(),
            amount,
            receipt.txid,
            receipt.internal_id,
        );

        // Persist identifiers for idempotency.
        state.withdraw.withdraw_id = receipt.internal_id.clone();
        state.withdraw.withdraw_txid = receipt.txid.clone();

        // Mark the CEX leg as completed.
        state.step = CexStep::Completed;
        Ok(())
    }

    async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> AppResult<SwapExecution> {
        let receive_amount = state
            .withdraw
            .size_out
            .clone()
            .ok_or_else(|| "receive amount missing".to_string())?;

        // Pay side: seized collateral we sent in, as recorded on the CEX state.
        let pay_amount = state.size_in.clone();

        // Compute an effective execution price in native units: receive / pay.
        let pay_f = pay_amount.to_f64();
        let recv_f = receive_amount.to_f64();

        let exec_price = if pay_f > 0.0 { recv_f / pay_f } else { 0.0 };
        let mid_price = if let (Some(mid_sum), Some(exec_sum)) =
            (state.trade.trade_mid_notional_sum, state.trade.trade_exec_notional_sum)
        {
            if exec_sum > LIQUIDITY_EPS {
                (mid_sum / exec_sum) * exec_price
            } else {
                exec_price
            }
        } else {
            exec_price
        };

        let slippage = state.trade.trade_weighted_slippage_bps.unwrap_or(0.0);
        let legs: Vec<SwapQuoteLeg> = state
            .trade
            .trade_slices
            .iter()
            .map(|slice| {
                let (base, quote) = parse_market_symbols(&slice.market).unwrap_or_else(|| {
                    (
                        state.deposit.deposit_asset.symbol().to_ascii_uppercase(),
                        state.withdraw.withdraw_asset.symbol().to_ascii_uppercase(),
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
                    pay_chain: state.deposit.deposit_asset.chain(),
                    pay_symbol,
                    pay_amount: f64_to_nat(pay_amount),
                    receive_chain: state.withdraw.withdraw_asset.chain(),
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
            pay_asset: state.deposit.deposit_asset.asset_id(),
            pay_amount: pay_amount.value,
            receive_asset: state.withdraw.withdraw_asset.asset_id(),
            receive_amount: receive_amount.value,
            mid_price,
            exec_price,
            slippage,
            legs,
            approval_count: state.deposit.approval_bump_count,
            ts: now_ts().max(0) as u64,
        };

        Ok(exec)
    }

    async fn preview_route(&self, receipt: &ExecutionReceipt) -> AppResult<CexRoutePreview> {
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
#[path = "mexc_finalizer_tests.rs"]
mod tests;
