use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::Address as EvmAddress;
use candid::{Nat, Principal};
use ic_ledger_types::{AccountIdentifier, Subaccount};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::amount_utils::nat_units_to_spend_amount;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeError, BridgeFeeBudget, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus,
    DepositNotice, cksol_transfer_without_deposit_id, resolve_route,
};
use liquidium_pipeline_connectors::outbound::UnsettledSender;
use liquidium_pipeline_core::outbound::{OutboundIntent, OutboundSender};
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackend, CexBackendError, OrderBookLevel, PriceLimit, SwapExecutionOptions, WithdrawStatus,
};
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait},
    transfer::actions::TransferActions,
};
use log::{debug, info, warn};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use super::mexc_utils::{LIQUIDITY_EPS, SlicePreview, TradeLeg, is_usd_stable_symbol, mexc_special_trade_legs};
use liquidium_pipeline_connectors::outbound::{AcceptedWithdrawal, CexWithdrawals, WithdrawRequest, prepare_cex_withdraw};

pub(super) const WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE: f64 = LIQUIDITY_EPS;
const NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN: f64 = 1e-15;
pub(super) const BRIDGE_FEE_TOKEN_SYMBOL: &str = "ckETH";
pub(super) const BRIDGE_FEE_TOKEN_CHAIN: &str = "ICP";
pub(super) const CKETH_DECIMALS: u8 = 18;
/// A direct withdrawal that MEXC accepted but never reports as delivered
/// must not wait forever. After this long the row parks for an operator.
const DIRECT_WITHDRAW_DELIVERY_TIMEOUT_SECS: i64 = 60 * 60;
/// How long a landed bridge deposit may wait for the provider to accept it.
/// The ckSOL minter reads it at finalized commitment, which takes seconds.
const DEPOSIT_NOTICE_WINDOW_SECS: i64 = 30 * 60;
const SOL_FINALITY_SECS: i64 = 20;

use crate::{
    bridge::runtime::BridgeRuntime,
    finalizers::bridge_planner::BridgePlanner,
    finalizers::cex_finalizer::{CexState, CexStep},
    outbound::driver::{OutboundError, OutboundRecord, step_outbound},
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    swappers::mexc::orderbook_quote::{simulate_buy_from_asks, simulate_sell_from_bids},
    utils::{ICP_LEDGER_PRINCIPAL, now_ts},
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
    /// Sends from the trader account that are saved before they run. Funding
    /// transfers go through it; it defaults to plain sends over `transfer_service`.
    pub(super) outbound: Arc<dyn OutboundSender>,
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
    /// Configured market universe used for route graph search.
    pub cex_mexc_available_pairs: Vec<String>,
    /// Maximum intermediate hops allowed in graph-based route discovery.
    pub cex_mexc_max_hops: usize,
    /// Extra end-to-end route costs included in conservative planner output.
    pub quote_route_fee_bps: u32,
    /// Price-move buffer for deposit/trade/withdraw latency.
    pub quote_delay_buffer_bps: u32,
    /// The shared bridge, when exchange legs may bridge. Carries the claim
    /// registry every venue consults before it attaches a lost handle.
    pub bridge: Option<Arc<BridgeRuntime>>,
    /// Resolves the receive-side `AssetId` supplied by the generic venue
    /// contract into the full chain token needed by deposits and withdrawals.
    /// Legacy receipt-based execution does not need this dependency.
    pub token_registry: Option<Arc<dyn TokenRegistryTrait>>,
    /// `approve_bumps` is only touched in short synchronous sections.
    approve_bumps: Mutex<HashMap<String, u8>>,
    /// `market_locks` is acquired/held in async trade flow, so it uses Tokio's async mutex.
    market_locks: TokioMutex<HashMap<String, Arc<TokioMutex<()>>>>,
    /// Process-local proof that a persisted multi-venue intent came from this
    /// live adapter. Missing proof after restart means the external call may
    /// already have happened, so the leg is parked instead of repeated.
    pub(super) multi_venue_armed_intents: Mutex<HashSet<String>>,
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
#[allow(dead_code)]
const DEFAULT_BUY_TRUNCATION_TRIGGER_RATIO: f64 = 0.25;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_OVERSPEND_BPS: u32 = 10;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_MAX_RETRIES: u32 = 1;
#[allow(dead_code)]
const DEFAULT_BUY_INVERSE_ENABLED: bool = true;
const DEFAULT_MEXC_MAX_HOPS: usize = 2;
const MAX_MEXC_MAX_HOPS: usize = 4;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    pub fn with_route_config(mut self, available_pairs: Vec<String>, max_hops: usize) -> Self {
        self.cex_mexc_available_pairs = Self::normalize_market_pairs(available_pairs);
        self.cex_mexc_max_hops = max_hops.min(MAX_MEXC_MAX_HOPS);
        self
    }

    pub fn with_outbound(mut self, outbound: Arc<dyn OutboundSender>) -> Self {
        self.outbound = outbound;
        self
    }

    /// The bridge runtime, for routes whose MEXC leg moves through a bridge.
    pub(super) fn bridge_runtime(&self) -> Result<&BridgeRuntime, String> {
        self.bridge
            .as_deref()
            .ok_or_else(|| "bridge runtime is not configured".to_string())
    }

    pub fn with_bridge_runtime(mut self, bridge: impl Into<Arc<BridgeRuntime>>) -> Self {
        self.bridge = Some(bridge.into());
        self
    }

    pub fn with_token_registry(mut self, token_registry: Arc<dyn TokenRegistryTrait>) -> Self {
        self.token_registry = Some(token_registry);
        self
    }

    pub fn with_quote_costs(mut self, route_fee_bps: u32, delay_buffer_bps: u32) -> Self {
        self.quote_route_fee_bps = route_fee_bps;
        self.quote_delay_buffer_bps = delay_buffer_bps;
        self
    }

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
            outbound: Arc::new(UnsettledSender::new(transfer_service.clone())),
            transfer_service,
            liquidator_principal,
            max_sell_slippage_bps,
            cex_min_exec_usd,
            cex_slice_target_ratio,
            cex_buy_truncation_trigger_ratio,
            cex_buy_inverse_overspend_bps,
            cex_buy_inverse_max_retries,
            cex_buy_inverse_enabled,
            cex_mexc_available_pairs: vec![],
            cex_mexc_max_hops: DEFAULT_MEXC_MAX_HOPS,
            quote_route_fee_bps: 0,
            quote_delay_buffer_bps: 0,
            bridge: None,
            token_registry: None,
            approve_bumps: Mutex::new(HashMap::new()),
            market_locks: TokioMutex::new(HashMap::new()),
            multi_venue_armed_intents: Mutex::new(HashSet::new()),
        }
    }
}

#[path = "bridge_planner_impl.rs"]
mod bridge_planner_impl;

#[path = "deposit/bridge.rs"]
mod deposit_bridge;
#[path = "deposit/confirmation.rs"]
mod deposit_check;
#[path = "deposit/destinations.rs"]
mod destinations;
#[path = "deposit/funding.rs"]
mod funding;
#[path = "trade/routing.rs"]
mod routing;
#[path = "sizing.rs"]
mod sizing;
#[path = "trade/execution.rs"]
mod slice_execution;
#[path = "trade/orders.rs"]
mod slice_orders;
#[path = "trade/records.rs"]
mod slice_records;
#[path = "trade/slicing.rs"]
mod slicing;
#[path = "withdraw/bridge.rs"]
mod withdraw_bridge;
#[path = "withdraw/direct.rs"]
mod withdraw_direct;

pub(super) use sizing::{BridgeDepositSizing, BridgedDepositArrival};
pub use withdraw_direct::mexc_withdraw_order_id;

/// Builds a leg's state from a liquidation receipt, as tests set legs up.
#[cfg(test)]
impl<B: CexBackend> MexcFinalizer<B> {
    pub(crate) async fn prepare(
        &self,
        liq_id: &str,
        receipt: &crate::stages::executor::ExecutionReceipt,
    ) -> Result<CexState, String> {
        let amount = &receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| "missing liquidation result".to_string())?
            .amounts
            .collateral_received;
        self.prepare_amount_scoped_state(
            liq_id,
            ChainTokenAmount::from_raw(receipt.request.collateral_asset.clone(), amount.clone()),
            receipt.request.debt_asset.clone(),
            None,
        )
    }
}

/// The CEX state machine one leg walks: deposit, trade, withdraw.
impl<B> MexcFinalizer<B>
where
    B: CexBackend,
{
    /// Runs the current step once.
    pub(super) async fn advance_current_step(&self, state: &mut CexState) -> Result<(), CexBackendError> {
        match state.step {
            CexStep::Deposit | CexStep::DepositPending => self.deposit(state).await,
            CexStep::Trade | CexStep::TradePending => self.trade(state).await,
            CexStep::Withdraw | CexStep::WithdrawPending => self.withdraw(state).await,
            CexStep::Completed | CexStep::Failed => Ok(()),
        }
    }

    /// On chain: collateral to the CEX deposit asset, sent to the deposit address.
    pub(super) async fn deposit(&self, state: &mut CexState) -> Result<(), CexBackendError> {
        self.ensure_plan_on_state(state);
        let planned_asset = <Self as BridgePlanner>::planned_deposit_asset(state);
        let planned_network = <Self as BridgePlanner>::planned_deposit_network(state);

        debug!(
            "[mexc] liq_id={} step=Deposit source_asset={} source_network={} cex_asset={} cex_network={} bridge_required={}",
            state.liq_id,
            state.deposit.deposit_asset,
            state.deposit.deposit_asset.chain(),
            planned_asset,
            planned_network,
            state.deposit.bridge.deposit_bridge_required
        );

        if !state.deposit.bridge.deposit_bridge_required {
            return self.deposit_direct(state, &planned_asset, &planned_network).await;
        }
        self.deposit_bridged(state, &planned_asset).await
    }

    /// Execute one resumable trade invocation for the current CEX route leg.
    ///
    /// Resolves the route, resumes at `trade_leg_index`, and runs the leg under
    /// the market lock. `execute_trade_leg_slices` has the slice algorithm.
    /// On success the state moves to `TradePending` for the next leg, or to
    /// `Withdraw` when the route is done. A paused leg resumes next cycle.
    /// On the CEX: the deposit asset to the withdraw asset.
    pub(super) async fn trade(&self, state: &mut CexState) -> Result<(), CexBackendError> {
        self.ensure_plan_on_state(state);
        state.market = format!(
            "{}_{}",
            <Self as BridgePlanner>::planned_deposit_asset(state),
            <Self as BridgePlanner>::planned_withdraw_asset(state)
        );

        // 1) Trace function entry with the currently persisted trade snapshot.
        debug!(
            "[mexc] liq_id={} step=Trade market={} side={} size_in={}",
            state.liq_id,
            state.market,
            state.side,
            state.size_in.formatted(),
        );

        // 2) Resolve the full conversion route (single leg or multi-hop).
        let legs = self.resolve_or_load_trade_legs(state).await?;

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
            // 8) Carry forward already computed output amount if present.
            let out_amt = state.trade.trade_next_amount_in;
            Self::finish_trade_route(state, out_amt);
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
            let out_amt = state
                .trade
                .trade_progress_total_out
                .or(state.trade.trade_next_amount_in)
                .map(|out_amt| out_amt.max(0.0));
            Self::finish_trade_route(state, out_amt);
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
        //     - executes IOC limit orders at the slippage cap,
        //     - records per-slice telemetry, then returns `(remaining_in, total_out)`.
        let Some((remaining_in, total_out)) = self
            .execute_trade_leg_slices(state, leg, idx, legs.len(), amount_in, target_bps)
            .await?
        else {
            // Paused with its progress saved; the next cycle resumes this leg.
            return Ok(());
        };

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

    /// From the CEX back to the chain.
    pub(super) async fn withdraw(&self, state: &mut CexState) -> Result<(), CexBackendError> {
        self.ensure_plan_on_state(state);
        let planned_asset = <Self as BridgePlanner>::planned_withdraw_asset(state);
        let planned_network = <Self as BridgePlanner>::planned_withdraw_network(state);

        debug!(
            "[mexc] liq_id={} step={:?} source_asset={} source_network={} final_asset={} final_network={} withdraw_address={} bridge_required={}",
            state.liq_id,
            state.step,
            planned_asset,
            planned_network,
            state.withdraw.withdraw_asset.symbol(),
            state.withdraw.withdraw_asset.chain(),
            state.withdraw.withdraw_address,
            state.withdraw.bridge.withdraw_bridge_required,
        );

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

        if !state.withdraw.bridge.withdraw_bridge_required {
            return self
                .withdraw_direct(state, &planned_asset, &planned_network, amount)
                .await;
        }
        self.withdraw_bridged(state, &planned_asset, &planned_network, amount)
            .await
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
