use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::Address as EvmAddress;
use async_trait::async_trait;
use candid::{Nat, Principal};
use ic_ledger_types::{AccountIdentifier, Subaccount};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeBackend, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus,
    FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX, resolve_route,
};
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackend, CexBackendError, OrderBookLevel, SwapExecutionOptions, WithdrawStatus,
};
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait},
    transfer::actions::TransferActions,
};
use log::{debug, info, warn};
use num_traits::ToPrimitive;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use super::mexc_utils::{LIQUIDITY_EPS, SlicePreview, TradeLeg, is_usd_stable_symbol, mexc_special_trade_legs};

const WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE: f64 = LIQUIDITY_EPS;
const NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN: f64 = 1e-15;
const MEXC_WITHDRAW_BELOW_MIN_RAW_CODE: i64 = 10254;
const BRIDGE_FEE_TOKEN_SYMBOL: &str = "ckETH";
const BRIDGE_FEE_TOKEN_CHAIN: &str = "ICP";
const CKETH_DECIMALS: i32 = 18;

use crate::{
    finalizers::bridge_planner::BridgePlanner,
    finalizers::cex_finalizer::{CexFinalizerLogic, CexRoutePreview, CexState, CexStep},
    stages::bridge_submit_lock::acquire_bridge_submit_lock,
    stages::executor::ExecutionReceipt,
    swappers::{
        mexc::orderbook_quote::{simulate_buy_from_asks, simulate_sell_from_bids},
        model::SwapExecution,
    },
    utils::{ICP_LEDGER_PRINCIPAL, now_ts},
};

#[derive(Debug, Clone)]
pub struct MexcBridgeConfig {
    pub bridge_ic_source_account: Account,
    pub bridge_evm_source_address: String,
    pub bridge_btc_source_address: String,
}

#[derive(Clone)]
pub struct MexcBridgeDependencies {
    pub backend: Arc<dyn BridgeBackend>,
    pub config: MexcBridgeConfig,
}

fn is_mexc_withdraw_below_min_error(err: &str) -> bool {
    // The CEX backend trait currently erases MEXC's typed error into a string.
    // Recover the stable raw code instead of matching MEXC's human-readable text.
    parse_mexc_raw_code(err) == Some(MEXC_WITHDRAW_BELOW_MIN_RAW_CODE)
}

fn parse_mexc_raw_code(err: &str) -> Option<i64> {
    let value = err.split("raw_code:").nth(1)?.trim_start();
    let end = value
        .find(|ch: char| !ch.is_ascii_digit() && ch != '-')
        .unwrap_or(value.len());
    value.get(..end)?.parse().ok()
}

fn nat_to_units(amount: &Nat, decimals: i32) -> Result<f64, String> {
    let raw = amount
        .0
        .to_f64()
        .ok_or_else(|| "bridge fee budget is too large to convert to decimal units".to_string())?;
    Ok(raw / 10f64.powi(decimals))
}

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
    /// Configured market universe used for route graph search.
    pub cex_mexc_available_pairs: Vec<String>,
    /// Maximum intermediate hops allowed in graph-based route discovery.
    pub cex_mexc_max_hops: usize,
    /// Extra end-to-end route costs included in conservative planner output.
    pub quote_route_fee_bps: u32,
    /// Price-move buffer for deposit/trade/withdraw latency.
    pub quote_delay_buffer_bps: u32,
    /// Optional bridge runtime used by liquidation-linked bridge submit/poll flows.
    pub bridge: Option<MexcBridgeDependencies>,
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
pub const MEXC_DEPOSIT_FEE_MULTIPLIER: u8 = 7;
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
    fn is_native_icp_token(token: &ChainToken) -> bool {
        match token {
            ChainToken::Icp { ledger, symbol, .. } => {
                symbol.eq_ignore_ascii_case("ICP")
                    && Principal::from_text(ICP_LEDGER_PRINCIPAL).is_ok_and(|icp_ledger| *ledger == icp_ledger)
            }
            _ => false,
        }
    }

    fn principal_default_account_id_hex(owner: Principal) -> String {
        AccountIdentifier::new(&owner, &Subaccount([0; 32])).to_hex()
    }

    fn validate_icp_account_id_hex(address: &str, context: &str) -> Result<String, String> {
        let normalized = address.trim().to_ascii_lowercase();
        AccountIdentifier::from_hex(&normalized).map_err(|err| {
            format!(
                "invalid native ICP ledger account id '{}' for {}: {}",
                address, context, err
            )
        })?;
        Ok(normalized)
    }

    fn native_icp_direct_withdraw_address(&self, requested_destination: &str) -> Result<String, String> {
        let requested_destination = requested_destination.trim();
        if requested_destination.is_empty() {
            return Ok(Self::principal_default_account_id_hex(self.liquidator_principal));
        }
        if let Ok(account_id) = Self::validate_icp_account_id_hex(requested_destination, "MEXC withdraw") {
            return Ok(account_id);
        }

        let account = <Self as BridgePlanner>::parse_icp_account_like(requested_destination)?;
        let subaccount = Subaccount(account.subaccount.unwrap_or([0; 32]));
        Ok(AccountIdentifier::new(&account.owner, &subaccount).to_hex())
    }

    fn complete_withdraw_as_below_min_dust(
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
        err: &str,
    ) {
        let msg = format!(
            "mexc withdraw below minimum; treating residual as CEX dust for liq_id {} asset={} network={} amount={} err={}",
            state.liq_id, planned_asset, planned_network, amount, err
        );
        warn!("{}", msg);
        state.last_error = Some(msg);
        state.withdraw.size_out = Some(ChainTokenAmount::from_raw(
            state.withdraw.withdraw_asset.clone(),
            Nat::from(0u8),
        ));
        state.step = CexStep::Completed;
    }

    fn direct_deposit_destination_account(
        token: &ChainToken,
        planned_network: &str,
        address: &str,
    ) -> Result<ChainAccount, String> {
        let destination = address.trim();
        if destination.is_empty() {
            return Err(format!(
                "invalid MEXC deposit address for network '{}': address is empty",
                planned_network
            ));
        }

        match token {
            ChainToken::Icp { .. } => {
                if !planned_network.eq_ignore_ascii_case("ICP") {
                    return Err(format!(
                        "unsupported MEXC deposit network '{}' for ICP asset {}; expected ICP",
                        planned_network,
                        token.symbol()
                    ));
                }
                if Self::is_native_icp_token(token) {
                    let account_id_hex = Self::validate_icp_account_id_hex(destination, "MEXC ICP deposit")?;
                    return Ok(ChainAccount::IcpLedger(account_id_hex));
                }
                let owner = Principal::from_text(destination).map_err(|e| {
                    format!(
                        "invalid MEXC ICP deposit address '{}' for network '{}': {e}",
                        destination, planned_network
                    )
                })?;
                Ok(ChainAccount::Icp(Account {
                    owner,
                    subaccount: None,
                }))
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if planned_network.eq_ignore_ascii_case("ICP") {
                    return Err(format!(
                        "unsupported MEXC deposit network '{}' for EVM asset {}; expected EVM-compatible network",
                        planned_network,
                        token.symbol()
                    ));
                }
                let evm_address = destination.parse::<EvmAddress>().map_err(|e| {
                    format!(
                        "invalid MEXC EVM deposit address '{}' for network '{}': {e}",
                        destination, planned_network
                    )
                })?;
                Ok(ChainAccount::Evm(evm_address.to_string()))
            }
        }
    }

    pub(super) fn compute_fee_adjusted_deposit_transfer(
        deposit_asset: &ChainToken,
        size_in: &ChainTokenAmount,
    ) -> Result<(Nat, ChainTokenAmount), String> {
        let fee = deposit_asset.fee() * MEXC_DEPOSIT_FEE_MULTIPLIER;
        let transfer_value = if fee > Nat::from(0u8) {
            if size_in.value.clone() <= fee {
                let fee_amount = ChainTokenAmount::from_raw(deposit_asset.clone(), fee.clone());
                return Err(format!(
                    "{}: deposit amount {} too small to cover fee {} for {}",
                    FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                    size_in.formatted(),
                    fee_amount.formatted(),
                    deposit_asset.symbol()
                ));
            }
            size_in.value.clone() - fee
        } else {
            size_in.value.clone()
        };
        let transfer_amount = ChainTokenAmount::from_raw(deposit_asset.clone(), transfer_value.clone());
        Ok((transfer_value, transfer_amount))
    }

    /// Computes the source-token reserve needed before submitting an ICP reverse bridge.
    ///
    /// Bridged deposits first transfer seized collateral into the configured bridge source account.
    /// For ckETH-minter reverse routes, that same source account must keep enough source token
    /// available for the bridge submit itself: ckERC20 routes need the source ledger approval fee,
    /// while native `ckETH -> ETH` also needs the quoted minter withdrawal fee budget. This helper
    /// returns the reserved amount and the net amount that can be submitted to the bridge.
    fn compute_reverse_bridge_source_fee_reserve(
        route: &BridgeRouteSpec,
        deposit_asset: &ChainToken,
        transfer_value: &Nat,
        transfer_amount: &ChainTokenAmount,
        source_fee_budget: f64,
    ) -> Result<Option<(ChainTokenAmount, ChainTokenAmount)>, String> {
        if !matches!(
            route.route_kind,
            BridgeRouteKind::CkEthErc20Reverse | BridgeRouteKind::CkEthToEth
        ) || !route.source_chain.eq_ignore_ascii_case("ICP")
            || !deposit_asset.symbol().eq_ignore_ascii_case(route.source_asset)
            || !deposit_asset.chain().eq_ignore_ascii_case(route.source_chain)
        {
            return Ok(None);
        }

        let source_fee_reserve = ChainTokenAmount::from_formatted(deposit_asset.clone(), source_fee_budget);
        if source_fee_reserve.value == Nat::from(0u8) {
            return Ok(None);
        }

        if transfer_value.clone() <= source_fee_reserve.value.clone() {
            return Err(format!(
                "{}: deposit amount {} too small to reserve reverse bridge source fee budget {} for {}",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                transfer_amount.formatted(),
                source_fee_reserve.formatted(),
                deposit_asset.symbol()
            ));
        }

        let bridge_amount_value = transfer_value.clone() - source_fee_reserve.value.clone();
        let bridge_amount = ChainTokenAmount::from_raw(deposit_asset.clone(), bridge_amount_value);
        Ok(Some((source_fee_reserve, bridge_amount)))
    }

    async fn ensure_minimum_bridge_amount(
        bridge: &MexcBridgeDependencies,
        route: &BridgeRouteSpec,
        amount: &ChainTokenAmount,
    ) -> Result<(), String> {
        let minimum = bridge
            .backend
            .get_minimum_bridge_amount(route.source_asset, route.source_chain, route.target_asset)
            .await?;
        if minimum <= 0.0 {
            return Ok(());
        }

        let current = amount.to_f64();
        if current < minimum {
            return Err(format!(
                "{}: {}@{} -> {} amount below minimum withdrawal (amount={} minimum={})",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                route.source_asset,
                route.source_chain,
                route.target_asset,
                current,
                minimum
            ));
        }

        Ok(())
    }

    fn compute_expected_bridge_deposit_amount(
        route: &BridgeRouteSpec,
        bridge_amount: f64,
        destination_fee_budget: f64,
    ) -> Result<f64, String> {
        if destination_fee_budget <= 0.0 {
            return Ok(bridge_amount);
        }

        if bridge_amount <= destination_fee_budget {
            return Err(format!(
                "{}: bridge amount {} too small to cover destination fee budget {} for {}@{} -> {}",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                bridge_amount,
                destination_fee_budget,
                route.source_asset,
                route.source_chain,
                route.target_asset
            ));
        }

        Ok(bridge_amount - destination_fee_budget)
    }

    fn normalize_market_pair(raw: &str) -> Option<String> {
        let normalized = raw.trim().replace(['/', '-'], "_").to_ascii_uppercase();
        if normalized.is_empty() {
            return None;
        }

        let mut parts = normalized.split('_').filter(|part| !part.is_empty());
        let base = parts.next()?;
        let quote = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        Some(format!("{}_{}", base, quote))
    }

    fn normalize_market_pairs(markets: Vec<String>) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for market in markets {
            let Some(normalized) = Self::normalize_market_pair(&market) else {
                continue;
            };
            if seen.insert(normalized.clone()) {
                out.push(normalized);
            }
        }
        out
    }

    pub fn with_route_config(mut self, available_pairs: Vec<String>, max_hops: usize) -> Self {
        self.cex_mexc_available_pairs = Self::normalize_market_pairs(available_pairs);
        self.cex_mexc_max_hops = max_hops.min(MAX_MEXC_MAX_HOPS);
        self
    }

    pub fn with_bridge_dependencies(mut self, bridge: MexcBridgeDependencies) -> Self {
        self.bridge = Some(bridge);
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

#[path = "helper_methods.rs"]
mod helper_methods;

#[async_trait]
impl<B> CexFinalizerLogic for MexcFinalizer<B>
where
    B: CexBackend,
{
    fn venue_id(&self) -> &'static str {
        "mexc"
    }

    async fn prepare(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<CexState, String> {
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

    async fn deposit(&self, state: &mut CexState) -> Result<(), String> {
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
            // Direct deposit path: existing behavior.
            if state.deposit.deposit_txid.is_none() {
                let baseline = match self.backend.get_balance(&planned_asset).await {
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
                    .get_deposit_address(&planned_asset, &planned_network)
                    .await?;

                if Self::is_native_icp_token(&state.deposit.deposit_asset)
                    && addr.tag.as_deref().is_some_and(|tag| !tag.trim().is_empty())
                {
                    let tag = addr.tag.clone().unwrap_or_default();
                    return Err(format!(
                        "native ICP MEXC deposit returned unsupported memo/tag for liq_id {} asset={} network={} address={} tag={}",
                        state.liq_id, planned_asset, planned_network, addr.address, tag
                    ));
                }

                state.deposit.bridge.deposit_bridge_destination_snapshot = Some(addr.address.clone());

                let (transfer_value, transfer_amount) =
                    Self::compute_fee_adjusted_deposit_transfer(&state.deposit.deposit_asset, &state.size_in)?;
                let destination = Self::direct_deposit_destination_account(
                    &state.deposit.deposit_asset,
                    &planned_network,
                    &addr.address,
                )?;

                let tx_id = self
                    .transfer_service
                    .transfer(&state.deposit.deposit_asset, &destination, transfer_value.clone())
                    .await
                    .map_err(|error| error.to_string())?;

                state.trade.trade_next_amount_in = Some(transfer_amount.to_f64());
                state.deposit.deposit_txid = Some(tx_id);
                state.deposit.deposit_sent_at_ts = Some(now_ts());

                if matches!(state.deposit.deposit_asset, ChainToken::Icp { .. })
                    && !Self::is_native_icp_token(&state.deposit.deposit_asset)
                {
                    let approved = self
                        .maybe_bump_mexc_approval(&state.liq_id, &state.deposit.deposit_asset)
                        .await;

                    if approved > 0 {
                        state.deposit.approval_bump_count = Some(approved);
                    }
                }

                state.step = CexStep::DepositPending;
                return Ok(());
            }

            return self.check_deposit(state).await;
        }

        let bridge = self
            .bridge
            .as_ref()
            .ok_or_else(|| "bridge runtime not configured for bridged deposit".to_string())?;
        let source_symbol = state.deposit.deposit_asset.symbol();
        let source_chain = state.deposit.deposit_asset.chain();
        let route = resolve_route(&source_symbol, &source_chain, &planned_asset).ok_or_else(|| {
            format!(
                "bridge route not found for deposit {}@{} -> {}",
                source_symbol, source_chain, planned_asset
            )
        })?;

        let mut deposit_fee_budget = None;

        // Phase A for bridged deposit: transfer seized collateral into configured bridge source.
        if state.deposit.deposit_txid.is_none() {
            let baseline = match self.backend.get_balance(&planned_asset).await {
                Ok(bal) => bal,
                Err(e) => {
                    debug!(
                        "[mexc] liq_id={} could not get baseline balance before bridged deposit: {} (using 0.0)",
                        state.liq_id, e
                    );
                    0.0
                }
            };
            state.deposit.deposit_balance_before = Some(baseline);

            let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
            let bridge_destination = <Self as BridgePlanner>::bridge_source_transfer_destination(
                route.source_chain,
                &bridge_source_address,
            )?;

            let (transfer_value, mut transfer_amount) =
                Self::compute_fee_adjusted_deposit_transfer(&state.deposit.deposit_asset, &state.size_in)?;

            let fee_budget = bridge
                .backend
                .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
                .await?;

            if let Some((source_fee_reserve, bridge_amount)) = Self::compute_reverse_bridge_source_fee_reserve(
                route,
                &state.deposit.deposit_asset,
                &transfer_value,
                &transfer_amount,
                fee_budget.source_fee_budget,
            )? {
                transfer_amount = bridge_amount;
                let source_transfer_amount =
                    ChainTokenAmount::from_raw(state.deposit.deposit_asset.clone(), transfer_value.clone());
                info!(
                    "[mexc] liq_id={} bridged deposit reserving reverse bridge source fee budget: reserve={} source_transfer={} bridge_amount={} route={}@{}->{}",
                    state.liq_id,
                    source_fee_reserve.formatted(),
                    source_transfer_amount.formatted(),
                    transfer_amount.formatted(),
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
            }

            Self::ensure_minimum_bridge_amount(bridge, route, &transfer_amount).await?;
            let bridge_submit_amount = transfer_amount.to_f64();
            let bridge_expected_amount = Self::compute_expected_bridge_deposit_amount(
                route,
                bridge_submit_amount,
                fee_budget.destination_fee_budget,
            )?;

            let tx_id = self
                .transfer_service
                .transfer(&state.deposit.deposit_asset, &bridge_destination, transfer_value)
                .await
                .map_err(|error| error.to_string())?;

            state.deposit.bridge.deposit_bridge_submit_amount = Some(bridge_submit_amount);
            state.deposit.bridge.deposit_bridge_expected_amount = Some(bridge_expected_amount);
            state.deposit.bridge.deposit_bridge_provider_fee_budget_native_units =
                fee_budget.provider_fee_budget_native_units.clone();
            deposit_fee_budget = Some(fee_budget);
            state.deposit.deposit_txid = Some(tx_id);
            state.deposit.deposit_sent_at_ts = Some(now_ts());
        }

        // Submit bridge once, then transition to pending.
        if state.deposit.bridge.deposit_bridge_id.is_none() {
            let cex_destination = self
                .backend
                .get_deposit_address(&planned_asset, &planned_network)
                .await?;
            state.deposit.bridge.deposit_bridge_destination_snapshot = Some(cex_destination.address.clone());

            let bridge_destination = <Self as BridgePlanner>::route_destination_for_deposit(
                route.destination_kind,
                &cex_destination.address,
            )?;
            let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
            let bridge_amount = state
                .deposit
                .bridge
                .deposit_bridge_submit_amount
                .unwrap_or_else(|| state.size_in.to_f64());
            let _submit_guard =
                acquire_bridge_submit_lock(route.source_asset, route.source_chain, &bridge_source_address).await;

            let fee_budget = match deposit_fee_budget.take() {
                Some(fee_budget) => fee_budget,
                None => {
                    bridge
                        .backend
                        .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
                        .await?
                }
            };
            if state
                .deposit
                .bridge
                .deposit_bridge_provider_fee_budget_native_units
                .is_none()
            {
                state.deposit.bridge.deposit_bridge_provider_fee_budget_native_units =
                    fee_budget.provider_fee_budget_native_units.clone();
            }
            let provider_fee_budget_native_units = state
                .deposit
                .bridge
                .deposit_bridge_provider_fee_budget_native_units
                .clone()
                .or(fee_budget.provider_fee_budget_native_units.clone());

            let source_available = bridge
                .backend
                .get_source_balance(route.source_asset, route.source_chain, &bridge_source_address)
                .await?;
            let required_source_available = bridge_amount + fee_budget.source_fee_budget;
            if source_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < required_source_available {
                info!(
                    "[mexc] liq_id={} bridged deposit waiting for source funding: available={} expected={} tolerance={} source={} route={}@{}->{}",
                    state.liq_id,
                    source_available,
                    required_source_available,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    bridge_source_address,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                state.step = CexStep::DepositPending;
                return Ok(());
            }

            let bridge_amount = if source_available < bridge_amount {
                info!(
                    "[mexc] liq_id={} adjusting bridged deposit amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={}@{}->{})",
                    state.liq_id,
                    bridge_amount,
                    source_available,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    bridge_source_address,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                source_available
            } else {
                bridge_amount
            };

            if route.route_kind == BridgeRouteKind::CkEthErc20Reverse
                && let Some(required_fee_budget) = provider_fee_budget_native_units.as_ref()
            {
                let required_fee_budget = nat_to_units(required_fee_budget, CKETH_DECIMALS)?;
                let fee_available = bridge
                    .backend
                    .get_source_balance(BRIDGE_FEE_TOKEN_SYMBOL, BRIDGE_FEE_TOKEN_CHAIN, &bridge_source_address)
                    .await?;
                if fee_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < required_fee_budget {
                    info!(
                        "[mexc] liq_id={} bridged deposit waiting for fee funding: available={} expected={} tolerance={} source={} fee_asset={}@{} route={}@{}->{}",
                        state.liq_id,
                        fee_available,
                        required_fee_budget,
                        WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                        bridge_source_address,
                        BRIDGE_FEE_TOKEN_SYMBOL,
                        BRIDGE_FEE_TOKEN_CHAIN,
                        route.source_asset,
                        route.source_chain,
                        route.target_asset,
                    );
                    state.step = CexStep::DepositPending;
                    return Ok(());
                }
            }

            let submission = bridge
                .backend
                .submit_bridge(BridgeRequest {
                    asset: route.source_asset.to_string(),
                    source_chain: route.source_chain.to_string(),
                    source_address: bridge_source_address,
                    target_asset: route.target_asset.to_string(),
                    destination: bridge_destination,
                    amount: bridge_amount,
                    provider_fee_budget_native_units,
                })
                .await?;

            state.deposit.bridge.deposit_bridge_id = Some(submission.bridge_id);
            state.deposit.bridge.deposit_bridge_submitted_at_ts = Some(now_ts());
            state.trade.trade_next_amount_in = state.deposit.bridge.deposit_bridge_expected_amount;
            state.step = CexStep::DepositPending;
            return Ok(());
        }

        state.deposit.bridge.deposit_bridge_polled_at_ts = Some(now_ts());
        let bridge_id = state
            .deposit
            .bridge
            .deposit_bridge_id
            .clone()
            .ok_or_else(|| "missing bridge id while polling bridged deposit".to_string())?;
        match bridge.backend.get_bridge_status(&bridge_id).await? {
            BridgeStatus::Completed => {
                self.check_deposit(state).await?;
                if !matches!(state.step, CexStep::Trade) {
                    state.step = CexStep::DepositPending;
                }
                Ok(())
            }
            BridgeStatus::Pending | BridgeStatus::Unknown => {
                state.step = CexStep::DepositPending;
                Ok(())
            }
            BridgeStatus::Failed { reason } => Err(format!(
                "bridged deposit failed for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "unknown failure".to_string())
            )),
            BridgeStatus::Canceled { reason } => Err(format!(
                "bridged deposit canceled for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "no reason provided".to_string())
            )),
        }
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
    async fn trade(&self, state: &mut CexState) -> Result<(), CexBackendError> {
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

    async fn withdraw(&self, state: &mut CexState) -> Result<(), String> {
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
            ));
        }

        if !state.withdraw.bridge.withdraw_bridge_required {
            let direct_destination = if planned_network.eq_ignore_ascii_case("ICP") {
                if Self::is_native_icp_token(&state.withdraw.withdraw_asset) {
                    self.native_icp_direct_withdraw_address(&state.withdraw.withdraw_address)?
                } else {
                    let raw = state.withdraw.withdraw_address.trim();
                    if raw.is_empty() {
                        return Err(format!(
                            "missing ICP withdraw destination for {}@{}",
                            planned_asset, planned_network
                        ));
                    }
                    raw.to_string()
                }
            } else if planned_network.eq_ignore_ascii_case("ETH")
                || planned_network.to_ascii_lowercase().starts_with("evm")
            {
                let raw = state.withdraw.withdraw_address.trim();
                if raw.is_empty() {
                    return Err(format!(
                        "missing EVM withdraw destination for {}@{}",
                        planned_asset, planned_network
                    ));
                }
                raw.parse::<EvmAddress>()
                    .map_err(|e| {
                        format!(
                            "invalid EVM withdraw destination '{}' for {}@{}: {e}",
                            raw, planned_asset, planned_network
                        )
                    })?
                    .to_string()
            } else {
                let raw = state.withdraw.withdraw_address.trim();
                if raw.is_empty() {
                    return Err(format!(
                        "missing withdraw destination for {}@{}",
                        planned_asset, planned_network
                    ));
                }
                raw.to_string()
            };

            if state.withdraw.withdraw_address != direct_destination {
                state.withdraw.withdraw_address = direct_destination;
            }

            if state.withdraw.withdraw_id.is_some() || state.withdraw.withdraw_txid.is_some() {
                state.step = CexStep::Completed;
                return Ok(());
            }

            let receipt = match self
                .backend
                .withdraw(
                    &planned_asset,
                    &planned_network,
                    &state.withdraw.withdraw_address,
                    amount,
                )
                .await
            {
                Ok(receipt) => receipt,
                Err(err) if is_mexc_withdraw_below_min_error(&err) => {
                    Self::complete_withdraw_as_below_min_dust(state, &planned_asset, &planned_network, amount, &err);
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            state.withdraw.withdraw_id = receipt.internal_id.clone();
            state.withdraw.withdraw_txid = receipt.txid.clone();
            state.step = CexStep::Completed;
            return Ok(());
        }

        let bridge = self
            .bridge
            .as_ref()
            .ok_or_else(|| "bridge runtime not configured for bridged withdraw".to_string())?;
        let final_symbol = state.withdraw.withdraw_asset.symbol();
        let route = resolve_route(&planned_asset, &planned_network, &final_symbol).ok_or_else(|| {
            format!(
                "bridge route not found for withdraw {}@{} -> {}",
                planned_asset, planned_network, final_symbol
            )
        })?;

        if state.withdraw.bridge.withdraw_bridge_destination_snapshot.is_none() {
            state.withdraw.bridge.withdraw_bridge_destination_snapshot = Some(state.withdraw.withdraw_address.clone());
        }

        let bridge_source_address = self.resolve_bridge_source_address(route.source_chain)?;
        if state.withdraw.withdraw_address != bridge_source_address {
            state.withdraw.withdraw_address = bridge_source_address.clone();
        }

        // Submit CEX withdraw once (to bridge source), then keep polling in WithdrawPending.
        if state.withdraw.withdraw_id.is_none() && state.withdraw.withdraw_txid.is_none() {
            let receipt = match self
                .backend
                .withdraw(
                    &planned_asset,
                    &planned_network,
                    &state.withdraw.withdraw_address,
                    amount,
                )
                .await
            {
                Ok(receipt) => receipt,
                Err(err) if is_mexc_withdraw_below_min_error(&err) => {
                    Self::complete_withdraw_as_below_min_dust(state, &planned_asset, &planned_network, amount, &err);
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            state.withdraw.withdraw_id = receipt.internal_id.clone();
            state.withdraw.withdraw_txid = receipt.txid.clone();
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }

        // Once CEX withdrawal is completed, submit bridge once.
        if state.withdraw.bridge.withdraw_bridge_id.is_none() {
            let withdraw_id = state
                .withdraw
                .withdraw_id
                .clone()
                .ok_or_else(|| "missing withdraw_id while waiting bridged withdraw completion".to_string())?;

            let withdraw_snapshot = self
                .backend
                .get_withdraw_status_snapshot_by_id(&planned_asset, &withdraw_id)
                .await?;
            info!(
                "[mexc] liq_id={} withdraw snapshot: withdraw_id={} status={:?} txid_present={} fee={:?}",
                state.liq_id,
                withdraw_id,
                withdraw_snapshot.status,
                withdraw_snapshot.txid.as_ref().is_some_and(|tx| !tx.trim().is_empty()),
                withdraw_snapshot.transaction_fee,
            );

            match withdraw_snapshot.status {
                WithdrawStatus::Pending | WithdrawStatus::Unknown => {
                    if withdraw_snapshot.txid.is_some() {
                        info!(
                            "[mexc] liq_id={} withdraw status={:?} but txid is present for withdraw_id={}; proceeding to source-balance bridge preflight",
                            state.liq_id, withdraw_snapshot.status, withdraw_id
                        );
                    } else {
                        state.step = CexStep::WithdrawPending;
                        return Ok(());
                    }
                }
                WithdrawStatus::Failed => {
                    return Err(format!(
                        "cex withdraw failed for liq_id {} withdraw_id={}",
                        state.liq_id, withdraw_id
                    ));
                }
                WithdrawStatus::Canceled => {
                    return Err(format!(
                        "cex withdraw canceled for liq_id {} withdraw_id={}",
                        state.liq_id, withdraw_id
                    ));
                }
                WithdrawStatus::Completed => {}
            }

            // Keep the txid in state when available so operators can correlate
            // pending bridge flow with the underlying CEX withdrawal broadcast.
            if state.withdraw.withdraw_txid.is_none() {
                state.withdraw.withdraw_txid = withdraw_snapshot.txid.clone();
            }

            let final_destination_snapshot = state
                .withdraw
                .bridge
                .withdraw_bridge_destination_snapshot
                .clone()
                .ok_or_else(|| "missing final destination snapshot for bridged withdraw".to_string())?;

            let bridge_destination =
                self.bridge_destination_for_final_liquidator(route.destination_kind, &final_destination_snapshot)?;

            let mut bridge_amount = amount;
            if let Some(withdraw_fee) = withdraw_snapshot.transaction_fee {
                if withdraw_fee > LIQUIDITY_EPS {
                    let adjusted_amount = bridge_amount - withdraw_fee;
                    if adjusted_amount <= LIQUIDITY_EPS {
                        return Err(format!(
                            "cex withdraw fee exceeds bridged amount for liq_id {}: amount={} fee={} withdraw_id={}",
                            state.liq_id, bridge_amount, withdraw_fee, withdraw_id
                        ));
                    }
                    info!(
                        "[mexc] liq_id={} applying CEX withdraw fee to bridged amount: gross={} fee={} net={} asset={} network={} withdraw_id={}",
                        state.liq_id,
                        bridge_amount,
                        withdraw_fee,
                        adjusted_amount,
                        planned_asset,
                        planned_network,
                        withdraw_id
                    );
                    bridge_amount = adjusted_amount;
                }
            }
            let _submit_guard =
                acquire_bridge_submit_lock(route.source_asset, route.source_chain, &bridge_source_address).await;

            let source_available = bridge
                .backend
                .get_source_balance(route.source_asset, route.source_chain, &bridge_source_address)
                .await?;

            // CEX withdrawals can arrive net of withdrawal/network fees; use the reported fee
            // above to compute the expected net amount. Native ETH source balances are reported
            // as bridgeable balance after reserving gas, so submit that bridgeable amount instead
            // of waiting forever for the gross source balance to become bridgeable.
            if source_available + WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE < bridge_amount {
                if matches!(route.route_kind, BridgeRouteKind::EthToCkEth)
                    && source_available > WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE
                {
                    let adjusted_source_available = (source_available - NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN).max(0.0);
                    if adjusted_source_available <= WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE {
                        info!(
                            "[mexc] liq_id={} bridged native ETH withdraw waiting for source funding after safety margin: available={} adjusted={} expected={} tolerance={} source={} route={}@{}->{}",
                            state.liq_id,
                            source_available,
                            adjusted_source_available,
                            bridge_amount,
                            WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                            bridge_source_address,
                            route.source_asset,
                            route.source_chain,
                            route.target_asset,
                        );
                        state.step = CexStep::WithdrawPending;
                        return Ok(());
                    }
                    info!(
                        "[mexc] liq_id={} bridged native ETH withdraw adjusting to bridgeable source balance: amount {} -> {} (source includes gas reserve; safety_margin={} tolerance={} source={} route={}@{}->{})",
                        state.liq_id,
                        bridge_amount,
                        adjusted_source_available,
                        NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN,
                        WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                        bridge_source_address,
                        route.source_asset,
                        route.source_chain,
                        route.target_asset,
                    );
                    bridge_amount = adjusted_source_available;
                } else {
                    info!(
                        "[mexc] liq_id={} bridged withdraw waiting for source funding: available={} expected={} tolerance={} source={} route={}@{}->{}",
                        state.liq_id,
                        source_available,
                        bridge_amount,
                        WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                        bridge_source_address,
                        route.source_asset,
                        route.source_chain,
                        route.target_asset,
                    );
                    state.step = CexStep::WithdrawPending;
                    return Ok(());
                }
            }

            if source_available < bridge_amount {
                info!(
                    "[mexc] liq_id={} adjusting bridged withdraw amount within source balance tolerance: amount {} -> {} (tolerance={} source={} route={}@{}->{})",
                    state.liq_id,
                    bridge_amount,
                    source_available,
                    WITHDRAW_BRIDGE_SOURCE_BALANCE_TOLERANCE,
                    bridge_source_address,
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                );
                bridge_amount = source_available;
            }

            let submission = bridge
                .backend
                .submit_bridge(BridgeRequest {
                    asset: route.source_asset.to_string(),
                    source_chain: route.source_chain.to_string(),
                    source_address: bridge_source_address,
                    target_asset: route.target_asset.to_string(),
                    destination: bridge_destination,
                    amount: bridge_amount,
                    provider_fee_budget_native_units: None,
                })
                .await?;
            state.withdraw.bridge.withdraw_bridge_id = Some(submission.bridge_id);
            state.withdraw.bridge.withdraw_bridge_submitted_at_ts = Some(now_ts());
            state.step = CexStep::WithdrawPending;
            return Ok(());
        }

        let bridge_id = state
            .withdraw
            .bridge
            .withdraw_bridge_id
            .clone()
            .ok_or_else(|| "missing bridge id while polling bridged withdraw".to_string())?;
        state.withdraw.bridge.withdraw_bridge_polled_at_ts = Some(now_ts());
        match bridge.backend.get_bridge_status(&bridge_id).await? {
            BridgeStatus::Completed => {
                state.step = CexStep::Completed;
                Ok(())
            }
            BridgeStatus::Pending | BridgeStatus::Unknown => {
                state.step = CexStep::WithdrawPending;
                Ok(())
            }
            BridgeStatus::Failed { reason } => Err(format!(
                "bridged withdraw failed for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "unknown failure".to_string())
            )),
            BridgeStatus::Canceled { reason } => Err(format!(
                "bridged withdraw canceled for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "no reason provided".to_string())
            )),
        }
    }

    async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {
        self.finish_state(state)
    }

    async fn preview_route(&self, receipt: &ExecutionReceipt) -> Result<CexRoutePreview, String> {
        let state = self.prepare("preview", receipt).await?;
        let initial_amount = state.size_in.to_f64();
        if initial_amount <= LIQUIDITY_EPS {
            return Ok(CexRoutePreview {
                is_executable: false,
                estimated_receive_amount: 0.0,
                estimated_price_impact_bps: 0.0,
                reason: Some("non-positive amount_in".to_string()),
            });
        }
        let legs = self
            .resolve_trade_legs_for_symbols(
                &<Self as BridgePlanner>::planned_deposit_asset(&state),
                &<Self as BridgePlanner>::planned_withdraw_asset(&state),
            )
            .await?;
        let route_preview = self.preview_resolved_trade_route(&legs, initial_amount).await?;

        Ok(CexRoutePreview {
            is_executable: true,
            estimated_receive_amount: route_preview.receive_amount,
            estimated_price_impact_bps: route_preview.price_impact_bps,
            reason: None,
        })
    }
}

#[cfg(test)]
#[path = "mexc_finalizer_tests.rs"]
mod tests;
