use candid::Nat;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::BridgeRouteKind,
    cex_backend::{CexBackend, CexBackendError, FundingRouteError},
};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::types::side_cost::SideCost;
use log::warn;

use super::{finalizer::CexVenueFinalizer, utils::LIQUIDITY_EPS};
use crate::{
    finalizers::{
        bridge_legs::{
            BridgeLegError, bridged_deposit_arrival, ensure_return_gas, fee_adjusted_transfer, net_of_withdraw_fee,
            solana_repayment_bridge_amount,
        },
        bridge_planner::{BridgePlan, BridgePlanner, BridgeTransportPlan},
        cex::state::{
            CexDepositBridgeState, CexDepositState, CexRouteLeg, CexState, CexStep, CexTradeState,
            CexWithdrawBridgeState, CexWithdrawState,
        },
    },
    swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, amount_after_bps_haircut},
    utils::now_ts,
};

/// Amount-scoped CEX preview used by the thin multi-venue adapter. The full
/// `CexState` remains the single execution state of the CEX step machine.
pub(super) struct CexPreparedPreview {
    pub state: CexState,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
    /// The output at the order book's price for the whole allocation, before
    /// the route's fixed fees.
    pub market_receive: Nat,
    pub side_costs: Vec<SideCost>,
}

/// Why one transport of a leg was not used. The preview moves to the next
/// transport on either; the message is for the operator.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TransportRejection {
    /// This transport cannot carry the amount: no market, too little depth, a
    /// refused route, or a fee or minimum that leaves nothing.
    CannotCarry { transport: String, reason: String },
    /// Every market quoted, but the book is thin for the amount.
    ImpactAboveCap {
        transport: String,
        impact_bps: f64,
        cap_bps: f64,
    },
}

impl std::fmt::Display for TransportRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotCarry { transport, reason } => write!(f, "{transport}: {reason}"),
            Self::ImpactAboveCap {
                transport,
                impact_bps,
                cap_bps,
            } => write!(
                f,
                "{transport}: impact {impact_bps:.2} bps is above the {cap_bps:.2} bps cap"
            ),
        }
    }
}

fn rejection_summary(rejections: &[TransportRejection]) -> String {
    rejections
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Why one prepared state cannot be quoted.
#[derive(Debug)]
enum PreviewFailure {
    /// This transport cannot carry the amount. The next transport may.
    Transport(String),
    /// A read failed, so nothing is known about any transport. The venue is
    /// unavailable this cycle; a slower transport must not stand in for an outage.
    Venue(String),
}

impl From<CexBackendError> for PreviewFailure {
    fn from(error: CexBackendError) -> Self {
        match error {
            CexBackendError::BelowMinimum(reason) => Self::Transport(reason),
            other => Self::Venue(other.to_string()),
        }
    }
}

impl From<BridgeLegError> for PreviewFailure {
    fn from(error: BridgeLegError) -> Self {
        match error {
            // Both refuse this transport only; another one may still carry the amount.
            BridgeLegError::BelowMinimum(reason) | BridgeLegError::GasShortfall(reason) => Self::Transport(reason),
            other => Self::Venue(other.to_string()),
        }
    }
}

impl From<FundingRouteError> for PreviewFailure {
    fn from(error: FundingRouteError) -> Self {
        match error {
            FundingRouteError::BelowMinimum(reason) | FundingRouteError::Unsupported(reason) => Self::Transport(reason),
            FundingRouteError::Unavailable(reason) => Self::Venue(reason),
        }
    }
}

fn transport_side_label(side: &BridgeTransportPlan) -> String {
    if side.bridge_required {
        format!("{}@{} via bridge", side.cex_asset, side.cex_network)
    } else {
        format!("{}@{}", side.cex_asset, side.cex_network)
    }
}

/// `ICP@ICP -> USDT@ETH via bridge`: the plan as the operator reads it in a log.
pub(super) fn transport_label(plan: &BridgePlan) -> String {
    format!(
        "{} -> {}",
        transport_side_label(&plan.deposit),
        transport_side_label(&plan.withdraw)
    )
}

impl<B> CexVenueFinalizer<B>
where
    B: CexBackend,
{
    /// Builds the venue-local CEX state from an exact allocation. Keeping this
    /// separate from `ExecutionReceipt` prevents a split leg from accidentally
    /// depositing the liquidation's full collateral amount.
    pub(super) fn prepare_amount_scoped_state(
        &self,
        execution_id: &str,
        pay_amount: ChainTokenAmount,
        receive_token: ChainToken,
        receive_address: Option<String>,
    ) -> Result<CexState, String> {
        let bridge_plan = self.resolve_bridge_plan_for_assets(&pay_amount.token, &receive_token);
        Ok(self.amount_scoped_state_for_plan(execution_id, pay_amount, receive_token, receive_address, bridge_plan))
    }

    /// The venue-local state for one transport plan. The preview builds one per
    /// candidate and keeps the state of the transport it chose.
    fn amount_scoped_state_for_plan(
        &self,
        execution_id: &str,
        pay_amount: ChainTokenAmount,
        receive_token: ChainToken,
        receive_address: Option<String>,
        bridge_plan: BridgePlan,
    ) -> CexState {
        CexState {
            liq_id: execution_id.to_string(),
            send_saved: false,
            side_costs: Vec::new(),
            step: CexStep::Deposit,
            last_error: None,
            market: format!("{}_{}", bridge_plan.deposit.cex_asset, bridge_plan.withdraw.cex_asset),
            side: "sell".to_string(),
            size_in: pay_amount.clone(),
            deposit: CexDepositState {
                deposit_asset: pay_amount.token,
                deposit_txid: None,
                deposit_balance_before: None,
                deposit_sent_at_ts: None,
                deposit_credit_since_ts: None,
                approval_bump_count: None,
                deposit_funding: None,
                bridge: CexDepositBridgeState {
                    deposit_planned_asset: Some(bridge_plan.deposit.cex_asset),
                    deposit_planned_network: Some(bridge_plan.deposit.cex_network),
                    deposit_bridge_required: bridge_plan.deposit.bridge_required,
                    deposit_bridge_id: None,
                    deposit_bridge_submitted_at_ts: None,
                    deposit_bridge_polled_at_ts: None,
                    deposit_bridge_destination_snapshot: None,
                    deposit_bridge_submit_amount: None,
                    deposit_bridge_expected_amount: None,
                    deposit_bridge_provider_fee_budget_native_units: None,
                    deposit_bridge_call: None,
                    deposit_bridge_call_unrecorded: false,
                },
            },
            trade: CexTradeState {
                trade_leg_index: None,
                trade_leg_total: None,
                trade_resolved_legs: Vec::new(),
                trade_last_market: None,
                trade_last_side: None,
                trade_last_amount_in: None,
                trade_last_amount_out: None,
                trade_next_amount_in: None,
                trade_weighted_slippage_bps: None,
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
                trade_settlement_waiting_since_ts: None,
                trade_applied_client_order_ids: Vec::new(),
            },
            withdraw: CexWithdrawState {
                withdraw_asset: receive_token,
                withdraw_address: receive_address.unwrap_or_else(|| self.liquidator_principal.to_text()),
                withdraw_id: None,
                withdraw_txid: None,
                withdraw_accepted_at_ts: None,
                withdraw_attempt: 0,
                withdraw_failures: 0,
                withdraw_send: None,
                size_out: None,
                bridge: CexWithdrawBridgeState {
                    withdraw_bridge_reverts: 0,
                    withdraw_planned_asset: Some(bridge_plan.withdraw.cex_asset),
                    withdraw_planned_network: Some(bridge_plan.withdraw.cex_network),
                    withdraw_bridge_required: bridge_plan.withdraw.bridge_required,
                    withdraw_bridge_id: None,
                    withdraw_bridge_submitted_at_ts: None,
                    withdraw_bridge_polled_at_ts: None,
                    withdraw_bridge_mint_receipt: None,
                    withdraw_bridge_destination_snapshot: None,
                    withdraw_bridge_call: None,
                    withdraw_bridge_funding: None,
                    withdraw_bridge_send_handle: None,
                    withdraw_bridge_funding_txid: None,
                    withdraw_bridge_funded_at_ts: None,
                },
            },
        }
    }

    /// Resolves the receive token required by the leg's deposit/withdraw state and
    /// validates that the request's exact pay allocation is internally sound.
    pub(super) fn prepare_swap_request(&self, execution_id: &str, request: &SwapRequest) -> Result<CexState, String> {
        let (pay_amount, receive_token, receive_address) = self.swap_request_parts(request)?;
        self.prepare_amount_scoped_state(execution_id, pay_amount, receive_token, receive_address)
    }

    /// The exact pay amount, the resolved receive token and the payout address of a request.
    fn swap_request_parts(
        &self,
        request: &SwapRequest,
    ) -> Result<(ChainTokenAmount, ChainToken, Option<String>), String> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err(format!(
                "{} request pay asset does not match pay amount token",
                self.profile.name()
            ));
        }
        let registry = self
            .token_registry
            .as_ref()
            .ok_or_else(|| "CEX multi-venue adapter requires a token registry".to_string())?;
        let receive_token = registry.resolve(&request.receive_asset)?;
        Ok((
            request.pay_amount.clone(),
            receive_token,
            request.receive_address.clone(),
        ))
    }

    /// Converts a completed venue-local state into the generic execution
    /// result. It intentionally has no receipt or WAL dependency.
    pub(super) fn finish_state(&self, state: &CexState) -> Result<SwapExecution, String> {
        let receive_amount = state
            .withdraw
            .size_out
            .clone()
            .ok_or_else(|| "receive amount missing".to_string())?;
        let pay_amount = state.size_in.clone();
        let pay_f = pay_amount.to_f64();
        let recv_f = receive_amount.to_f64();
        let exec_price = if pay_f > 0.0 { recv_f / pay_f } else { 0.0 };
        // From the saved slices, so a row that an older build saved reports the
        // same way. At the mids the route would have returned 1 / (1 - slippage) more.
        let slippage_bps = if state.trade.trade_slices.is_empty() {
            state.trade.trade_weighted_slippage_bps.unwrap_or(0.0)
        } else {
            Self::route_slippage_bps(&state.trade.trade_slices)
        };
        let slippage = slippage_bps / crate::swappers::model::BPS_PER_RATIO_UNIT;
        let mid_price = if slippage < 1.0 {
            exec_price / (1.0 - slippage)
        } else {
            exec_price
        };

        let legs = state
            .trade
            .trade_slices
            .iter()
            .map(|slice| {
                let (base, quote) = super::utils::parse_market_symbols(&slice.market).unwrap_or_else(|| {
                    (
                        state.deposit.deposit_asset.symbol().to_ascii_uppercase(),
                        state.withdraw.withdraw_asset.symbol().to_ascii_uppercase(),
                    )
                });
                let (pay_symbol, recv_symbol) = if slice.side.eq_ignore_ascii_case("sell") {
                    (base, quote)
                } else {
                    (quote, base)
                };
                let pay_amount = super::utils::slice_amount_to_nat(slice.amount_in, &pay_symbol);
                let recv_amount = super::utils::slice_amount_to_nat(slice.amount_out, &recv_symbol);

                SwapQuoteLeg {
                    venue: self.profile.venue_id().to_string(),
                    route_id: slice.market.clone(),
                    pay_chain: state.deposit.deposit_asset.chain(),
                    pay_symbol,
                    pay_amount,
                    receive_chain: state.withdraw.withdraw_asset.chain(),
                    receive_symbol: recv_symbol,
                    receive_amount: recv_amount,
                    price: slice.exec_price,
                    lp_fee: Nat::from(0u8),
                    gas_fee: Nat::from(0u8),
                }
            })
            .collect();

        Ok(SwapExecution {
            swap_id: 0,
            request_id: 0,
            status: "completed".to_string(),
            pay_asset: state.deposit.deposit_asset.asset_id(),
            pay_amount: pay_amount.value,
            receive_asset: state.withdraw.withdraw_asset.asset_id(),
            receive_amount: receive_amount.value,
            mid_price,
            exec_price,
            realized_slippage_bps: slippage_bps,
            legs,
            approval_count: state.deposit.approval_bump_count,
            ts: now_ts().max(0) as u64,
            side_costs: state.side_costs.clone(),
            side_cost_receive: None,
        })
    }

    /// Quotes and prepares the exact request amount without performing a CEX
    /// side effect. The transports are tried fast first. The first one whose book
    /// is sound wins: every market quotes the amount, and the impact is at or
    /// under the cap. When every transport that quotes is thin, the least thin one
    /// is returned: the cap chooses between transports, it does not refuse the
    /// venue, and the plan's edge check and oracle guard judge the quote as before.
    /// Resolved markets are saved into the chosen `CexState`, which remains the
    /// only execution state of the CEX step machine.
    pub(super) async fn preview_swap_request(
        &self,
        execution_id: &str,
        request: &SwapRequest,
    ) -> Result<CexPreparedPreview, String> {
        let (pay_amount, receive_token, receive_address) = self.swap_request_parts(request)?;
        if pay_amount.value == 0u8 {
            return Err(format!(
                "{} cannot quote a non-positive pay amount",
                self.profile.name()
            ));
        }
        if let Some(requested_bps) = request.max_slippage_bps {
            amount_after_bps_haircut(&Nat::from(0u8), requested_bps)?;
        }
        let pair = format!("{} -> {}", pay_amount.token.symbol(), receive_token.symbol());
        let cap_bps = self.transport_impact_cap_bps;
        let mut rejections: Vec<TransportRejection> = Vec::new();
        let mut least_thin: Option<(String, CexPreparedPreview)> = None;
        for plan in self.bridge_plan_candidates(&pay_amount.token, &receive_token) {
            let transport = transport_label(&plan);
            let state = self.amount_scoped_state_for_plan(
                execution_id,
                pay_amount.clone(),
                receive_token.clone(),
                receive_address.clone(),
                plan,
            );
            let preview = match self.preview_prepared_state(request, state).await {
                Ok(preview) => preview,
                Err(PreviewFailure::Transport(reason)) => {
                    rejections.push(TransportRejection::CannotCarry { transport, reason });
                    continue;
                }
                Err(PreviewFailure::Venue(reason)) => {
                    return Err(format!(
                        "{} cannot quote {pair} ({transport}): {reason}",
                        self.profile.name()
                    ));
                }
            };
            let impact_bps = preview.quote.estimated_price_impact_bps;
            if impact_bps <= cap_bps {
                if !rejections.is_empty() {
                    warn!(
                        "[cex] liq_id={execution_id} {} takes {transport} for {pair}: the faster transports are not sound: {}",
                        self.profile.name(),
                        rejection_summary(&rejections)
                    );
                }
                return Ok(preview);
            }
            rejections.push(TransportRejection::ImpactAboveCap {
                transport: transport.clone(),
                impact_bps,
                cap_bps,
            });
            if least_thin
                .as_ref()
                .is_none_or(|(_, best)| impact_bps < best.quote.estimated_price_impact_bps)
            {
                least_thin = Some((transport, preview));
            }
        }
        if let Some((transport, preview)) = least_thin {
            warn!(
                "[cex] liq_id={execution_id} {} takes {transport} for {pair} with the least impact; every transport is above the cap: {}",
                self.profile.name(),
                rejection_summary(&rejections)
            );
            return Ok(preview);
        }
        Err(format!(
            "{} has no transport that can carry {pair}: {}",
            self.profile.name(),
            rejection_summary(&rejections)
        ))
    }

    /// Quotes one prepared state: the funding route, the trade legs and what
    /// arrives after fees and the return bridge.
    async fn preview_prepared_state(
        &self,
        request: &SwapRequest,
        mut state: CexState,
    ) -> Result<CexPreparedPreview, PreviewFailure> {
        let mut side_costs = Vec::new();
        let initial_amount = self.preview_trade_input(&state, &mut side_costs).await?;
        if initial_amount <= LIQUIDITY_EPS {
            return Err(PreviewFailure::Transport(
                "the deposit fees leave nothing to trade".to_string(),
            ));
        }
        // The exchange confirms, before any funds move, that it credits this
        // deposit and pays out to this address.
        let route = self
            .funding_route(&state, initial_amount)
            .map_err(PreviewFailure::Venue)?;
        self.backend.check_funding_route(&route).await?;
        // Order book reads are untyped, so a failed read counts as a market that
        // cannot carry the amount. A typed read error is a follow-up.
        let legs = self
            .resolve_trade_legs_for_amount(
                &<Self as BridgePlanner>::planned_deposit_asset(&state),
                &<Self as BridgePlanner>::planned_withdraw_asset(&state),
                initial_amount,
            )
            .await
            .map_err(PreviewFailure::Transport)?;
        let route_preview = self
            .preview_resolved_trade_route(&legs, initial_amount)
            .await
            .map_err(PreviewFailure::Transport)?;

        state.trade.trade_resolved_legs = legs
            .iter()
            .map(|leg| CexRouteLeg {
                market: leg.market.clone(),
                side: leg.side.clone(),
            })
            .collect();
        state.trade.trade_leg_total = Some(legs.len() as u32);

        // The order book's price for the whole allocation, before fixed fees.
        let market_receive = ChainTokenAmount::from_formatted(
            state.withdraw.withdraw_asset.clone(),
            (state.size_in.to_f64() * route_preview.execution_price).max(0.0),
        )
        .value;
        let net_receive = self
            .preview_receive(&state, route_preview.receive_amount.max(0.0), &mut side_costs)
            .await?;
        let execution_price = net_receive / initial_amount;
        // The leg starts with the side costs it plans to pay.
        state.record_side_costs(&side_costs);
        let receive_amount = ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), net_receive);
        let route_id = if legs.is_empty() {
            format!(
                "{}_{}",
                state.size_in.token.symbol(),
                state.withdraw.withdraw_asset.symbol()
            )
        } else {
            legs.iter()
                .map(|leg| format!("{}:{}", leg.market, leg.side))
                .collect::<Vec<_>>()
                .join(">")
        };
        let quote = SwapQuote {
            pay_asset: request.pay_asset.clone(),
            pay_amount: request.pay_amount.value.clone(),
            receive_asset: request.receive_asset.clone(),
            receive_amount: receive_amount.value.clone(),
            mid_price: route_preview.reference_price,
            exec_price: execution_price,
            estimated_price_impact_bps: route_preview.price_impact_bps,
            legs: vec![SwapQuoteLeg {
                venue: self.profile.venue_id().to_string(),
                route_id,
                pay_chain: state.size_in.token.chain(),
                pay_symbol: state.size_in.token.symbol(),
                pay_amount: request.pay_amount.value.clone(),
                receive_chain: receive_amount.token.chain(),
                receive_symbol: receive_amount.token.symbol(),
                receive_amount: receive_amount.value.clone(),
                price: execution_price,
                lp_fee: Nat::from(0u8),
                gas_fee: Nat::from(0u8),
            }],
        };
        // The book walk is already net of price impact and the taker fee, so only costs it
        // cannot see apply here. Execution still enforces the slippage cap on each slice.
        let conservative_haircut_bps = self
            .quote_route_fee_bps
            .checked_add(self.quote_delay_buffer_bps)
            .ok_or_else(|| PreviewFailure::Venue("CEX conservative quote haircut overflowed u32".to_string()))?;
        let conservative_value =
            amount_after_bps_haircut(&receive_amount.value, conservative_haircut_bps).map_err(PreviewFailure::Venue)?;

        Ok(CexPreparedPreview {
            state,
            quote,
            conservative_receive: ChainTokenAmount::from_raw(receive_amount.token, conservative_value),
            market_receive,
            side_costs,
        })
    }

    /// The amount that reaches the exchange to trade, sized as the deposit sizes it.
    async fn preview_trade_input(
        &self,
        state: &CexState,
        side_costs: &mut Vec<SideCost>,
    ) -> Result<f64, PreviewFailure> {
        if !state.deposit.bridge.deposit_bridge_required {
            let (_, executable_pay) = fee_adjusted_transfer(
                &state.deposit.deposit_asset,
                &state.size_in,
                self.deposit_fee_count(&state.deposit.deposit_asset),
            )?;
            return Ok(executable_pay.to_f64());
        }
        // A bridged candidate exists only with a bridge and a route, so these are configuration faults.
        let bridge = self.bridge_runtime().map_err(PreviewFailure::Venue)?;
        let route = Self::deposit_route(state, &<Self as BridgePlanner>::planned_deposit_asset(state))
            .map_err(PreviewFailure::Venue)?;
        let arrival = bridged_deposit_arrival(bridge, route, &state.deposit.deposit_asset, &state.size_in).await?;
        side_costs.extend(arrival.sizing.fee_budget.side_costs);
        Ok(arrival.expected_amount)
    }

    /// What arrives as the final asset after the exchange's withdrawal fee and the
    /// return bridge, as the withdrawal meets them.
    async fn preview_receive(
        &self,
        state: &CexState,
        trade_out: f64,
        side_costs: &mut Vec<SideCost>,
    ) -> Result<f64, PreviewFailure> {
        let planned_asset = <Self as BridgePlanner>::planned_withdraw_asset(state);
        let planned_network = <Self as BridgePlanner>::planned_withdraw_network(state);
        // Untyped like the order book reads: an unknown asset or network and a
        // failed read both count against this transport.
        let terms = self
            .backend
            .get_withdraw_terms(&planned_asset, &planned_network)
            .await
            .map_err(PreviewFailure::Transport)?;
        // The exchange refuses a smaller withdrawal, and the leg would end as dust.
        if trade_out < terms.minimum {
            return Err(PreviewFailure::Transport(format!(
                "{planned_asset} withdrawal of {trade_out} is below its minimum {}",
                terms.minimum
            )));
        }
        let mut net = net_of_withdraw_fee(trade_out, terms.fee).map_err(PreviewFailure::Transport)?;

        let (true, Some(route)) = (
            state.withdraw.bridge.withdraw_bridge_required,
            Self::withdraw_route(state),
        ) else {
            return Ok(net);
        };
        let bridge = self.bridge_runtime().map_err(PreviewFailure::Venue)?;
        let budget = bridge
            .backend
            .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
            .await
            .map_err(PreviewFailure::Venue)?;
        ensure_return_gas(bridge, route, &budget)
            .await
            .map_err(PreviewFailure::Transport)?;
        if route.route_kind == BridgeRouteKind::SolanaToIcp {
            let gross = ChainTokenAmount::from_formatted(state.withdraw.withdraw_asset.clone(), net);
            net = solana_repayment_bridge_amount(bridge, route, &gross, budget.source_fee_budget)
                .await?
                .to_f64();
        }
        side_costs.extend(budget.side_costs);
        Ok(net)
    }
}

#[cfg(test)]
mod preview_failure_tests {
    use super::*;

    /// A gas shortfall belongs to one bridge route. It must not hide a direct
    /// transport that already has a quote.
    #[test]
    fn a_gas_shortfall_refuses_its_transport_and_not_the_venue() {
        let failure = PreviewFailure::from(BridgeLegError::GasShortfall("no ckETH".to_string()));
        assert!(matches!(failure, PreviewFailure::Transport(_)), "{failure:?}");

        let failure = PreviewFailure::from(BridgeLegError::Other("read failed".to_string()));
        assert!(matches!(failure, PreviewFailure::Venue(_)), "{failure:?}");
    }
}
