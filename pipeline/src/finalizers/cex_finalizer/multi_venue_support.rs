use candid::Nat;
use liquidium_pipeline_connectors::backend::bridge_backend::resolve_route;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, FundingRoutePreflight};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::CexFinalizer;
use crate::finalizers::cex_finalizer::utils::LIQUIDITY_EPS;
use crate::{
    finalizers::{
        bridge_planner::BridgePlanner,
        cex_finalizer::{
            CexDepositBridgeState, CexDepositState, CexRouteLeg, CexState, CexStep, CexTradeState,
            CexWithdrawBridgeState, CexWithdrawState,
        },
    },
    swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, amount_after_bps_haircut},
    utils::now_ts,
};

/// Amount-scoped CEX preview used by the thin multi-venue adapter. The full
/// `CexState` remains the single execution state owned by `CexFinalizerLogic`.
pub(super) struct CexPreparedPreview {
    pub state: CexState,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
}

impl<B> CexFinalizer<B>
where
    B: CexBackend,
{
    fn funding_preflight_withdraw_destination(&self, state: &CexState) -> Result<String, String> {
        let planned_asset = <Self as BridgePlanner>::planned_withdraw_asset(state);
        let planned_network = <Self as BridgePlanner>::planned_withdraw_network(state);

        if state.withdraw.bridge.withdraw_bridge_required {
            let final_symbol = state.withdraw.withdraw_asset.symbol();
            let route = resolve_route(&planned_asset, &planned_network, &final_symbol).ok_or_else(|| {
                format!(
                    "bridge route not found for withdraw {}@{} -> {}",
                    planned_asset, planned_network, final_symbol
                )
            })?;
            return self.resolve_bridge_source_address(route.source_chain);
        }

        if planned_network.eq_ignore_ascii_case("ICP") && Self::is_native_icp_token(&state.withdraw.withdraw_asset) {
            return self.native_icp_direct_withdraw_address(&state.withdraw.withdraw_address);
        }

        Ok(state.withdraw.withdraw_address.clone())
    }

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

        Ok(CexState {
            liq_id: execution_id.to_string(),
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
                approval_bump_count: None,
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
                    deposit_bridge_revert_resubmits: 0,
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
                trade_settlement_waiting_since_ts: None,
            },
            withdraw: CexWithdrawState {
                withdraw_asset: receive_token,
                withdraw_address: receive_address.unwrap_or_else(|| self.liquidator_principal.to_text()),
                withdraw_id: None,
                withdraw_txid: None,
                size_out: None,
                bridge: CexWithdrawBridgeState {
                    withdraw_planned_asset: Some(bridge_plan.withdraw.cex_asset),
                    withdraw_planned_network: Some(bridge_plan.withdraw.cex_network),
                    withdraw_bridge_required: bridge_plan.withdraw.bridge_required,
                    withdraw_bridge_id: None,
                    withdraw_bridge_submitted_at_ts: None,
                    withdraw_bridge_polled_at_ts: None,
                    withdraw_bridge_destination_snapshot: None,
                    withdraw_bridge_expected_amount: None,
                    withdraw_bridge_revert_resubmits: 0,
                },
            },
        })
    }

    /// Resolves the receive token required by the CEX deposit/withdraw state and
    /// validates that the request's exact pay allocation is internally sound.
    fn prepare_swap_request(&self, execution_id: &str, request: &SwapRequest) -> Result<CexState, String> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err(format!(
                "{} request pay asset does not match pay amount token",
                self.profile.venue_id()
            ));
        }
        let registry = self.token_registry.as_ref().ok_or_else(|| {
            format!(
                "{} multi-venue adapter requires a token registry",
                self.profile.venue_id()
            )
        })?;
        let receive_token = registry.resolve(&request.receive_asset)?;
        self.prepare_amount_scoped_state(
            execution_id,
            request.pay_amount.clone(),
            receive_token,
            request.receive_address.clone(),
        )
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

        let legs = state
            .trade
            .trade_slices
            .iter()
            .map(|slice| {
                let (base, quote) = crate::finalizers::cex_finalizer::utils::parse_market_symbols(&slice.market)
                    .unwrap_or_else(|| {
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
                    venue: self.profile.venue_id().to_string(),
                    route_id: slice.market.clone(),
                    pay_chain: state.deposit.deposit_asset.chain(),
                    pay_symbol,
                    pay_amount: crate::finalizers::cex_finalizer::utils::f64_to_nat(pay_amount),
                    receive_chain: state.withdraw.withdraw_asset.chain(),
                    receive_symbol: recv_symbol,
                    receive_amount: crate::finalizers::cex_finalizer::utils::f64_to_nat(recv_amount),
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
            realized_slippage_bps: state.trade.trade_weighted_slippage_bps.unwrap_or(0.0),
            legs,
            approval_count: state.deposit.approval_bump_count,
            ts: now_ts().max(0) as u64,
        })
    }

    /// Quotes and prepares the exact request amount without performing a CEX
    /// side effect. Resolved markets are saved into the existing `CexState`,
    /// which remains the only execution state used by `CexFinalizerLogic`.
    pub(super) async fn preview_swap_request(
        &self,
        execution_id: &str,
        request: &SwapRequest,
    ) -> Result<CexPreparedPreview, String> {
        // Build the venue-owned execution state for exactly this requested allocation.
        let mut state = self.prepare_swap_request(execution_id, request)?;

        // Reject a zero raw ledger amount before doing order-book or funding API reads.
        if state.size_in.value == Nat::from(0u8) {
            return Err(format!(
                "{} cannot quote a non-positive pay amount",
                self.profile.venue_id()
            ));
        }

        // Validate the caller's slippage value with the shared bounded-bps helper.
        // A zero amount is sufficient here because only the bps validation is needed.
        if let Some(requested_bps) = request.max_slippage_bps {
            amount_after_bps_haircut(&Nat::from(0u8), requested_bps)?;
        }

        // Remove the source-ledger transfer fee from the amount that can reach the CEX.
        let (_, executable_pay) =
            self.compute_fee_adjusted_deposit_transfer(&state.deposit.deposit_asset, &state.size_in)?;

        // Convert the executable input into native decimal units for order-book simulation.
        let initial_amount = executable_pay.to_f64();

        // Guard against fees or conversion precision reducing the executable amount to zero.
        if initial_amount <= LIQUIDITY_EPS {
            return Err(format!(
                "{} cannot quote a non-positive pay amount",
                self.profile.venue_id()
            ));
        }

        // Resolve the asset that will actually be credited to the CEX after any input bridge.
        let deposit_asset = <Self as BridgePlanner>::planned_deposit_asset(&state);

        // Resolve the asset that the CEX must produce before any output bridge.
        let withdraw_asset = <Self as BridgePlanner>::planned_withdraw_asset(&state);

        // Kraken evaluates every amount-fillable route and selects the best net output.
        // MEXC keeps its legacy deterministic route selection for compatibility.
        let (legs, route_preview) = if self.profile.best_route_preview_required() {
            self.resolve_best_trade_route_for_amount(&deposit_asset, &withdraw_asset, initial_amount)
                .await?
        } else {
            // Resolve the venue's preferred route without comparing alternate route outputs.
            let legs = self
                .resolve_trade_legs_for_symbols(&deposit_asset, &withdraw_asset)
                .await?;

            // Simulate the selected route against current order-book depth and venue fees.
            let preview = self.preview_resolved_trade_route(&legs, initial_amount).await?;

            // Return the route and its simulation in the same shape as best-route selection.
            (legs, preview)
        };

        // Venues such as Kraken require funding methods and destinations to be
        // proven usable before their quote is exposed to the planner.
        if self.profile.funding_preflight_required() {
            // Use the direct receiver or the bridge source address, as appropriate.
            let withdraw_destination = self.funding_preflight_withdraw_destination(&state)?;

            // Validate exact assets, networks, destination, and estimated amounts
            // without creating an address or submitting any external side effect.
            self.backend
                .validate_funding_route(&FundingRoutePreflight {
                    // Asset expected to arrive in the exchange account.
                    deposit_asset: <Self as BridgePlanner>::planned_deposit_asset(&state),
                    // Exchange deposit network selected by the bridge plan.
                    deposit_network: <Self as BridgePlanner>::planned_deposit_network(&state),
                    // Asset that will be withdrawn after all trade legs complete.
                    withdraw_asset: <Self as BridgePlanner>::planned_withdraw_asset(&state),
                    // Exchange withdrawal network selected by the bridge plan.
                    withdraw_network: <Self as BridgePlanner>::planned_withdraw_network(&state),
                    // Exact preverified receiver required for later withdrawal.
                    withdraw_address: withdraw_destination,
                    // Estimated amount entering the first trade leg.
                    deposit_amount: initial_amount,
                    // Estimated amount available for the eventual withdrawal.
                    withdraw_amount: route_preview.receive_amount,
                })
                .await?;
        }

        // Persist the resolved route so execution and recovery never rediscover
        // a different route after the quote has been committed.
        state.trade.trade_resolved_legs = legs
            .iter()
            .map(|leg| CexRouteLeg {
                // Store only internal canonical market names in persisted state.
                market: leg.market.clone(),
                // Store the input-oriented side used by execution.
                side: leg.side.clone(),
            })
            .collect();

        // Persist the expected leg count for progress tracking and recovery checks.
        state.trade.trade_leg_total = Some(legs.len() as u32);

        // Convert the simulated decimal output into the destination token's exact
        // ledger representation, flooring according to that token's decimals.
        let receive_amount = ChainTokenAmount::from_formatted(
            state.withdraw.withdraw_asset.clone(),
            route_preview.receive_amount.max(0.0),
        );

        // Give no-op conversions a stable asset-pair identifier.
        let route_id = if legs.is_empty() {
            format!(
                "{}_{}",
                state.size_in.token.symbol(),
                state.withdraw.withdraw_asset.symbol()
            )
        } else {
            // Encode each executable market and side in traversal order.
            legs.iter()
                .map(|leg| format!("{}:{}", leg.market, leg.side))
                .collect::<Vec<_>>()
                .join(">")
        };

        // Build the planner-facing quote while keeping the detailed execution
        // state owned by this CEX adapter.
        let quote = SwapQuote {
            // Preserve the request's canonical source asset identifier.
            pay_asset: request.pay_asset.clone(),
            // Report the full allocated ledger amount, including its transfer fee budget.
            pay_amount: request.pay_amount.value.clone(),
            // Preserve the request's canonical destination asset identifier.
            receive_asset: request.receive_asset.clone(),
            // Report the simulated, token-rounded output.
            receive_amount: receive_amount.value.clone(),
            // Reference price is the route price before simulated execution impact.
            mid_price: route_preview.reference_price,
            // Execution price reflects depth, route fees, and route direction.
            exec_price: route_preview.execution_price,
            // Surface the amount-scoped aggregate impact for planner policy checks.
            estimated_price_impact_bps: route_preview.price_impact_bps,
            // A CEX conversion is represented as one planner leg whose route_id
            // contains the internal sequence of exchange markets.
            legs: vec![SwapQuoteLeg {
                // Tag the leg with the active venue profile.
                venue: self.profile.venue_id().to_string(),
                // Persist the human-readable internal route description.
                route_id,
                // Describe the original source chain and symbol.
                pay_chain: state.size_in.token.chain(),
                pay_symbol: state.size_in.token.symbol(),
                // Keep the exact amount allocated to this venue.
                pay_amount: request.pay_amount.value.clone(),
                // Describe the final destination chain and symbol.
                receive_chain: receive_amount.token.chain(),
                receive_symbol: receive_amount.token.symbol(),
                // Keep the exact simulated destination amount.
                receive_amount: receive_amount.value.clone(),
                // Use the normalized receive-per-pay execution price.
                price: route_preview.execution_price,
                // CEX taker fees are already included in the simulated output.
                lp_fee: Nat::from(0u8),
                // Chain/bridge gas is budgeted by execution rather than this quote field.
                gas_fee: Nat::from(0u8),
            }],
        };

        // Only costs the order-book simulation cannot already see: route fees it
        // does not model, and the book moving between this quote and the fill.
        //
        // Measured price impact is deliberately absent. `receive_amount` is the
        // result of walking the live book and applying the venue's taker fee,
        // so impact has already been taken out of it. Subtracting
        // `max_sell_slippage_bps` here charged for the same thing twice -- once
        // as measured, once as the worst value permitted -- which understated
        // every CEX plan by the full slippage cap and refused liquidations on a
        // profit floor they had in fact cleared.
        //
        // Nothing is given up by dropping it: this figure is a planning
        // estimate that is never compared against a fill. Slippage is enforced
        // live and per slice against the book at execution time, in
        // `execute_trade_leg_slices`, and that guard still uses the full cap.
        //
        // Checked arithmetic so a bad configuration cannot wrap.
        let conservative_haircut_bps = self
            .quote_route_fee_bps
            .checked_add(self.quote_delay_buffer_bps)
            .ok_or_else(|| format!("{} conservative quote haircut overflowed u32", self.profile.venue_id()))?;

        // Floor the simulated output by the costs above, giving the planner the
        // figure it ranks venues and gates the minimum edge on.
        let conservative_value = amount_after_bps_haircut(&receive_amount.value, conservative_haircut_bps)?;

        // Return both the public quote and the venue-owned initial execution state.
        Ok(CexPreparedPreview {
            // The caller persists this before any deposit, order, or withdrawal.
            state,
            // The planner compares this normalized quote with other venues.
            quote,
            // The planner uses this stricter amount when deciding route safety.
            conservative_receive: ChainTokenAmount::from_raw(receive_amount.token, conservative_value),
        })
    }
}
