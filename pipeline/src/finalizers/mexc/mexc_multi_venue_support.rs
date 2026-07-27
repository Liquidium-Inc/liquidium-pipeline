use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{mexc_finalizer::MexcFinalizer, mexc_utils::LIQUIDITY_EPS};
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

/// Amount-scoped MEXC preview used by the thin multi-venue adapter. The full
/// `CexState` remains the single execution state owned by `CexFinalizerLogic`.
pub(super) struct MexcPreparedPreview {
    pub state: CexState,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
}

impl<B> MexcFinalizer<B>
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
                },
            },
        })
    }

    /// Resolves the receive token required by MEXC's deposit/withdraw state and
    /// validates that the request's exact pay allocation is internally sound.
    fn prepare_swap_request(&self, execution_id: &str, request: &SwapRequest) -> Result<CexState, String> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err("MEXC request pay asset does not match pay amount token".to_string());
        }
        let registry = self
            .token_registry
            .as_ref()
            .ok_or_else(|| "MEXC multi-venue adapter requires a token registry".to_string())?;
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
                let (base, quote) = super::mexc_utils::parse_market_symbols(&slice.market).unwrap_or_else(|| {
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
                    pay_amount: super::mexc_utils::f64_to_nat(pay_amount),
                    receive_chain: state.withdraw.withdraw_asset.chain(),
                    receive_symbol: recv_symbol,
                    receive_amount: super::mexc_utils::f64_to_nat(recv_amount),
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
    ) -> Result<MexcPreparedPreview, String> {
        let mut state = self.prepare_swap_request(execution_id, request)?;
        let initial_amount = state.size_in.to_f64();
        if initial_amount <= LIQUIDITY_EPS {
            return Err("MEXC cannot quote a non-positive pay amount".to_string());
        }
        let legs = self
            .resolve_trade_legs_for_symbols(
                &<Self as BridgePlanner>::planned_deposit_asset(&state),
                &<Self as BridgePlanner>::planned_withdraw_asset(&state),
            )
            .await?;
        let route_preview = self.preview_resolved_trade_route(&legs, initial_amount).await?;

        state.trade.trade_resolved_legs = legs
            .iter()
            .map(|leg| CexRouteLeg {
                market: leg.market.clone(),
                side: leg.side.clone(),
            })
            .collect();
        state.trade.trade_leg_total = Some(legs.len() as u32);

        let receive_amount = ChainTokenAmount::from_formatted(
            state.withdraw.withdraw_asset.clone(),
            route_preview.receive_amount.max(0.0),
        );
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
            exec_price: route_preview.execution_price,
            estimated_price_impact_bps: route_preview.price_impact_bps,
            legs: vec![SwapQuoteLeg {
                venue: "mexc".to_string(),
                route_id,
                pay_chain: state.size_in.token.chain(),
                pay_symbol: state.size_in.token.symbol(),
                pay_amount: request.pay_amount.value.clone(),
                receive_chain: receive_amount.token.chain(),
                receive_symbol: receive_amount.token.symbol(),
                receive_amount: receive_amount.value.clone(),
                price: route_preview.execution_price,
                lp_fee: Nat::from(0u8),
                gas_fee: Nat::from(0u8),
            }],
        };
        let max_slippage_bps = request
            .max_slippage_bps
            .unwrap_or(self.max_sell_slippage_bps.max(0.0) as u32);
        let conservative_value = amount_after_bps_haircut(&receive_amount.value, max_slippage_bps)?;

        Ok(MexcPreparedPreview {
            state,
            quote,
            conservative_receive: ChainTokenAmount::from_raw(receive_amount.token, conservative_value),
        })
    }
}
