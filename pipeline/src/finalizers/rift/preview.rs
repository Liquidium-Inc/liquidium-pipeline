//! The amount-scoped preview: the bridge out sized as execution sizes it, a
//! Rift quote for what arrives, and the bridge back's costs. Nothing moves.
use candid::Nat;
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeFeeBudget, BridgeRouteSpec};
use liquidium_pipeline_connectors::backend::rift_backend::{RiftAsset, RiftBackend, RiftQuoteRequest};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::side_cost::SideCost,
};

use super::{
    chain::RiftChain,
    finalizer::{RiftVenueFinalizer, decimal_text, rift_amount},
    state::{RiftState, RiftStep},
};
use crate::{
    finalizers::{
        bridge_legs::{bridged_deposit_arrival, ensure_return_gas},
        multi_venue::RIFT_VENUE_ID,
    },
    swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, amount_after_bps_haircut},
    utils::now_ts,
};

/// What the order asks below the expected arrival. A minter that takes its
/// fee out of the amount at execution time can deliver less than the
/// estimate; one more fee estimate covers a rise, and Rift swaps the overfund whole.
pub(super) fn order_headroom(out: &BridgeRouteSpec, budget: &BridgeFeeBudget) -> f64 {
    if out.route_kind.fee_comes_out_of_amount() {
        budget.destination_fee_budget.max(0.0)
    } else {
        0.0
    }
}

/// What the order asks for: the expected arrival less the headroom, or
/// nothing when the fees eat the arrival.
pub(super) fn order_amount(expected: &ChainTokenAmount, headroom: &ChainTokenAmount) -> Option<ChainTokenAmount> {
    if expected.value <= headroom.value {
        return None;
    }
    Some(ChainTokenAmount::from_raw(
        expected.token.clone(),
        expected.value.clone() - headroom.value.clone(),
    ))
}

/// The one leg of a Rift route: the whole allocation for what Rift pays out.
fn swap_leg(state: &RiftState, receive_amount: &ChainTokenAmount, price: f64) -> SwapQuoteLeg {
    SwapQuoteLeg {
        venue: RIFT_VENUE_ID.to_string(),
        route_id: format!(
            "{}>{}",
            RiftAsset::new(&state.pay_chain, &state.pay_symbol).as_str(),
            RiftAsset::new(RiftChain::PAYOUT.name(), &state.receive_symbol).as_str()
        ),
        pay_chain: state.size_in.token.chain(),
        pay_symbol: state.size_in.token.symbol(),
        pay_amount: state.size_in.value.clone(),
        receive_chain: state.final_asset.chain(),
        receive_symbol: state.final_asset.symbol(),
        receive_amount: receive_amount.value.clone(),
        price,
        lp_fee: Nat::from(0u8),
        gas_fee: Nat::from(0u8),
    }
}

pub(super) struct RiftPreview {
    pub state: RiftState,
    pub quote: SwapQuote,
    pub conservative_receive: ChainTokenAmount,
    pub market_receive: Nat,
    pub side_costs: Vec<SideCost>,
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    /// The exact pay amount, the resolved receive token and the payout account of a request.
    pub(super) fn swap_request_parts(
        &self,
        request: &SwapRequest,
    ) -> Result<(ChainTokenAmount, ChainToken, String), String> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err("Rift request pay asset does not match pay amount token".to_string());
        }
        let registry = self
            .token_registry
            .as_ref()
            .ok_or_else(|| "Rift multi-venue adapter requires a token registry".to_string())?;
        let receive_token = registry.resolve(&request.receive_asset)?;
        let destination = request
            .receive_address
            .clone()
            .unwrap_or_else(|| self.liquidator_principal.to_text());
        Ok((request.pay_amount.clone(), receive_token, destination))
    }

    /// Quotes the exact allocation. The preview quote sizes the plan only; the
    /// leg takes a fresh quote when it orders and refuses one under the floor.
    pub(super) async fn preview_swap_request(
        &self,
        execution_id: &str,
        request: &SwapRequest,
    ) -> Result<RiftPreview, String> {
        let (pay_amount, final_asset, final_destination) = self.swap_request_parts(request)?;
        let (pay_chain, _) = Self::rift_asset(&pay_amount.token)
            .ok_or_else(|| format!("Rift does not carry {}", pay_amount.token.symbol()))?;
        let (out, back) = Self::leg_routes(&pay_amount.token, &final_asset)?;
        let arrival = bridged_deposit_arrival(&self.bridge, out, &pay_amount.token, &pay_amount)
            .await
            .map_err(|error| error.to_string())?;
        let mut side_costs = arrival.sizing.fee_budget.side_costs.clone();
        let pay_token = &pay_amount.token;
        let expected = ChainTokenAmount::from_formatted(pay_token.clone(), arrival.expected_amount);
        let arrival_headroom =
            ChainTokenAmount::from_formatted(pay_token.clone(), order_headroom(out, &arrival.sizing.fee_budget));
        let amount_in =
            order_amount(&expected, &arrival_headroom).ok_or("the bridge fees leave nothing for Rift to swap")?;
        let pair = format!("{}.{}->{}", pay_chain.name(), out.target_asset, back.source_asset);
        let quote = self
            .backend
            .quote(&RiftQuoteRequest {
                from: RiftAsset::new(pay_chain.name(), out.target_asset),
                to: RiftAsset::new(RiftChain::PAYOUT.name(), back.source_asset),
                amount_in: decimal_text(&amount_in),
                venues: self.venues.clone(),
            })
            .await
            .map_err(|error| format!("Rift cannot quote {pair}: {error}"))?;
        let receive_amount =
            rift_amount(&final_asset, &quote.estimated_amount_out).map_err(|error| error.to_string())?;
        if receive_amount.value == 0u8 {
            return Err(format!("Rift quotes nothing for {} {pair}", decimal_text(&amount_in)));
        }
        let budget = self
            .bridge
            .backend
            .get_fee_budget(back.source_asset, back.source_chain, back.target_asset)
            .await
            .map_err(|error| error.to_string())?;
        ensure_return_gas(&self.bridge, back, &budget).await?;
        side_costs.extend(budget.side_costs);

        // Rift reports no impact and no fees inside the quote, so its price is
        // the only price: the plan's oracle guard judges it.
        let price = receive_amount.to_f64() / amount_in.to_f64();
        let market_receive = ChainTokenAmount::from_formatted(final_asset.clone(), pay_amount.to_f64() * price).value;
        let haircut_bps = self
            .quote_route_fee_bps
            .checked_add(self.quote_delay_buffer_bps)
            .ok_or_else(|| "Rift conservative quote haircut overflowed u32".to_string())?;
        let conservative_receive = ChainTokenAmount::from_raw(
            final_asset.clone(),
            amount_after_bps_haircut(&receive_amount.value, haircut_bps)?,
        );

        let mut state = RiftState {
            liq_id: execution_id.to_string(),
            step: RiftStep::Order,
            last_error: None,
            size_in: pay_amount.clone(),
            final_asset: final_asset.clone(),
            final_destination,
            pay_chain: pay_chain.name().to_string(),
            pay_symbol: out.target_asset.to_string(),
            receive_symbol: back.source_asset.to_string(),
            amount_in,
            arrival_headroom,
            min_amount_out: conservative_receive.clone(),
            quote_id: None,
            order: None,
            funding: None,
            funding_txid: None,
            funding_sent_at_ts: None,
            bridge_out: Default::default(),
            bridge_in: Default::default(),
            size_out: None,
            failure: None,
            side_costs: Vec::new(),
            send_saved: false,
        };
        state.record_side_costs(&side_costs);
        let swap_quote = SwapQuote {
            pay_asset: request.pay_asset.clone(),
            pay_amount: request.pay_amount.value.clone(),
            receive_asset: request.receive_asset.clone(),
            receive_amount: receive_amount.value.clone(),
            mid_price: price,
            exec_price: price,
            estimated_price_impact_bps: 0.0,
            legs: vec![swap_leg(&state, &receive_amount, price)],
        };
        Ok(RiftPreview {
            state,
            quote: swap_quote,
            conservative_receive,
            market_receive,
            side_costs,
        })
    }

    /// Converts a completed leg into the generic execution result.
    pub(super) fn finish_state(&self, state: &RiftState) -> Result<SwapExecution, String> {
        let receive_amount = state
            .size_out
            .clone()
            .ok_or_else(|| "receive amount missing".to_string())?;
        let pay_f = state.size_in.to_f64();
        let exec_price = if pay_f > 0.0 {
            receive_amount.to_f64() / pay_f
        } else {
            0.0
        };
        Ok(SwapExecution {
            swap_id: 0,
            request_id: 0,
            status: "completed".to_string(),
            pay_asset: state.size_in.token.asset_id(),
            pay_amount: state.size_in.value.clone(),
            receive_asset: state.final_asset.asset_id(),
            receive_amount: receive_amount.value.clone(),
            mid_price: exec_price,
            exec_price,
            realized_slippage_bps: 0.0,
            legs: vec![swap_leg(state, &receive_amount, exec_price)],
            approval_count: None,
            ts: now_ts().max(0) as u64,
            side_costs: state.side_costs.clone(),
            side_cost_receive: None,
        })
    }
}
