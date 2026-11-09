//! The amount-scoped preview: the bridge out sized as execution sizes it, a
//! Rift quote for what arrives, and the bridge back's costs. Nothing moves.
use candid::Nat;
use liquidium_pipeline_connectors::backend::rift_backend::{RiftAsset, RiftBackend, RiftQuoteRequest};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::side_cost::SideCost,
};

use super::{
    finalizer::{RIFT_CHAIN, RiftVenueFinalizer, decimal_text, parse_amount},
    state::{RiftState, RiftStep},
};
use crate::{
    finalizers::{bridge_legs::bridged_deposit_arrival, multi_venue::RIFT_VENUE_ID},
    swappers::model::{LIQUIDITY_EPS, SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, amount_after_bps_haircut},
    utils::now_ts,
};

/// The one leg of a Rift route: the whole allocation for what Rift pays out.
fn swap_leg(state: &RiftState, receive_amount: &ChainTokenAmount, price: f64) -> SwapQuoteLeg {
    SwapQuoteLeg {
        venue: RIFT_VENUE_ID.to_string(),
        route_id: format!(
            "{}>{}",
            RiftAsset::new(RIFT_CHAIN, &state.pay_symbol).as_str(),
            RiftAsset::new(RIFT_CHAIN, &state.receive_symbol).as_str()
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
        let (out, back) = Self::leg_routes(&pay_amount.token, &final_asset)?;
        let arrival = bridged_deposit_arrival(&self.bridge, out, &pay_amount.token, &pay_amount)
            .await
            .map_err(|error| error.to_string())?;
        let mut side_costs = arrival.sizing.fee_budget.side_costs.clone();
        let amount_in = arrival.expected_amount;
        if amount_in <= LIQUIDITY_EPS {
            return Err("the bridge fees leave nothing for Rift to swap".to_string());
        }
        let pair = format!("{}->{}", out.target_asset, back.source_asset);
        let quote = self
            .backend
            .quote(&RiftQuoteRequest {
                from: RiftAsset::new(RIFT_CHAIN, out.target_asset),
                to: RiftAsset::new(RIFT_CHAIN, back.source_asset),
                amount_in: decimal_text(amount_in, pay_amount.token.decimals()),
            })
            .await
            .map_err(|error| format!("Rift cannot quote {pair}: {error}"))?;
        let amount_out = parse_amount(&quote.estimated_amount_out).map_err(|error| error.to_string())?;
        if amount_out <= LIQUIDITY_EPS {
            return Err(format!("Rift quotes nothing for {amount_in} {pair}"));
        }
        let budget = self
            .bridge
            .backend
            .get_fee_budget(back.source_asset, back.source_chain, back.target_asset)
            .await
            .map_err(|error| error.to_string())?;
        side_costs.extend(budget.side_costs);

        // Rift reports no impact and no fees inside the quote, so its price is
        // the only price: the plan's oracle guard judges it.
        let price = amount_out / amount_in;
        let receive_amount = ChainTokenAmount::from_formatted(final_asset.clone(), amount_out);
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
            pay_symbol: out.target_asset.to_string(),
            receive_symbol: back.source_asset.to_string(),
            amount_in,
            min_amount_out: conservative_receive.to_f64(),
            quote_id: None,
            order: None,
            funding: None,
            funding_txid: None,
            funding_sent_at_ts: None,
            bridge_out: Default::default(),
            bridge_in: Default::default(),
            size_out: None,
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
