//! Shared fixtures for the Rift venue tests: tokens, a wired finalizer and a
//! leg built from a preview.
use std::{collections::HashMap, sync::Arc};

use candid::{Nat, Principal};
use liquidium_pipeline_connectors::backend::{
    bridge_backend::{BridgeFeeBudget, MockBridgeBackend},
    rift_backend::{MockRiftBackend, RiftAsset, RiftOrder, RiftOrderState, RiftOrderStatus, RiftQuote},
};
use liquidium_pipeline_core::{
    outbound::{MockOutboundSender, SendOutcome},
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistry},
};

use super::*;
use crate::{
    bridge::runtime::BridgeRuntime,
    finalizers::{
        multi_venue::{RIFT_VENUE_ID, VenueLegCheckpoint, VenueLegProgress},
        rift::state::{RiftOrderRecord, RiftState, RiftStep},
    },
    persistance::{VenueLegQuote, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
};

mod bridge_back;
mod bridge_out;
mod flow;
mod operator;
mod order;
mod preview;
mod wait;

pub(super) const MAIN_WALLET: &str = "0x1111111111111111111111111111111111111111";
pub(super) const BRIDGE_BTC_WALLET: &str = "bc1p57z9rf82tcpcgcr373ya43lj4x66x2xnumkk995usg6c5nuus6lqv63wcr";
pub(super) const DEPOSIT_ADDRESS: &str = "0xdddddddddddddddddddddddddddddddddddddddd";

pub(super) fn ckusdc() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

pub(super) fn ckusdt() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::management_canister(),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

pub(super) fn cketh() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_slice(&[9]),
        symbol: "ckETH".to_string(),
        decimals: 18,
        fee: Nat::from(2_000_000_000_000u64),
    }
}

pub(super) fn ckbtc() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_slice(&[7]),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(10u64),
    }
}

pub(super) fn request(raw_pay: u64) -> SwapRequest {
    SwapRequest {
        pay_asset: ckusdc().asset_id(),
        pay_amount: ChainTokenAmount::from_raw(ckusdc(), Nat::from(raw_pay)),
        receive_asset: ckusdt().asset_id(),
        receive_address: None,
        max_slippage_bps: Some(100),
        venue_hint: Some(RIFT_VENUE_ID.to_string()),
    }
}

pub(super) fn registry() -> Arc<TokenRegistry> {
    Arc::new(TokenRegistry::new(HashMap::from([
        (ckusdc().asset_id(), ckusdc()),
        (ckusdt().asset_id(), ckusdt()),
        (ckbtc().asset_id(), ckbtc()),
        (cketh().asset_id(), cketh()),
    ])))
}

/// A leg saved before the receive chain existed paid out on Ethereum.
#[test]
fn an_older_saved_leg_receives_on_ethereum() {
    let mut saved = serde_json::to_value(state_at(RiftStep::Order)).unwrap();
    saved.as_object_mut().unwrap().remove("receive_chain");
    saved["order"].as_object_mut().unwrap().remove("to_address");

    let state: RiftState = serde_json::from_value(saved).unwrap();

    assert_eq!(state.receive_chain(), Ok(RiftChain::Ethereum));
    assert_eq!(state.order.unwrap().to_address, None);
}

/// The step was called `Wait` before; a row saved then still loads, and saves the new name.
#[test]
fn a_leg_saved_at_the_old_wait_step_loads_at_swap() {
    let mut saved = serde_json::to_value(state_at(RiftStep::Swap)).unwrap();
    saved["step"] = serde_json::json!("Wait");

    let state: RiftState = serde_json::from_value(saved).unwrap();

    assert_eq!(state.step, RiftStep::Swap);
    assert_eq!(serde_json::to_value(&state).unwrap()["step"], "Swap");
}

/// A state at `step` for a leg that pays ckUSDT and receives ckBTC on Bitcoin.
pub(super) fn state_receiving_ckbtc(step: RiftStep) -> RiftState {
    let mut state = state_at(step);
    state.size_in = ChainTokenAmount::from_raw(ckusdt(), Nat::from(100_010_000u64));
    state.final_asset = ckbtc();
    state.receive_chain = "bitcoin".to_string();
    state.pay_symbol = "USDT".to_string();
    state.receive_symbol = "BTC".to_string();
    state
}

/// A bridge whose withdrawal of `asset` keeps `source_fee` back, takes
/// `destination_fee` out of the amount, and refuses less than `minimum`.
/// Every other route is free of charge.
pub(super) fn minter_bridge(
    asset: &'static str,
    source_fee: f64,
    destination_fee: f64,
    minimum: f64,
) -> MockBridgeBackend {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_fee_budget().returning(move |pay, _, _| {
        Ok(if pay == asset {
            BridgeFeeBudget {
                source_fee_budget: source_fee,
                destination_fee_budget: destination_fee,
                ..BridgeFeeBudget::default()
            }
        } else {
            BridgeFeeBudget::default()
        })
    });
    bridge
        .expect_get_minimum_bridge_amount()
        .returning(move |pay, _, _| Ok(if pay == asset { minimum } else { 0.0 }));
    bridge
}

/// A bridge whose fee budgets are free of charge and carry no side costs.
pub(super) fn free_bridge() -> MockBridgeBackend {
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_fee_budget()
        .returning(|_, _, _| Ok(BridgeFeeBudget::default()));
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.0));
    bridge
}

pub(super) fn quote(amount_in: &str, amount_out: &str) -> RiftQuote {
    RiftQuote {
        id: "quote-1".to_string(),
        from: RiftAsset::new("ethereum", "USDC"),
        to: RiftAsset::new("ethereum", "USDT"),
        amount_in: amount_in.to_string(),
        estimated_amount_out: amount_out.to_string(),
        estimated_execution_cost: "0.1".to_string(),
        expires_at: "2026-11-09T12:10:00Z".to_string(),
        venues: vec!["uniswap".to_string()],
    }
}

pub(super) fn order(amount_in: &str) -> RiftOrder {
    RiftOrder {
        id: "order-1".to_string(),
        quote_id: "quote-1".to_string(),
        deposit_address: DEPOSIT_ADDRESS.to_string(),
        amount_in: amount_in.to_string(),
        deposit_deadline: "2026-11-16T12:00:00Z".to_string(),
        status: RiftOrderStatus::AwaitingDeposit,
    }
}

pub(super) fn order_state(status: RiftOrderStatus, amount_out: Option<&str>) -> RiftOrderState {
    RiftOrderState {
        id: "order-1".to_string(),
        status,
        amount_out: amount_out.map(str::to_string),
        payout_tx: amount_out.map(|_| "0xpayout".to_string()),
        refund_address: Some(MAIN_WALLET.to_string()),
    }
}

/// A trader sender whose funding transfer lands on the first try.
pub(super) fn landing_sender() -> MockOutboundSender {
    let mut sender = MockOutboundSender::new();
    sender.expect_prepare().returning(|_| {
        Ok(liquidium_pipeline_core::outbound::PreparedSend {
            sender: "icp".into(),
            identity: "funding-1".into(),
            payload: "{}".into(),
            replay_safe: true,
        })
    });
    sender.expect_submit().returning(|_| SendOutcome::Landed {
        receipt: "block-9".into(),
    });
    sender
}

pub(super) fn finalizer(
    rift: MockRiftBackend,
    bridge: MockBridgeBackend,
    trader_sender: MockOutboundSender,
) -> RiftVenueFinalizer<MockRiftBackend> {
    finalizer_with_runtime(rift, BridgeRuntime::for_tests(Arc::new(bridge)), trader_sender)
}

pub(super) fn finalizer_with_runtime(
    rift: MockRiftBackend,
    runtime: BridgeRuntime,
    trader_sender: MockOutboundSender,
) -> RiftVenueFinalizer<MockRiftBackend> {
    RiftVenueFinalizer::new(
        Arc::new(rift),
        Arc::new(runtime),
        Arc::new(trader_sender),
        Principal::anonymous(),
        RiftRefundAddresses {
            ethereum: MAIN_WALLET.to_string(),
            bitcoin: BRIDGE_BTC_WALLET.to_string(),
        },
    )
    .with_token_registry(registry())
    .with_quote_costs(10, 20)
}

/// A state at `step` with an order saved, past the preview.
pub(super) fn state_at(step: RiftStep) -> RiftState {
    RiftState {
        liq_id: "liq-1".to_string(),
        step,
        last_error: None,
        size_in: ChainTokenAmount::from_raw(ckusdc(), Nat::from(100_010_000u64)),
        final_asset: ckusdt(),
        final_destination: Principal::anonymous().to_text(),
        pay_chain: "ethereum".to_string(),
        receive_chain: "ethereum".to_string(),
        pay_symbol: "USDC".to_string(),
        receive_symbol: "USDT".to_string(),
        amount_in: ChainTokenAmount::from_formatted(ckusdc(), 100.0),
        arrival_headroom: ChainTokenAmount::from_raw(ckusdc(), Nat::from(0u8)),
        min_amount_out: ChainTokenAmount::from_formatted(ckusdt(), 99.0),
        quote_id: None,
        order: Some(RiftOrderRecord {
            id: "order-1".to_string(),
            quote_id: "quote-1".to_string(),
            deposit_address: DEPOSIT_ADDRESS.to_string(),
            amount_in: "100".to_string(),
            deposit_deadline: "2026-11-16T12:00:00Z".to_string(),
            status: None,
            amount_out: None,
            payout_tx: None,
            polled_at_ts: None,
            wait_since_ts: None,
            delivered_at_ts: None,
            refund_tx: None,
            to_address: None,
        }),
        funding: None,
        funding_txid: None,
        funding_sent_at_ts: None,
        bridge_out: Default::default(),
        bridge_in: Default::default(),
        size_out: None,
        failure: None,
        side_costs: Vec::new(),
        send_saved: false,
    }
}

pub(super) struct NoopCheckpoint;

#[async_trait::async_trait]
impl VenueLegCheckpoint for NoopCheckpoint {
    async fn checkpoint(&self, _progress: VenueLegProgress) -> Result<(), String> {
        Ok(())
    }
}

pub(super) fn leg_from_preview(preview: crate::finalizers::multi_venue::VenueRoutePreview) -> VenueLegState {
    VenueLegState {
        leg_id: "rift-leg-0".to_string(),
        venue_id: RIFT_VENUE_ID.to_string(),
        request: preview.request,
        quote: VenueLegQuote {
            pay_amount: ChainTokenAmount::from_raw(ckusdc(), preview.quote.pay_amount),
            estimated_receive: ChainTokenAmount::from_raw(ckusdt(), preview.quote.receive_amount),
            conservative_receive: preview.conservative_receive,
            estimated_price_impact_bps: preview.quote.estimated_price_impact_bps,
            route_id: preview.quote.legs[0].route_id.clone(),
            side_costs: Vec::new(),
        },
        execution: preview.initial_execution_state,
        status: VenueLegStatus::Planned,
        result: None,
        last_error: None,
        failure: None,
    }
}
