use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use serde_json::json;

use super::*;
use crate::{
    persistance::{FinalizerDecisionSnapshot, VenueExecutionState},
    swappers::model::{SwapExecution, SwapRequest},
};

fn token(ledger: u8, symbol: &str, decimals: u8) -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_slice(&[ledger]),
        symbol: symbol.to_string(),
        decimals,
        fee: Nat::from(10u64),
    }
}

fn amount(token: &ChainToken, value: u64) -> ChainTokenAmount {
    ChainTokenAmount::from_raw(token.clone(), Nat::from(value))
}

fn leg(leg_id: &str, venue_id: &str, pay: &ChainTokenAmount, receive: &ChainTokenAmount) -> VenueLegState {
    VenueLegState {
        leg_id: leg_id.to_string(),
        venue_id: venue_id.to_string(),
        request: SwapRequest {
            pay_asset: pay.token.asset_id(),
            pay_amount: pay.clone(),
            receive_asset: receive.token.asset_id(),
            receive_address: None,
            max_slippage_bps: Some(100),
            venue_hint: Some(venue_id.to_string()),
        },
        quote: VenueLegQuote {
            pay_amount: pay.clone(),
            estimated_receive: receive.clone(),
            conservative_receive: receive.clone(),
            estimated_slippage_bps: 50.0,
            route_id: format!("{venue_id}-route"),
        },
        execution: VenueExecutionState::new(venue_id, &json!({ "step": "planned" })).expect("execution state"),
        status: VenueLegStatus::Planned,
        result: None,
        last_error: None,
    }
}

fn execution(pay: &ChainTokenAmount, receive: &ChainTokenAmount) -> SwapExecution {
    SwapExecution {
        swap_id: 1,
        request_id: 2,
        status: "completed".to_string(),
        pay_asset: pay.token.asset_id(),
        pay_amount: pay.value.clone(),
        receive_asset: receive.token.asset_id(),
        receive_amount: receive.value.clone(),
        mid_price: 1.0,
        exec_price: 0.99,
        slippage: 100.0,
        legs: Vec::new(),
        approval_count: None,
        ts: 123,
    }
}

fn state_with_venues(venues: &[(&str, u64)]) -> MultiVenueExecutionState {
    let pay_token = token(1, "ICP", 8);
    let receive_token = token(2, "ckUSDC", 6);
    let total = venues.iter().map(|(_, value)| *value).sum();
    let total_pay = amount(&pay_token, total);
    let estimated_receive = amount(&receive_token, total * 5);
    let legs = venues
        .iter()
        .enumerate()
        .map(|(index, (venue_id, value))| {
            let pay = amount(&pay_token, *value);
            let receive = amount(&receive_token, *value * 5);
            leg(&format!("{venue_id}-{index}"), venue_id, &pay, &receive)
        })
        .collect();

    MultiVenueExecutionState {
        plan: MultiVenueExecutionPlan {
            strategy_id: "icpswap_first".to_string(),
            total_pay,
            receive_asset: receive_token.asset_id(),
            debt_repaid: amount(&receive_token, total * 4),
            allocation_reason: if venues.len() == 1 {
                MultiVenueAllocationReason::SingleVenue {
                    venue_id: venues[0].0.to_string(),
                }
            } else {
                MultiVenueAllocationReason::PriceImpactSplit
            },
            min_net_edge_bps: 150,
            estimated_receive: estimated_receive.clone(),
            conservative_receive: estimated_receive,
            combined_net_edge_bps: 175.0,
            quoted_at: 123,
        },
        legs,
        outcome: MultiVenueExecutionOutcome::Running,
    }
}

fn envelope(state: MultiVenueExecutionState) -> FinalizerMetaV2 {
    FinalizerMetaV2 {
        version: FINALIZER_META_V2_VERSION,
        payload: FinalizerMetaPayload::MultiVenueSwap(state),
    }
}

#[test]
fn envelope_has_exact_version_kind_and_state_shape() {
    let encoded = serde_json::to_value(envelope(state_with_venues(&[("icpswap", 100)]))).expect("serialize envelope");

    assert_eq!(encoded["version"], FINALIZER_META_V2_VERSION);
    assert_eq!(encoded["kind"], "multi_venue_swap");
    assert!(encoded["state"]["plan"].is_object());
    assert!(encoded["state"]["legs"].is_array());
    assert_eq!(encoded["state"]["outcome"], json!({ "status": "running" }));
    assert!(encoded.get("payload").is_none());
}

#[test]
fn mexc_and_icpswap_leg_vector_round_trips() {
    let original = envelope(state_with_venues(&[("icpswap", 75), ("mexc", 25)]));
    let encoded = serde_json::to_vec(&original).expect("serialize envelope");
    let decoded: FinalizerMetaV2 = serde_json::from_slice(&encoded).expect("deserialize envelope");

    assert_eq!(decoded, original);
    decoded.validate().expect("valid multi-venue envelope");
}

#[test]
fn arbitrary_venue_id_round_trips_without_schema_changes() {
    let original = envelope(state_with_venues(&[("kraken", 100)]));
    let encoded = serde_json::to_vec(&original).expect("serialize envelope");
    let decoded: FinalizerMetaV2 = serde_json::from_slice(&encoded).expect("deserialize envelope");

    assert_eq!(decoded, original);
    decoded.validate().expect("arbitrary venue should be valid");
}

#[test]
fn legacy_decision_snapshot_defaults_multi_venue_allocation_to_none() {
    let snapshot: FinalizerDecisionSnapshot = serde_json::from_value(json!({
        "mode": "hybrid",
        "chosen": "cex",
        "reason": "legacy decision",
        "min_required_bps": 150.0,
        "dex_preview_gross_bps": 200.0,
        "dex_preview_net_bps": 175.0,
        "cex_preview_gross_bps": 190.0,
        "cex_preview_net_bps": 165.0,
        "ts": 123
    }))
    .expect("legacy decision snapshot should decode");

    assert!(snapshot.multi_venue_allocation.is_none());
}

#[test]
fn completed_result_round_trips_inside_a_leg() {
    let mut original = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut original.payload;
    let pay = state.legs[0].request.pay_amount.clone();
    let receive = state.legs[0].quote.estimated_receive.clone();
    state.legs[0].result = Some(execution(&pay, &receive));
    state.legs[0].status = VenueLegStatus::Completed;

    let encoded = serde_json::to_vec(&original).expect("serialize completed envelope");
    let decoded: FinalizerMetaV2 = serde_json::from_slice(&encoded).expect("deserialize completed envelope");
    assert_eq!(decoded, original);
}

#[test]
fn validation_rejects_unsupported_version() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    meta.version = FINALIZER_META_V2_VERSION + 1;

    assert!(
        meta.validate()
            .expect_err("version must fail")
            .contains("unsupported finalizer meta version")
    );
}

#[test]
fn validation_rejects_empty_leg_id() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[0].leg_id = "  ".to_string();

    assert!(
        meta.validate()
            .expect_err("empty leg id must fail")
            .contains("leg_id must not be empty")
    );
}

#[test]
fn validation_rejects_duplicate_leg_id() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 75), ("mexc", 25)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[1].leg_id = state.legs[0].leg_id.clone();

    assert!(
        meta.validate()
            .expect_err("duplicate leg id must fail")
            .contains("duplicate venue leg_id")
    );
}

#[test]
fn validation_rejects_empty_venue_id() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[0].venue_id.clear();

    assert!(
        meta.validate()
            .expect_err("empty venue id must fail")
            .contains("venue_id must not be empty")
    );
}

#[test]
fn validation_rejects_mismatched_execution_venue() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[0].execution.venue = "mexc".to_string();

    assert!(
        meta.validate()
            .expect_err("venue mismatch must fail")
            .contains("venue mismatch")
    );
}

#[test]
fn validation_rejects_allocation_total_mismatch() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[0].request.pay_amount.value = Nat::from(99u64);

    assert!(
        meta.validate()
            .expect_err("allocation mismatch must fail")
            .contains("allocations sum")
    );
}

#[test]
fn validation_rejects_allocation_token_mismatch() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 100)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[0].request.pay_amount.token = token(9, "OTHER", 8);

    assert!(
        meta.validate()
            .expect_err("token mismatch must fail")
            .contains("pay token mismatch")
    );
}

#[test]
fn validation_rejects_duplicate_venue_for_icpswap_first() {
    let mut meta = envelope(state_with_venues(&[("icpswap", 75), ("mexc", 25)]));
    let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload;
    state.legs[1].venue_id = "icpswap".to_string();
    state.legs[1].execution.venue = "icpswap".to_string();

    assert!(
        meta.validate()
            .expect_err("duplicate venue must fail")
            .contains("does not allow multiple legs")
    );
}
