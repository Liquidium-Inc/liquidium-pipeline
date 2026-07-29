use std::{collections::HashMap, sync::Arc};

use candid::{Nat, Principal};
use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::{MockCexBackend, OrderBook, OrderBookLevel};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistry},
    transfer::actions::MockTransferActions,
};

use super::*;
use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext},
    persistance::{VenueLegQuote, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
};

struct NoopCheckpoint;

fn planning_context() -> VenuePlanningContext {
    VenuePlanningContext {
        liquidation_id: "42".to_string(),
    }
}

#[async_trait::async_trait]
impl VenueLegCheckpoint for NoopCheckpoint {
    async fn checkpoint(&self, _progress: VenueLegProgress) -> Result<(), String> {
        Ok(())
    }
}

fn pay_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "PAY".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    }
}

fn receive_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::management_canister(),
        symbol: "RECV".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

fn request(raw_pay: u64) -> SwapRequest {
    let pay = pay_token();
    let receive = receive_token();
    SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(pay, Nat::from(raw_pay)),
        receive_asset: receive.asset_id(),
        receive_address: None,
        max_slippage_bps: Some(100),
        venue_hint: Some("mexc".to_string()),
    }
}

fn finalizer() -> MexcFinalizer<MockCexBackend> {
    let mut backend = MockCexBackend::new();
    backend.expect_get_orderbook().returning(|market, _| match market {
        "PAY_RECV" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 10.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }),
        _ => Err(format!("unexpected market {market}")),
    });
    let tokens = HashMap::from([
        (pay_token().asset_id(), pay_token()),
        (receive_token().asset_id(), receive_token()),
    ]);
    MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
    .with_token_registry(Arc::new(TokenRegistry::new(tokens)))
}

fn finalizer_without_registry() -> MexcFinalizer<MockCexBackend> {
    MexcFinalizer::new(
        Arc::new(MockCexBackend::new()),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
}

fn finalizer_with_permanent_deposit_error() -> MexcFinalizer<MockCexBackend> {
    let mut backend = MockCexBackend::new();
    backend.expect_get_orderbook().returning(|market, _| match market {
        "PAY_RECV" => Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 10.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }),
        _ => Err(format!("unexpected market {market}")),
    });
    backend.expect_get_balance().returning(|_| Ok(0.0));
    backend
        .expect_get_deposit_address()
        .returning(|_, _| Err(format!("{FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX}: test amount floor")));
    let tokens = HashMap::from([
        (pay_token().asset_id(), pay_token()),
        (receive_token().asset_id(), receive_token()),
    ]);
    MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
    .with_token_registry(Arc::new(TokenRegistry::new(tokens)))
}

fn finalizer_with_retryable_deposit_error() -> MexcFinalizer<MockCexBackend> {
    let mut backend = MockCexBackend::new();
    backend.expect_get_orderbook().returning(|_, _| {
        Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 10.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        })
    });
    backend.expect_get_balance().times(1).returning(|_| Ok(0.0));
    backend
        .expect_get_deposit_address()
        .times(1)
        .returning(|_, _| Err("deposit address request timed out".to_string()));
    let tokens = HashMap::from([
        (pay_token().asset_id(), pay_token()),
        (receive_token().asset_id(), receive_token()),
    ]);
    MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(MockTransferActions::new()),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
    .with_token_registry(Arc::new(TokenRegistry::new(tokens)))
}

fn leg_from_preview(preview: VenueRoutePreview) -> VenueLegState {
    VenueLegState {
        leg_id: "mexc-leg-0".to_string(),
        venue_id: "mexc".to_string(),
        request: preview.request,
        quote: VenueLegQuote {
            pay_amount: ChainTokenAmount::from_raw(pay_token(), preview.quote.pay_amount),
            estimated_receive: ChainTokenAmount::from_raw(receive_token(), preview.quote.receive_amount),
            conservative_receive: preview.conservative_receive,
            estimated_price_impact_bps: preview.quote.estimated_price_impact_bps,
            route_id: preview.quote.legs[0].route_id.clone(),
        },
        execution: preview.initial_execution_state,
        status: VenueLegStatus::Planned,
        result: None,
        last_error: None,
    }
}

#[tokio::test]
async fn preview_uses_the_exact_leg_allocation_and_persists_the_route() {
    let finalizer = finalizer();
    let request = request(200_000_000); // 2 PAY, not the liquidation's full receipt amount.

    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect("MEXC preview should succeed");

    assert_eq!(preview.quote.pay_amount, Nat::from(200_000_000u64));
    assert!(preview.quote.receive_amount < Nat::from(20_000_000u64));
    assert!(preview.conservative_receive.value < preview.quote.receive_amount);
    let execution = preview
        .initial_execution_state
        .decode::<MexcVenueExecutionState>("mexc")
        .expect("state should decode")
        .expect("state should be MEXC");
    assert_eq!(execution.cex.size_in, request.pay_amount);
    assert_eq!(execution.cex.trade.trade_resolved_legs.len(), 1);
    assert_eq!(execution.cex.trade.trade_resolved_legs[0].market, "PAY_RECV");
    uuid::Uuid::parse_str(execution.cex.liq_id.trim_start_matches("mexc-"))
        .expect("execution ID should contain a restart-safe UUID");
}

#[tokio::test]
async fn preview_persists_the_per_leg_receive_address() {
    let finalizer = finalizer();
    let mut request = request(100_000_000);
    request.receive_address = Some(Principal::from_slice(&[42]).to_text());

    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect("MEXC preview should succeed");
    let execution = preview
        .initial_execution_state
        .decode::<MexcVenueExecutionState>("mexc")
        .expect("state should decode")
        .expect("state should be MEXC");

    assert_eq!(
        execution.cex.withdraw.withdraw_address,
        request.receive_address.unwrap()
    );
}

#[test]
fn adapter_configuration_rejects_a_missing_token_registry_before_preview() {
    let error = MultiVenueAdapter::validate_configuration(&finalizer_without_registry())
        .expect_err("registry is required for adapter registration");

    assert!(error.contains("token registry"));
}

#[tokio::test]
async fn preview_rejects_zero_amount_before_orderbook_io() {
    let tokens = HashMap::from([
        (pay_token().asset_id(), pay_token()),
        (receive_token().asset_id(), receive_token()),
    ]);
    let finalizer = finalizer_without_registry().with_token_registry(Arc::new(TokenRegistry::new(tokens)));

    // This backend has no orderbook expectation, so any route-resolution I/O
    // before the amount check would fail the test.
    let error = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(0))
        .await
        .expect_err("zero amount must be rejected");

    assert!(error.contains("non-positive pay amount"));
}

#[tokio::test]
async fn preview_rejects_slippage_above_ten_thousand_bps() {
    let finalizer = finalizer();
    let mut request = request(100_000_000);
    request.max_slippage_bps = Some(10_001);

    let error = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request)
        .await
        .expect_err("invalid basis-point limit must not be clamped");

    assert!(error.contains("exceeds 10000"));
}

#[test]
fn persisted_adapter_state_defaults_a_missing_persistence_gate_to_closed() {
    let state = MexcVenueExecutionState {
        cex: finalizer()
            .prepare_amount_scoped_state(
                "mexc-test",
                ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
                receive_token(),
                None,
            )
            .expect("state"),
        ready_to_advance: true,
        intent_id: None,
        operator_required: false,
    };
    let mut encoded = serde_json::to_value(state).expect("encode state");
    encoded
        .as_object_mut()
        .expect("state object")
        .remove("ready_to_advance");

    let decoded: MexcVenueExecutionState = serde_json::from_value(encoded).expect("decode legacy state");
    assert!(!decoded.ready_to_advance);
}

#[tokio::test]
async fn first_advance_only_opens_the_parent_persistence_gate() {
    let finalizer = finalizer();
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let leg = leg_from_preview(preview);

    // No transfer/backend side-effect expectations are configured. Any such
    // call would fail this test; the first cycle must only return durable state.
    let progress = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("gate transition should succeed");
    let execution = progress
        .execution
        .decode::<MexcVenueExecutionState>("mexc")
        .expect("state should decode")
        .expect("state should be MEXC");

    assert!(execution.ready_to_advance);
    assert_eq!(execution.cex.step, CexStep::Deposit);
    assert_eq!(progress.status, VenueLegStatus::Running);
    assert!(progress.result.is_none());
}

#[tokio::test]
async fn restart_with_an_outstanding_intent_requires_operator_instead_of_replaying() {
    let original = finalizer();
    let preview = MultiVenueAdapter::preview(&original, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);
    let armed = MultiVenueAdapter::advance(&original, &leg, &NoopCheckpoint)
        .await
        .expect("first cycle should persist an intent");
    leg.execution = armed.execution;
    leg.status = armed.status;

    // A reconstructed adapter has no process-local proof that the external
    // call did not already happen before the crash. It must not call MEXC or
    // the transfer service again.
    let restarted = finalizer();
    let progress = MultiVenueAdapter::advance(&restarted, &leg, &NoopCheckpoint)
        .await
        .expect("ambiguous intent should be parked");

    assert_eq!(progress.status, VenueLegStatus::OperatorRequired);
    assert!(
        progress
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("ambiguous submission state"))
    );

    // Once an operator explicitly re-enqueues the parked row, recovery clears
    // the stale process-local intent and persists a fresh gate before retrying.
    leg.execution = progress.execution;
    leg.status = progress.status;
    let rearmed = MultiVenueAdapter::recover(&restarted, &leg, &NoopCheckpoint)
        .await
        .expect("explicit recovery should re-arm after restart");
    let execution = rearmed
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("decode re-armed state")
        .expect("MEXC state");
    assert_eq!(rearmed.status, VenueLegStatus::Running);
    assert!(!execution.operator_required);
    assert!(execution.ready_to_advance);
    assert!(execution.intent_id.is_some());
}

#[tokio::test]
async fn completed_leg_returns_a_result_without_parent_wal_access() {
    let finalizer = finalizer();
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);
    let mut execution = leg
        .execution
        .decode::<MexcVenueExecutionState>("mexc")
        .expect("state should decode")
        .expect("state should be MEXC");
    execution.cex.step = CexStep::Completed;
    execution.cex.withdraw.size_out = Some(ChainTokenAmount::from_raw(receive_token(), Nat::from(10_000_000u64)));
    leg.execution = VenueExecutionState::new("mexc", &execution).expect("state should encode");

    let progress = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("completed leg should project to a result");

    assert_eq!(progress.status, VenueLegStatus::Completed);
    assert_eq!(
        progress.result.expect("result should exist").pay_amount,
        Nat::from(100_000_000u64)
    );
}

#[tokio::test]
async fn adapter_rejects_a_mutated_committed_allocation() {
    let finalizer = finalizer();
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);
    leg.request.pay_amount.value = Nat::from(200_000_000u64);

    let error = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect_err("mutated allocation must be rejected");
    assert!(error.contains("persisted allocation"));
}

#[tokio::test]
async fn recover_accepts_each_durable_pending_reconciliation_phase() {
    let finalizer = finalizer();
    for pending_step in [CexStep::DepositPending, CexStep::TradePending, CexStep::WithdrawPending] {
        let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
            .await
            .expect("preview should succeed");
        let mut leg = leg_from_preview(preview);
        let mut execution = leg
            .execution
            .decode::<MexcVenueExecutionState>("mexc")
            .expect("state should decode")
            .expect("state should be MEXC");
        execution.cex.step = pending_step;
        leg.execution = VenueExecutionState::new("mexc", &execution).expect("state should encode");

        let progress = MultiVenueAdapter::recover(&finalizer, &leg, &NoopCheckpoint)
            .await
            .expect("pending leg should enter reconciliation");
        let recovered = progress
            .execution
            .decode::<MexcVenueExecutionState>("mexc")
            .expect("state should decode")
            .expect("state should be MEXC");

        assert!(recovered.ready_to_advance);
        assert_eq!(recovered.cex.step, pending_step);
        assert_eq!(progress.status, VenueLegStatus::Running);
    }
}

#[tokio::test]
async fn permanent_cex_error_marks_only_the_mexc_leg_failed() {
    let finalizer = finalizer_with_permanent_deposit_error();
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);

    let armed = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("first cycle should arm the leg");
    leg.execution = armed.execution;
    let failed = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("permanent failure should be returned as leg progress");

    assert_eq!(failed.status, VenueLegStatus::FailedPermanent);
    assert!(
        failed
            .last_error
            .as_deref()
            .is_some_and(|error| error.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX))
    );
    // Diagnostics only: a permanent failure must not also ask to be retried.
    assert_eq!(failed.retryable_error, None);

    // A leg already persisted as failed reports the same way when re-advanced.
    leg.execution = failed.execution;
    leg.status = failed.status;
    let replayed = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("an already failed leg re-reports its terminal state");
    assert_eq!(replayed.status, VenueLegStatus::FailedPermanent);
    assert!(replayed.last_error.is_some());
    assert_eq!(replayed.retryable_error, None);
}

#[tokio::test]
async fn ordinary_cex_error_remains_retryable_without_operator_intervention() {
    let finalizer = finalizer_with_retryable_deposit_error();
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);

    let armed = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("first cycle should arm the leg");
    leg.execution = armed.execution;
    let retryable = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("ordinary CEX failure should become durable retryable progress");
    assert_eq!(retryable.status, VenueLegStatus::Running);
    assert_eq!(
        retryable.retryable_error.as_deref(),
        Some("deposit address request timed out")
    );
    let execution = retryable
        .execution
        .decode::<MexcVenueExecutionState>("mexc")
        .expect("decode retryable state")
        .expect("MEXC state");
    assert!(!execution.operator_required);
    assert!(!execution.ready_to_advance);
    assert!(execution.intent_id.is_none());
    assert_eq!(
        execution.cex.last_error,
        Some("deposit address request timed out".to_string())
    );
}
