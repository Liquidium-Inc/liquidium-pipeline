use std::{collections::HashMap, sync::Arc};

use candid::{Nat, Principal};
use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackendError, MockCexBackend, OrderBook, OrderBookLevel, SwapFillReport,
};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistry},
    transfer::actions::MockTransferActions,
};

use super::*;
use crate::{
    finalizers::{
        cex_finalizer::CexRouteLeg,
        multi_venue::{MultiVenueAdapter, VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext},
    },
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
async fn restart_polls_a_direct_deposit_then_gates_the_trade_separately() {
    let mut cex = finalizer()
        .prepare_amount_scoped_state(
            "deposit-receipt-poll",
            ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
            receive_token(),
            None,
        )
        .unwrap();
    cex.step = CexStep::DepositPending;
    cex.deposit.deposit_txid = Some("persisted-transfer".into());
    cex.deposit.deposit_balance_before = Some(0.0);
    cex.trade.trade_next_amount_in = Some(0.9999);
    let mut execution = after_restart(cex);

    for (balance, expected_step) in [(0.0, CexStep::DepositPending), (1.0, CexStep::Trade)] {
        let mut restarted = finalizer();
        Arc::get_mut(&mut restarted.backend)
            .unwrap()
            .expect_get_balance()
            .times(1)
            .returning(move |asset| {
                assert_eq!(asset, "PAY");
                Ok(balance)
            });
        execution.ready_to_advance = true;
        execution.intent_id = Some("lost-process-local-gate".into());
        let restored = serde_json::from_str(&serde_json::to_string(&execution).unwrap()).unwrap();
        let progress = restarted.advance_decoded_leg(restored).await.unwrap();
        assert_eq!(progress.status, VenueLegStatus::Running);
        assert!(progress.last_error.is_none());
        execution = progress.execution.decode(VENUE_ID).unwrap().unwrap();
        assert_eq!(execution.cex.step, expected_step);
        assert_eq!(execution.cex.deposit.deposit_txid.as_deref(), Some("persisted-transfer"));
        assert!(!execution.operator_required);
        assert!(!execution.ready_to_advance);
        assert!(execution.intent_id.is_none());
    }

    // Confirmation must not submit an order; the trade needs a new durable gate.
    let progress = finalizer().advance_decoded_leg(execution).await.unwrap();
    let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
    assert_eq!(execution.cex.step, CexStep::Trade);
    assert!(execution.ready_to_advance);
    assert!(execution.intent_id.is_some());
}

/// A leg as the orchestrator hands it back after a process restart: armed in a
/// process that no longer exists.
fn after_restart(cex: CexState) -> MexcVenueExecutionState {
    MexcVenueExecutionState {
        cex,
        ready_to_advance: true,
        intent_id: Some("lost-process-local-gate".into()),
        operator_required: false,
    }
}

fn deposit_poll_finalizer(
    bridge: liquidium_pipeline_connectors::backend::bridge_backend::MockBridgeBackend,
) -> MexcFinalizer<MockCexBackend> {
    finalizer().with_bridge_runtime(crate::finalizers::bridge_runtime::BridgeRuntime::for_tests(Arc::new(bridge)))
}

fn amount_check_request(token: ChainToken, raw: u64) -> SwapRequest {
    SwapRequest {
        pay_asset: token.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(token, Nat::from(raw)),
        ..request(1)
    }
}

fn cketh_amount_check_request(raw: u64) -> SwapRequest {
    let token = ChainToken::Icp {
        ledger: Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").unwrap(),
        symbol: "ckETH".into(),
        decimals: 18,
        fee: Nat::from(2_000_000_000_000u64),
    };
    amount_check_request(token, raw)
}

#[tokio::test]
async fn cksol_planning_amount_check_uses_the_minter_minimum_after_the_approval_reserve() {
    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeFeeBudget, MockBridgeBackend};

    let cksol = ChainToken::Icp {
        ledger: Principal::from_text("ls5lp-lqaaa-aaaar-qb5oa-cai").unwrap(),
        symbol: "ckSOL".into(),
        decimals: 9,
        fee: Nat::from(500u64),
    };
    // 7 ledger fees of 500 come off the transfer, then a 1_000 lamport reserve.
    for (raw, below_minimum) in [
        (9_000_000, true),
        (10_004_000, true),
        (10_004_500, false),
        (10_005_000, false),
    ] {
        let mut bridge = MockBridgeBackend::new();
        bridge
            .expect_get_fee_budget()
            .times(1)
            .returning(|asset, chain, target| {
                assert_eq!((asset, chain, target), ("ckSOL", "ICP", "SOL"));
                Ok(BridgeFeeBudget {
                    source_fee_budget: 0.000_001,
                    ..BridgeFeeBudget::default()
                })
            });
        bridge
            .expect_get_minimum_bridge_amount()
            .times(1)
            .returning(|_, _, _| Ok(0.01));
        bridge.expect_submit_bridge().times(0);
        let finalizer = deposit_poll_finalizer(bridge);

        let result = finalizer.check_amount(&amount_check_request(cksol.clone(), raw)).await;

        if below_minimum {
            let Err(VenueAmountError::BelowMinimum(reason)) = result else {
                panic!("expected a below-minimum decision for {raw}, got {result:?}");
            };
            assert!(reason.contains("ckSOL -> SOL"), "{reason}");
            assert!(reason.contains("0.01"), "{reason}");
        } else {
            result.unwrap_or_else(|error| panic!("{raw} lamports must pass: {error}"));
        }
    }
}

#[tokio::test]
async fn cketh_planning_amount_check_reserves_fees_before_comparing_the_bridge_minimum() {
    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeFeeBudget, MockBridgeBackend};

    for (raw, below_minimum) in [
        (4_000_000_000_000_000, true),
        (5_050_000_000_000_000, true),
        (5_064_000_000_000_000, false),
        (5_065_000_000_000_000, false),
    ] {
        let mut bridge = MockBridgeBackend::new();
        bridge
            .expect_get_fee_budget()
            .times(1)
            .returning(|asset, chain, target| {
                assert_eq!((asset, chain, target), ("ckETH", "ICP", "ETH"));
                Ok(BridgeFeeBudget {
                    source_fee_budget: 0.00005,
                    ..BridgeFeeBudget::default()
                })
            });
        bridge
            .expect_get_minimum_bridge_amount()
            .times(1)
            .returning(|_, _, _| Ok(0.005));
        bridge.expect_submit_bridge().times(0);
        bridge.expect_get_source_balance().times(0);
        let finalizer = deposit_poll_finalizer(bridge);

        let result = finalizer.check_amount(&cketh_amount_check_request(raw)).await;

        if below_minimum {
            let Err(VenueAmountError::BelowMinimum(reason)) = result else {
                panic!("expected a below-minimum decision, got {result:?}");
            };
            assert!(reason.contains("ckETH -> ETH"));
            assert!(reason.contains("0.005"));
        } else {
            result.expect("the exact minimum and larger amounts remain executable");
        }
    }
}

#[tokio::test]
async fn cketh_planning_amount_check_keeps_bridge_outages_retryable() {
    use liquidium_pipeline_connectors::backend::bridge_backend::MockBridgeBackend;
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_fee_budget()
        .times(1)
        .returning(|_, _, _| Err("minter unavailable".into()));
    bridge.expect_submit_bridge().times(0);
    let finalizer = deposit_poll_finalizer(bridge);

    assert!(matches!(
        finalizer
            .check_amount(&cketh_amount_check_request(4_000_000_000_000_000))
            .await,
        Err(VenueAmountError::Unavailable(_))
    ));
}

#[tokio::test]
async fn restart_only_polls_bridged_deposits_with_both_durable_submission_ids() {
    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeStatus, MockBridgeBackend};

    for (has_transfer, has_bridge, legacy_plan, bridge_completed) in [
        (true, true, false, false),
        (true, true, false, true),
        (true, false, false, false),
        (false, true, false, false),
        (true, false, true, false),
        (true, true, true, false),
    ] {
        let can_poll = has_transfer && has_bridge;
        let mut bridge = MockBridgeBackend::new();
        bridge.expect_submit_bridge().times(0);
        bridge
            .expect_get_bridge_status()
            .times(usize::from(can_poll))
            .returning(move |id| {
                assert_eq!(id, "persisted-deposit-bridge");
                Ok(if bridge_completed { BridgeStatus::Completed } else { BridgeStatus::Pending })
            });
        let mut restarted = deposit_poll_finalizer(bridge);
        Arc::get_mut(&mut restarted.backend)
            .unwrap()
            .expect_get_balance()
            .times(usize::from(can_poll && bridge_completed))
            .returning(|asset| {
                assert_eq!(asset, "ETH");
                Ok(0.01)
            });
        let cketh = ChainToken::Icp {
            ledger: Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").unwrap(),
            symbol: "ckETH".into(),
            decimals: 18,
            fee: Nat::from(2_000_000_000_000u64),
        };
        let mut cex = restarted
            .prepare_amount_scoped_state(
                "bridged-deposit-poll",
                ChainTokenAmount::from_formatted(cketh, 0.01),
                receive_token(),
                None,
            )
            .unwrap();
        cex.step = CexStep::DepositPending;
        cex.deposit.deposit_txid = has_transfer.then(|| "persisted-transfer".into());
        cex.deposit.bridge.deposit_bridge_id = has_bridge.then(|| "persisted-deposit-bridge".into());
        cex.deposit.deposit_balance_before = Some(0.0);
        cex.deposit.bridge.deposit_bridge_expected_amount = Some(0.009);
        if legacy_plan {
            cex.deposit.bridge.deposit_planned_asset = None;
            cex.deposit.bridge.deposit_planned_network = None;
            cex.deposit.bridge.deposit_bridge_required = false;
        }
        let execution = after_restart(cex);
        let restored = serde_json::from_str(&serde_json::to_string(&execution).unwrap()).unwrap();
        let progress = restarted.advance_decoded_leg(restored).await.unwrap();
        assert_eq!(
            progress.status,
            if can_poll { VenueLegStatus::Running } else { VenueLegStatus::OperatorRequired }
        );
        let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert!(execution.cex.deposit.bridge.deposit_bridge_required);
        assert_eq!(execution.operator_required, !can_poll);
        assert_eq!(
            execution.cex.step,
            if can_poll && bridge_completed { CexStep::Trade } else { CexStep::DepositPending }
        );
    }
}

#[tokio::test]
async fn restart_surfaces_a_failed_deposit_bridge_without_resubmitting() {
    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeStatus, MockBridgeBackend};

    let mut bridge = MockBridgeBackend::new();
    bridge.expect_submit_bridge().times(0);
    bridge.expect_get_bridge_status().times(1).returning(|_| {
        Ok(BridgeStatus::Failed {
            reason: Some("minter rejected the deposit".into()),
        })
    });
    let restarted_finalizer = deposit_poll_finalizer(bridge);
    let cketh = ChainToken::Icp {
        ledger: Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").unwrap(),
        symbol: "ckETH".into(),
        decimals: 18,
        fee: Nat::from(2_000_000_000_000u64),
    };
    let mut cex = restarted_finalizer
        .prepare_amount_scoped_state(
            "bridged-deposit-failed",
            ChainTokenAmount::from_formatted(cketh, 0.01),
            receive_token(),
            None,
        )
        .unwrap();
    cex.step = CexStep::DepositPending;
    cex.deposit.deposit_txid = Some("persisted-transfer".into());
    cex.deposit.bridge.deposit_bridge_id = Some("persisted-deposit-bridge".into());
    cex.deposit.deposit_balance_before = Some(0.0);

    let progress = restarted_finalizer.advance_decoded_leg(after_restart(cex)).await.unwrap();

    // A failed bridge is an ordinary retryable error: the leg keeps its ids,
    // spends retry budget, and is never resubmitted from here.
    assert_eq!(progress.status, VenueLegStatus::Running);
    let error = progress.retryable_error.expect("bridge failure is reported");
    assert!(error.contains("minter rejected the deposit"), "{error}");
    let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
    assert_eq!(execution.cex.step, CexStep::DepositPending);
    assert_eq!(execution.cex.deposit.bridge.deposit_bridge_id.as_deref(), Some("persisted-deposit-bridge"));
    assert!(!execution.operator_required);
}

#[tokio::test]
async fn restart_direct_deposit_without_baseline_records_one_and_keeps_waiting() {
    let mut restarted_finalizer = finalizer();
    Arc::get_mut(&mut restarted_finalizer.backend)
        .unwrap()
        .expect_get_balance()
        .times(1)
        .returning(|_| Ok(1.0));
    let mut cex = restarted_finalizer
        .prepare_amount_scoped_state(
            "deposit-no-baseline",
            ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
            receive_token(),
            None,
        )
        .unwrap();
    cex.step = CexStep::DepositPending;
    cex.deposit.deposit_txid = Some("persisted-transfer".into());
    cex.deposit.deposit_balance_before = None;

    let progress = restarted_finalizer.advance_decoded_leg(after_restart(cex)).await.unwrap();

    // Without a baseline the balance cannot prove this deposit, so the step
    // records one and waits; the total-balance fallback needs elapsed time.
    assert_eq!(progress.status, VenueLegStatus::Running);
    assert!(progress.retryable_error.is_none());
    let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
    assert_eq!(execution.cex.step, CexStep::DepositPending);
    assert_eq!(execution.cex.deposit.deposit_balance_before, Some(1.0));
    assert!(execution.cex.deposit.deposit_sent_at_ts.is_some());
    assert!(!execution.ready_to_advance);
}

#[tokio::test]
async fn restart_keeps_the_gate_when_legacy_withdraw_planning_introduces_a_bridge() {
    use liquidium_pipeline_connectors::backend::bridge_backend::MockBridgeBackend;

    let restarted = deposit_poll_finalizer(MockBridgeBackend::new());
    let mut cex = restarted
        .prepare_amount_scoped_state(
            "legacy-withdraw-plan",
            ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
            ChainToken::Icp {
                ledger: Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap(),
                symbol: "ckUSDC".into(),
                decimals: 6,
                fee: Nat::from(10_000u64),
            },
            None,
        )
        .unwrap();
    cex.step = CexStep::WithdrawPending;
    cex.withdraw.withdraw_id = Some("persisted-withdrawal".into());
    cex.withdraw.bridge.withdraw_planned_asset = None;
    cex.withdraw.bridge.withdraw_planned_network = None;
    cex.withdraw.bridge.withdraw_bridge_required = false;
    let progress = restarted
        .advance_decoded_leg(after_restart(cex))
        .await
        .unwrap();
    assert_eq!(progress.status, VenueLegStatus::OperatorRequired);
}

#[tokio::test]
async fn restart_can_reconcile_a_direct_withdrawal_receipt_without_resubmission() {
    use liquidium_pipeline_connectors::backend::cex_backend::{WithdrawStatus, WithdrawStatusSnapshot};
    let mut restarted = finalizer();
    Arc::get_mut(&mut restarted.backend)
        .unwrap()
        .expect_get_withdraw_status_snapshot_by_id()
        .times(1)
        .returning(|_, id| {
            assert_eq!(id, "persisted-withdrawal");
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("ledger:123".to_string()),
                transaction_fee: Some(0.02),
            })
        });
    let mut cex = restarted
        .prepare_amount_scoped_state(
            "receipt-poll",
            ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
            receive_token(),
            None,
        )
        .unwrap();
    cex.step = CexStep::WithdrawPending;
    cex.withdraw.withdraw_id = Some("persisted-withdrawal".to_string());
    cex.withdraw.size_out = Some(ChainTokenAmount::from_formatted(receive_token(), 10.0));
    let progress = restarted
        .advance_decoded_leg(after_restart(cex))
        .await
        .unwrap();
    assert_eq!(progress.status, VenueLegStatus::Completed);
    assert_eq!(progress.result.unwrap().receive_amount, Nat::from(9_980_000u64));
}

#[tokio::test]
async fn restart_polls_a_persisted_helper_instead_of_parking_or_resubmitting() {
    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeStatus, MockBridgeBackend};

    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_bridge_status().times(1).returning(|id| {
        assert_eq!(id, "persisted-helper");
        Ok(BridgeStatus::Pending)
    });
    bridge.expect_submit_bridge().times(0);
    let restarted = deposit_poll_finalizer(bridge);
    let mut cex = restarted
        .prepare_amount_scoped_state(
            "bridge-receipt-poll",
            ChainTokenAmount::from_raw(pay_token(), Nat::from(100_000_000u64)),
            receive_token(),
            None,
        )
        .unwrap();
    cex.withdraw.withdraw_asset = ChainToken::Icp {
        ledger: Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap(),
        symbol: "ckUSDC".into(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    };
    cex.step = CexStep::WithdrawPending;
    cex.withdraw.withdraw_id = Some("persisted-withdrawal".into());
    cex.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
        cex.withdraw.withdraw_asset.clone(),
        10.0,
    ));
    cex.withdraw.bridge.withdraw_bridge_required = true;
    cex.withdraw.bridge.withdraw_bridge_id = Some("persisted-helper".into());
    cex.withdraw.bridge.withdraw_planned_asset = Some("USDC".into());
    cex.withdraw.bridge.withdraw_planned_network = Some("ETH".into());
    let progress = restarted
        .advance_decoded_leg(after_restart(cex))
        .await
        .unwrap();
    assert_eq!(progress.status, VenueLegStatus::Running);
    assert!(progress.last_error.is_none());
    assert!(progress.result.is_none());
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

/// Builds a finalizer whose trade slice always draws the given venue response.
fn finalizer_with_trade_result(
    swap_result: Result<SwapFillReport, CexBackendError>,
    balance_calls: usize,
) -> MexcFinalizer<MockCexBackend> {
    let mut backend = MockCexBackend::new();
    backend.expect_get_orderbook().returning(|_, _| {
        Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price: 10.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![OrderBookLevel {
                price: 10.0,
                quantity: 1_000_000.0,
            }],
        })
    });
    backend
        .expect_execute_swap_detailed_with_options()
        .returning(move |_, _, _, _| swap_result.clone());
    backend.expect_get_balance().times(balance_calls).returning(|_| Ok(0.0));
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

/// Places a committed leg on its trade step with one resolved USDT-quoted
/// market, so the slice runner reaches the venue call without route discovery.
async fn leg_at_trade_step(finalizer: &MexcFinalizer<MockCexBackend>, waiting_since_ts: Option<i64>) -> VenueLegState {
    let preview = MultiVenueAdapter::preview(finalizer, &planning_context(), &request(100_000_000))
        .await
        .expect("preview should succeed");
    let mut leg = leg_from_preview(preview);
    let mut execution = leg
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("state should decode")
        .expect("state should be MEXC");
    execution.cex.step = CexStep::Trade;
    execution.cex.trade.trade_resolved_legs = vec![CexRouteLeg {
        market: "PAY_USDT".to_string(),
        side: "sell".to_string(),
    }];
    execution.cex.trade.trade_next_amount_in = Some(1.0);
    execution.cex.trade.trade_settlement_waiting_since_ts = waiting_since_ts;
    leg.execution = VenueExecutionState::new(VENUE_ID, &execution).expect("state should encode");

    let armed = MultiVenueAdapter::advance(finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("first cycle should arm the leg");
    leg.execution = armed.execution;
    leg
}

#[tokio::test]
async fn venue_settlement_rejection_waits_instead_of_spending_the_retry_budget() {
    let finalizer = finalizer_with_trade_result(
        Err(CexBackendError::PendingSettlement(
            "Swap err: code=Oversold msg=Oversold".to_string(),
        )),
        0,
    );
    let leg = leg_at_trade_step(&finalizer, None).await;

    let waiting = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("a settling venue should produce durable waiting progress");

    // The leg is still working, so the parent keeps re-offering the same order.
    assert_eq!(waiting.status, VenueLegStatus::Running);
    // The decisive assertion: a wait must not be charged to the retry budget,
    // which is what parked liquidation 1551 roughly a minute before its
    // already-credited deposit became tradable.
    assert_eq!(waiting.retryable_error, None);
    assert!(
        waiting
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("Oversold"))
    );

    let execution = waiting
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("decode waiting state")
        .expect("MEXC state");
    assert_eq!(execution.cex.step, CexStep::Trade);
    assert!(!execution.operator_required);
    assert!(execution.cex.trade.trade_settlement_waiting_since_ts.is_some());
}

#[tokio::test]
async fn prefix_shaped_other_error_does_not_enter_settlement_wait() {
    let message = "cex pending settlement: ordinary backend failure";
    let finalizer = finalizer_with_trade_result(Err(CexBackendError::Other(message.to_string())), 0);
    let leg = leg_at_trade_step(&finalizer, None).await;

    let retryable = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("an ordinary backend failure should produce retryable progress");

    assert_eq!(retryable.status, VenueLegStatus::Running);
    assert_eq!(retryable.retryable_error.as_deref(), Some(message));
    let execution = retryable
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("decode retryable state")
        .expect("MEXC state");
    assert_eq!(execution.cex.trade.trade_settlement_waiting_since_ts, None);
    assert!(!execution.operator_required);
}

#[tokio::test]
async fn venue_settlement_wait_parks_for_an_operator_once_the_deadline_passes() {
    // One balance read is expected: the give-up path reports what the account
    // actually holds so an operator can tell a stuck credit from an empty one.
    let finalizer = finalizer_with_trade_result(
        Err(CexBackendError::PendingSettlement(
            "Swap err: code=Oversold msg=Oversold".to_string(),
        )),
        1,
    );
    let expired_since = now_ts() - (SETTLEMENT_WAIT_TIMEOUT_SECS + 1);
    let leg = leg_at_trade_step(&finalizer, Some(expired_since)).await;

    let parked = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("an expired settlement wait should park the leg");

    assert_eq!(parked.status, VenueLegStatus::OperatorRequired);
    assert_eq!(parked.retryable_error, None);
    let reported = parked.last_error.expect("parked leg keeps its diagnosis");
    assert!(reported.contains("not settled after"), "unexpected message: {reported}");
    assert!(reported.contains("free PAY=0"), "unexpected message: {reported}");

    let execution = parked
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("decode parked state")
        .expect("MEXC state");
    assert!(execution.operator_required);
}

#[tokio::test]
async fn a_settled_fill_clears_the_recorded_wait() {
    let finalizer = finalizer_with_trade_result(
        Ok(SwapFillReport {
            input_consumed: 1.0,
            output_received: 10.0,
        }),
        0,
    );
    // The leg enters this cycle already carrying a wait from earlier refusals.
    let leg = leg_at_trade_step(&finalizer, Some(now_ts() - 60)).await;

    let progress = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .expect("a filled slice should advance the leg");

    assert_eq!(progress.status, VenueLegStatus::Running);
    assert_eq!(progress.retryable_error, None);
    let execution = progress
        .execution
        .decode::<MexcVenueExecutionState>(VENUE_ID)
        .expect("decode filled state")
        .expect("MEXC state");
    // Cleared, so a later refusal is timed from its own first occurrence
    // rather than inheriting a stale deadline.
    assert_eq!(execution.cex.trade.trade_settlement_waiting_since_ts, None);
}

mod bridged_deposit_restart {
    //! The persisted submit phase decides what a restart may do with a bridged
    //! deposit that has no handle yet.
    use super::*;
    use crate::finalizers::bridge_claims::{BridgeClaims, MockBridgeClaimLookup};
    use crate::finalizers::cex_finalizer::{BridgeSubmitAttempt, BridgeSubmitPhase};
    use liquidium_pipeline_connectors::backend::SIDE_EFFECT_AMBIGUOUS_PREFIX;
    use liquidium_pipeline_connectors::backend::bridge_backend::{
        BridgeFeeBudget, BridgeSubmissionCandidate, MockBridgeBackend,
    };

    fn cketh_state(finalizer: &MexcFinalizer<MockCexBackend>, phase: BridgeSubmitPhase) -> CexState {
        let cketh = ChainToken::Icp {
            ledger: Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").unwrap(),
            symbol: "ckETH".into(),
            decimals: 18,
            fee: Nat::from(2_000_000_000_000u64),
        };
        let mut cex = finalizer
            .prepare_amount_scoped_state(
                "bridged-deposit-restart",
                ChainTokenAmount::from_formatted(cketh, 0.01),
                receive_token(),
                None,
            )
            .unwrap();
        cex.step = CexStep::DepositPending;
        cex.deposit.deposit_txid = Some("persisted-transfer".into());
        cex.deposit.deposit_balance_before = Some(0.0);
        cex.deposit.bridge.deposit_bridge_submit_amount = Some(0.009);
        cex.deposit.bridge.deposit_bridge_expected_amount = Some(0.0089);
        cex.deposit.bridge.deposit_bridge_submit_phase = Some(phase);
        cex
    }

    fn attempt() -> BridgeSubmitAttempt {
        BridgeSubmitAttempt::test("ckETH", "0x1111111111111111111111111111111111111111", 0.009, Some(100))
    }

    fn zero_fee(bridge: &mut MockBridgeBackend) {
        bridge.expect_get_fee_budget().returning(|_, _, _| {
            Ok(BridgeFeeBudget {
                source_fee_budget: 0.0,
                destination_fee_budget: 0.0,
                provider_fee_budget_native_units: None,
            })
        });
    }

    /// A finalizer whose registry answers "nothing claimed" to every lookup.
    fn lookup_finalizer(bridge: MockBridgeBackend) -> MexcFinalizer<MockCexBackend> {
        let mut claims = MockBridgeClaimLookup::new();
        claims.expect_claims_since().returning(|_| Ok(BridgeClaims::default()));
        finalizer().with_bridge_runtime(
            crate::finalizers::bridge_runtime::BridgeRuntime::for_tests(Arc::new(bridge)).with_claims(Arc::new(claims)),
        )
    }

    #[tokio::test]
    async fn a_funding_wait_resumes_without_the_gate_and_without_a_call() {
        let mut bridge = MockBridgeBackend::new();
        zero_fee(&mut bridge);
        bridge.expect_get_source_balance().times(1).returning(|_, _, _| Ok(0.0));
        bridge.expect_prepare_submission().times(0);
        bridge.expect_submit_bridge().times(0);
        let mut restarted = deposit_poll_finalizer(bridge);
        Arc::get_mut(&mut restarted.backend)
            .unwrap()
            .expect_get_deposit_address()
            .times(1)
            .returning(|asset, network| {
                Ok(liquidium_pipeline_connectors::backend::cex_backend::DepositAddress {
                    asset: asset.into(),
                    network: network.into(),
                    address: "0x1111111111111111111111111111111111111111".into(),
                    tag: None,
                })
            });
        let cex = cketh_state(&restarted, BridgeSubmitPhase::WaitingFunds);

        let progress = restarted.advance_decoded_leg(after_restart(cex)).await.unwrap();

        assert_eq!(progress.status, VenueLegStatus::Running);
        let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert!(!execution.operator_required);
        assert_eq!(
            execution.cex.deposit.bridge.deposit_bridge_submit_phase,
            Some(BridgeSubmitPhase::WaitingFunds)
        );
        assert!(execution.cex.deposit.bridge.deposit_bridge_id.is_none());
    }

    #[tokio::test]
    async fn a_recorded_attempt_is_looked_up_and_attached_before_any_call() {
        let mut bridge = MockBridgeBackend::new();
        zero_fee(&mut bridge);
        bridge.expect_submit_bridge().times(0);
        bridge.expect_find_submissions().times(1).returning(|request, cursor, _| {
            assert_eq!(request.asset, "ckETH");
            assert_eq!(cursor, 100);
            Ok(vec![BridgeSubmissionCandidate {
                bridge_id: "ic-withdraw-eth:105".into(),
                sequence: 105,
            }])
        });
        let restarted = lookup_finalizer(bridge);
        let cex = cketh_state(&restarted, BridgeSubmitPhase::Submitting(attempt()));

        let progress = restarted.advance_decoded_leg(after_restart(cex)).await.unwrap();

        assert_eq!(progress.status, VenueLegStatus::Running);
        let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert!(!execution.operator_required);
        assert_eq!(
            execution.cex.deposit.bridge.deposit_bridge_id.as_deref(),
            Some("ic-withdraw-eth:105")
        );
        assert!(execution.cex.deposit.bridge.deposit_bridge_submit_phase.is_none());
        assert_eq!(execution.cex.trade.trade_next_amount_in, Some(0.0089));
    }

    #[tokio::test]
    async fn an_attempt_the_provider_cannot_disambiguate_parks_for_an_operator() {
        let mut bridge = MockBridgeBackend::new();
        zero_fee(&mut bridge);
        bridge.expect_submit_bridge().times(0);
        bridge.expect_find_submissions().times(1).returning(|_, _, _| {
            Ok([105, 106]
                .map(|sequence| BridgeSubmissionCandidate {
                    bridge_id: format!("ic-withdraw-eth:{sequence}"),
                    sequence,
                })
                .to_vec())
        });
        let restarted = lookup_finalizer(bridge);
        let cex = cketh_state(&restarted, BridgeSubmitPhase::Submitting(attempt()));

        let progress = restarted.advance_decoded_leg(after_restart(cex)).await.unwrap();

        assert_eq!(progress.status, VenueLegStatus::OperatorRequired);
        assert!(progress.retryable_error.is_none());
        let error = progress.last_error.expect("reason for the operator");
        assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
        let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert!(execution.operator_required);
        let Some(BridgeSubmitPhase::Submitting(kept)) = execution.cex.deposit.bridge.deposit_bridge_submit_phase else {
            panic!("the attempt stays for the operator");
        };
        assert!(kept.called_at_ts.is_some());
        assert!(kept.parked_for_operator);
    }

    /// An operator releases a parked leg by clearing its operator flag and
    /// setting it back to running. The attempt stays, so the lookup runs again.
    #[tokio::test]
    async fn a_released_park_allows_a_new_call_only_after_an_empty_lookup() {
        let mut bridge = MockBridgeBackend::new();
        zero_fee(&mut bridge);
        bridge.expect_submit_bridge().times(0);
        bridge.expect_find_submissions().times(1).returning(|_, _, _| Ok(vec![]));
        let released = lookup_finalizer(bridge);
        let mut parked = attempt();
        parked.called_at_ts = Some(1);
        parked.parked_for_operator = true;
        let cex = cketh_state(&released, BridgeSubmitPhase::Submitting(parked));
        let cleared = MexcVenueExecutionState {
            cex,
            ready_to_advance: false,
            intent_id: None,
            operator_required: false,
        };

        // The first cycle arms a fresh intent; the second runs the lookup.
        let armed = released.advance_decoded_leg(cleared).await.unwrap();
        assert_eq!(armed.status, VenueLegStatus::Running);
        let armed = armed.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert!(armed.ready_to_advance);

        let progress = released.advance_decoded_leg(armed).await.unwrap();
        assert_eq!(progress.status, VenueLegStatus::Running);
        let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
        assert_eq!(
            execution.cex.deposit.bridge.deposit_bridge_submit_phase,
            Some(BridgeSubmitPhase::WaitingFunds)
        );
    }
}

#[tokio::test]
async fn an_unanswered_icp_funding_transfer_stays_retryable_with_its_created_at_time() {
    use liquidium_pipeline_connectors::backend::SIDE_EFFECT_AMBIGUOUS_PREFIX;
    use liquidium_pipeline_core::transfer::actions::TransferFailure;

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
    backend.expect_get_balance().returning(|_| Ok(0.0));
    backend.expect_get_deposit_address().returning(|_, _| {
        Ok(liquidium_pipeline_connectors::backend::cex_backend::DepositAddress {
            asset: "PAY".into(),
            network: "ICP".into(),
            address: "aaaaa-aa".into(),
            tag: None,
        })
    });
    let mut transfers = MockTransferActions::new();
    transfers
        .expect_transfer_deduplicated()
        .times(1)
        .returning(|_, _, _, _created_at_time_nanos| Err(TransferFailure::Ambiguous("replica timed out".into())));
    let tokens = HashMap::from([
        (pay_token().asset_id(), pay_token()),
        (receive_token().asset_id(), receive_token()),
    ]);
    let finalizer = MexcFinalizer::new(
        Arc::new(backend),
        Arc::new(transfers),
        Principal::anonymous(),
        200.0,
        0.0001,
        0.7,
    )
    .with_token_registry(Arc::new(TokenRegistry::new(tokens)));
    let preview = MultiVenueAdapter::preview(&finalizer, &planning_context(), &request(100_000_000))
        .await
        .unwrap();
    let mut leg = leg_from_preview(preview);
    leg.execution = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint)
        .await
        .unwrap()
        .execution;

    let progress = MultiVenueAdapter::advance(&finalizer, &leg, &NoopCheckpoint).await.unwrap();

    // The ledger deduplicates the retry, so the leg retries instead of parking.
    assert_eq!(progress.status, VenueLegStatus::Running);
    let error = progress.retryable_error.expect("the retry spends budget like any error");
    assert!(!error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
    let execution = progress.execution.decode::<MexcVenueExecutionState>(VENUE_ID).unwrap().unwrap();
    assert!(!execution.operator_required);
    assert!(execution.cex.deposit.deposit_txid.is_none());
    assert!(execution.cex.deposit.deposit_transfer_created_at_ns.is_some(), "saved for the retry");
}
