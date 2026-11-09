//! The whole leg, one cycle at a time: order, bridge out to the order, wait,
//! bridge back, done. Then the adapter's typed ends.
use std::sync::Mutex;

use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeMintReceipt, BridgeSend, BridgeStatus, BridgeSubmission, DepositNotice,
};
use liquidium_pipeline_core::outbound::PreparedSend;

use super::*;
use crate::finalizers::multi_venue::{MultiVenueAdapter, VenuePlanningContext};
use crate::persistance::VenueLegFailure;

/// A helper deposit as the EVM bridge signs it.
fn evm_bridge_send() -> BridgeSend {
    BridgeSend {
        send: PreparedSend {
            sender: "evm".into(),
            identity: "0xhelper".into(),
            payload: "signed-call".into(),
            replay_safe: true,
        },
        handle: "mint-handle".into(),
    }
}

fn mint_receipt() -> BridgeMintReceipt {
    BridgeMintReceipt {
        transaction_hash: "0xhelper".into(),
        log_index: Nat::from(1u64),
        mint_block_index: Nat::from(42u64),
        amount: Nat::from(99_400_000u64),
        recipient: Account {
            owner: Principal::anonymous(),
            subaccount: None,
        },
        token_symbol: "ckUSDT".into(),
    }
}

/// A bridge wallet sender whose helper call lands on the first try.
fn evm_landing_sender() -> MockOutboundSender {
    let mut sender = MockOutboundSender::new();
    sender.expect_submit().times(1).returning(|_| SendOutcome::Landed {
        receipt: "0xhelper".into(),
    });
    sender
}

#[tokio::test]
async fn a_leg_pays_the_order_through_the_bridge_and_mints_what_rift_delivered() {
    let mut rift = MockRiftBackend::new();
    // One quote sizes the plan, a fresh one backs the order.
    rift.expect_quote()
        .times(2)
        .returning(|request| Ok(quote(&request.amount_in, "99.5")));
    rift.expect_create_order()
        .times(1)
        .returning(|_, _, _| Ok(order("100")));
    rift.expect_order_state()
        .times(1)
        .returning(|_| Ok(order_state(RiftOrderStatus::Delivered, Some("99.4"))));

    let mut bridge = free_bridge();
    bridge
        .expect_get_source_balance()
        .returning(|asset, chain, _| match (asset, chain) {
            ("ckUSDC", "ICP") => Ok(100.0),
            ("USDT", "ETH") => Ok(99.4),
            other => panic!("unexpected source balance read {other:?}"),
        });
    bridge.expect_prepare_submission().times(1).returning(|_| Ok(None));
    bridge.expect_submit_bridge().times(1).returning(|request| {
        assert_eq!(request.asset, "ckUSDC");
        assert_eq!(request.target_asset, "USDC");
        assert!((request.amount - 100.0).abs() < 1e-9, "{}", request.amount);
        match &request.destination {
            BridgeDestination::EvmAddress(address) => {
                assert_eq!(format!("{address:#x}"), DEPOSIT_ADDRESS);
            }
            other => panic!("the bridge must pay the Rift deposit address, not {other:?}"),
        }
        Ok(BridgeSubmission {
            bridge_id: "bridge-out-1".to_string(),
        })
    });
    bridge.expect_prepare_bridge_send().times(1).returning(|request| {
        assert_eq!(request.asset, "USDT");
        assert_eq!(request.target_asset, "ckUSDT");
        assert!((request.amount - 99.4).abs() < 1e-9, "{}", request.amount);
        Ok(evm_bridge_send())
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "bridge-back-1".to_string(),
        }))
    });
    bridge
        .expect_get_bridge_status()
        .returning(|bridge_id| match bridge_id {
            "bridge-out-1" => Ok(BridgeStatus::Completed),
            "bridge-back-1" => Ok(BridgeStatus::Minted(mint_receipt())),
            other => panic!("unexpected bridge id {other}"),
        });
    let runtime = BridgeRuntime::for_tests(Arc::new(bridge)).with_outbound(Arc::new(evm_landing_sender()));
    let finalizer = finalizer_with_runtime(rift, runtime, landing_sender());

    let preview = finalizer
        .preview_swap_request("liq-flow", &request(100_010_000))
        .await
        .unwrap();
    let mut state = preview.state;
    assert_eq!(state.amount_in, 100.0);

    let mut steps = Vec::new();
    for _ in 0..20 {
        if state.step == RiftStep::Completed {
            break;
        }
        state.send_saved = false;
        finalizer
            .advance_current_step(&mut state)
            .await
            .unwrap_or_else(|error| panic!("step {:?} failed: {error}", state.step));
        steps.push((state.step, state.send_saved));
    }

    assert_eq!(state.step, RiftStep::Completed, "steps: {steps:?}");
    assert_eq!(
        steps,
        vec![
            (RiftStep::Order, true),       // the quote id is saved
            (RiftStep::BridgeOut, false),  // the order is created
            (RiftStep::BridgeOut, true),   // the funding transfer is saved
            (RiftStep::BridgeOut, false),  // it lands
            (RiftStep::BridgeOut, true),   // the bridge call is saved
            (RiftStep::BridgeOut, false),  // it is submitted
            (RiftStep::Wait, false),       // the bridge completed
            (RiftStep::BridgeBack, false), // Rift delivered
            (RiftStep::BridgeBack, true),  // the helper call is saved
            (RiftStep::BridgeBack, false), // it lands
            (RiftStep::BridgeBack, false), // the minter is told
            (RiftStep::Completed, false),  // the mint is seen
        ]
    );
    assert_eq!(state.funding_txid.as_deref(), Some("block-9"));
    assert_eq!(state.bridge_out.deposit_bridge_id.as_deref(), Some("bridge-out-1"));
    assert_eq!(state.bridge_in.withdraw_bridge_id.as_deref(), Some("bridge-back-1"));
    assert_eq!(
        state.size_out.as_ref().map(|out| out.value.clone()),
        Some(Nat::from(99_400_000u64))
    );

    let result = finalizer.finish_state(&state).unwrap();
    assert_eq!(result.receive_amount, Nat::from(99_400_000u64));
    assert_eq!(result.legs[0].venue, RIFT_VENUE_ID);
}

#[tokio::test]
async fn a_bridge_minimum_before_funding_ends_the_leg_with_its_funds_in_the_trader_account() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .returning(|request| Ok(quote(&request.amount_in, "99.5")));
    rift.expect_create_order().returning(|_, _, _| Ok(order("100")));
    let mut bridge = MockBridgeBackend::new();
    bridge
        .expect_get_fee_budget()
        .returning(|_, _, _| Ok(BridgeFeeBudget::default()));
    // The route minimum rises after the plan: zero at the preview, then above the amount.
    let reads = Mutex::new(0u8);
    bridge.expect_get_minimum_bridge_amount().returning(move |_, _, _| {
        let mut reads = reads.lock().unwrap();
        *reads += 1;
        Ok(if *reads == 1 { 0.0 } else { 1_000.0 })
    });
    let finalizer = finalizer(rift, bridge, MockOutboundSender::new());
    let context = VenuePlanningContext {
        liquidation_id: "1".to_string(),
    };
    let preview = finalizer.preview(&context, &request(100_010_000)).await.unwrap();
    let mut leg = leg_from_preview(preview);

    let mut progress = None;
    for _ in 0..6 {
        let step = finalizer.advance(&leg, &NoopCheckpoint).await.unwrap();
        leg.execution = step.execution.clone();
        if step.status != VenueLegStatus::Running {
            progress = Some(step);
            break;
        }
    }
    let progress = progress.expect("the leg ends");

    assert_eq!(progress.status, VenueLegStatus::FailedPermanent);
    assert_eq!(progress.failure, Some(VenueLegFailure::BelowMinimum));
    assert!(progress.retryable_error.is_none());
    assert!(progress.last_error.unwrap().contains("minimum"));
}

#[tokio::test]
async fn a_refunded_order_parks_the_leg_for_an_operator() {
    let mut rift = MockRiftBackend::new();
    rift.expect_order_state()
        .returning(|_| Ok(order_state(RiftOrderStatus::Refunded, None)));
    let finalizer = finalizer(rift, MockBridgeBackend::new(), MockOutboundSender::new());
    let state = state_at(RiftStep::Wait);
    let execution = serde_json::json!({ "rift": state, "ready_to_advance": true, "intent_id": "rift-intent-x" });
    let request = request(100_010_000);
    let leg = VenueLegState {
        leg_id: "rift-leg-1".to_string(),
        venue_id: RIFT_VENUE_ID.to_string(),
        request: request.clone(),
        quote: VenueLegQuote {
            pay_amount: request.pay_amount.clone(),
            estimated_receive: ChainTokenAmount::from_formatted(ckusdt(), 99.5),
            conservative_receive: ChainTokenAmount::from_formatted(ckusdt(), 99.0),
            estimated_price_impact_bps: 0.0,
            route_id: "ethereum.usdc>ethereum.usdt".to_string(),
            side_costs: Vec::new(),
        },
        execution: crate::persistance::VenueExecutionState {
            venue: RIFT_VENUE_ID.to_string(),
            state: execution,
        },
        status: VenueLegStatus::Running,
        result: None,
        last_error: None,
        failure: None,
    };

    // The intent was armed by another process: a restart. The wait step only reads, so it resumes.
    let progress = finalizer.advance(&leg, &NoopCheckpoint).await.unwrap();

    assert_eq!(progress.status, VenueLegStatus::OperatorRequired);
    assert!(progress.last_error.unwrap().contains("refunded"));
    let questions = finalizer
        .operator_questions(&VenueLegState {
            execution: progress.execution,
            ..leg
        })
        .unwrap();
    // The only question is the refund itself; its transaction ends the leg.
    assert_eq!(questions.len(), 1, "{questions:?}");
    assert!(questions[0].detail.contains("refund"), "{}", questions[0].detail);
}

/// Keeps every checkpoint, so a test can see what was durable before a call.
#[derive(Default)]
struct RecordingCheckpoint(Mutex<Vec<VenueLegProgress>>);

#[async_trait::async_trait]
impl VenueLegCheckpoint for RecordingCheckpoint {
    async fn checkpoint(&self, progress: VenueLegProgress) -> Result<(), String> {
        self.0.lock().unwrap().push(progress);
        Ok(())
    }
}

fn rift_of(execution: &crate::persistance::VenueExecutionState) -> RiftState {
    serde_json::from_value(execution.state.get("rift").unwrap().clone()).unwrap()
}

#[tokio::test]
async fn the_quote_id_is_durable_before_the_order_is_created() {
    let mut rift = MockRiftBackend::new();
    rift.expect_quote()
        .times(2)
        .returning(|request| Ok(quote(&request.amount_in, "99.5")));
    rift.expect_create_order()
        .times(1)
        .returning(|_, _, _| Ok(order("100")));
    let finalizer = finalizer(rift, free_bridge(), MockOutboundSender::new());
    let context = VenuePlanningContext {
        liquidation_id: "2".to_string(),
    };
    let preview = finalizer.preview(&context, &request(100_010_000)).await.unwrap();
    let mut leg = leg_from_preview(preview);
    let checkpoint = RecordingCheckpoint::default();

    // Cycle one arms the gate and calls nothing.
    let armed = finalizer.advance(&leg, &checkpoint).await.unwrap();
    assert!(checkpoint.0.lock().unwrap().is_empty());
    assert!(rift_of(&armed.execution).quote_id.is_none());
    leg.execution = armed.execution;

    // Cycle two quotes, writes the quote id, then creates the order.
    let ordered = finalizer.advance(&leg, &checkpoint).await.unwrap();
    let durable = checkpoint.0.lock().unwrap();
    assert_eq!(durable.len(), 1);
    let before_order = rift_of(&durable[0].execution);
    assert_eq!(before_order.quote_id.as_deref(), Some("quote-1"));
    assert!(before_order.order.is_none(), "the checkpoint precedes the order call");
    let after = rift_of(&ordered.execution);
    assert_eq!(after.order.map(|order| order.id).as_deref(), Some("order-1"));
    assert_eq!(after.step, RiftStep::BridgeOut);
}
