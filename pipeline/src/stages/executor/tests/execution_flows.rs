use super::*;

#[tokio::test]
async fn executor_calls_liquidate_with_slippage_without_allowance_preflight() {
    let lending_canister = lending_canister();
    let first_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");
    let second_ledger = p("ryjl3-tyaaa-aaaaa-aaaba-cai");

    let mut agent = MockPipelineAgent::new();

    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .withf(move |canister, method, arg| {
            if *canister != lending_canister || method != "liquidate_with_slippage" {
                return false;
            }

            match candid::Decode!(arg, LiquidationRequest, Nat) {
                Ok((liq_req, min_collateral_amount)) => {
                    liq_req.debt_amount == Nat::from(11_244u64) && min_collateral_amount == Nat::from(42u64)
                }
                Err(_) => false,
            }
        })
        .times(2)
        .returning(move |_, _, _| Ok(Ok(make_liquidation_result(31u128))));

    let executor = test_executor(agent, accepting_intent_store(2));

    let request_fail = make_request(
        p("tfeop-4aaaa-aaaaa-aaaaa-aaaaa-aaaaa-bdai"),
        ChainToken::Icp {
            ledger: first_ledger,
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        },
    );
    let request_ok = make_request(
        p("2vxsx-fae"),
        ChainToken::Icp {
            ledger: second_ledger,
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        },
    );

    let receipts = executor
        .process(&vec![request_fail.clone(), request_ok.clone()])
        .await
        .expect("executor process should succeed");
    assert_eq!(receipts.len(), 2);

    assert!(
        matches!(receipts[0].status, ExecutionStatus::Success),
        "first request should execute via liquidate_with_slippage call"
    );
    assert!(receipts[0].liquidation_result.is_some());

    assert!(
        matches!(receipts[1].status, ExecutionStatus::Success),
        "second request should continue and execute"
    );
    assert!(receipts[1].liquidation_result.is_some());
}

/// A pending change transfer must not prevent the accepted liquidation
/// receipt from entering the durable handoff journal.
#[tokio::test]
async fn pending_change_still_accepts_the_liquidation_intent() {
    let lending_canister = lending_canister();

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(move |_, _, _| {
            let mut result = make_liquidation_result(1_556u128);
            result.change_tx.status = TransferStatus::Pending;
            Ok(Ok(result))
        });
    agent
        .expect_call_query::<Option<LiquidationInflowRecord>>()
        .withf(move |canister, method, arg| {
            *canister == lending_canister
                && method == "get_liquidation_inflow"
                && Decode!(arg, u128).is_ok_and(|id| id == 1_556)
        })
        .times(1)
        .returning(|_, _, _| Ok(Some(inflow_record())));

    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
    intents
        .expect_mark_accepted()
        .withf(|_, liquidation_id, receipt| liquidation_id == "1556" && !receipt.change_received)
        .times(1)
        .returning(|_, _, _| Ok(()));

    let executor = test_executor(agent, intents);

    let request = icp_request();

    let receipts = executor
        .process(&vec![request])
        .await
        .expect("executor process should succeed");

    assert_eq!(receipts.len(), 1);
    assert!(
        !receipts[0].change_received,
        "pending change must stay visible on the receipt"
    );
    assert!(
        matches!(receipts[0].status, ExecutionStatus::Success),
        "a pending change does not fail the liquidation, status was {:?}",
        receipts[0].status
    );
}

/// A failed deposit that moved no money leaves no pool record, so the
/// executor asks once and reports nothing.
#[tokio::test]
async fn a_failed_liquidation_asks_the_pool_what_it_still_owes() {
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
    intents.expect_mark_failed().times(1).returning(|_, _, _, _| Ok(()));
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(|_, _, _| {
            let mut result = make_liquidation_result(9);
            result.status = LiquidationStatus::FailedLiquidation("deposit underlying failed".to_string());
            result.change_tx.status = TransferStatus::Pending;
            Ok(Ok(result))
        });
    agent
        .expect_call_query::<Option<LiquidationInflowRecord>>()
        .withf(|_, method, arg| method == "get_liquidation_inflow" && Decode!(arg, u128).is_ok_and(|id| id == 9))
        .times(1)
        .returning(|_, _, _| Ok(None));
    let executor = test_executor(agent, intents);

    let receipts = executor
        .process(&vec![icp_request()])
        .await
        .expect("a failed liquidation is a receipt, not an error");
    assert!(matches!(receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
}

#[tokio::test]
async fn intent_is_durable_before_the_liquidation_call_begins() {
    let persisted = Arc::new(AtomicBool::new(false));
    let store_flag = persisted.clone();
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(move |_, _| {
        store_flag.store(true, Ordering::SeqCst);
        Ok(())
    });
    intents.expect_mark_accepted().times(1).returning(|_, _, _| Ok(()));

    let call_flag = persisted.clone();
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(move |_, _, _| {
            assert!(
                call_flag.load(Ordering::SeqCst),
                "external call began before intent commit"
            );
            Ok(Ok(make_liquidation_result(77)))
        });

    let executor = test_executor(agent, intents);
    executor
        .process(&vec![icp_request()])
        .await
        .expect("liquidation succeeds");
}

#[tokio::test]
async fn intent_write_failure_prevents_the_external_call() {
    let mut intents = MockLiquidationIntentStore::new();
    intents
        .expect_create_submitting()
        .times(1)
        .returning(|_, _| Err(anyhow::anyhow!("disk unavailable")));
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(0);
    let executor = test_executor(agent, intents);

    let error = executor
        .process(&vec![icp_request()])
        .await
        .expect_err("missing intent durability must stop submission");
    assert!(error.contains("failed to persist liquidation intent"));
}

#[tokio::test]
async fn acceptance_failures_retry_only_persistence_and_preserve_the_receipt() {
    for recover in [true, false] {
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        let mut sequence = mockall::Sequence::new();
        intents
            .expect_mark_accepted()
            .times(if recover { 1 } else { 3 })
            .in_sequence(&mut sequence)
            .returning(|_, _, _| Err(anyhow::anyhow!("database locked")));
        if recover {
            intents
                .expect_mark_accepted()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|_, _, _| Ok(()));
        } else {
            intents
                .expect_mark_ambiguous()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|_, _, receipt| {
                    let receipt = receipt.expect("retain the successful chain receipt");
                    assert_eq!(receipt.liquidation_result.unwrap().id, Nat::from(77u64));
                    Ok(())
                });
        }
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Ok(Ok(make_liquidation_result(77))));
        let watchdog = Arc::new(RecordingWatchdog::default());
        let executor = test_executor(agent, intents).with_watchdog(watchdog.clone());
        let result = executor.process(&vec![icp_request()]).await;
        let alerts = watchdog.0.lock().unwrap().clone();
        if recover {
            assert!(result.is_ok());
            assert!(alerts.is_empty());
        } else {
            assert!(result.unwrap_err().contains("operator reconciliation required"));
            assert_eq!(alerts.len(), 1);
            assert_eq!(alerts[0].1, "persist-liquidation-result");
        }
    }
}

/// Review findings 15 and 16: a lost reply left a liquidation that may
/// have run with no WAL row and no alert.
#[tokio::test]
async fn a_lost_reply_alerts_once_for_its_own_intent() {
    let created = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = created.clone();
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(2).returning(move |id, _| {
        seen.lock().unwrap().push(id.to_string());
        Ok(())
    });
    intents.expect_mark_ambiguous().times(2).returning(|_, _, _| Ok(()));
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(2)
        .returning(|_, _, _| Err("response lost".to_string()));
    let watchdog = Arc::new(RecordingWatchdog::default());
    let executor = test_executor(agent, intents).with_watchdog(watchdog.clone());

    executor
        .process(&vec![icp_request(), icp_request()])
        .await
        .expect("ambiguous results remain observable");

    let mut alerts = watchdog.0.lock().unwrap().clone();
    let mut intent_ids = created.lock().unwrap().clone();
    alerts.sort();
    intent_ids.sort();
    assert_eq!(
        alerts,
        intent_ids
            .into_iter()
            .map(|id| (id, "liquidation-call-unanswered".to_string()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn transport_failure_parks_the_intent_as_ambiguous() {
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
    intents
        .expect_mark_ambiguous()
        .withf(|_, error, receipt| error == "response lost" && receipt.is_none())
        .times(1)
        .returning(|_, _, _| Ok(()));
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(|_, _, _| Err("response lost".to_string()));
    let executor = test_executor(agent, intents);

    let receipts = executor
        .process(&vec![icp_request()])
        .await
        .expect("ambiguous result remains observable");
    assert!(matches!(receipts[0].status, ExecutionStatus::LiquidationCallFailed(_)));
}

#[tokio::test]
async fn canister_rejection_marks_a_definite_failure_without_handoff() {
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
    intents
        .expect_mark_failed()
        .withf(|_, liquidation_id, receipt, error| {
            liquidation_id.is_none() && receipt.is_some() && error.contains("InsufficientCollateral")
        })
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(|_, _, _| Ok(Err(ProtocolError::InsufficientCollateral)));
    let executor = test_executor(agent, intents);

    let receipts = executor
        .process(&vec![icp_request()])
        .await
        .expect("definite rejection remains observable");
    assert!(matches!(receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
}
