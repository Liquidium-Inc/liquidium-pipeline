//! The SOL -> ckSOL return and SOL routes.
use super::*;

#[tokio::test]
async fn mexc_cksol_repayment_preview_uses_icp_usdt_sol_route() {
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    cex.expect_get_orderbook().returning(|market, _| {
        assert!(!market.contains("CKSOL"), "ckSOL must translate to native SOL");
        assert!(!market.contains("SOL_USDC"), "SOL must trade through USDT");
        let price = match market {
            "ICP_USDT" => 5.0,
            "SOL_USDT" => 100.0,
            _ => return Err("unlisted market".to_string()),
        };
        Ok(OrderBook {
            bids: vec![OrderBookLevel {
                price,
                quantity: 1_000.0,
            }],
            asks: vec![OrderBookLevel {
                price,
                quantity: 1_000.0,
            }],
            taker_fee_bps: 10.0,
        })
    });
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_source_fee_budget().returning(|asset, chain, target| {
        assert_eq!((asset, chain, target), ("SOL", "SOL", "ckSOL"));
        Ok(0.002)
    });
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.02));
    bridge.expect_get_fee_budget().returning(|asset, chain, target| {
        assert_eq!((asset, chain, target), ("SOL", "SOL", "ckSOL"));
        Ok(BridgeFeeBudget {
            source_fee_budget: 0.002,
            side_costs: vec![deposit_call_cycles()],
            ..BridgeFeeBudget::default()
        })
    });
    let pay = native_icp_token();
    let receive = cksol_token();
    let registry = liquidium_pipeline_core::tokens::token_registry::TokenRegistry::new(HashMap::from([
        (pay.asset_id(), pay.clone()),
        (receive.asset_id(), receive.clone()),
    ]));
    let finalizer = cksol_finalizer(cex, bridge)
        .with_token_registry(Arc::new(registry))
        .with_route_config(vec!["ICP_USDT".into(), "SOL_USDT".into()], 2);
    let request = crate::swappers::model::SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_formatted(pay, 10.0),
        receive_asset: receive.asset_id(),
        receive_address: None,
        max_slippage_bps: Some(200),
        venue_hint: Some("mexc".into()),
    };
    let preview = finalizer.preview_swap_request("1705", &request).await.unwrap();
    assert_eq!(preview.state.market, "ICP_SOL");
    assert_eq!(
        preview.state.withdraw.bridge.withdraw_planned_network.as_deref(),
        Some("SOL")
    );
    assert!(preview.state.withdraw.bridge.withdraw_bridge_required);
    assert_eq!(preview.quote.legs[0].route_id, "ICP_USDT:sell>SOL_USDT:buy");
    assert!(preview.quote.receive_amount > Nat::from(490_000_000u64));
    assert!(preview.quote.receive_amount < Nat::from(498_000_000u64));
    assert!(preview.conservative_receive.value < preview.quote.receive_amount);
    // The return's cycles reach the planner as a side cost (audit L10).
    assert_eq!(preview.side_costs, vec![deposit_call_cycles()]);
    assert_eq!(preview.state.side_costs, vec![deposit_call_cycles()]);
}

fn deposit_call_cycles() -> SideCost {
    SideCost::new(
        "cycles for the ckSOL deposit call",
        CYCLES_SYMBOL,
        CYCLES_DECIMALS,
        Nat::from(55_000_000_000u64),
    )
}

#[tokio::test]
async fn mexc_cksol_repayment_waits_for_funding_reserves_fees_and_records_mint() {
    let recipient = repayment_account();
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    cex.expect_withdraw()
        .times(1)
        .returning(|asset, network, address, amount, _withdraw_order_id| {
            assert_eq!((asset, network), ("SOL", "SOL"));
            assert_eq!(address, "So11111111111111111111111111111111111111112");
            assert_eq!(amount, 1.5);
            Ok(liquidium_pipeline_connectors::backend::cex_backend::WithdrawalReceipt {
                asset: asset.into(),
                network: network.into(),
                amount,
                internal_id: Some("sol-withdraw".into()),
                txid: Some("sol-signature".into()),
            })
        });
    // Only the cycles before the send read the exchange and the source balance.
    cex.expect_get_withdraw_status_snapshot_by_id()
        .times(2)
        .returning(|_, _| {
            Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Completed,
                txid: Some("sol-signature".into()),
                transaction_fee: Some(0.01),
            })
        });
    let mut bridge = MockBridgeBackend::new();
    let balances = Arc::new(std::sync::Mutex::new(vec![1.49, 0.5]));
    bridge
        .expect_get_source_balance()
        .times(2)
        .returning(move |asset, chain, _| {
            assert_eq!((asset, chain), ("SOL", "SOL"));
            Ok(balances.lock().unwrap().pop().unwrap())
        });
    bridge
        .expect_get_source_fee_budget()
        .times(1)
        .returning(|_, _, _| Ok(0.002));
    bridge
        .expect_get_minimum_bridge_amount()
        .times(1)
        .returning(|_, _, _| Ok(0.02));
    bridge.expect_submit_bridge().times(0);
    bridge.expect_prepare_bridge_send().times(1).returning(move |request| {
        assert_eq!(
            (
                request.asset.as_str(),
                request.source_chain.as_str(),
                request.target_asset.as_str()
            ),
            ("SOL", "SOL", "ckSOL")
        );
        assert!((request.amount - 1.488).abs() < 1e-9);
        assert_eq!(request.destination, BridgeDestination::IcpAccount(recipient));
        Ok(sol_bridge_send())
    });
    let notices = Arc::new(std::sync::Mutex::new(vec![
        DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "cksol-deposit-v2:test".into(),
        }),
        DepositNotice::NotYet {
            reason: "ValueTooSmall".into(),
        },
    ]));
    bridge
        .expect_track_deposit()
        .times(2)
        .returning(move |asset, chain, destination, _handle, receipt| {
            assert_eq!((asset, chain, receipt), ("SOL", "SOL", "sol-sig"));
            assert_eq!(*destination, BridgeDestination::IcpAccount(recipient));
            Ok(notices.lock().unwrap().pop().unwrap())
        });
    bridge
        .expect_get_bridge_status()
        .times(1)
        .returning(move |_| Ok(cksol_mint(recipient)));
    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        recipient.owner,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(
        bridge_dependencies(Arc::new(bridge))
            .with_outbound(Arc::new(sol_deposit_sender()))
            .with_claims(Arc::new(open_deposit_sends(vec![vec![]; 3]))),
    );
    let mut state = cksol_repayment_state(&finalizer, 1.5);

    finalizer.withdraw(&mut state).await.expect("the first cycle saves the withdrawal request");
    finalizer.withdraw(&mut state).await.unwrap(); // exchange withdrawal
    finalizer.withdraw(&mut state).await.unwrap(); // source balance too low
    assert!(!state.withdraw.bridge.deposit_send_started());
    finalizer.withdraw(&mut state).await.unwrap(); // the SOL send is saved
    assert!(state.withdraw.bridge.withdraw_bridge_funding.is_some());
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    // A restart resumes the saved send without the gate, and sends that send.
    let mut state: CexState = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    assert!(state.resume_after_restart(now_ts()));
    finalizer.withdraw(&mut state).await.unwrap(); // sent, open
    finalizer.withdraw(&mut state).await.unwrap(); // landed
    assert_eq!(state.withdraw.bridge.withdraw_bridge_funding_txid.as_deref(), Some("sol-sig"));
    assert!(state.withdraw.bridge.withdraw_bridge_funding.is_none());
    finalizer.withdraw(&mut state).await.unwrap(); // not final yet: no notice, no cycles
    state.withdraw.bridge.withdraw_bridge_funded_at_ts = Some(now_ts() - SOL_FINALITY_SECS);
    finalizer.withdraw(&mut state).await.unwrap(); // the minter does not see it yet
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
    finalizer.withdraw(&mut state).await.unwrap(); // the minter accepts it
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:test"));
    assert!(matches!(state.step, CexStep::WithdrawPending));

    let mut resumed: CexState = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    finalizer.withdraw(&mut resumed).await.unwrap();
    assert!(matches!(resumed.step, CexStep::Completed));
    assert_eq!(resumed.withdraw.size_out.unwrap().value, Nat::from(1_487_995_000u64));
}

#[tokio::test]
async fn mexc_cksol_return_waits_while_another_row_has_an_open_sol_deposit() {
    use crate::bridge::claims::OpenDepositSend;
    let send = |liq_id: &str, asset: &str| OpenDepositSend {
        liq_id: liq_id.into(),
        asset: asset.into(),
        network: "SOL".into(),
    };
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    // The wait reads nothing from MEXC.
    cex.expect_get_withdraw_status_snapshot_by_id().times(1).returning(|_, _| {
        Ok(WithdrawStatusSnapshot {
            status: WithdrawStatus::Completed,
            txid: Some("sol-signature".into()),
            transaction_fee: Some(0.01),
        })
    });
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_source_balance().returning(|_, _, _| Ok(1.49));
    bridge.expect_get_source_fee_budget().returning(|_, _, _| Ok(0.002));
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.02));
    bridge
        .expect_prepare_bridge_send()
        .times(1)
        .returning(|_| Ok(sol_bridge_send()));
    // The first scan finds another row's open SOL deposit. The next two, before
    // and under the submit lock, find only this row and another route.
    let claims = open_deposit_sends(vec![
        vec![send("other-row", "SOL")],
        vec![send("1705", "SOL"), send("other-row", "ETH")],
        vec![send("1705", "SOL"), send("other-row", "ETH")],
    ]);
    let finalizer = MexcFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        repayment_account().owner,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(
        bridge_dependencies(Arc::new(bridge))
            .with_outbound(Arc::new(liquidium_pipeline_core::outbound::MockOutboundSender::new()))
            .with_claims(Arc::new(claims)),
    );
    let mut state = cksol_repayment_state(&finalizer, 1.5);
    state.step = CexStep::WithdrawPending;
    state.withdraw.withdraw_id = Some("sol-withdraw".into());
    assert_eq!(state.liq_id, "1705");

    finalizer.withdraw(&mut state).await.unwrap();
    assert!(!state.withdraw.bridge.deposit_send_started(), "another row's deposit is open");
    assert!(matches!(state.step, CexStep::WithdrawPending));

    finalizer.withdraw(&mut state).await.unwrap();
    assert!(state.withdraw.bridge.withdraw_bridge_funding.is_some());
}

#[tokio::test]
async fn mexc_cksol_deposit_notice_parks_after_its_window_and_resumes_on_acceptance() {
    let mut bridge = MockBridgeBackend::new();
    let notices = Arc::new(std::sync::Mutex::new(vec![
        DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "cksol-deposit-v2:late".into(),
        }),
        DepositNotice::NotYet {
            reason: "TemporarilyUnavailable".into(),
        },
    ]));
    bridge
        .expect_track_deposit()
        .times(2)
        .returning(move |_, _, _, _, _| Ok(notices.lock().unwrap().pop().unwrap()));
    let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
    let mut state = cksol_funded_state(&finalizer, now_ts() - DEPOSIT_NOTICE_WINDOW_SECS - 1);

    let error = finalizer.withdraw(&mut state).await.expect_err("the window ran out");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
    assert!(error.contains("sol-sig"), "{error}");
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    // The run after an operator's release asks the minter first.
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:late"));
}

#[tokio::test]
async fn mexc_cksol_dropped_sweep_is_notified_again_without_a_new_send() {
    let recipient = repayment_account();
    let mut bridge = MockBridgeBackend::new();
    let statuses = Arc::new(std::sync::Mutex::new(vec![
        cksol_mint(recipient),
        BridgeStatus::Failed {
            reason: Some("SOL sweep dropped: sweep-1".into()),
        },
    ]));
    bridge
        .expect_get_bridge_status()
        .times(2)
        .returning(move |_| Ok(statuses.lock().unwrap().pop().unwrap()));
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, receipt| {
        assert_eq!(receipt, "sol-sig");
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "cksol-deposit-v2:second".into(),
        }))
    });
    bridge.expect_prepare_bridge_send().times(0);
    let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
    let funded_at = now_ts() - 60;
    let mut state = cksol_funded_state(&finalizer, funded_at);
    state.withdraw.bridge.withdraw_bridge_id = Some("cksol-deposit-v2:first".into());

    finalizer.withdraw(&mut state).await.unwrap();
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
    assert_eq!(state.withdraw.bridge.withdraw_bridge_funded_at_ts, Some(funded_at));
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:second"));
    finalizer.withdraw(&mut state).await.unwrap();
    assert!(matches!(state.step, CexStep::Completed));
}

#[tokio::test]
async fn mexc_cksol_lost_deposit_id_is_answered_by_an_operator() {
    use crate::outbound::driver::OperatorDecision;
    let mut bridge = MockBridgeBackend::new();
    let handles = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen = handles.clone();
    bridge
        .expect_track_deposit()
        .times(2)
        .returning(move |_, _, _, handle, _| {
            seen.lock().unwrap().push(handle.to_string());
            Ok(if handle == "42" {
                DepositNotice::Accepted(BridgeSubmission {
                    bridge_id: "cksol-deposit-v2:recorded".into(),
                })
            } else {
                DepositNotice::NotYet {
                    reason: "ValueTooSmall".into(),
                }
            })
        });
    let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
    let mut state = cksol_funded_state(&finalizer, now_ts() - DEPOSIT_NOTICE_WINDOW_SECS - 1);

    finalizer.withdraw(&mut state).await.expect_err("the window ran out");
    let questions = MexcFinalizer::<MockCexBackend>::operator_questions_for(&state);
    assert_eq!(questions.len(), 1, "{questions:?}");
    assert!(questions[0].detail.contains("sol-sig"), "{questions:?}");

    let landed = |receipt: &str| OperatorDecision::Landed {
        receipt: receipt.into(),
    };
    MexcFinalizer::<MockCexBackend>::answer_operator(&mut state, Some(&landed("abc"))).expect_err("not a number");
    MexcFinalizer::<MockCexBackend>::answer_operator(&mut state, Some(&landed(" 42 "))).unwrap();
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:recorded"));
    assert_eq!(*handles.lock().unwrap(), ["", "42"]);
}

#[tokio::test]
async fn mexc_cksol_old_handle_without_an_id_asks_the_minter() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_bridge_status().times(0);
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, receipt| {
        assert_eq!(receipt, "old-sig");
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "cksol-deposit-v2:found".into(),
        }))
    });
    let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
    let mut state = cksol_funded_state(&finalizer, 0);
    let funds = &mut state.withdraw.bridge;
    funds.withdraw_bridge_funding_txid = None;
    funds.withdraw_bridge_funded_at_ts = None;
    funds.withdraw_bridge_submitted_at_ts = Some(now_ts() - 60);
    let account = icrc_ledger_types::icrc1::account::Account {
        owner: Principal::anonymous(),
        subaccount: None,
    };
    funds.withdraw_bridge_id = Some(format!(
        "cksol-deposit-v2:{}",
        serde_json::json!({"account": account, "deposit_id": null, "transfer_signature": "old-sig", "notification_error": null})
    ));

    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_funding_txid.as_deref(), Some("old-sig"));
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:found"));
}

#[tokio::test]
async fn mexc_cksol_sweep_dropped_after_the_window_parks() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_bridge_status().times(1).returning(|_| {
        Ok(BridgeStatus::Failed {
            reason: Some("SOL sweep dropped: sweep-2".into()),
        })
    });
    bridge.expect_track_deposit().times(1).returning(|_, _, _, _, _| {
        Ok(DepositNotice::Accepted(BridgeSubmission {
            bridge_id: "cksol-deposit-v2:third".into(),
        }))
    });
    bridge.expect_prepare_bridge_send().times(0);
    let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
    let mut state = cksol_funded_state(&finalizer, now_ts() - DEPOSIT_NOTICE_WINDOW_SECS - 1);
    state.withdraw.bridge.withdraw_bridge_id = Some("cksol-deposit-v2:second".into());

    let error = finalizer.withdraw(&mut state).await.expect_err("the sweep keeps failing");
    assert!(error.starts_with(SIDE_EFFECT_AMBIGUOUS_PREFIX), "{error}");
    assert!(state.withdraw.bridge.withdraw_bridge_id.is_none());

    // A release gives one new notice.
    finalizer.withdraw(&mut state).await.unwrap();
    assert_eq!(state.withdraw.bridge.withdraw_bridge_id.as_deref(), Some("cksol-deposit-v2:third"));
}

#[test]
fn a_restart_resumes_a_deposit_send_and_only_settles_an_unsafe_one() {
    use crate::outbound::driver::{OutboundPhase, OutboundRecord};
    use liquidium_pipeline_core::outbound::PreparedSend;
    let finalizer = cksol_finalizer(MockCexBackend::new(), MockBridgeBackend::new());
    let mut state = cksol_repayment_state(&finalizer, 1.5);
    state.step = CexStep::WithdrawPending;
    state.withdraw.withdraw_id = Some("sol-withdraw".into());
    assert!(!state.clone().resume_after_restart(5), "the bridge has not started");

    let record = |replay_safe, phase| OutboundRecord {
        prepared: PreparedSend {
            sender: "solana".into(),
            identity: "sol-sig".into(),
            payload: "tx".into(),
            replay_safe,
        },
        phase,
        decision: None,
    };
    let restarted = |funding: Option<OutboundRecord>, txid: Option<&str>| {
        let mut state = state.clone();
        state.withdraw.bridge.withdraw_bridge_funding = funding;
        state.withdraw.bridge.withdraw_bridge_funding_txid = txid.map(str::to_string);
        let resumes = state.resume_after_restart(5);
        (resumes, state.withdraw.bridge.withdraw_bridge_funding.map(|record| record.phase))
    };
    assert_eq!(
        restarted(Some(record(false, OutboundPhase::Prepared)), None),
        (true, Some(OutboundPhase::Submitted { at: 5 })),
        "a crash may have sent an unsafe send, so it only settles"
    );
    assert_eq!(
        restarted(Some(record(true, OutboundPhase::Prepared)), None),
        (true, Some(OutboundPhase::Prepared))
    );
    assert_eq!(
        restarted(None, Some("sol-sig")),
        (true, None),
        "only the notice is left, and it cannot move funds"
    );
}

#[tokio::test]
async fn mexc_cksol_repayment_rejects_underfunded_bridge_before_submission() {
    let mut bridge = MockBridgeBackend::new();
    bridge.expect_get_source_fee_budget().returning(|_, _, _| Ok(0.002));
    bridge.expect_get_minimum_bridge_amount().returning(|_, _, _| Ok(0.02));
    bridge.expect_submit_bridge().times(0);
    let deps = bridge_dependencies(Arc::new(bridge));
    let route = liquidium_pipeline_connectors::backend::bridge_backend::resolve_route("SOL", "SOL", "ckSOL").unwrap();
    for gross in [0.001, 0.021, f64::NAN, f64::INFINITY] {
        let amount = ChainTokenAmount::from_formatted(cksol_token(), gross);
        assert!(
            MexcFinalizer::<MockCexBackend>::solana_repayment_bridge_amount(&deps, route, &amount)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn mexc_cksol_repayment_rejects_mint_receipt_for_wrong_account_or_token() {
    let recipient = repayment_account();
    for wrong_account in [true, false] {
        let mut bridge = MockBridgeBackend::new();
        bridge.expect_get_bridge_status().times(1).returning(move |_| {
            Ok(BridgeStatus::Minted(
                liquidium_pipeline_connectors::backend::bridge_backend::BridgeMintReceipt {
                    transaction_hash: "sig".into(),
                    log_index: Nat::from(77u8),
                    mint_block_index: Nat::from(99u8),
                    amount: Nat::from(100_000_000u64),
                    recipient: if wrong_account {
                        icrc_ledger_types::icrc1::account::Account {
                            owner: recipient.owner,
                            subaccount: None,
                        }
                    } else {
                        recipient
                    },
                    token_symbol: if wrong_account { "ckSOL".into() } else { "ckETH".into() },
                },
            ))
        });
        let finalizer = cksol_finalizer(MockCexBackend::new(), bridge);
        let mut state = cksol_repayment_state(&finalizer, 0.1);
        state.step = CexStep::WithdrawPending;
        state.withdraw.withdraw_id = Some("withdraw".into());
        state.withdraw.bridge.withdraw_bridge_id = Some("cksol-deposit-v2:test".into());
        assert!(finalizer.withdraw(&mut state).await.is_err());
        assert!(matches!(state.step, CexStep::WithdrawPending));
    }
}

#[tokio::test]
async fn mexc_sol_routes_always_use_usdt_even_with_a_direct_usdc_book() {
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    cex.expect_get_orderbook().returning(|market, _| {
        assert_ne!(market, "SOL_USDC");
        assert_ne!(market, "USDC_SOL");
        match market {
            "USDC_USDT" => Ok(liquid_orderbook()),
            "USDT_USDC" => Err("unlisted inverse market".into()),
            _ => panic!("unexpected market {market}"),
        }
    });
    let finalizer = cksol_finalizer(cex, MockBridgeBackend::new())
        .with_route_config(vec!["SOL_USDC".into(), "SOL_USDT".into(), "USDC_USDT".into()], 2);
    for (pay, receive, expected) in [
        ("USDC", "SOL", vec![("USDC_USDT", "sell"), ("SOL_USDT", "buy")]),
        ("SOL", "USDC", vec![("SOL_USDT", "sell"), ("USDC_USDT", "buy")]),
        ("USDT", "SOL", vec![("SOL_USDT", "buy")]),
        ("SOL", "USDT", vec![("SOL_USDT", "sell")]),
        ("SOL", "SOL", vec![]),
    ] {
        let legs = finalizer.resolve_trade_legs_for_symbols(pay, receive).await.unwrap();
        let route: Vec<_> = legs
            .iter()
            .map(|leg| (leg.market.as_str(), leg.side.as_str()))
            .collect();
        assert_eq!(route, expected);
    }
}

#[tokio::test]
async fn mexc_sol_usdc_is_not_used_as_an_intermediate_hop() {
    let mut cex = MockCexBackend::new();
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    cex.expect_get_orderbook().returning(|market, _| {
        assert!(matches!(market, "ICP_USDC" | "USDC_ICP"));
        Err("unlisted market".into())
    });
    let finalizer = cksol_finalizer(cex, MockBridgeBackend::new())
        .with_route_config(vec!["ICP_SOL".into(), "SOL_USDC".into(), "SOL_USDT".into()], 2);
    assert!(finalizer.resolve_trade_legs_for_symbols("ICP", "USDC").await.is_err());
}
