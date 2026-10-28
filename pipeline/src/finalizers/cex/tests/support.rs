//! Shared fixtures for the MEXC finalizer tests: receipts, tokens and wired finalizers.
use super::*;

pub(super) const TEST_MAX_SELL_SLIPPAGE_BPS: f64 = 200.0;
pub(super) const MEXC_ICP_DEPOSIT_ACCOUNT_ID_HEX: &str =
    "f0b4ad64ab441cc8792da85f948281a4b78b0038120081766aefcd29519bf2f6";
/// Minimum USD chunk used by test finalizer instances.
/// Keep tiny so tests do not trigger dust skipping unless explicitly intended.
pub(super) const TEST_CEX_MIN_EXEC_USD: f64 = 0.0001;
/// Slice target ratio used by test finalizer instances.
pub(super) const TEST_CEX_SLICE_TARGET_RATIO: f64 = 0.7;

pub(super) fn is_valid_mexc_client_order_id(value: &str) -> bool {
    let len = value.len();
    if !(1..=32).contains(&len) {
        return false;
    }

    value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

pub(super) fn make_execution_receipt(liq_id: u128) -> ExecutionReceipt {
    let collateral_token = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    };

    let debt_token = ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(1_000u64),
    };

    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(0u32),
        receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
        buy_bad_debt: false,
    };

    let liq_result = LiquidationResult {
        id: liq_id,
        timestamp: 0,
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(1_000_000u64),
            debt_repaid: Nat::from(2_000_000u64),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
    };

    let req = ExecutorRequest {
        liquidation,
        swap_args: None,
        debt_asset: debt_token.clone(),
        collateral_asset: collateral_token.clone(),
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_ref_price: Nat::from(0u8),
        ref_price_at: 0,
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    };

    ExecutionReceipt {
        request: req,
        liquidation_result: Some(liq_result),
        status: ExecutionStatus::Success,
        change_received: false,
    }
}

pub(super) fn make_execution_receipt_with_assets(
    liq_id: u128,
    collateral_token: ChainToken,
    debt_token: ChainToken,
) -> ExecutionReceipt {
    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(0u32),
        receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
        buy_bad_debt: false,
    };

    let liq_result = LiquidationResult {
        id: liq_id,
        timestamp: 0,
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(1_000_000u64),
            debt_repaid: Nat::from(2_000_000u64),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Pending,
        },
    };

    let req = ExecutorRequest {
        liquidation,
        swap_args: None,
        debt_asset: debt_token,
        collateral_asset: collateral_token,
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_ref_price: Nat::from(0u8),
        ref_price_at: 0,
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    };

    ExecutionReceipt {
        request: req,
        liquidation_result: Some(liq_result),
        status: ExecutionStatus::Success,
        change_received: false,
    }
}

pub(super) fn ckusdc_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

pub(super) fn cketh_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").expect("ledger"),
        symbol: "ckETH".to_string(),
        decimals: 18,
        fee: Nat::from(2_000_000_000_000u64),
    }
}

pub(super) fn ckbtc_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::anonymous(),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(1_000u64),
    }
}

pub(super) fn native_icp_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("ICP ledger principal"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    }
}

/// MEXC withdrawals with no fee and no minimum, on a route MEXC accepts.
pub(super) fn free_withdrawals(cex: &mut MockCexBackend) {
    cex.expect_get_withdraw_terms()
        .returning(|_, _| Ok(WithdrawTerms { fee: 0.0, minimum: 0.0 }));
    cex.expect_check_funding_route().returning(|_| Ok(()));
}

pub(super) fn liquid_orderbook() -> OrderBook {
    OrderBook {
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 1_000_000.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.001,
            quantity: 1_000_000.0,
        }],
        taker_fee_bps: 10.0,
    }
}

pub(super) fn bridge_dependencies(backend: Arc<MockBridgeBackend>) -> BridgeRuntime {
    BridgeRuntime::for_tests(backend)
}

/// A helper deposit as the EVM bridge signs it.
pub(super) fn evm_bridge_send() -> liquidium_pipeline_connectors::backend::bridge_backend::BridgeSend {
    liquidium_pipeline_connectors::backend::bridge_backend::BridgeSend {
        send: liquidium_pipeline_core::outbound::PreparedSend {
            sender: "evm".into(),
            identity: "0xhelper".into(),
            payload: "signed-call".into(),
            replay_safe: true,
        },
        handle: "mint-handle".into(),
    }
}

/// A bridge wallet sender whose send lands on the first try.
pub(super) fn landing_sender() -> liquidium_pipeline_core::outbound::MockOutboundSender {
    let mut sender = liquidium_pipeline_core::outbound::MockOutboundSender::new();
    sender
        .expect_submit()
        .times(1)
        .returning(|_| liquidium_pipeline_core::outbound::SendOutcome::Landed {
            receipt: "0xhelper".into(),
        });
    sender
}

/// Runs the two funding cycles: the first saves the transfer, the second sends it.
pub(super) async fn fund(finalizer: &CexVenueFinalizer<MockCexBackend>, state: &mut CexState) {
    finalizer.deposit(state).await.expect("the first cycle saves the funding transfer");
    assert!(state.deposit.deposit_funding.is_some(), "the transfer is saved before it is sent");
    finalizer.deposit(state).await.expect("the second cycle sends it");
    assert!(state.deposit.deposit_txid.is_some(), "the funding transfer landed");
}

pub(super) fn repayment_account() -> icrc_ledger_types::icrc1::account::Account {
    icrc_ledger_types::icrc1::account::Account {
        owner: Principal::from_slice(&[42]),
        subaccount: Some([7; 32]),
    }
}

/// The finalizer most tests use: an anonymous liquidator and the test tunables.
pub(super) fn test_finalizer<C: CexBackend>(
    backend: Arc<C>,
    transfers: Arc<dyn TransferActions>,
) -> CexVenueFinalizer<C> {
    CexVenueFinalizer::new(
        backend,
        transfers,
        Principal::anonymous(),
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
}

pub(super) fn cksol_finalizer(cex: MockCexBackend, bridge: MockBridgeBackend) -> CexVenueFinalizer<MockCexBackend> {
    CexVenueFinalizer::new(
        Arc::new(cex),
        Arc::new(MockTransferActions::new()),
        repayment_account().owner,
        TEST_MAX_SELL_SLIPPAGE_BPS,
        TEST_CEX_MIN_EXEC_USD,
        TEST_CEX_SLICE_TARGET_RATIO,
    )
    .with_bridge_runtime(bridge_dependencies(Arc::new(bridge)))
}

pub(super) fn cksol_repayment_state(finalizer: &CexVenueFinalizer<MockCexBackend>, amount: f64) -> CexState {
    let mut state = finalizer
        .prepare_amount_scoped_state(
            "1705",
            ChainTokenAmount::from_formatted(native_icp_token(), 10.0),
            cksol_token(),
            Some(repayment_account().to_string()),
        )
        .unwrap();
    state.step = CexStep::Withdraw;
    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(cksol_token(), amount));
    state
}

pub(super) fn cksol_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text("ls5lp-lqaaa-aaaar-qb5oa-cai").unwrap(),
        symbol: "ckSOL".to_string(),
        decimals: 9,
        fee: Nat::from(500u64),
    }
}

/// The signed SOL transfer that the bridge backend returns.
pub(super) fn sol_bridge_send() -> liquidium_pipeline_connectors::backend::bridge_backend::BridgeSend {
    liquidium_pipeline_connectors::backend::bridge_backend::BridgeSend {
        send: liquidium_pipeline_core::outbound::PreparedSend {
            sender: "solana".into(),
            identity: "sol-sig".into(),
            payload: "signed-transfer".into(),
            replay_safe: true,
        },
        handle: String::new(),
    }
}

/// The bridge wallet's sender: the saved send is open once, then landed.
pub(super) fn sol_deposit_sender() -> liquidium_pipeline_core::outbound::MockOutboundSender {
    use liquidium_pipeline_core::outbound::SendOutcome;
    let mut sender = liquidium_pipeline_core::outbound::MockOutboundSender::new();
    sender.expect_submit().times(1).returning(|_| SendOutcome::Open {
        reason: "sent".into(),
    });
    sender.expect_settle().times(1).returning(|_| SendOutcome::Landed {
        receipt: "sol-sig".into(),
    });
    sender.expect_open_window_secs().returning(|_| 900);
    sender
}

pub(super) fn cksol_mint(recipient: icrc_ledger_types::icrc1::account::Account) -> BridgeStatus {
    BridgeStatus::Minted(liquidium_pipeline_connectors::backend::bridge_backend::BridgeMintReceipt {
        transaction_hash: "sol-sig".into(),
        log_index: Nat::from(77u8),
        mint_block_index: Nat::from(99u8),
        amount: Nat::from(1_487_995_000u64),
        recipient,
        token_symbol: "ckSOL".into(),
    })
}

/// A ckSOL repayment row whose SOL send landed and waits for the minter.
pub(super) fn cksol_funded_state(finalizer: &CexVenueFinalizer<MockCexBackend>, funded_at: i64) -> CexState {
    let mut state = cksol_repayment_state(finalizer, 1.5);
    state.step = CexStep::WithdrawPending;
    state.withdraw.withdraw_id = Some("sol-withdraw".into());
    state.withdraw.bridge.withdraw_bridge_destination_snapshot = Some(repayment_account().to_string());
    state.withdraw.bridge.withdraw_bridge_funding_txid = Some("sol-sig".into());
    state.withdraw.bridge.withdraw_bridge_funded_at_ts = Some(funded_at);
    state
}

/// A claim registry that answers each scan with the next list of open sends.
pub(super) fn open_deposit_sends(
    scans: Vec<Vec<crate::bridge::claims::OpenDepositSend>>,
) -> crate::bridge::claims::MockBridgeClaimLookup {
    let scans = std::sync::Mutex::new(scans.into_iter());
    let mut claims = crate::bridge::claims::MockBridgeClaimLookup::new();
    claims.expect_claims_since().returning(move |_| {
        Ok(crate::bridge::claims::BridgeClaims {
            open_deposit_sends: scans.lock().unwrap().next().expect("unexpected claim scan"),
            ..Default::default()
        })
    });
    claims
}

/// Resolves and previews the trade route of a receipt, the way a leg preview does.
pub(super) async fn preview_receipt_route(
    finalizer: &CexVenueFinalizer<MockCexBackend>,
    receipt: &ExecutionReceipt,
) -> Result<crate::finalizers::cex::route_preview::CexRoutePreview, String> {
    let state = finalizer.prepare("preview", receipt).await?;
    let legs = finalizer
        .resolve_trade_legs_for_symbols(
            &<CexVenueFinalizer<MockCexBackend> as BridgePlanner>::planned_deposit_asset(&state),
            &<CexVenueFinalizer<MockCexBackend> as BridgePlanner>::planned_withdraw_asset(&state),
        )
        .await?;
    finalizer
        .preview_resolved_trade_route(&legs, state.size_in.to_f64())
        .await
}
