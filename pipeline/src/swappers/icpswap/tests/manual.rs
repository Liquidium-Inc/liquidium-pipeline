use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use mockall::Sequence;

use super::{
    client::MockIcpswapManualClient,
    execution::IcpswapExecutionStateStore,
    manual::{advance_manual, is_slippage_error},
    state::{initial_slippage_bps, retry_backoff_nanos, retry_slippage_bps},
    types::{
        IcpswapError, IcpswapExecutionPlan, IcpswapExecutionState, IcpswapManualClientError, IcpswapStep,
        IcpswapUnusedBalance,
    },
};

fn p(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn owner() -> Account {
    Account {
        owner: p(4),
        subaccount: None,
    }
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn plan() -> IcpswapExecutionPlan {
    IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(token(p(1), "ICP", 10), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(token(p(1), "ICP", 10), Nat::from(10u64)),
        ChainTokenAmount::from_raw(token(p(2), "ckUSDC", 5), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(token(p(2), "ckUSDC", 5), Nat::from(5u64)),
        500,
        123,
    )
    .expect("plan")
}

fn unused(input: u64, output: u64) -> IcpswapUnusedBalance {
    IcpswapUnusedBalance {
        balance0: Nat::from(input),
        balance1: Nat::from(output),
    }
}

struct Store(Mutex<IcpswapExecutionState>);

impl Store {
    fn new() -> Self {
        Self(Mutex::new(IcpswapExecutionState::prepare("run-1", plan(), owner())))
    }

    fn state(&self) -> IcpswapExecutionState {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for Store {
    async fn load(&self, _: &str) -> Result<Option<IcpswapExecutionState>, String> {
        Ok(Some(self.state()))
    }

    async fn persist(&self, _: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        *self.0.lock().unwrap() = state.clone();
        Ok(())
    }
}

#[tokio::test]
async fn manual_success_persists_deposit_trade_and_withdraw_boundaries() {
    let store = Arc::new(Store::new());
    let observed = store.clone();
    let mut client = MockIcpswapManualClient::new();
    client
        .expect_manual_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(100_010u64)));
    client.expect_manual_approve().times(0);

    let mut unused_sequence = Sequence::new();
    for value in [
        unused(0, 0),
        unused(100_000, 0),
        unused(100_000, 0),
        unused(0, 119_000),
        unused(0, 119_000),
        unused(0, 0),
    ] {
        client
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_deposit_from().times(1).return_once(move |_, args| {
        let persisted = observed.state();
        assert_eq!(persisted.step, IcpswapStep::DepositPending);
        assert_eq!(args.amount, Nat::from(100_000u64));
        Ok(args.amount.clone())
    });
    client.expect_swap_manual().times(1).return_once(|_, args| {
        assert_eq!(args.amount_out_minimum, "118500");
        Ok(Nat::from(119_000u64))
    });
    let mut balance_sequence = Sequence::new();
    client
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(10u64)));
    client
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(119_005u64)));
    client.expect_withdraw_manual().times(1).return_once(|_, args| {
        assert_eq!(args.amount, Nat::from(119_000u64));
        assert_eq!(args.fee, Nat::from(5u64));
        Ok(args.amount.clone())
    });
    client.expect_quote_manual().times(0);

    let first = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect("deposit");
    assert_eq!(first.step, IcpswapStep::Trade);
    let second = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("trade");
    assert_eq!(second.step, IcpswapStep::Withdraw);
    let third = advance_manual(&client, store.as_ref(), "run-1", owner(), 3_000)
        .await
        .expect("withdraw");
    assert_eq!(third.step, IcpswapStep::Completed);
    assert_eq!(third.withdraw.wallet_credited_amount, Some(Nat::from(118_995u64)));
}

#[tokio::test]
async fn confirmed_slippage_requotes_and_retries_without_crossing_original_floor() {
    let store = Arc::new(Store::new());
    {
        store.0.lock().unwrap().step = IcpswapStep::Trade;
    }
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [
        unused(100_000, 0),
        unused(100_000, 0),
        unused(100_000, 0),
        unused(0, 116_000),
    ] {
        client
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut swap_sequence = Sequence::new();
    client
        .expect_swap_manual()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, args| {
            assert_eq!(args.amount_out_minimum, "118500");
            Err(IcpswapManualClientError::Protocol {
                method: "swap",
                error: IcpswapError::InternalError("slippage check failed".to_string()),
            })
        });
    client
        .expect_swap_manual()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, args| {
            assert_eq!(args.amount_out_minimum, "115050");
            Ok(Nat::from(116_000u64))
        });
    client
        .expect_quote_manual()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(118_000u64)));

    let error = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("slippage");
    assert!(error.contains("slippage"));
    let after_failure = store.state();
    let trade = &after_failure.trade;
    assert_eq!(trade.retry_count, 1);
    assert_eq!(trade.next_retry_at_nanos, Some(2_000_001_000));

    let waiting = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000_000_000)
        .await
        .expect("backoff wait");
    assert_eq!(waiting.step, IcpswapStep::Trade);

    let retried = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000_001_000)
        .await
        .expect("retry");
    assert_eq!(retried.step, IcpswapStep::Withdraw);
    assert!(retried.trade.current_amount_out_minimum.value >= retried.trade.original_hard_minimum_out.value);
}

#[tokio::test]
async fn decoded_slippage_survives_an_immediate_reconciliation_query_failure() {
    let store = Arc::new(Store::new());
    {
        store.0.lock().unwrap().step = IcpswapStep::Trade;
    }
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    client
        .expect_manual_unused_balance()
        .times(1)
        .in_sequence(&mut unused_sequence)
        .return_once(|_, _| Ok(unused(100_000, 0)));
    client
        .expect_manual_unused_balance()
        .times(1)
        .in_sequence(&mut unused_sequence)
        .return_once(|pool, _| {
            Err(IcpswapManualClientError::Query {
                pool,
                method: "getUserUnusedBalance",
                message: "temporarily unavailable".to_string(),
            })
        });
    for value in [unused(100_000, 0), unused(100_000, 0)] {
        client
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap_manual().times(1).return_once(|_, _| {
        Err(IcpswapManualClientError::Protocol {
            method: "swap",
            error: IcpswapError::InternalError("slippage check failed".to_string()),
        })
    });
    client.expect_quote_manual().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("query failure");
    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::TradePending);
    assert!(persisted.trade.swap_protocol_error.is_some());

    let resumed = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("resume decoded error");
    assert_eq!(resumed.step, IcpswapStep::Trade);
    assert_eq!(resumed.trade.retry_count, 1);
}

#[tokio::test]
async fn ambiguous_swap_enters_operator_required_and_resume_never_resubmits() {
    let store = Arc::new(Store::new());
    {
        store.0.lock().unwrap().step = IcpswapStep::Trade;
    }
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(100_000, 0), unused(100_000, 0)] {
        client
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap_manual().times(1).return_once(|_, _| {
        Err(IcpswapManualClientError::SubmissionUnknown {
            pool: p(9),
            method: "swap",
            message: "timeout".to_string(),
        })
    });
    client.expect_quote_manual().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("ambiguous");
    assert_eq!(store.state().step, IcpswapStep::OperatorRequired);

    let resumed = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("read-only reconcile");
    assert_eq!(resumed.step, IcpswapStep::OperatorRequired);
}

#[tokio::test]
async fn non_slippage_swap_failure_withdraws_icp_back_to_owner() {
    let store = Arc::new(Store::new());
    {
        store.0.lock().unwrap().step = IcpswapStep::Trade;
    }
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(100_000, 0), unused(100_000, 0), unused(0, 0)] {
        client
            .expect_manual_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap_manual().times(1).return_once(|_, _| {
        Err(IcpswapManualClientError::Protocol {
            method: "swap",
            error: IcpswapError::InternalError("pool temporarily unavailable".to_string()),
        })
    });
    let mut balance_sequence = Sequence::new();
    client
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(0u8)));
    client
        .expect_ledger_balance()
        .times(1)
        .in_sequence(&mut balance_sequence)
        .return_once(|_, _| Ok(Nat::from(99_990u64)));
    client
        .expect_withdraw_manual()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone()));
    client.expect_quote_manual().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("protocol failure");
    assert_eq!(store.state().step, IcpswapStep::Recover);

    let recovered = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("recovery");
    assert_eq!(recovered.step, IcpswapStep::Refunded);
    assert_eq!(recovered.recovery.wallet_credited_amount, Some(Nat::from(99_990u64)));
}

#[tokio::test]
async fn third_retry_quote_below_hard_floor_moves_to_recovery_without_submission() {
    let store = Arc::new(Store::new());
    {
        let mut state = store.0.lock().unwrap();
        state.step = IcpswapStep::Trade;
        state.trade.retry_count = 3;
    }
    let mut client = MockIcpswapManualClient::new();
    client
        .expect_quote_manual()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(113_999u64)));
    client.expect_manual_unused_balance().times(0);
    client.expect_swap_manual().times(0);

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 10_000)
        .await
        .expect("quote evaluation");
    assert_eq!(result.step, IcpswapStep::Recover);
}

#[test]
fn retry_policy_and_slippage_classifier_are_bounded() {
    assert_eq!(initial_slippage_bps(500), 125);
    assert_eq!(retry_slippage_bps(500, 0), 125);
    assert_eq!(retry_slippage_bps(500, 1), 250);
    assert_eq!(retry_slippage_bps(500, 2), 375);
    assert_eq!(retry_slippage_bps(500, 3), 500);
    assert_eq!(retry_slippage_bps(100, 3), 100);
    assert_eq!(retry_backoff_nanos(1), 2_000_000_000);
    assert_eq!(retry_backoff_nanos(2), 4_000_000_000);
    assert_eq!(retry_backoff_nanos(3), 8_000_000_000);
    assert!(is_slippage_error("slippage check failed"));
    assert!(is_slippage_error("amountOutMinimum not satisfied"));
    assert!(!is_slippage_error("unsupported token"));
}

#[test]
fn legacy_one_step_records_are_rejected() {
    let legacy = serde_json::json!({
        "plan": plan(),
        "phase": "AwaitingOutput",
        "submitted_at": 123
    });
    assert!(serde_json::from_value::<IcpswapExecutionState>(legacy).is_err());
}

#[test]
fn flattened_manual_state_uses_unique_step_prefixed_wire_fields() {
    let mut state = IcpswapExecutionState::prepare("run-1", plan(), owner());
    state.deposit.input_pool_balance_before = Some(Nat::from(11u8));
    state.trade.input_pool_balance_before = Some(Nat::from(22u8));
    state.withdraw.pool_balance_before = Some(Nat::from(33u8));
    state.recovery.pool_balance_before = Some(Nat::from(44u8));

    let json = serde_json::to_value(&state).unwrap();
    let state_json = json.as_object().unwrap();
    assert_eq!(
        state_json["deposit_input_pool_balance_before"],
        serde_json::to_value(Nat::from(11u8)).unwrap()
    );
    assert_eq!(
        state_json["trade_input_pool_balance_before"],
        serde_json::to_value(Nat::from(22u8)).unwrap()
    );
    assert_eq!(
        state_json["withdraw_pool_balance_before"],
        serde_json::to_value(Nat::from(33u8)).unwrap()
    );
    assert_eq!(
        state_json["recovery_pool_balance_before"],
        serde_json::to_value(Nat::from(44u8)).unwrap()
    );

    let decoded: IcpswapExecutionState = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, state);
}
