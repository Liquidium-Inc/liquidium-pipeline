use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::{
    account::{Account, principal_to_subaccount},
    transfer::TransferArg,
};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use mockall::Sequence;

use super::{
    client::MockIcpswapManualClient,
    execution::IcpswapExecutionStateStore,
    identity::IcpswapExecutionIdentity,
    manual::advance_manual,
    reconciliation::is_slippage_error,
    state::{
        DEPOSIT_OBSERVATION_RETRY_NANOS, MAX_DEPOSIT_OBSERVATION_ATTEMPTS, MAX_DEPOSIT_SUBMISSION_RETRIES,
        PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS, initial_slippage_bps, retry_backoff_nanos, retry_slippage_bps,
    },
    transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
    types::{
        ICPSWAP_STATE_VERSION, IcpswapClientError, IcpswapError, IcpswapExecutionPlan, IcpswapExecutionState,
        IcpswapStep, IcpswapUnusedBalance,
    },
};

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

fn p(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

#[test]
fn pool_subaccount_uses_the_documented_principal_encoding() {
    let principal = p(4);
    let bytes = principal.as_slice();
    let subaccount = principal_to_subaccount(principal);

    assert_eq!(subaccount[0], bytes.len() as u8);
    assert_eq!(&subaccount[1..=bytes.len()], bytes);
    assert!(subaccount[bytes.len() + 1..].iter().all(|byte| *byte == 0));
}

fn owner() -> Account {
    let (identity, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1").expect("identity");
    Account {
        owner: identity.principal,
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
    )
    .expect("plan")
}

fn prepared_pool_state() -> IcpswapExecutionState {
    let plan = plan();
    let (identity, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1").expect("identity");
    let child = Account {
        owner: identity.principal,
        subaccount: None,
    };
    let funding = IcpswapFundingState::new(
        Account {
            owner: p(8),
            subaccount: None,
        },
        child,
        plan.input_ledger_fee.clone(),
    );
    let settlement = IcpswapSettlementState {
        kind: None,
        destination: Account {
            owner: p(7),
            subaccount: None,
        },
        fee: plan.output_ledger_fee.clone(),
        transfer: IcpswapLedgerTransferState::default(),
        interrupted_transfer: None,
        interrupted_observed_debit: None,
        recovery_credit: None,
        residual_dust: None,
    };
    let mut state = IcpswapExecutionState::prepare("run-1", plan, identity, funding, settlement).expect("state");
    // These tests audit only the existing child-to-pool workflow. Funding and
    // final forwarding have dedicated tests at the session boundary.
    state.step = IcpswapStep::Transfer;
    state
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
        Self(Mutex::new(prepared_pool_state()))
    }

    fn state(&self) -> IcpswapExecutionState {
        self.0.lock().unwrap().clone()
    }
}

fn set_deposit_step(store: &Store) {
    let mut state = store.0.lock().unwrap();
    state.step = IcpswapStep::Deposit;
    state.transfer.args = Some(TransferArg {
        from_subaccount: None,
        to: owner(),
        fee: Some(Nat::from(10u64)),
        created_at_time: Some(1_000),
        memo: None,
        amount: Nat::from(100_010u64),
    });
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
    let observed_transfer = store.clone();
    let mut client = MockIcpswapManualClient::new();
    client.expect_ledger_transfer().times(1).return_once(move |_, args| {
        let persisted = observed_transfer.state();
        assert_eq!(persisted.step, IcpswapStep::TransferPending);
        assert_eq!(args.amount, Nat::from(100_010u64));
        assert_eq!(args.fee, Some(Nat::from(10u64)));
        assert_eq!(args.created_at_time, Some(1_000));
        Ok(Nat::from(77u64))
    });

    let mut unused_sequence = Sequence::new();
    for value in [unused(0, 0), unused(100_000, 0), unused(0, 119_000), unused(0, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_deposit().times(1).return_once(move |_, args| {
        let persisted = observed.state();
        assert_eq!(persisted.step, IcpswapStep::DepositPending);
        assert_eq!(args.amount, Nat::from(100_010u64));
        assert_eq!(args.fee, Nat::from(10u64));
        Ok(args.amount.clone() - args.fee.clone())
    });
    client
        .expect_ledger_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));
    client.expect_swap().times(1).return_once(|_, args| {
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
    client.expect_withdraw().times(1).return_once(|_, args| {
        assert_eq!(args.amount, Nat::from(119_000u64));
        assert_eq!(args.fee, Nat::from(5u64));
        Ok(args.amount.clone())
    });
    client.expect_requote().times(0);

    let first = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect("transfer");
    assert_eq!(first.step, IcpswapStep::Deposit);
    let second = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("deposit");
    assert_eq!(second.step, IcpswapStep::Trade);
    let third = advance_manual(&client, store.as_ref(), "run-1", owner(), 3_000)
        .await
        .expect("trade");
    assert_eq!(third.step, IcpswapStep::Withdraw);
    assert_eq!(third.trade.gross_output_amount, Some(Nat::from(119_000u64)));
    let fourth = advance_manual(&client, store.as_ref(), "run-1", owner(), 4_000)
        .await
        .expect("withdraw");
    assert_eq!(fourth.step, IcpswapStep::Forward);
    assert_eq!(fourth.withdraw.wallet_credited_amount, Some(Nat::from(118_995u64)));
}

#[tokio::test]
async fn ambiguous_output_withdrawal_returns_success_when_reconciliation_completes() {
    let store = Arc::new(Store::new());
    {
        let mut state = store.0.lock().unwrap();
        state.step = IcpswapStep::Withdraw;
        state.trade.gross_output_amount = Some(Nat::from(119_000u64));
    }

    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(0, 119_000), unused(0, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut balance_sequence = Sequence::new();
    for value in [10u64, 119_005] {
        client
            .expect_ledger_balance()
            .times(1)
            .in_sequence(&mut balance_sequence)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client.expect_withdraw().times(1).return_once(|pool, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool,
            method: "withdraw",
            message: "response lost".to_string(),
        })
    });

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 4_000)
        .await
        .expect("observed output withdrawal is terminal");

    assert_eq!(result.step, IcpswapStep::Forward);
    assert_eq!(result.withdraw.wallet_credited_amount, Some(Nat::from(118_995u64)));
    assert_eq!(result.last_error, None);
}

#[tokio::test]
async fn ambiguous_recovery_returns_success_when_reconciliation_completes() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Recover;

    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(0, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut balance_sequence = Sequence::new();
    for value in [10u64, 100_000] {
        client
            .expect_ledger_balance()
            .times(1)
            .in_sequence(&mut balance_sequence)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client.expect_withdraw().times(1).return_once(|pool, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool,
            method: "withdraw",
            message: "response lost".to_string(),
        })
    });

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 4_000)
        .await
        .expect("observed recovery is terminal");

    assert_eq!(result.step, IcpswapStep::Forward);
    assert_eq!(result.recovery.wallet_credited_amount, Some(Nat::from(99_990u64)));
    assert_eq!(result.last_error, None);
}

#[tokio::test]
async fn ambiguous_transfer_resume_reuses_the_persisted_deduplication_arguments() {
    let store = Arc::new(Store::new());
    let first_args = Arc::new(Mutex::new(None::<TransferArg>));
    let mut client = MockIcpswapManualClient::new();

    let mut transfer_sequence = Sequence::new();
    let captured = first_args.clone();
    client
        .expect_ledger_transfer()
        .times(1)
        .in_sequence(&mut transfer_sequence)
        .return_once(move |ledger, args| {
            assert_eq!(ledger, p(1));
            *captured.lock().unwrap() = Some(args);
            Err(IcpswapClientError::LedgerTransfer {
                ledger,
                message: "response lost".to_string(),
            })
        });
    let expected = first_args.clone();
    client
        .expect_ledger_transfer()
        .times(1)
        .in_sequence(&mut transfer_sequence)
        .return_once(move |_, args| {
            assert_eq!(Some(args), *expected.lock().unwrap());
            Ok(Nat::from(88u64))
        });

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("first transfer response is ambiguous");
    assert_eq!(store.state().step, IcpswapStep::TransferPending);

    let resumed = advance_manual(&client, store.as_ref(), "run-1", owner(), 9_000)
        .await
        .expect("deduplicated transfer retry");
    assert_eq!(resumed.step, IcpswapStep::Deposit);
    assert_eq!(resumed.transfer.block_index, Some(Nat::from(88u64)));
    assert_eq!(resumed.transfer.args.unwrap().created_at_time, Some(1_000));
}

#[tokio::test]
async fn transfer_aged_out_of_the_deduplication_window_requires_an_operator() {
    let store = Arc::new(Store::new());
    let mut client = MockIcpswapManualClient::new();
    client.expect_ledger_transfer().times(1).return_once(|ledger, _| {
        Err(IcpswapClientError::LedgerTransferTooOld {
            ledger,
            message: "created_at_time is too far in the past".to_string(),
        })
    });

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("a stale transfer window cannot be replayed");

    // Parked rather than left pending: replaying is refused forever and
    // deduplication can no longer prove whether the input moved.
    let parked = store.state();
    assert_eq!(parked.step, IcpswapStep::OperatorRequired);
    assert_eq!(parked.operator_pending_step, Some(IcpswapStep::TransferPending));
    assert_eq!(parked.transfer.block_index, None);

    // A further cycle reconciles instead of resubmitting the doomed arguments.
    let mut parked_client = MockIcpswapManualClient::new();
    parked_client.expect_ledger_transfer().times(0);
    let resumed = advance_manual(&parked_client, store.as_ref(), "run-1", owner(), 9_000)
        .await
        .expect("operator reconciliation is a no-op for a pending transfer");
    assert_eq!(resumed.step, IcpswapStep::OperatorRequired);
}

#[tokio::test]
async fn future_dated_transfer_is_resubmitted_with_refreshed_arguments() {
    let store = Arc::new(Store::new());
    let mut client = MockIcpswapManualClient::new();
    let mut transfer_sequence = Sequence::new();
    client
        .expect_ledger_transfer()
        .times(1)
        .in_sequence(&mut transfer_sequence)
        .return_once(|ledger, args| {
            assert_eq!(args.created_at_time, Some(5_000));
            Err(IcpswapClientError::LedgerTransferCreatedInFuture {
                ledger,
                message: "current ledger time is 1000".to_string(),
            })
        });
    client
        .expect_ledger_transfer()
        .times(1)
        .in_sequence(&mut transfer_sequence)
        .return_once(|_, args| {
            // Nothing landed under the rejected timestamp, so the retry carries a
            // current one instead of replaying a timestamp the ledger refuses.
            assert_eq!(args.created_at_time, Some(6_000));
            Ok(Nat::from(88u64))
        });

    advance_manual(&client, store.as_ref(), "run-1", owner(), 5_000)
        .await
        .expect_err("a future-dated transfer is rejected outright");
    let rejected = store.state();
    assert_eq!(rejected.step, IcpswapStep::Transfer);
    assert_eq!(rejected.transfer.args, None);

    let resumed = advance_manual(&client, store.as_ref(), "run-1", owner(), 6_000)
        .await
        .expect("refreshed transfer arguments are accepted");
    assert_eq!(resumed.step, IcpswapStep::Deposit);
    assert_eq!(resumed.transfer.block_index, Some(Nat::from(88u64)));
}

#[tokio::test]
async fn pending_transfer_without_persisted_arguments_requires_an_operator() {
    let store = Arc::new(Store::new());
    {
        let mut state = store.0.lock().unwrap();
        state.step = IcpswapStep::TransferPending;
        state.transfer.args = None;
    }

    let mut client = MockIcpswapManualClient::new();
    client.expect_ledger_transfer().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("a pending transfer without arguments cannot be replayed");
    let parked = store.state();
    assert_eq!(parked.step, IcpswapStep::OperatorRequired);
    assert_eq!(parked.operator_pending_step, Some(IcpswapStep::TransferPending));
}

#[tokio::test]
async fn ambiguous_deposit_is_resubmitted_when_the_deposit_account_is_unchanged() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());
    store.0.lock().unwrap().transfer.block_index = Some(Nat::from(77u64));

    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [
        unused(0, 0),
        unused(0, 0),
        unused(0, 0),
        unused(0, 0),
        unused(0, 0),
        unused(0, 0),
    ] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut deposit_sequence = Sequence::new();
    client
        .expect_deposit()
        .times(1)
        .in_sequence(&mut deposit_sequence)
        .return_once(|pool, _| {
            Err(IcpswapClientError::SubmissionUnknown {
                pool,
                method: "deposit",
                message: "response lost".to_string(),
            })
        });
    client
        .expect_deposit()
        .times(1)
        .in_sequence(&mut deposit_sequence)
        .return_once(|_, args| Ok(args.amount.clone() - args.fee.clone()));
    let mut ledger_sequence = Sequence::new();
    for balance in [100_010u64, 100_010u64, 100_010u64] {
        client
            .expect_ledger_balance()
            .times(1)
            .in_sequence(&mut ledger_sequence)
            .return_once(move |_, _| Ok(Nat::from(balance)));
    }
    let submitted_at = 2_000;
    advance_manual(&client, store.as_ref(), "run-1", owner(), submitted_at)
        .await
        .expect("ambiguous submission should schedule observation");
    let pending = store.state();
    assert_eq!(pending.step, IcpswapStep::DepositPending);
    assert_eq!(pending.deposit.observation_attempts, 0);
    assert_eq!(
        pending.next_attempt_at_nanos,
        Some(submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS)
    );

    advance_manual(
        &client,
        store.as_ref(),
        "run-1",
        owner(),
        submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS - 1,
    )
    .await
    .expect("early polling should not observe or submit");
    assert_eq!(store.state().deposit.observation_attempts, 0);

    for attempt in 1..MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
        let now = submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS * u64::from(attempt);
        advance_manual(&client, store.as_ref(), "run-1", owner(), now)
            .await
            .expect("non-final observation should remain pending");
        let pending = store.state();
        assert_eq!(pending.step, IcpswapStep::DepositPending);
        assert_eq!(pending.deposit.observation_attempts, attempt);
        assert_eq!(
            pending.next_attempt_at_nanos,
            Some(now + DEPOSIT_OBSERVATION_RETRY_NANOS)
        );
    }

    let final_observation_at =
        submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS * u64::from(MAX_DEPOSIT_OBSERVATION_ATTEMPTS);
    advance_manual(&client, store.as_ref(), "run-1", owner(), final_observation_at)
        .await
        .expect("an unchanged deposit account makes deposit-only replay safe");
    let scheduled = store.state();
    assert_eq!(scheduled.step, IcpswapStep::DepositPending);
    assert!(scheduled.deposit.ready_to_submit);
    assert_eq!(scheduled.deposit.observation_attempts, 0);
    assert_eq!(scheduled.deposit.submission_retry_count, 1);
    assert_eq!(
        scheduled.next_attempt_at_nanos,
        Some(final_observation_at + DEPOSIT_OBSERVATION_RETRY_NANOS)
    );

    advance_manual(
        &client,
        store.as_ref(),
        "run-1",
        owner(),
        final_observation_at + DEPOSIT_OBSERVATION_RETRY_NANOS - 1,
    )
    .await
    .expect("an early poll must not resubmit the deposit");
    assert_eq!(store.state().step, IcpswapStep::DepositPending);

    let completed_retry = advance_manual(
        &client,
        store.as_ref(),
        "run-1",
        owner(),
        final_observation_at + DEPOSIT_OBSERVATION_RETRY_NANOS,
    )
    .await
    .expect("the due deposit-only retry should execute");
    assert_eq!(completed_retry.step, IcpswapStep::Trade);
    assert!(!completed_retry.deposit.ready_to_submit);
    assert_eq!(completed_retry.deposit.submission_retry_count, 0);
}

#[tokio::test]
async fn ambiguous_deposit_with_a_moved_deposit_account_requires_an_operator() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1 + MAX_DEPOSIT_OBSERVATION_ATTEMPTS as usize)
        .returning(|_, _| Ok(unused(0, 0)));
    client.expect_deposit().times(1).return_once(|pool, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool,
            method: "deposit",
            message: "response lost".to_string(),
        })
    });
    let mut ledger_sequence = Sequence::new();
    for balance in [100_010u64, 0u64] {
        client
            .expect_ledger_balance()
            .times(1)
            .in_sequence(&mut ledger_sequence)
            .return_once(move |_, _| Ok(Nat::from(balance)));
    }

    let submitted_at = 2_000;
    advance_manual(&client, store.as_ref(), "run-1", owner(), submitted_at)
        .await
        .expect("ambiguous submission");
    for attempt in 1..=MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
        let result = advance_manual(
            &client,
            store.as_ref(),
            "run-1",
            owner(),
            submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS * u64::from(attempt),
        )
        .await;
        if attempt < MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
            result.expect("non-final observation");
        } else {
            result.expect_err("a moved deposit account makes replay unsafe");
        }
    }

    let parked = store.state();
    assert_eq!(parked.step, IcpswapStep::OperatorRequired);
    assert_eq!(parked.operator_pending_step, Some(IcpswapStep::DepositPending));
    assert!(parked.last_error.unwrap().contains("balance moved"));
}

#[tokio::test]
async fn ambiguous_deposit_retries_are_bounded() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());
    store.0.lock().unwrap().deposit.submission_retry_count = MAX_DEPOSIT_SUBMISSION_RETRIES;

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1 + MAX_DEPOSIT_OBSERVATION_ATTEMPTS as usize)
        .returning(|_, _| Ok(unused(0, 0)));
    client
        .expect_ledger_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));
    client.expect_deposit().times(1).return_once(|pool, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool,
            method: "deposit",
            message: "response lost".to_string(),
        })
    });

    let submitted_at = 2_000;
    advance_manual(&client, store.as_ref(), "run-1", owner(), submitted_at)
        .await
        .expect("ambiguous submission");
    for attempt in 1..=MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
        let result = advance_manual(
            &client,
            store.as_ref(),
            "run-1",
            owner(),
            submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS * u64::from(attempt),
        )
        .await;
        if attempt < MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
            result.expect("non-final observation");
        } else {
            result.expect_err("the retry budget must park the leg");
        }
    }

    let parked = store.state();
    assert_eq!(parked.step, IcpswapStep::OperatorRequired);
    assert_eq!(parked.deposit.submission_retry_count, MAX_DEPOSIT_SUBMISSION_RETRIES);
}

#[tokio::test]
async fn ambiguous_deposit_that_appears_during_retry_advances_without_replaying() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());

    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(0, 0), unused(100_000, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_deposit().times(1).return_once(|pool, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool,
            method: "deposit",
            message: "response lost".to_string(),
        })
    });
    client
        .expect_ledger_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));

    let submitted_at = 2_000;
    advance_manual(&client, store.as_ref(), "run-1", owner(), submitted_at)
        .await
        .expect("ambiguous submission should schedule observation");
    let confirmed = advance_manual(
        &client,
        store.as_ref(),
        "run-1",
        owner(),
        submitted_at + DEPOSIT_OBSERVATION_RETRY_NANOS,
    )
    .await
    .expect("delayed deposit credit should reconcile");
    assert_eq!(confirmed.step, IcpswapStep::Trade);
    assert_eq!(confirmed.deposit.observation_attempts, 0);
    assert_eq!(confirmed.next_attempt_at_nanos, None);
}

#[tokio::test]
async fn confirmed_deposit_advances_without_an_extra_balance_query() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1)
        .return_once(|_, _| Ok(unused(0, 0)));
    client
        .expect_deposit()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone() - args.fee.clone()));
    client
        .expect_ledger_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));

    let confirmed = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("confirmed deposit");
    assert_eq!(confirmed.step, IcpswapStep::Trade);
}

#[tokio::test]
async fn unexpected_deposit_credit_recovers_instead_of_submitting_an_oversized_swap() {
    let store = Arc::new(Store::new());
    set_deposit_step(store.as_ref());

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1)
        .return_once(|_, _| Ok(unused(0, 0)));
    client
        .expect_deposit()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(99_990u64)));
    client
        .expect_ledger_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_010u64)));
    client.expect_swap().times(0);

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("definite short deposit");
    assert_eq!(result.step, IcpswapStep::Recover);
    assert!(result.last_error.unwrap().contains("recovering the deposited input"));
}

#[tokio::test]
async fn pre_submission_swap_error_does_not_consume_retry_or_widen_slippage() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Trade;

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1)
        .return_once(|_, _| Ok(unused(100_000, 0)));
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::Encode {
            method: "swap",
            message: "invalid arguments".to_string(),
        })
    });
    client.expect_requote().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("encoding failure");
    let state = store.state();
    assert_eq!(state.step, IcpswapStep::Trade);
    assert_eq!(state.trade.retry_evaluation_count, 0);
    assert_eq!(state.trade.slippage_retry_count, 0);
    assert_eq!(state.trade.pending_since_nanos, None);
}

#[tokio::test]
async fn decoded_swap_success_advances_without_a_reconciliation_query() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Trade;

    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1)
        .return_once(|_, _| Ok(unused(100_000, 0)));
    client
        .expect_swap()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(119_000u64)));
    client.expect_requote().times(0);

    let state = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect("decoded swap success");
    assert_eq!(state.step, IcpswapStep::Withdraw);
    assert_eq!(state.trade.gross_output_amount, Some(Nat::from(119_000u64)));
}

#[tokio::test]
async fn confirmed_slippage_requotes_and_retries_without_crossing_original_floor() {
    let store = Arc::new(Store::new());
    {
        store.0.lock().unwrap().step = IcpswapStep::Trade;
    }
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(100_000, 0), unused(100_000, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut swap_sequence = Sequence::new();
    client
        .expect_swap()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, args| {
            assert_eq!(args.amount_out_minimum, "118500");
            Err(IcpswapClientError::Protocol {
                method: "swap",
                error: IcpswapError::InternalError("slippage check failed".to_string()),
            })
        });
    client
        .expect_swap()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, args| {
            assert_eq!(args.amount_out_minimum, "115050");
            Ok(Nat::from(116_000u64))
        });
    client
        .expect_requote()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(118_000u64)));

    let error = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("slippage");
    assert!(error.contains("slippage"));
    let after_failure = store.state();
    let trade = &after_failure.trade;
    assert_eq!(trade.retry_evaluation_count, 1);
    assert_eq!(trade.slippage_retry_count, 1);
    assert_eq!(trade.next_retry_at_nanos, Some(2_000_001_000));

    let waiting = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000_000_000)
        .await
        .expect("backoff wait");
    assert_eq!(waiting.step, IcpswapStep::Trade);

    let retried = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000_001_000)
        .await
        .expect("retry");
    assert_eq!(retried.step, IcpswapStep::Withdraw);
    assert!(retried.trade.current_amount_out_minimum.value >= retried.plan.amount_out_minimum.value);
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
        .expect_unused_balance()
        .times(1)
        .in_sequence(&mut unused_sequence)
        .return_once(|_, _| Ok(unused(100_000, 0)));
    client
        .expect_unused_balance()
        .times(1)
        .in_sequence(&mut unused_sequence)
        .return_once(|pool, _| {
            Err(IcpswapClientError::Transport {
                canister: pool,
                method: "getUserUnusedBalance",
                message: "temporarily unavailable".to_string(),
            })
        });
    client
        .expect_unused_balance()
        .times(1)
        .in_sequence(&mut unused_sequence)
        .return_once(|_, _| Ok(unused(100_000, 0)));
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::Protocol {
            method: "swap",
            error: IcpswapError::InternalError("slippage check failed".to_string()),
        })
    });
    client.expect_requote().times(0);

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
    assert_eq!(resumed.trade.retry_evaluation_count, 1);
    assert_eq!(resumed.trade.slippage_retry_count, 1);
}

#[tokio::test]
async fn ambiguous_swap_is_reconciled_then_requoted_without_widening_slippage() {
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
        unused(100_000, 0),
        unused(100_000, 0),
    ] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    let mut swap_sequence = Sequence::new();
    client
        .expect_swap()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, _| {
            Err(IcpswapClientError::SubmissionUnknown {
                pool: p(9),
                method: "swap",
                message: "timeout".to_string(),
            })
        });
    client
        .expect_swap()
        .times(1)
        .in_sequence(&mut swap_sequence)
        .return_once(|_, args| {
            assert_eq!(args.amount_out_minimum, "116525");
            Ok(Nat::from(117_000u64))
        });
    client
        .expect_requote()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(118_000u64)));

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("ambiguous");
    assert_eq!(store.state().step, IcpswapStep::TradePending);

    let waiting = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("read-only reconciliation");
    assert_eq!(waiting.step, IcpswapStep::TradePending);

    let timeout_at = 1_000 + PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS;
    let scheduled = advance_manual(&client, store.as_ref(), "run-1", owner(), timeout_at)
        .await
        .expect("schedule retry");
    assert_eq!(scheduled.step, IcpswapStep::Trade);
    assert_eq!(scheduled.trade.retry_evaluation_count, 1);
    assert_eq!(scheduled.trade.slippage_retry_count, 0);

    let retried = advance_manual(&client, store.as_ref(), "run-1", owner(), timeout_at + 2_000_000_000)
        .await
        .expect("same-slippage retry");
    assert_eq!(retried.step, IcpswapStep::Withdraw);
}

#[tokio::test]
async fn ambiguous_swap_with_enough_input_for_two_trades_requires_operator() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Trade;
    let mut client = MockIcpswapManualClient::new();
    for value in [unused(200_000, 0), unused(200_000, 0), unused(200_000, 0)] {
        client
            .expect_unused_balance()
            .times(1)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool: p(9),
            method: "swap",
            message: "timeout".to_string(),
        })
    });
    client.expect_requote().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("ambiguous");
    let timeout_at = 1_000 + PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS;
    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), timeout_at)
        .await
        .expect("operator transition");
    assert_eq!(result.step, IcpswapStep::OperatorRequired);
    assert!(result.last_error.unwrap().contains("multiple swaps"));
}

#[tokio::test]
async fn operator_reconciliation_recovers_input_when_deposit_was_one_fee_short() {
    let store = Arc::new(Store::new());
    {
        let mut state = store.0.lock().unwrap();
        state.step = IcpswapStep::OperatorRequired;
        state.operator_pending_step = Some(IcpswapStep::TradePending);
        state.trade.input_pool_balance_before = Some(Nat::from(99_990u64));
        state.trade.output_pool_balance_before = Some(Nat::from(0u8));
        state.last_error = Some("legacy swap trap classified as ambiguous".to_string());
    }
    let mut client = MockIcpswapManualClient::new();
    client
        .expect_unused_balance()
        .times(1)
        .return_once(|_, _| Ok(unused(99_990, 0)));

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect("read-only recovery decision");

    assert_eq!(result.step, IcpswapStep::Recover);
    assert_eq!(result.operator_pending_step, None);
    assert!(result.last_error.unwrap().contains("below the planned"));
}

#[tokio::test]
async fn partial_trade_balance_deltas_require_operator_after_timeout() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Trade;
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(50_000, 50_000), unused(50_000, 50_000)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool: p(9),
            method: "swap",
            message: "timeout".to_string(),
        })
    });

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("ambiguous");
    assert_eq!(store.state().step, IcpswapStep::TradePending);

    let timeout_at = 1_000 + PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS;
    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), timeout_at)
        .await
        .expect("operator transition");
    assert_eq!(result.step, IcpswapStep::OperatorRequired);
    assert!(result.last_error.unwrap().contains("inconsistent"));
}

#[tokio::test]
async fn ambiguous_swap_that_settles_is_never_resubmitted() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().step = IcpswapStep::Trade;
    let mut client = MockIcpswapManualClient::new();
    let mut unused_sequence = Sequence::new();
    for value in [unused(100_000, 0), unused(100_000, 0), unused(0, 119_000)] {
        client
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::SubmissionUnknown {
            pool: p(9),
            method: "swap",
            message: "timeout".to_string(),
        })
    });
    client.expect_requote().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("ambiguous");
    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("settlement reconciliation");
    assert_eq!(result.step, IcpswapStep::Withdraw);
    assert_eq!(result.trade.gross_output_amount, Some(Nat::from(119_000u64)));
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
            .expect_unused_balance()
            .times(1)
            .in_sequence(&mut unused_sequence)
            .return_once(move |_, _| Ok(value));
    }
    client.expect_swap().times(1).return_once(|_, _| {
        Err(IcpswapClientError::Protocol {
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
        .expect_withdraw()
        .times(1)
        .return_once(|_, args| Ok(args.amount.clone()));
    client.expect_requote().times(0);

    advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("protocol failure");
    assert_eq!(store.state().step, IcpswapStep::Recover);

    let recovered = advance_manual(&client, store.as_ref(), "run-1", owner(), 2_000)
        .await
        .expect("recovery");
    assert_eq!(recovered.step, IcpswapStep::Forward);
    assert_eq!(recovered.recovery.wallet_credited_amount, Some(Nat::from(99_990u64)));
}

#[tokio::test]
async fn third_retry_quote_below_hard_floor_moves_to_recovery_without_submission() {
    let store = Arc::new(Store::new());
    {
        let mut state = store.0.lock().unwrap();
        state.step = IcpswapStep::Trade;
        state.trade.retry_evaluation_count = 3;
    }
    let mut client = MockIcpswapManualClient::new();
    client
        .expect_requote()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(113_999u64)));
    client.expect_unused_balance().times(0);
    client.expect_swap().times(0);

    let result = advance_manual(&client, store.as_ref(), "run-1", owner(), 10_000)
        .await
        .expect("quote evaluation");
    assert_eq!(result.step, IcpswapStep::Recover);
    assert_eq!(result.trade.slippage_retry_count, 0);
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
fn incompatible_state_versions_are_rejected_instead_of_silently_migrated() {
    let state = prepared_pool_state();
    let mut json = serde_json::to_value(state).unwrap();
    json.as_object_mut().unwrap().remove("schema_version");

    assert!(serde_json::from_value::<IcpswapExecutionState>(json).is_err());
}

#[tokio::test]
async fn unsupported_state_version_is_rejected_before_any_external_call() {
    let store = Arc::new(Store::new());
    store.0.lock().unwrap().schema_version = ICPSWAP_STATE_VERSION + 1;
    let client = MockIcpswapManualClient::new();

    let error = advance_manual(&client, store.as_ref(), "run-1", owner(), 1_000)
        .await
        .expect_err("version mismatch");

    assert!(error.contains("unsupported ICPSwap state version"));
}

#[tokio::test]
async fn non_default_owner_is_rejected_before_any_external_call() {
    let store = Arc::new(Store::new());
    let client = MockIcpswapManualClient::new();
    let configured_owner = Account {
        owner: owner().owner,
        subaccount: Some([7; 32]),
    };

    let error = advance_manual(&client, store.as_ref(), "run-1", configured_owner, 1_000)
        .await
        .expect_err("non-default owner");

    assert!(error.contains("differs from the supplied client owner"));
}

#[test]
fn flattened_manual_state_uses_unique_step_prefixed_wire_fields() {
    let mut state = prepared_pool_state();
    state.transfer.block_index = Some(Nat::from(5u8));
    state.deposit.input_pool_balance_before = Some(Nat::from(11u8));
    state.deposit.input_ledger_balance_before = Some(Nat::from(12u8));
    state.deposit.ready_to_submit = true;
    state.deposit.submission_retry_count = 2;
    state.trade.input_pool_balance_before = Some(Nat::from(22u8));
    state.withdraw.pool_balance_before = Some(Nat::from(33u8));
    state.recovery.pool_balance_before = Some(Nat::from(44u8));

    let json = serde_json::to_value(&state).unwrap();
    let state_json = json.as_object().unwrap();
    assert_eq!(
        state_json["transfer_block_index"],
        serde_json::to_value(Nat::from(5u8)).unwrap()
    );
    assert_eq!(
        state_json["deposit_input_pool_balance_before"],
        serde_json::to_value(Nat::from(11u8)).unwrap()
    );
    assert_eq!(
        state_json["deposit_input_ledger_balance_before"],
        serde_json::to_value(Nat::from(12u8)).unwrap()
    );
    assert_eq!(state_json["deposit_ready_to_submit"], true);
    assert_eq!(state_json["deposit_observation_attempts"], 0);
    assert_eq!(state_json["deposit_submission_retry_count"], 2);
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

#[test]
fn persisted_gross_output_accepts_the_previous_wire_field_name() {
    let mut state = prepared_pool_state();
    state.trade.gross_output_amount = Some(Nat::from(119_000u64));

    let mut json = serde_json::to_value(&state).unwrap();
    let fields = json.as_object_mut().unwrap();
    let gross = fields.remove("trade_gross_output_amount").unwrap();
    fields.insert("trade_swap_returned_amount".to_string(), gross);

    let decoded: IcpswapExecutionState = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.trade.gross_output_amount, Some(Nat::from(119_000u64)));
}
