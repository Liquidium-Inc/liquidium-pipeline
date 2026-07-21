use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Int, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{
    client::MockIcpswapRecoveryClient,
    execution::IcpswapExecutionStateStore,
    recovery::recover,
    types::{
        IcpswapDepositInfo, IcpswapDepositStatus, IcpswapExecutionPhase, IcpswapExecutionPlan, IcpswapExecutionState,
        IcpswapOneStepSwapInfo, IcpswapOneStepSwapStatus, IcpswapPoolToken, IcpswapRecoveryClientError,
        IcpswapRefundInfo, IcpswapRefundStatus, IcpswapSwapInfo, IcpswapSwapStatus, IcpswapTransaction,
        IcpswapTransactionAction, IcpswapTransfer, IcpswapUnusedBalance, IcpswapWithdrawInfo, IcpswapWithdrawStatus,
    },
};

fn p(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn account(owner: Principal) -> Account {
    Account {
        owner,
        subaccount: None,
    }
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.into(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn state(phase: IcpswapExecutionPhase) -> IcpswapExecutionState {
    let input = token(p(1), "IN", 10);
    let output = token(p(2), "OUT", 5);
    let plan = IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan");
    let mut state = IcpswapExecutionState::planned(plan);
    state.phase = phase;
    state.pool_transaction_start = Some(Nat::from(42u64));
    state.pool_transaction_id = Some(Nat::from(42u64));
    state.submitted_at = Some(100);
    state.recovery_amount = Some(ChainTokenAmount::from_raw(input, Nat::from(100_000u64)));
    if phase == IcpswapExecutionPhase::RecoveryWithdrawSubmitted {
        state.recovery_transaction_start = Some(Nat::from(43u64));
        state.recovery_submitted_at = Some(600);
        state.recovery_attempted = true;
    }
    state
}

fn transfer(token: Principal, from: Principal, to: Principal, amount: u64, fee: u64, index: u64) -> IcpswapTransfer {
    IcpswapTransfer {
        token,
        standard: "ICRC2".into(),
        from: account(from),
        to: account(to),
        amount: Nat::from(amount),
        fee: Nat::from(fee),
        memo: None,
        index: Nat::from(index),
    }
}

fn failed_swap(id: u64) -> (Nat, IcpswapTransaction) {
    let id = Nat::from(id);
    (
        id.clone(),
        IcpswapTransaction {
            id,
            timestamp: Int::from(1),
            owner: p(4),
            canister_id: p(9),
            action: IcpswapTransactionAction::OneStepSwap(IcpswapOneStepSwapInfo {
                deposit: IcpswapDepositInfo {
                    transfer: transfer(p(1), p(4), p(9), 100_000, 10, 700),
                    status: IcpswapDepositStatus::Completed,
                    err: None,
                },
                withdraw: IcpswapWithdrawInfo {
                    transfer: transfer(p(2), p(9), p(4), 0, 5, 0),
                    status: IcpswapWithdrawStatus::Failed,
                    err: Some("swap failed".into()),
                },
                swap: IcpswapSwapInfo {
                    token_in: IcpswapPoolToken {
                        address: p(1),
                        standard: "ICRC2".into(),
                    },
                    token_out: IcpswapPoolToken {
                        address: p(2),
                        standard: "ICRC2".into(),
                    },
                    amount_in: Nat::from(100_000u64),
                    amount_out: Nat::from(0u8),
                    amount_in_fee: Nat::from(10u64),
                    amount_out_fee: Nat::from(5u64),
                    status: IcpswapSwapStatus::Failed,
                    err: Some("swap failed".into()),
                },
                status: IcpswapOneStepSwapStatus::Failed,
                err: Some("swap failed".into()),
            }),
        },
    )
}

fn refund(id: u64, status: IcpswapRefundStatus) -> (Nat, IcpswapTransaction) {
    let id = Nat::from(id);
    (
        id.clone(),
        IcpswapTransaction {
            id,
            timestamp: Int::from(2),
            owner: p(4),
            canister_id: p(9),
            action: IcpswapTransactionAction::Refund(IcpswapRefundInfo {
                related_index: Nat::from(42u64),
                transfer: transfer(p(1), p(9), p(4), 100_000, 10, 901),
                status,
                err: None,
            }),
        },
    )
}

fn withdrawal(id: u64, status: IcpswapWithdrawStatus) -> (Nat, IcpswapTransaction) {
    let id = Nat::from(id);
    (
        id.clone(),
        IcpswapTransaction {
            id,
            timestamp: Int::from(3),
            owner: p(4),
            canister_id: p(9),
            action: IcpswapTransactionAction::Withdraw(IcpswapWithdrawInfo {
                transfer: transfer(p(1), p(9), p(4), 100_000, 10, 902),
                status,
                err: (status == IcpswapWithdrawStatus::Failed).then(|| "ledger rejected transfer".into()),
            }),
        },
    )
}

struct Store(Mutex<IcpswapExecutionState>);

#[async_trait]
impl IcpswapExecutionStateStore for Store {
    async fn load(&self, _: &str) -> Result<Option<IcpswapExecutionState>, String> {
        Ok(Some(self.0.lock().unwrap().clone()))
    }

    async fn persist(&self, _: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        *self.0.lock().unwrap() = state.clone();
        Ok(())
    }
}

#[tokio::test]
async fn persists_at_most_once_intent_before_withdrawal_call() {
    let store = Arc::new(Store(Mutex::new(state(IcpswapExecutionPhase::FundsInPool))));
    let observed_store = store.clone();
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42)]));
    client.expect_unused_balance().times(1).return_once(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(150_000u64),
            balance1: Nat::from(0u8),
        })
    });
    client.expect_withdraw().times(1).return_once(move |pool, args| {
        let persisted = observed_store.0.lock().unwrap().clone();
        assert_eq!(persisted.phase, IcpswapExecutionPhase::RecoveryWithdrawSubmitted);
        assert_eq!(persisted.recovery_transaction_start, Some(Nat::from(43u64)));
        assert_eq!(persisted.recovery_submitted_at, Some(600));
        assert!(persisted.recovery_attempted);
        assert_eq!(pool, p(9));
        assert_eq!(args.token, p(1).to_text());
        assert_eq!(args.amount, Nat::from(100_000u64));
        assert_eq!(args.fee, Nat::from(10u64));
        Ok(args.amount.clone())
    });

    let result = recover(&client, store.as_ref(), "liq", account(p(4)), 600, 300)
        .await
        .expect("submitted");

    assert_eq!(result.phase, IcpswapExecutionPhase::RecoveryWithdrawSubmitted);
}

#[tokio::test]
async fn late_automatic_refund_preempts_manual_withdrawal() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::FundsInPool)));
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42), refund(43, IcpswapRefundStatus::Completed)]));
    client.expect_unused_balance().times(0);
    client.expect_withdraw().times(0);

    let result = recover(&client, &store, "liq", account(p(4)), 600, 300)
        .await
        .expect("refunded");

    assert_eq!(result.phase, IcpswapExecutionPhase::Refunded);
    assert_eq!(result.refund_transaction_id, Some(Nat::from(43u64)));
    assert_eq!(result.refund_ledger_block_index, Some(Nat::from(901u64)));
    assert_eq!(result.returned_gross_amount.unwrap().value, Nat::from(100_000u64));
}

#[tokio::test]
async fn ambiguous_withdrawal_is_never_resubmitted() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::FundsInPool)));
    let mut first_client = MockIcpswapRecoveryClient::new();
    first_client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42)]));
    first_client.expect_unused_balance().times(1).return_once(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(100_000u64),
            balance1: Nat::from(0u8),
        })
    });
    first_client.expect_withdraw().times(1).return_once(|pool, _| {
        Err(IcpswapRecoveryClientError::SubmissionUnknown {
            pool,
            method: "withdraw",
            message: "timeout".into(),
        })
    });

    recover(&first_client, &store, "liq", account(p(4)), 600, 300)
        .await
        .expect_err("ambiguous");
    assert_eq!(
        store.0.lock().unwrap().phase,
        IcpswapExecutionPhase::RecoveryWithdrawSubmitted
    );

    let mut retry_client = MockIcpswapRecoveryClient::new();
    retry_client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42)]));
    retry_client.expect_unused_balance().times(0);
    retry_client.expect_withdraw().times(0);

    let result = recover(&retry_client, &store, "liq", account(p(4)), 700, 300)
        .await
        .expect("reconciliation only");
    assert_eq!(result.phase, IcpswapExecutionPhase::RecoveryWithdrawSubmitted);
}

#[tokio::test]
async fn completed_exact_withdrawal_confirms_refund_and_ledger_block() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::RecoveryWithdrawSubmitted)));
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42), withdrawal(43, IcpswapWithdrawStatus::Completed)]));
    client.expect_unused_balance().times(0);
    client.expect_withdraw().times(0);

    let result = recover(&client, &store, "liq", account(p(4)), 700, 300)
        .await
        .expect("refunded");

    assert_eq!(result.phase, IcpswapExecutionPhase::Refunded);
    assert_eq!(result.recovery_transaction_id, Some(Nat::from(43u64)));
    assert_eq!(result.recovery_ledger_block_index, Some(Nat::from(902u64)));
    assert_eq!(result.returned_gross_amount.unwrap().value, Nat::from(100_000u64));
}

#[tokio::test]
async fn failed_withdrawal_becomes_terminal() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::RecoveryWithdrawSubmitted)));
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42), withdrawal(43, IcpswapWithdrawStatus::Failed)]));

    let result = recover(&client, &store, "liq", account(p(4)), 700, 300)
        .await
        .expect("terminal state");

    assert_eq!(result.phase, IcpswapExecutionPhase::FailedTerminal);
    assert!(result.last_error.unwrap().contains("ledger rejected transfer"));
}

#[tokio::test]
async fn multiple_matching_withdrawals_become_terminal() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::RecoveryWithdrawSubmitted)));
    let mut client = MockIcpswapRecoveryClient::new();
    client.expect_transactions_by_owner().times(1).return_once(|_, _| {
        Ok(vec![
            failed_swap(42),
            withdrawal(43, IcpswapWithdrawStatus::Completed),
            withdrawal(44, IcpswapWithdrawStatus::Completed),
        ])
    });
    client.expect_withdraw().times(0);

    let result = recover(&client, &store, "liq", account(p(4)), 700, 300)
        .await
        .expect("terminal state");

    assert_eq!(result.phase, IcpswapExecutionPhase::FailedTerminal);
    assert!(result.last_error.unwrap().contains("multiple transactions"));
}

#[tokio::test]
async fn missing_withdrawal_after_timeout_becomes_terminal_without_resubmission() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::RecoveryWithdrawSubmitted)));
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42)]));
    client.expect_withdraw().times(0);

    let result = recover(&client, &store, "liq", account(p(4)), 900, 300)
        .await
        .expect("terminal state");

    assert_eq!(result.phase, IcpswapExecutionPhase::FailedTerminal);
    assert!(result.last_error.unwrap().contains("not observed before timeout"));
}

#[tokio::test]
async fn definite_protocol_error_returns_to_funds_in_pool() {
    let store = Store(Mutex::new(state(IcpswapExecutionPhase::FundsInPool)));
    let mut client = MockIcpswapRecoveryClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![failed_swap(42)]));
    client.expect_unused_balance().times(1).return_once(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(100_000u64),
            balance1: Nat::from(0u8),
        })
    });
    client.expect_withdraw().times(1).return_once(|_, _| {
        Err(IcpswapRecoveryClientError::Protocol {
            method: "withdraw",
            error: super::types::IcpswapError::InsufficientFunds,
        })
    });

    recover(&client, &store, "liq", account(p(4)), 600, 300)
        .await
        .expect_err("definite protocol error");

    let persisted = store.0.lock().unwrap().clone();
    assert_eq!(persisted.phase, IcpswapExecutionPhase::FundsInPool);
    assert!(persisted.recovery_transaction_start.is_none());
    assert!(persisted.recovery_submitted_at.is_none());
}
