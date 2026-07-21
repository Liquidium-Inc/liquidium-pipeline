use std::sync::Mutex;

use async_trait::async_trait;
use candid::{Int, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{
    client::MockIcpswapReconciliationClient, execution::IcpswapExecutionStateStore, reconciliation::reconcile, types::*,
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
fn state() -> IcpswapExecutionState {
    let input = token(p(1), "IN", 10);
    let output = token(p(2), "OUT", 5);
    let plan = IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan");
    let mut state = IcpswapExecutionState::planned(plan);
    state.phase = IcpswapExecutionPhase::SubmissionUnknown;
    state.pool_transaction_start = Some(Nat::from(42u64));
    state.submitted_at = Some(100);
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

fn swap_transaction(id: u64, status: IcpswapOneStepSwapStatus) -> (Nat, IcpswapTransaction) {
    let completed = status == IcpswapOneStepSwapStatus::Completed;
    let failed = status == IcpswapOneStepSwapStatus::Failed;
    let info = IcpswapOneStepSwapInfo {
        deposit: IcpswapDepositInfo {
            transfer: transfer(p(1), p(4), p(9), 100_000, 10, 700),
            status: IcpswapDepositStatus::Completed,
            err: None,
        },
        withdraw: IcpswapWithdrawInfo {
            transfer: transfer(p(2), p(9), p(4), 119_500, 5, 900),
            status: if completed {
                IcpswapWithdrawStatus::Completed
            } else if failed {
                IcpswapWithdrawStatus::Failed
            } else {
                IcpswapWithdrawStatus::Created
            },
            err: failed.then(|| "swap failed".into()),
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
            amount_out: Nat::from(119_500u64),
            amount_in_fee: Nat::from(10u64),
            amount_out_fee: Nat::from(5u64),
            status: if failed {
                IcpswapSwapStatus::Failed
            } else if completed {
                IcpswapSwapStatus::Completed
            } else {
                IcpswapSwapStatus::Created
            },
            err: failed.then(|| "swap failed".into()),
        },
        status,
        err: failed.then(|| "swap failed".into()),
    };
    let id = Nat::from(id);
    (
        id.clone(),
        IcpswapTransaction {
            id,
            timestamp: Int::from(1),
            owner: p(4),
            canister_id: p(9),
            action: IcpswapTransactionAction::OneStepSwap(info),
        },
    )
}

fn refund_transaction(id: u64, related: u64) -> (Nat, IcpswapTransaction) {
    let id = Nat::from(id);
    (
        id.clone(),
        IcpswapTransaction {
            id,
            timestamp: Int::from(2),
            owner: p(4),
            canister_id: p(9),
            action: IcpswapTransactionAction::Refund(IcpswapRefundInfo {
                related_index: Nat::from(related),
                transfer: transfer(p(1), p(9), p(4), 100_000, 10, 901),
                status: IcpswapRefundStatus::Completed,
                err: None,
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
async fn completed_pool_transaction_confirms_exact_output_and_block_index() {
    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(42, IcpswapOneStepSwapStatus::Completed)]));
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 1_000, 500)
        .await
        .expect("reconciled");

    assert_eq!(result.phase, IcpswapExecutionPhase::Completed);
    assert_eq!(result.pool_transaction_id, Some(Nat::from(42u64)));
    assert_eq!(result.settlement_ledger_block_index, Some(Nat::from(900u64)));
    assert_eq!(result.gross_swap_output.unwrap().value, Nat::from(119_500u64));
}

#[tokio::test]
async fn failed_swap_waits_for_and_then_confirms_linked_refund() {
    let mut client = MockIcpswapReconciliationClient::new();
    client.expect_transactions_by_owner().times(1).return_once(|_, _| {
        Ok(vec![
            swap_transaction(42, IcpswapOneStepSwapStatus::Failed),
            refund_transaction(43, 42),
        ])
    });
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 1_000, 500)
        .await
        .expect("reconciled");

    assert_eq!(result.phase, IcpswapExecutionPhase::Refunded);
    assert_eq!(result.refund_transaction_id, Some(Nat::from(43u64)));
    assert_eq!(result.refund_ledger_block_index, Some(Nat::from(901u64)));
}

#[tokio::test]
async fn cursor_excludes_old_identical_transaction() {
    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(41, IcpswapOneStepSwapStatus::Completed)]));
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 1_000, 500)
        .await
        .expect("pending");

    assert_eq!(result.phase, IcpswapExecutionPhase::SubmissionUnknown);
    assert!(result.pool_transaction_id.is_none());
}

#[tokio::test]
async fn multiple_matching_new_transactions_are_rejected_as_ambiguous() {
    let mut client = MockIcpswapReconciliationClient::new();
    client.expect_transactions_by_owner().times(1).return_once(|_, _| {
        Ok(vec![
            swap_transaction(42, IcpswapOneStepSwapStatus::Completed),
            swap_transaction(43, IcpswapOneStepSwapStatus::Completed),
        ])
    });
    let store = Store(Mutex::new(state()));

    let error = reconcile(&client, &store, "liq", account(p(4)), 1_000, 500)
        .await
        .unwrap_err();
    assert_eq!(error, IcpswapReconciliationError::AmbiguousTransaction);
}

#[tokio::test]
async fn failed_swap_waits_before_inspecting_unused_balance() {
    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(42, IcpswapOneStepSwapStatus::Failed)]));
    client.expect_unused_balance().times(0);
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 599, 500)
        .await
        .expect("refund pending");

    assert_eq!(result.phase, IcpswapExecutionPhase::RefundPending);
    assert!(result.recovery_amount.is_none());
}

#[tokio::test]
async fn failed_swap_caps_recovery_at_this_liquidation_input() {
    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(42, IcpswapOneStepSwapStatus::Failed)]));
    client
        .expect_unused_balance()
        .withf(|pool, owner| *pool == p(9) && *owner == p(4))
        .times(1)
        .return_once(|_, _| {
            Ok(IcpswapUnusedBalance {
                balance0: Nat::from(150_000u64),
                balance1: Nat::from(7u64),
            })
        });
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 600, 500)
        .await
        .expect("funds discovered");

    assert_eq!(result.phase, IcpswapExecutionPhase::FundsInPool);
    assert_eq!(result.recovery_amount.unwrap().value, Nat::from(100_000u64));
}

#[tokio::test]
async fn recovery_uses_balance_matching_input_side_of_pool() {
    let mut execution_state = state();
    std::mem::swap(&mut execution_state.plan.token0, &mut execution_state.plan.token1);

    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(42, IcpswapOneStepSwapStatus::Failed)]));
    client.expect_unused_balance().times(1).return_once(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(7u64),
            balance1: Nat::from(75_000u64),
        })
    });
    let store = Store(Mutex::new(execution_state));

    let result = reconcile(&client, &store, "liq", account(p(4)), 600, 500)
        .await
        .expect("funds discovered");

    assert_eq!(result.phase, IcpswapExecutionPhase::FundsInPool);
    assert_eq!(result.recovery_amount.unwrap().value, Nat::from(75_000u64));
}

#[tokio::test]
async fn balance_too_small_to_pay_input_fee_remains_refund_pending() {
    let mut client = MockIcpswapReconciliationClient::new();
    client
        .expect_transactions_by_owner()
        .times(1)
        .return_once(|_, _| Ok(vec![swap_transaction(42, IcpswapOneStepSwapStatus::Failed)]));
    client.expect_unused_balance().times(1).return_once(|_, _| {
        Ok(IcpswapUnusedBalance {
            balance0: Nat::from(10u64),
            balance1: Nat::from(1_000u64),
        })
    });
    let store = Store(Mutex::new(state()));

    let result = reconcile(&client, &store, "liq", account(p(4)), 600, 500)
        .await
        .expect("refund pending");

    assert_eq!(result.phase, IcpswapExecutionPhase::RefundPending);
    assert!(result.recovery_amount.is_none());
}
