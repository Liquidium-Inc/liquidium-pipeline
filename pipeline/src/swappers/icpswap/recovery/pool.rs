//! At-most-once recovery of input tokens left in an ICPSwap pool.
//!
//! ICPSwap's `withdraw` update is not idempotent. We therefore persist the
//! transaction cursor and `RecoveryWithdrawSubmitted` before issuing it. Once
//! that intent exists, retries only inspect pool transactions; they never call
//! `withdraw` again, even when the original call result was ambiguous.

use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use super::{
    client::IcpswapRecoveryClient,
    execution::IcpswapExecutionStateStore,
    types::{
        IcpswapExecutionPhase, IcpswapExecutionState, IcpswapRecoveryClientError, IcpswapRecoveryError,
        IcpswapRefundInfo, IcpswapRefundStatus, IcpswapTransaction, IcpswapTransactionAction, IcpswapWithdrawArgs,
        IcpswapWithdrawInfo, IcpswapWithdrawStatus,
    },
};

/// Advances one recovery operation by at most one token-moving side effect.
///
/// `FundsInPool` rechecks late refunds and the current unused balance, writes
/// the at-most-once intent, and submits `withdraw`. `RecoveryWithdrawSubmitted`
/// only reconciles the resulting transaction or times out to `FailedTerminal`.
pub async fn recover(
    client: &dyn IcpswapRecoveryClient,
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    owner: Account,
    now_nanos: u64,
    recovery_timeout_nanos: u64,
) -> Result<IcpswapExecutionState, IcpswapRecoveryError> {
    let mut state = load_state(store, liquidation_id).await?;
    match state.phase {
        IcpswapExecutionPhase::FundsInPool => {
            prepare_and_submit(client, store, liquidation_id, &mut state, owner, now_nanos).await?
        }
        IcpswapExecutionPhase::RecoveryWithdrawSubmitted => {
            reconcile_withdrawal(client, &mut state, owner, now_nanos, recovery_timeout_nanos).await?;
            persist(store, liquidation_id, &state).await?;
        }
        phase => return Err(IcpswapRecoveryError::InvalidPhase(phase)),
    }
    Ok(state)
}

async fn prepare_and_submit(
    client: &dyn IcpswapRecoveryClient,
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    state: &mut IcpswapExecutionState,
    owner: Account,
    now_nanos: u64,
) -> Result<(), IcpswapRecoveryError> {
    let transactions = query_transactions(client, state, owner).await?;
    if apply_late_refund(state, &transactions, owner)? {
        persist(store, liquidation_id, state).await?;
        return Ok(());
    }

    let requested = state
        .recovery_amount
        .as_ref()
        .ok_or(IcpswapRecoveryError::MissingRecoveryAmount)?;
    let unused = client
        .unused_balance(state.plan.pool, owner.owner)
        .await
        .map_err(IcpswapRecoveryError::Query)?;
    let unused_input = if state.plan.token_in == state.plan.token0 {
        unused.balance0
    } else {
        unused.balance1
    };
    let amount = std::cmp::min(
        std::cmp::min(requested.value.clone(), unused_input),
        state.plan.amount_in.value.clone(),
    );

    if amount <= state.plan.input_ledger_fee.value {
        state.last_error = Some(format!(
            "ICPSwap unused input balance {amount} cannot cover ledger fee {}",
            state.plan.input_ledger_fee.value
        ));
        persist(store, liquidation_id, state).await?;
        return Ok(());
    }

    state.recovery_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        amount.clone(),
    ));
    state.recovery_transaction_start = Some(next_transaction_id(&transactions));
    state.recovery_transaction_id = None;
    state.recovery_ledger_block_index = None;
    state.recovery_submitted_at = Some(now_nanos);
    state.recovery_attempted = true;
    state.phase = IcpswapExecutionPhase::RecoveryWithdrawSubmitted;
    state.last_error = None;

    // This is the at-most-once boundary. A crash after this checkpoint may
    // leave funds for operator recovery, but can never cause a duplicate call.
    persist(store, liquidation_id, state).await?;

    let args = IcpswapWithdrawArgs {
        token: state.plan.token_in.to_text(),
        fee: state.plan.input_ledger_fee.value.clone(),
        amount: amount.clone(),
    };
    match client.withdraw(state.plan.pool, &args).await {
        Ok(returned_amount) => {
            if returned_amount != amount {
                state.last_error = Some(format!(
                    "ICPSwap withdraw returned amount {returned_amount}, expected {amount}; reconciling by transaction"
                ));
            }
            persist(store, liquidation_id, state).await?;
            Ok(())
        }
        Err(error @ IcpswapRecoveryClientError::SubmissionUnknown { .. }) => {
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, state).await?;
            Err(error.into())
        }
        Err(error @ (IcpswapRecoveryClientError::Encode { .. } | IcpswapRecoveryClientError::Protocol { .. })) => {
            // Encoding and decoded protocol errors prove that no withdrawal
            // was accepted, so a later invocation may safely try again.
            state.phase = IcpswapExecutionPhase::FundsInPool;
            state.recovery_transaction_start = None;
            state.recovery_submitted_at = None;
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, state).await?;
            Err(error.into())
        }
    }
}

async fn reconcile_withdrawal(
    client: &dyn IcpswapRecoveryClient,
    state: &mut IcpswapExecutionState,
    owner: Account,
    now_nanos: u64,
    recovery_timeout_nanos: u64,
) -> Result<(), IcpswapRecoveryError> {
    let transactions = query_transactions(client, state, owner).await?;
    if apply_late_refund(state, &transactions, owner)? {
        return Ok(());
    }

    let start = state
        .recovery_transaction_start
        .clone()
        .ok_or(IcpswapRecoveryError::MissingRecoveryTransactionCursor)?;
    let (tx_id, transaction) = match find_withdrawal(&transactions, state, owner, &start) {
        WithdrawalMatch::One(id, transaction) => (id, transaction),
        WithdrawalMatch::Ambiguous => {
            fail_terminal(state, "multiple transactions match the recovery withdrawal intent");
            return Ok(());
        }
        WithdrawalMatch::Missing => {
            if recovery_timed_out(state, now_nanos, recovery_timeout_nanos) {
                fail_terminal(state, "recovery withdrawal transaction was not observed before timeout");
            }
            return Ok(());
        }
    };

    let IcpswapTransactionAction::Withdraw(info) = &transaction.action else {
        fail_terminal(state, "persisted recovery transaction is not a Withdraw action");
        return Ok(());
    };
    if !withdrawal_matches(transaction, info, state, owner) {
        fail_terminal(state, "recovery withdrawal transaction does not match persisted intent");
        return Ok(());
    }

    state.recovery_transaction_id = Some(tx_id.clone());
    match info.status {
        IcpswapWithdrawStatus::Completed => {
            if info.transfer.amount <= info.transfer.fee {
                fail_terminal(state, "completed recovery withdrawal has no transferable credit");
            } else {
                state.returned_gross_amount = Some(ChainTokenAmount::from_raw(
                    state.plan.amount_in.token.clone(),
                    info.transfer.amount.clone(),
                ));
                state.recovery_ledger_block_index = Some(info.transfer.index.clone());
                state.phase = IcpswapExecutionPhase::Refunded;
                state.last_error = None;
            }
        }
        IcpswapWithdrawStatus::Failed => {
            fail_terminal(
                state,
                &format!(
                    "ICPSwap recovery withdrawal {tx_id} failed: {}",
                    info.err.as_deref().unwrap_or("unknown error")
                ),
            );
        }
        IcpswapWithdrawStatus::Created | IcpswapWithdrawStatus::CreditCompleted => {
            if recovery_timed_out(state, now_nanos, recovery_timeout_nanos) {
                fail_terminal(state, "recovery withdrawal did not complete before timeout");
            }
        }
    }
    Ok(())
}

enum WithdrawalMatch<'a> {
    Missing,
    One(&'a Nat, &'a IcpswapTransaction),
    Ambiguous,
}

fn find_withdrawal<'a>(
    transactions: &'a [(Nat, IcpswapTransaction)],
    state: &IcpswapExecutionState,
    owner: Account,
    start: &Nat,
) -> WithdrawalMatch<'a> {
    if let Some(id) = &state.recovery_transaction_id {
        return transactions
            .iter()
            .find(|(outer_id, tx)| outer_id == id && &tx.id == id)
            .map_or(WithdrawalMatch::Missing, |(id, tx)| WithdrawalMatch::One(id, tx));
    }

    let mut matches = transactions.iter().filter(|(id, transaction)| {
        id >= start
            && id == &transaction.id
            && match &transaction.action {
                IcpswapTransactionAction::Withdraw(info) => withdrawal_matches(transaction, info, state, owner),
                _ => false,
            }
    });
    let Some((id, transaction)) = matches.next() else {
        return WithdrawalMatch::Missing;
    };
    if matches.next().is_some() {
        return WithdrawalMatch::Ambiguous;
    }
    WithdrawalMatch::One(id, transaction)
}

fn withdrawal_matches(
    transaction: &IcpswapTransaction,
    info: &IcpswapWithdrawInfo,
    state: &IcpswapExecutionState,
    owner: Account,
) -> bool {
    let Some(amount) = &state.recovery_amount else {
        return false;
    };
    transaction.owner == owner.owner
        && transaction.canister_id == state.plan.pool
        && info.transfer.token == state.plan.token_in
        && info.transfer.from == pool_account(state.plan.pool)
        && info.transfer.to == owner
        && info.transfer.amount == amount.value
        && info.transfer.fee == state.plan.input_ledger_fee.value
}

fn apply_late_refund(
    state: &mut IcpswapExecutionState,
    transactions: &[(Nat, IcpswapTransaction)],
    owner: Account,
) -> Result<bool, IcpswapRecoveryError> {
    let swap_tx_id = state
        .pool_transaction_id
        .as_ref()
        .ok_or(IcpswapRecoveryError::MissingSwapTransactionId)?;
    let mut refunds = transactions
        .iter()
        .filter_map(|(id, transaction)| match &transaction.action {
            IcpswapTransactionAction::Refund(info) if &info.related_index == swap_tx_id => {
                Some((id, transaction, info))
            }
            _ => None,
        });
    let first = refunds.next();
    if refunds.next().is_some() {
        fail_terminal(state, "multiple automatic refunds reference the failed swap");
        return Ok(true);
    }
    let Some((refund_id, transaction, refund)) = first else {
        return Ok(false);
    };
    if refund_id != &transaction.id || !refund_matches(state, transaction, refund, owner) {
        fail_terminal(state, "late automatic refund does not match the persisted execution");
        return Ok(true);
    }

    state.refund_transaction_id = Some(refund_id.clone());
    match refund.status {
        IcpswapRefundStatus::Completed if refund.transfer.amount > refund.transfer.fee => {
            state.returned_gross_amount = Some(ChainTokenAmount::from_raw(
                state.plan.amount_in.token.clone(),
                refund.transfer.amount.clone(),
            ));
            state.refund_ledger_block_index = Some(refund.transfer.index.clone());
            state.phase = IcpswapExecutionPhase::Refunded;
            state.last_error = None;
        }
        IcpswapRefundStatus::Failed => fail_terminal(
            state,
            &format!(
                "late automatic refund {refund_id} failed: {}",
                refund.err.as_deref().unwrap_or("unknown error")
            ),
        ),
        IcpswapRefundStatus::Created | IcpswapRefundStatus::CreditCompleted => {
            state.phase = IcpswapExecutionPhase::RefundPending;
        }
        IcpswapRefundStatus::Completed => {
            fail_terminal(state, "completed late refund has no transferable credit");
        }
    }
    Ok(true)
}

fn refund_matches(
    state: &IcpswapExecutionState,
    transaction: &IcpswapTransaction,
    refund: &IcpswapRefundInfo,
    owner: Account,
) -> bool {
    transaction.owner == owner.owner
        && transaction.canister_id == state.plan.pool
        && refund.transfer.token == state.plan.token_in
        && refund.transfer.from == pool_account(state.plan.pool)
        && refund.transfer.to == owner
        && refund.transfer.fee == state.plan.input_ledger_fee.value
        && refund.transfer.amount <= state.plan.amount_in.value
}

async fn query_transactions(
    client: &dyn IcpswapRecoveryClient,
    state: &IcpswapExecutionState,
    owner: Account,
) -> Result<Vec<(Nat, IcpswapTransaction)>, IcpswapRecoveryError> {
    client
        .transactions_by_owner(state.plan.pool, owner.owner)
        .await
        .map_err(IcpswapRecoveryError::Query)
}

fn next_transaction_id(transactions: &[(Nat, IcpswapTransaction)]) -> Nat {
    transactions
        .iter()
        .map(|(id, _)| id.clone())
        .max()
        .map(|id| id + Nat::from(1u8))
        .unwrap_or_else(|| Nat::from(0u8))
}

fn recovery_timed_out(state: &IcpswapExecutionState, now_nanos: u64, timeout_nanos: u64) -> bool {
    state
        .recovery_submitted_at
        .is_none_or(|submitted_at| now_nanos.saturating_sub(submitted_at) >= timeout_nanos)
}

fn fail_terminal(state: &mut IcpswapExecutionState, message: &str) {
    state.phase = IcpswapExecutionPhase::FailedTerminal;
    state.last_error = Some(message.to_string());
}

fn pool_account(pool: Principal) -> Account {
    Account {
        owner: pool,
        subaccount: None,
    }
}

async fn load_state(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
) -> Result<IcpswapExecutionState, IcpswapRecoveryError> {
    store
        .load(liquidation_id)
        .await
        .map_err(IcpswapRecoveryError::Persistence)?
        .ok_or(IcpswapRecoveryError::MissingState)
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    state: &IcpswapExecutionState,
) -> Result<(), IcpswapRecoveryError> {
    store
        .persist(liquidation_id, state)
        .await
        .map_err(IcpswapRecoveryError::Persistence)
}
