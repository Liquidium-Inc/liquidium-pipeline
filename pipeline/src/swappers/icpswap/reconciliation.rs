//! Durable ICPSwap settlement reconciliation.
//!
//! `depositFromAndSwap` does not settle synchronously: the pool records a
//! one-step transaction and later sends either the output token or an input
//! refund. Before submission, execution persists `pool_transaction_start`,
//! which is one greater than the largest transaction ID already visible for
//! this owner. Reconciliation therefore cannot accidentally select an older,
//! otherwise identical swap.
//!
//! A new transaction is accepted only when its owner, pool, token pair,
//! deposit amount, and both ledger fees match the persisted execution plan.
//! More than one match is treated as ambiguous instead of guessing. Once a
//! match is found, its transaction ID is persisted and all later retries look
//! up that exact ID.
//!
//! Completion is based on ICPSwap's transaction records, not an aggregate
//! account balance delta:
//!
//! - a completed one-step withdrawal proves output settlement and supplies
//!   the output ledger block index;
//! - a failed one-step swap moves to `RefundPending`;
//! - a completed `Refund` whose `relatedIndex` equals the one-step transaction
//!   proves refund settlement and supplies the input ledger block index.
//!
//! This function is read/reconcile-only. It never submits a swap or a manual
//! withdrawal, so retrying it cannot duplicate either side effect.

use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use super::{
    client::IcpswapReconciliationClient,
    execution::IcpswapExecutionStateStore,
    types::{
        IcpswapExecutionPhase, IcpswapExecutionState, IcpswapOneStepSwapInfo, IcpswapOneStepSwapStatus,
        IcpswapReconciliationError, IcpswapRefundInfo, IcpswapRefundStatus, IcpswapTransaction,
        IcpswapTransactionAction, IcpswapWithdrawStatus,
    },
};

/// Reconciles one persisted ICPSwap execution without submitting a new swap.
///
/// Algorithm:
/// 1. Load the durable execution state and ignore phases owned by another step.
/// 2. Query this owner's pool history and find the exact one-step swap, first
///    using the pre-submission cursor and then its persisted transaction ID.
/// 3. Validate the transaction against the persisted pool, tokens, amount,
///    owner, and fees before accepting completed output or a linked refund.
/// 4. If the swap failed but no refund exists, wait for ICPSwap's automatic
///    refund window. After it expires, inspect the unused input-token balance,
///    cap it at this liquidation's input, and persist `FundsInPool` for the
///    separate recovery-withdrawal step.
/// 5. Persist every newly discovered ID, ledger block, amount, and phase.
///
/// `now_nanos` is supplied by the caller so this function can compare one
/// explicit time snapshot with the persisted `submitted_at`. It prevents a
/// manual recovery decision before ICPSwap has had enough time to create its
/// automatic refund, and makes the timeout behavior deterministic in tests.
pub async fn reconcile(
    // Read-only ICPSwap client used to inspect the selected pool's history.
    client: &dyn IcpswapReconciliationClient,
    // Durable state store backed by the liquidation WAL in production.
    store: &dyn IcpswapExecutionStateStore,
    // WAL key identifying the liquidation being reconciled.
    liquidation_id: &str,
    // Liquidator account that submitted the swap and receives output/refunds.
    owner: Account,
    // Current wall-clock time in nanoseconds.
    now_nanos: u64,
    // Minimum time given to ICPSwap's automatic refund before manual recovery.
    automatic_refund_wait_nanos: u64,
) -> Result<IcpswapExecutionState, IcpswapReconciliationError> {
    let mut state = load_state(store, liquidation_id).await?;
    if !phase_needs_reconciliation(state.phase) {
        return Ok(state);
    }

    let start = transaction_start(&state)?;

    // Fetch only this owner's pool transactions. This may include swaps,
    // refunds, deposits, and other action types, so it is filtered below.
    let transactions = client
        .transactions_by_owner(state.plan.pool, owner.owner)
        .await
        // A failed query leaves the durable phase unchanged and is retryable.
        .map_err(IcpswapReconciliationError::Query)?;

    // Absence is a pending result, not a failure and never permission to
    // resubmit depositFromAndSwap. A later scheduler pass will query again.
    let Some((tx_id, transaction)) = find_swap_transaction(&transactions, &state, owner, &start)? else {
        return Ok(state);
    };

    // `info` is the validated inner OneStepSwap record. It contains ICPSwap's
    // deposit, swap, output withdrawal, overall status, and reported errors.
    let info = validate_swap_transaction(tx_id, transaction, &state, owner)?;
    state.pool_transaction_id = Some(tx_id.clone());
    apply_swap_outcome(
        client,
        &mut state,
        tx_id,
        info,
        &transactions,
        owner,
        now_nanos,
        automatic_refund_wait_nanos,
    )
    .await?;

    persist_state(store, liquidation_id, &state).await?;
    Ok(state)
}

async fn load_state(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
) -> Result<IcpswapExecutionState, IcpswapReconciliationError> {
    // Always begin from durable state. This makes every invocation resumable
    // and prevents an in-memory plan from replacing the selected pool/route.
    store
        .load(liquidation_id)
        .await
        .map_err(IcpswapReconciliationError::Persistence)?
        .ok_or(IcpswapReconciliationError::MissingTransactionCursor)
}

fn phase_needs_reconciliation(phase: IcpswapExecutionPhase) -> bool {
    // All other phases either precede submission, are terminal, or belong to
    // the separate recovery-withdrawal step.
    matches!(
        phase,
        IcpswapExecutionPhase::SubmissionUnknown
            | IcpswapExecutionPhase::AwaitingOutput
            | IcpswapExecutionPhase::RefundPending
    )
}

fn transaction_start(state: &IcpswapExecutionState) -> Result<Nat, IcpswapReconciliationError> {
    // Never fall back to zero for old state: doing so could match an old,
    // identical transaction and falsely settle this liquidation.
    state
        .pool_transaction_start
        .clone()
        .ok_or(IcpswapReconciliationError::MissingTransactionCursor)
}

fn find_swap_transaction<'a>(
    transactions: &'a [(Nat, IcpswapTransaction)],
    state: &IcpswapExecutionState,
    owner: Account,
    start: &Nat,
) -> Result<Option<(&'a Nat, &'a IcpswapTransaction)>, IcpswapReconciliationError> {
    // Once discovered, the persisted transaction ID is the only acceptable
    // match. Both copies of the ID must agree.
    if let Some(id) = &state.pool_transaction_id {
        return Ok(transactions
            .iter()
            .find(|(outer_id, tx)| outer_id == id && &tx.id == id)
            .map(|(outer_id, tx)| (outer_id, tx)));
    }

    // Before discovery, the cursor excludes older identical swaps. Refuse to
    // guess if concurrent activity produced more than one valid candidate.
    let mut matches = transactions
        .iter()
        .filter(|(id, tx)| id >= start && transaction_matches(tx, state, owner));
    let first = matches.next();
    if matches.next().is_some() {
        return Err(IcpswapReconciliationError::AmbiguousTransaction);
    }
    Ok(first.map(|(id, tx)| (id, tx)))
}

fn validate_swap_transaction<'a>(
    tx_id: &Nat,
    transaction: &'a IcpswapTransaction,
    state: &IcpswapExecutionState,
    owner: Account,
) -> Result<&'a IcpswapOneStepSwapInfo, IcpswapReconciliationError> {
    // Revalidate persisted-ID lookups as well as newly discovered candidates.
    if !transaction_matches(transaction, state, owner) {
        return Err(IcpswapReconciliationError::TransactionMismatch(tx_id.clone()));
    }
    let IcpswapTransactionAction::OneStepSwap(info) = &transaction.action else {
        return Err(IcpswapReconciliationError::TransactionMismatch(tx_id.clone()));
    };
    Ok(info)
}

async fn apply_swap_outcome(
    client: &dyn IcpswapReconciliationClient,
    state: &mut IcpswapExecutionState,
    tx_id: &Nat,
    info: &IcpswapOneStepSwapInfo,
    transactions: &[(Nat, IcpswapTransaction)],
    owner: Account,
    now_nanos: u64,
    automatic_refund_wait_nanos: u64,
) -> Result<(), IcpswapReconciliationError> {
    match info.status {
        IcpswapOneStepSwapStatus::Completed => complete_output(state, info, owner),
        IcpswapOneStepSwapStatus::Failed => {
            reconcile_refund(
                client,
                state,
                tx_id,
                transactions,
                owner,
                now_nanos,
                automatic_refund_wait_nanos,
            )
            .await
        }
        _ => {
            mark_awaiting_output(state, info);
            Ok(())
        }
    }
}

fn mark_awaiting_output(state: &mut IcpswapExecutionState, info: &IcpswapOneStepSwapInfo) {
    state.phase = IcpswapExecutionPhase::AwaitingOutput;
    if info.swap.amount_out > Nat::from(0u8) {
        state.gross_swap_output = Some(ChainTokenAmount::from_raw(
            state.plan.gross_quoted_out.token.clone(),
            info.swap.amount_out.clone(),
        ));
    }
}

async fn persist_state(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    state: &IcpswapExecutionState,
) -> Result<(), IcpswapReconciliationError> {
    // A restart resumes from this exact transaction observation and phase.
    store
        .persist(liquidation_id, state)
        .await
        .map_err(IcpswapReconciliationError::Persistence)
}

fn transaction_matches(tx: &IcpswapTransaction, state: &IcpswapExecutionState, owner: Account) -> bool {
    let IcpswapTransactionAction::OneStepSwap(info) = &tx.action else {
        return false;
    };
    tx.owner == owner.owner
        && tx.canister_id == state.plan.pool
        && info.deposit.transfer.token == state.plan.token_in
        && info.deposit.transfer.from == owner
        && info.deposit.transfer.to == pool_account(state.plan.pool)
        && info.deposit.transfer.amount == state.plan.amount_in.value
        && info.deposit.transfer.fee == state.plan.input_ledger_fee.value
        && info.swap.token_in.address == state.plan.token_in
        && info.swap.token_out.address == state.plan.token_out
        && info.swap.amount_in_fee == state.plan.input_ledger_fee.value
        && info.swap.amount_out_fee == state.plan.output_ledger_fee.value
}

fn complete_output(
    state: &mut IcpswapExecutionState,
    info: &IcpswapOneStepSwapInfo,
    owner: Account,
) -> Result<(), IcpswapReconciliationError> {
    // Validate the entire withdrawal record before accepting its ledger block
    // index. In particular, another inflow to the same account cannot satisfy
    // these pool/token/amount/fee checks.
    let transfer = &info.withdraw.transfer;
    if info.withdraw.status != IcpswapWithdrawStatus::Completed
        || transfer.token != state.plan.token_out
        || transfer.from != pool_account(state.plan.pool)
        || transfer.to != owner
        || transfer.amount != info.swap.amount_out
        || transfer.fee != state.plan.output_ledger_fee.value
        || transfer.amount <= transfer.fee
        || transfer.amount < state.plan.amount_out_minimum.value
    {
        return Err(IcpswapReconciliationError::TransactionMismatch(
            state.pool_transaction_id.clone().expect("transaction id was set"),
        ));
    }
    state.gross_swap_output = Some(ChainTokenAmount::from_raw(
        state.plan.gross_quoted_out.token.clone(),
        transfer.amount.clone(),
    ));
    state.settlement_ledger_block_index = Some(transfer.index.clone());
    state.phase = IcpswapExecutionPhase::Completed;
    state.last_error = None;
    Ok(())
}

async fn reconcile_refund(
    client: &dyn IcpswapReconciliationClient,
    state: &mut IcpswapExecutionState,
    swap_tx_id: &Nat,
    transactions: &[(Nat, IcpswapTransaction)],
    owner: Account,
    now_nanos: u64,
    automatic_refund_wait_nanos: u64,
) -> Result<(), IcpswapReconciliationError> {
    state.phase = IcpswapExecutionPhase::RefundPending;
    if let Some((refund_id, refund)) = find_linked_refund(transactions, swap_tx_id)? {
        apply_refund(state, swap_tx_id, refund_id, refund, owner)?;
        return Ok(());
    }

    if automatic_refund_window_elapsed(state, now_nanos, automatic_refund_wait_nanos) {
        discover_recoverable_input(client, state, owner).await?;
    }
    Ok(())
}

fn find_linked_refund<'a>(
    transactions: &'a [(Nat, IcpswapTransaction)],
    swap_tx_id: &Nat,
) -> Result<Option<(&'a Nat, &'a IcpswapRefundInfo)>, IcpswapReconciliationError> {
    // `relatedIndex` is the durable join key from a Refund to its failed swap.
    let mut refunds = transactions.iter().filter_map(|(id, tx)| match &tx.action {
        IcpswapTransactionAction::Refund(info) if &info.related_index == swap_tx_id => Some((id, info)),
        _ => None,
    });
    let first = refunds.next();
    if refunds.next().is_some() {
        return Err(IcpswapReconciliationError::AmbiguousTransaction);
    }
    Ok(first)
}

fn automatic_refund_window_elapsed(
    state: &IcpswapExecutionState,
    now_nanos: u64,
    automatic_refund_wait_nanos: u64,
) -> bool {
    state
        .submitted_at
        .is_some_and(|submitted_at| now_nanos.saturating_sub(submitted_at) >= automatic_refund_wait_nanos)
}

async fn discover_recoverable_input(
    client: &dyn IcpswapReconciliationClient,
    state: &mut IcpswapExecutionState,
    owner: Account,
) -> Result<(), IcpswapReconciliationError> {
    let unused = client
        .unused_balance(state.plan.pool, owner.owner)
        .await
        .map_err(IcpswapReconciliationError::Query)?;
    let unused_input = if state.plan.token_in == state.plan.token0 {
        unused.balance0
    } else {
        unused.balance1
    };

    // The pool balance is aggregate, so never attribute more than this
    // liquidation deposited. Leave any excess for its actual owner.
    let attributable = std::cmp::min(unused_input, state.plan.amount_in.value.clone());
    if attributable > state.plan.input_ledger_fee.value {
        state.recovery_amount = Some(ChainTokenAmount::from_raw(
            state.plan.amount_in.token.clone(),
            attributable,
        ));
        state.phase = IcpswapExecutionPhase::FundsInPool;
    }
    Ok(())
}

fn apply_refund(
    state: &mut IcpswapExecutionState,
    swap_tx_id: &Nat,
    refund_id: &Nat,
    refund: &IcpswapRefundInfo,
    owner: Account,
) -> Result<(), IcpswapReconciliationError> {
    validate_refund(state, refund, owner)?;
    state.refund_transaction_id = Some(refund_id.clone());
    if refund.status == IcpswapRefundStatus::Completed {
        if refund.transfer.amount <= refund.transfer.fee {
            return Err(IcpswapReconciliationError::TransactionMismatch(swap_tx_id.clone()));
        }
        state.refund_ledger_block_index = Some(refund.transfer.index.clone());
        state.phase = IcpswapExecutionPhase::Refunded;
        state.last_error = None;
    }
    Ok(())
}

fn validate_refund(
    state: &IcpswapExecutionState,
    refund: &IcpswapRefundInfo,
    owner: Account,
) -> Result<(), IcpswapReconciliationError> {
    let transfer = &refund.transfer;
    if transfer.token != state.plan.token_in
        || transfer.from != pool_account(state.plan.pool)
        || transfer.to != owner
        || transfer.fee != state.plan.input_ledger_fee.value
        || transfer.amount > state.plan.amount_in.value
    {
        return Err(IcpswapReconciliationError::TransactionMismatch(
            state.pool_transaction_id.clone().expect("transaction id was set"),
        ));
    }
    Ok(())
}

fn pool_account(pool: Principal) -> Account {
    Account {
        owner: pool,
        subaccount: None,
    }
}
