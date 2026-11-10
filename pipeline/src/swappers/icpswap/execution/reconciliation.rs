//! Read-only balance observations and explicit state transitions for the
//! durable manual ICPSwap workflow.

use candid::Nat;
use tracing::info;

use crate::swappers::icpswap::{
    client::IcpswapManualClient,
    manual::{pool_user_account, saved_deposit_account},
    state::{
        MAX_MANUAL_SLIPPAGE_STEPS, MAX_MANUAL_TRADE_RETRIES, MIN_UNCHANGED_TRADE_OBSERVATIONS,
        PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS, retry_backoff_nanos,
    },
    transfer_state::{IcpswapLedgerTransferState, IcpswapSettlementKind},
    types::{IcpswapClientError, IcpswapError, IcpswapOperatorAnswer, IcpswapState, IcpswapStep, IcpswapTransferState},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TradeObservation {
    /// The full planned input was consumed and the newly produced output met
    /// the persisted `amountOutMinimum`, so settlement is proven by balances.
    Succeeded(Nat),
    /// Both unused pool balances still equal their pre-swap baselines. There
    /// is no evidence yet that the swap executed.
    Unchanged,
    /// At least one balance moved, but the changes do not prove a complete
    /// swap with sufficient output. This includes partial or unexpected moves.
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnconfirmedTradeObservation {
    /// Neither unused pool balance changed from the pre-swap baselines. The
    /// swap has not been proven to execute, but a delayed update may still be
    /// in flight, so replay is considered only after the reconciliation wait.
    Unchanged,
    /// The pool balances changed, but not by enough to prove that the planned
    /// input was consumed and the minimum output was produced. The outcome is
    /// ambiguous and must not be replayed automatically.
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TradeRetryCause {
    ConfirmedSlippage,
    AmbiguousUnchanged,
    QuoteBelowHardFloor,
}

/// Checks whether ICPSwap credited the complete planned input to the owner's
/// unused pool balance.
///
/// The persisted pre-deposit pool balance isolates this execution from funds
/// that were already present. A result of `true` proves that at least
/// `amount_in` was added and the workflow may advance to the trade step. A
/// result of `false` only means the credit is not visible yet; it does not prove
/// that the deposit call was never submitted or make replay safe. Replay safety
/// is established separately from the ledger deposit-subaccount balance.
pub(crate) async fn observe_deposit(client: &dyn IcpswapManualClient, state: &IcpswapState) -> Result<bool, String> {
    let before = state
        .deposit
        .input_pool_balance_before
        .clone()
        .ok_or_else(|| "manual deposit is missing its input-pool baseline".to_string())?;
    let current = pool_balances(client, state)
        .await
        .map_err(|error| error.to_string())?
        .input;
    let required = before.clone() + state.plan.amount_in.value.clone();
    let confirmed = current >= required;
    info!(
        event = "icpswap_deposit_observation",
        execution_id = %state.execution_id,
        pool = %state.plan.pool,
        balance_before = %before,
        balance_current = %current,
        balance_required = %required,
        confirmed,
        "Observed ICPSwap deposit balance"
    );
    Ok(confirmed)
}

pub(crate) fn complete_deposit(state: &mut IcpswapState) {
    state.deposit.ready_to_submit = false;
    state.deposit.observation_attempts = 0;
    state.deposit.submission_retry_count = 0;
    state.next_attempt_at_nanos = None;
    state.step = IcpswapStep::Trade;
    state.operator_pending_step = None;
    state.last_error = None;
}

/// Compares the owner's current unused pool balances with the values persisted
/// immediately before submitting `swap`.
///
/// Both baselines are necessary: the input baseline proves that this attempt
/// consumed the planned input rather than merely observing pre-existing funds,
/// while the output baseline isolates the output produced by this attempt from
/// ckUSDC that was already sitting in the pool account.
pub(crate) async fn observe_trade(
    client: &dyn IcpswapManualClient,
    state: &IcpswapState,
) -> Result<TradeObservation, String> {
    // Unused input held by the pool immediately before submission. A successful
    // swap must reduce this by at least the complete planned input amount.
    let input_before = state
        .trade
        .input_pool_balance_before
        .clone()
        .ok_or_else(|| "manual swap is missing its input-pool baseline".to_string())?;
    // Unused output held by the pool immediately before submission. Subtracting
    // it from the current balance gives output attributable to this attempt.
    let output_before = state
        .trade
        .output_pool_balance_before
        .clone()
        .ok_or_else(|| "manual swap is missing its output-pool baseline".to_string())?;
    let PoolBalances {
        input: input_now,
        output: output_now,
    } = pool_balances(client, state).await.map_err(|error| error.to_string())?;
    if input_now == input_before && output_now == output_before {
        return Ok(TradeObservation::Unchanged);
    }
    let expected_input_max = nat_saturating_sub(&input_before, &state.plan.amount_in.value);
    let output_delta = nat_saturating_sub(&output_now, &output_before);
    if input_now <= expected_input_max && output_delta >= state.trade.current_amount_out_minimum.value {
        return Ok(TradeObservation::Succeeded(output_delta));
    }
    Ok(TradeObservation::Inconsistent)
}

/// Records gross output that is either returned by a decoded successful swap
/// response or proven later through pool-balance reconciliation.
pub(crate) fn complete_trade(state: &mut IcpswapState, gross_output: Nat) {
    state.trade.gross_output_amount = Some(gross_output);
    state.step = IcpswapStep::Withdraw;
    state.trade.swap_protocol_error = None;
    state.trade.pending_since_nanos = None;
    state.trade.unchanged_observations = 0;
    state.operator_pending_step = None;
    state.last_error = None;
}

pub(crate) fn resolve_swap_protocol_error(
    state: &mut IcpswapState,
    error: IcpswapError,
    observation: TradeObservation,
    now_nanos: u64,
) {
    let retryable_slippage = matches!(&error, IcpswapError::InternalError(message) if is_slippage_error(message));
    let message = format!("ICPSwap swap returned an error: {error:?}");
    match observation {
        TradeObservation::Succeeded(gross_output) => complete_trade(state, gross_output),
        TradeObservation::Unchanged if retryable_slippage => {
            schedule_trade_retry_or_recovery(state, now_nanos, TradeRetryCause::ConfirmedSlippage, message)
        }
        TradeObservation::Unchanged => {
            state.step = IcpswapStep::Recover;
            state.last_error = Some(message);
        }
        TradeObservation::Inconsistent => require_operator(state, IcpswapStep::TradePending, message),
    }
}

pub(crate) fn handle_unconfirmed_trade(
    state: &mut IcpswapState,
    now_nanos: u64,
    observation: UnconfirmedTradeObservation,
) {
    if observation == UnconfirmedTradeObservation::Unchanged {
        state.trade.unchanged_observations = state.trade.unchanged_observations.saturating_add(1);
    }
    let pending_since = *state.trade.pending_since_nanos.get_or_insert(now_nanos);
    let timed_out = now_nanos.saturating_sub(pending_since) >= PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS;
    if !timed_out {
        return;
    }

    if observation == UnconfirmedTradeObservation::Inconsistent {
        require_operator(
            state,
            IcpswapStep::TradePending,
            "pending swap produced inconsistent input/output balance deltas".to_string(),
        );
    } else if state.trade.unchanged_observations >= MIN_UNCHANGED_TRADE_OBSERVATIONS {
        if deposited_input_is_below_plan(state) {
            recover_after_insufficient_deposit(state);
        } else if trade_replay_is_single_spend(state) {
            schedule_trade_retry_or_recovery(
                state,
                now_nanos,
                TradeRetryCause::AmbiguousUnchanged,
                "ambiguous swap remained unchanged through the reconciliation timeout; retrying with a fresh quote"
                    .to_string(),
            );
        } else {
            require_operator(
                state,
                IcpswapStep::TradePending,
                "ambiguous swap cannot be replayed safely because the pool held enough input for multiple swaps"
                    .to_string(),
            );
        }
    }
}

fn trade_replay_is_single_spend(state: &IcpswapState) -> bool {
    let Some(input_before) = state.trade.input_pool_balance_before.as_ref() else {
        return false;
    };
    let amount = &state.plan.amount_in.value;
    input_before >= amount && input_before < &(amount.clone() * Nat::from(2u8))
}

fn deposited_input_is_below_plan(state: &IcpswapState) -> bool {
    state
        .trade
        .input_pool_balance_before
        .as_ref()
        .is_some_and(|input_before| input_before < &state.plan.amount_in.value)
}

/// A swap cannot consume the planned amount when its persisted pre-call input
/// balance was already smaller. Once read-only reconciliation still sees both
/// balances unchanged, recovery is safe and replaying the oversized swap is not.
fn recover_after_insufficient_deposit(state: &mut IcpswapState) {
    let input_before = state
        .trade
        .input_pool_balance_before
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "unknown".to_string());
    state.step = IcpswapStep::Recover;
    state.operator_pending_step = None;
    state.trade.pending_since_nanos = None;
    state.last_error = Some(format!(
        "pool held {input_before} input units before the rejected swap, below the planned {}; recovering the available input",
        state.plan.amount_in.value
    ));
}

pub(crate) async fn observe_output_withdrawal(
    client: &dyn IcpswapManualClient,
    state: &IcpswapState,
) -> Result<Option<Nat>, String> {
    let args = state
        .withdraw
        .withdraw_args
        .as_ref()
        .ok_or_else(|| "manual output withdrawal is missing its arguments".to_string())?;
    let pool_before = state
        .withdraw
        .pool_balance_before
        .as_ref()
        .ok_or_else(|| "manual output withdrawal is missing its pool baseline".to_string())?;
    let wallet_before = state
        .withdraw
        .wallet_balance_before
        .as_ref()
        .ok_or_else(|| "manual output withdrawal is missing its wallet baseline".to_string())?;
    let (pool, wallet_now) = tokio::try_join!(
        pool_balances(client, state),
        client.ledger_balance(state.plan.token_out, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = pool.output;
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        return Ok(Some(expected_credit));
    }
    Ok(None)
}

pub(crate) fn complete_output_withdrawal(state: &mut IcpswapState, wallet_credit: Nat) {
    state.withdraw.wallet_credited_amount = Some(wallet_credit);
    let settlement = &mut state.settlement;
    settlement.kind = Some(IcpswapSettlementKind::Output);
    settlement.fee = state.plan.output_ledger_fee.clone();
    settlement.transfer = IcpswapLedgerTransferState::default();
    settlement.interrupted_transfer = None;
    settlement.interrupted_observed_debit = None;
    settlement.recovery_credit = None;
    settlement.residual_dust = None;
    state.step = IcpswapStep::Forward;
    state.operator_pending_step = None;
    state.last_error = None;
}

pub(crate) async fn observe_recovery(
    client: &dyn IcpswapManualClient,
    state: &IcpswapState,
) -> Result<Option<Nat>, String> {
    let args = state
        .recovery
        .withdraw_args
        .as_ref()
        .ok_or_else(|| "manual recovery is missing its arguments".to_string())?;
    let pool_before = state
        .recovery
        .pool_balance_before
        .as_ref()
        .ok_or_else(|| "manual recovery is missing its pool baseline".to_string())?;
    let wallet_before = state
        .recovery
        .wallet_balance_before
        .as_ref()
        .ok_or_else(|| "manual recovery is missing its wallet baseline".to_string())?;
    let (pool, wallet_now) = tokio::try_join!(
        pool_balances(client, state),
        client.ledger_balance(state.plan.token_in, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = pool.input;
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        return Ok(Some(expected_credit));
    }
    Ok(None)
}

pub(crate) fn complete_recovery(state: &mut IcpswapState, wallet_credit: Nat) {
    state.recovery.wallet_credited_amount = Some(wallet_credit);
    let settlement = &mut state.settlement;
    settlement.kind = Some(IcpswapSettlementKind::Recovery);
    settlement.destination = state.funding.source;
    settlement.fee = state.plan.input_ledger_fee.clone();
    settlement.transfer = IcpswapLedgerTransferState::default();
    settlement.interrupted_transfer = None;
    settlement.interrupted_observed_debit = None;
    settlement.recovery_credit = None;
    settlement.residual_dust = None;
    state.step = IcpswapStep::Forward;
    state.operator_pending_step = None;
    state.last_error = None;
}

/// Checks a parked leg again, then applies the operator's answer if the check
/// proves nothing. An answer counts for one check, so a refused one needs a new release.
pub(crate) async fn reconcile_operator_required(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
) -> Result<(), String> {
    let answer = state.operator_answer.take();
    let absent = answer == Some(IcpswapOperatorAnswer::Absent);
    match state.operator_pending_step {
        Some(IcpswapStep::TransferPending) => {
            if let Some(answer) = answer {
                answer_parked_transfer(client, state, answer).await?;
            }
        }
        Some(IcpswapStep::DepositPending) => {
            if observe_deposit(client, state).await? {
                complete_deposit(state);
            } else if absent {
                redo_parked_deposit(client, state).await?;
            }
        }
        Some(IcpswapStep::TradePending) => match observe_trade(client, state).await? {
            TradeObservation::Succeeded(gross_output) => complete_trade(state, gross_output),
            TradeObservation::Unchanged if deposited_input_is_below_plan(state) => {
                recover_after_insufficient_deposit(state);
            }
            observation => {
                if let Some(answer) = answer {
                    answer_parked_trade(client, state, observation, answer).await?;
                }
            }
        },
        Some(IcpswapStep::WithdrawPending) => {
            if let Some(wallet_credit) = observe_output_withdrawal(client, state).await? {
                complete_output_withdrawal(state, wallet_credit);
            } else if absent {
                redo_parked_withdrawal(client, state, false).await?;
            }
        }
        Some(IcpswapStep::RecoverPending) => {
            if let Some(wallet_credit) = observe_recovery(client, state).await? {
                complete_recovery(state, wallet_credit);
            } else if absent {
                redo_parked_withdrawal(client, state, true).await?;
            }
        }
        Some(IcpswapStep::Forward) if absent => {
            // The balance is below one ledger fee, so forwarding it costs more than it is worth.
            let reason = state.last_error.take().unwrap_or_default();
            leave_park(
                state,
                IcpswapStep::Failed,
                "balance below the forwarding fee written off",
            );
            state.last_error = Some(format!("written off by an operator: {reason}"));
        }
        _ => {}
    }
    Ok(())
}

/// Moves a parked leg on after the balances agreed with the operator's answer.
fn leave_park(state: &mut IcpswapState, step: IcpswapStep, action: &str) {
    info!(
        event = "icpswap_operator_answer_used",
        execution_id = %state.execution_id,
        parked_step = ?state.operator_pending_step,
        next_step = ?step,
        action,
        "Operator answer moved a parked ICPSwap leg"
    );
    state.step = step;
    state.operator_pending_step = None;
    state.next_attempt_at_nanos = None;
    state.last_error = None;
}

/// Keeps the leg parked and records why the balances contradict the answer.
fn refuse_answer(state: &mut IcpswapState, reason: String) {
    state.last_error = Some(format!("operator answer refused: {reason}"));
}

async fn answer_parked_transfer(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
    answer: IcpswapOperatorAnswer,
) -> Result<(), String> {
    let fee = state.plan.input_ledger_fee.value.clone();
    let (deposit_account, amount) = match &state.transfer.args {
        Some(args) => (args.to, args.amount.clone()),
        None => (
            pool_user_account(state.plan.pool, state.owner.owner),
            state.plan.input_with_fee(),
        ),
    };
    let (deposited, wallet) = tokio::try_join!(
        client.ledger_balance(state.plan.token_in, &deposit_account),
        client.ledger_balance(state.plan.token_in, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    match answer {
        IcpswapOperatorAnswer::TransferLanded { block_index } => {
            if state.transfer.args.is_none() {
                refuse_answer(
                    state,
                    "the saved transfer is missing, so the deposit has no saved account".to_string(),
                );
            } else if deposited < amount {
                refuse_answer(
                    state,
                    format!("the pool deposit account holds {deposited}, below the transfer {amount}"),
                );
            } else {
                state.transfer.block_index = Some(block_index);
                leave_park(state, IcpswapStep::Deposit, "input transfer landed");
            }
        }
        IcpswapOperatorAnswer::Absent => {
            // The leg account holds only this leg's input, so a new transfer can move it once.
            let needed = amount.clone() + fee;
            if deposited >= amount {
                refuse_answer(state, format!("the pool deposit account already holds {deposited}"));
            } else if wallet < needed {
                refuse_answer(
                    state,
                    format!("the leg account holds {wallet}, below the {needed} a new transfer needs"),
                );
            } else {
                state.transfer = IcpswapTransferState::default();
                leave_park(
                    state,
                    IcpswapStep::Transfer,
                    "input transfer absent; a new transfer follows",
                );
            }
        }
        IcpswapOperatorAnswer::SwapLanded { .. } => {
            refuse_answer(state, "a swap output does not answer a parked transfer".to_string());
        }
    }
    Ok(())
}

async fn redo_parked_deposit(client: &dyn IcpswapManualClient, state: &mut IcpswapState) -> Result<(), String> {
    let deposit_account = saved_deposit_account(state)?;
    let amount = state.plan.input_with_fee();
    let held = client
        .ledger_balance(state.plan.token_in, &deposit_account)
        .await
        .map_err(|error| error.to_string())?;
    if held < amount {
        refuse_answer(
            state,
            format!("the pool deposit account holds {held}, below the deposit {amount}, so the pool may have taken it"),
        );
    } else {
        leave_park(state, IcpswapStep::Deposit, "deposit absent; the deposit runs again");
    }
    Ok(())
}

async fn answer_parked_trade(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
    observation: TradeObservation,
    answer: IcpswapOperatorAnswer,
) -> Result<(), String> {
    match answer {
        IcpswapOperatorAnswer::Absent if observation == TradeObservation::Unchanged => {
            // No second swap: the price has moved since the plan.
            state.trade.pending_since_nanos = None;
            leave_park(state, IcpswapStep::Recover, "swap absent; the input comes back");
        }
        IcpswapOperatorAnswer::Absent => {
            refuse_answer(
                state,
                "the pool balances moved since the swap, so it may have run".to_string(),
            );
        }
        IcpswapOperatorAnswer::SwapLanded { output } => {
            let before = state
                .trade
                .output_pool_balance_before
                .clone()
                .ok_or_else(|| "manual swap is missing its output-pool baseline".to_string())?;
            let pool = pool_balances(client, state).await.map_err(|error| error.to_string())?;
            let produced = nat_saturating_sub(&pool.output, &before);
            if produced < output {
                refuse_answer(
                    state,
                    format!("the pool holds {produced} new output, below the answered {output}"),
                );
            } else {
                state.trade.gross_output_amount = Some(output);
                leave_park(state, IcpswapStep::Withdraw, "swap landed");
            }
        }
        IcpswapOperatorAnswer::TransferLanded { .. } => {
            refuse_answer(state, "a transfer block does not answer a parked swap".to_string());
        }
    }
    Ok(())
}

/// A second withdrawal cannot take more than the pool holds for this leg, so
/// it runs again only while the pool still holds the first one's amount.
async fn redo_parked_withdrawal(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
    recovery: bool,
) -> Result<(), String> {
    let (args, next) = if recovery {
        (state.recovery.withdraw_args.as_ref(), IcpswapStep::Recover)
    } else {
        (state.withdraw.withdraw_args.as_ref(), IcpswapStep::Withdraw)
    };
    let amount = args
        .map(|args| args.amount.clone())
        .ok_or_else(|| "parked ICPSwap withdrawal is missing its arguments".to_string())?;
    let pool = pool_balances(client, state).await.map_err(|error| error.to_string())?;
    let held = if recovery { pool.input } else { pool.output };
    if held < amount {
        refuse_answer(
            state,
            format!("the pool holds {held}, below the withdrawal {amount}, so it may have run"),
        );
    } else {
        leave_park(state, next, "withdrawal absent; the withdrawal runs again");
    }
    Ok(())
}

pub(crate) fn schedule_trade_retry_or_recovery(
    state: &mut IcpswapState,
    now_nanos: u64,
    cause: TradeRetryCause,
    message: String,
) {
    state.trade.pending_since_nanos = None;
    state.trade.unchanged_observations = 0;
    state.trade.swap_protocol_error = None;
    if state.trade.retry_evaluation_count >= MAX_MANUAL_TRADE_RETRIES {
        state.step = IcpswapStep::Recover;
        state.trade.next_retry_at_nanos = None;
    } else {
        state.trade.retry_evaluation_count += 1;
        if cause == TradeRetryCause::ConfirmedSlippage {
            state.trade.slippage_retry_count = state
                .trade
                .slippage_retry_count
                .saturating_add(1)
                .min(MAX_MANUAL_SLIPPAGE_STEPS);
        }
        state.trade.next_retry_at_nanos =
            Some(now_nanos.saturating_add(retry_backoff_nanos(state.trade.retry_evaluation_count)));
        state.step = IcpswapStep::Trade;
    }
    state.last_error = Some(message);
}

pub(crate) fn require_operator(state: &mut IcpswapState, pending: IcpswapStep, message: String) {
    state.operator_pending_step = Some(pending);
    state.step = IcpswapStep::OperatorRequired;
    state.last_error = Some(message);
}

/// This leg's unused balances in the pool, named by the swap direction.
pub(crate) struct PoolBalances {
    pub(crate) input: Nat,
    pub(crate) output: Nat,
}

pub(crate) async fn pool_balances(
    client: &dyn IcpswapManualClient,
    state: &IcpswapState,
) -> Result<PoolBalances, IcpswapClientError> {
    let unused = client.unused_balance(state.plan.pool, state.owner.owner).await?;
    let (input, output) = if state.plan.zero_for_one {
        (unused.balance0, unused.balance1)
    } else {
        (unused.balance1, unused.balance0)
    };
    Ok(PoolBalances { input, output })
}

/// `candid::Nat` has no saturating subtraction and panics on underflow, so the
/// balance-delta comparisons share this one clamp.
pub(crate) fn nat_saturating_sub(left: &Nat, right: &Nat) -> Nat {
    if left > right {
        left.clone() - right.clone()
    } else {
        Nat::from(0u8)
    }
}

pub(crate) fn is_slippage_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("slippage")
        || normalized.contains("amountoutminimum")
        || normalized.contains("amount out minimum")
        || normalized.contains("too little received")
        || normalized.contains("price limit")
}
