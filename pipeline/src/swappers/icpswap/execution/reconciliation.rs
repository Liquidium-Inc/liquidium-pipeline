//! Read-only balance observations and explicit state transitions for the
//! durable manual ICPSwap workflow.

use candid::Nat;

use crate::swappers::icpswap::{
    client::IcpswapManualClient,
    state::{
        MAX_MANUAL_SLIPPAGE_STEPS, MAX_MANUAL_TRADE_RETRIES, MIN_UNCHANGED_TRADE_OBSERVATIONS,
        PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS, retry_backoff_nanos,
    },
    types::{IcpswapError, IcpswapExecutionPlan, IcpswapState, IcpswapStep, IcpswapUnusedBalance},
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

pub(crate) async fn observe_deposit(client: &dyn IcpswapManualClient, state: &IcpswapState) -> Result<bool, String> {
    let before = state
        .deposit
        .input_pool_balance_before
        .clone()
        .ok_or_else(|| "manual deposit is missing its input-pool baseline".to_string())?;
    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let current = input_balance(&state.plan, &unused);
    Ok(current >= before + state.plan.amount_in.value.clone())
}

pub(crate) fn complete_deposit(state: &mut IcpswapState) {
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
    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let input_now = input_balance(&state.plan, &unused);
    let output_now = output_balance(&state.plan, &unused);
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
    let (unused, wallet_now) = tokio::try_join!(
        client.unused_balance(state.plan.pool, state.owner.owner),
        client.ledger_balance(state.plan.token_out, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = output_balance(&state.plan, &unused);
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        return Ok(Some(expected_credit));
    }
    Ok(None)
}

pub(crate) fn complete_output_withdrawal(state: &mut IcpswapState, wallet_credit: Nat) {
    state.withdraw.wallet_credited_amount = Some(wallet_credit);
    state.step = IcpswapStep::Completed;
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
    let (unused, wallet_now) = tokio::try_join!(
        client.unused_balance(state.plan.pool, state.owner.owner),
        client.ledger_balance(state.plan.token_in, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = input_balance(&state.plan, &unused);
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        return Ok(Some(expected_credit));
    }
    Ok(None)
}

pub(crate) fn complete_recovery(state: &mut IcpswapState, wallet_credit: Nat) {
    state.recovery.wallet_credited_amount = Some(wallet_credit);
    state.step = IcpswapStep::Refunded;
    state.operator_pending_step = None;
    state.last_error = None;
}

pub(crate) async fn reconcile_operator_required(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
) -> Result<(), String> {
    match state.operator_pending_step {
        Some(IcpswapStep::DepositPending) => {
            if observe_deposit(client, state).await? {
                complete_deposit(state);
            }
        }
        Some(IcpswapStep::TradePending) => match observe_trade(client, state).await? {
            TradeObservation::Succeeded(gross_output) => complete_trade(state, gross_output),
            TradeObservation::Unchanged if deposited_input_is_below_plan(state) => {
                recover_after_insufficient_deposit(state);
            }
            TradeObservation::Unchanged | TradeObservation::Inconsistent => {}
        },
        Some(IcpswapStep::WithdrawPending) => {
            if let Some(wallet_credit) = observe_output_withdrawal(client, state).await? {
                complete_output_withdrawal(state, wallet_credit);
            }
        }
        Some(IcpswapStep::RecoverPending) => {
            if let Some(wallet_credit) = observe_recovery(client, state).await? {
                complete_recovery(state, wallet_credit);
            }
        }
        _ => {}
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

pub(crate) fn input_balance(plan: &IcpswapExecutionPlan, unused: &IcpswapUnusedBalance) -> Nat {
    if plan.zero_for_one {
        unused.balance0.clone()
    } else {
        unused.balance1.clone()
    }
}

pub(crate) fn output_balance(plan: &IcpswapExecutionPlan, unused: &IcpswapUnusedBalance) -> Nat {
    if plan.zero_for_one {
        unused.balance1.clone()
    } else {
        unused.balance0.clone()
    }
}

fn nat_saturating_sub(left: &Nat, right: &Nat) -> Nat {
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
