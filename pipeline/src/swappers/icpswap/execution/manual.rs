//! Durable implementation of ICPSwap's official
//! `depositFrom -> swap -> withdraw` workflow.

use candid::Nat;
#[cfg(test)]
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::swappers::icpswap::{
    client::IcpswapManualClient,
    execution::{IcpswapExecutionStateStore, pool_spender},
    plan::{amount_out_minimum, nat_to_decimal_text, required_allowance},
    state::{MAX_MANUAL_SLIPPAGE_RETRIES, retry_backoff_nanos, retry_slippage_bps},
    types::{
        IcpswapApprovalRequest, IcpswapDepositArgs, IcpswapError, IcpswapManualClientError, IcpswapState, IcpswapStep,
        IcpswapSwapArgs, IcpswapUnusedBalance, IcpswapWithdrawArgs,
    },
};

#[cfg(test)]
use crate::swappers::icpswap::types::IcpswapExecutionState;

#[cfg(test)]
pub(crate) async fn advance_manual(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    owner: Account,
    now_nanos: u64,
) -> Result<IcpswapExecutionState, String> {
    let mut state = store
        .load(execution_id)
        .await?
        .ok_or_else(|| format!("missing persisted ICPSwap state for {execution_id}"))?;
    validate_state(&state, execution_id, owner)?;

    match state.step {
        IcpswapStep::Deposit => deposit(client, store, execution_id, &mut state, now_nanos).await?,
        IcpswapStep::DepositPending => {
            reconcile_deposit(client, &mut state).await?;
            persist(store, execution_id, &state).await?;
        }
        IcpswapStep::Trade => trade(client, store, execution_id, &mut state, now_nanos).await?,
        IcpswapStep::TradePending => {
            if !reconcile_trade(client, &mut state).await?
                && let Some(error) = state.trade.swap_protocol_error.clone()
            {
                handle_persisted_swap_protocol_error(client, &mut state, error, now_nanos).await?;
            }
            persist(store, execution_id, &state).await?;
        }
        IcpswapStep::Withdraw => withdraw_output(client, store, execution_id, &mut state, now_nanos).await?,
        IcpswapStep::WithdrawPending => {
            reconcile_output_withdrawal(client, &mut state).await?;
            persist(store, execution_id, &state).await?;
        }
        IcpswapStep::Recover => recover_input(client, store, execution_id, &mut state, now_nanos).await?,
        IcpswapStep::RecoverPending => {
            reconcile_recovery(client, &mut state).await?;
            persist(store, execution_id, &state).await?;
        }
        IcpswapStep::OperatorRequired => {
            reconcile_operator_required(client, &mut state).await?;
            persist(store, execution_id, &state).await?;
        }
        IcpswapStep::Completed | IcpswapStep::Refunded | IcpswapStep::Failed => {}
    }

    Ok(state)
}

pub(crate) async fn deposit_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Deposit => deposit(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::DepositPending => {
            reconcile_deposit(client, state).await?;
            persist(store, execution_id, state).await
        }
        _ => Err(format!("deposit cannot handle ICPSwap step {:?}", state.step)),
    }
}

pub(crate) async fn trade_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Trade => trade(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::TradePending => {
            if !reconcile_trade(client, state).await?
                && let Some(error) = state.trade.swap_protocol_error.clone()
            {
                handle_persisted_swap_protocol_error(client, state, error, now_nanos).await?;
            }
            persist(store, execution_id, state).await
        }
        _ => Err(format!("trade cannot handle ICPSwap step {:?}", state.step)),
    }
}

pub(crate) async fn withdraw_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Withdraw => withdraw_output(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::WithdrawPending => {
            reconcile_output_withdrawal(client, state).await?;
            persist(store, execution_id, state).await
        }
        _ => Err(format!("withdraw cannot handle ICPSwap step {:?}", state.step)),
    }
}

pub(crate) async fn recover_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Recover => recover_input(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::RecoverPending => {
            reconcile_recovery(client, state).await?;
            persist(store, execution_id, state).await
        }
        _ => Err(format!("recover cannot handle ICPSwap step {:?}", state.step)),
    }
}

pub(crate) async fn operator_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
) -> Result<(), String> {
    if state.step != IcpswapStep::OperatorRequired {
        return Err(format!(
            "operator reconciliation cannot handle ICPSwap step {:?}",
            state.step
        ));
    }
    reconcile_operator_required(client, state).await?;
    persist(store, execution_id, state).await
}

async fn deposit(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let spender = pool_spender(state.plan.pool);
    let required = required_allowance(&state.plan);
    let current = client
        .manual_allowance(state.plan.token_in, &state.owner, &spender)
        .await
        .map_err(|error| error.to_string())?;
    if current < required {
        let created_at = match state.deposit.approval_created_at {
            Some(value) => value,
            None => {
                state.deposit.approval_created_at = Some(now_nanos);
                persist(store, execution_id, state).await?;
                now_nanos
            }
        };
        match client
            .manual_approve(IcpswapApprovalRequest {
                ledger: state.plan.token_in,
                owner: state.owner,
                spender,
                current_allowance: current,
                required_allowance: required.clone(),
                created_at_time: created_at,
            })
            .await
        {
            Ok(block) => state.deposit.approval_block_index = Some(block),
            Err(error) => {
                let refreshed = client
                    .manual_allowance(state.plan.token_in, &state.owner, &spender)
                    .await
                    .map_err(|query| query.to_string())?;
                if refreshed < required {
                    state.last_error = Some(error.to_string());
                    persist(store, execution_id, state).await?;
                    return Err(error.to_string());
                }
            }
        }
    }

    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.deposit.input_pool_balance_before = Some(input_balance(&state.plan, &unused));
    state.deposit.output_pool_balance_before = Some(output_balance(&state.plan, &unused));
    let args = IcpswapDepositArgs {
        token: state.plan.token_in.to_text(),
        amount: state.plan.amount_in.value.clone(),
        fee: state.plan.input_ledger_fee.value.clone(),
    };
    state.deposit.deposit_args = Some(args.clone());
    state.deposit.deposit_submitted_at = Some(now_nanos);
    state.step = IcpswapStep::DepositPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;

    match client.deposit_from(state.plan.pool, &args).await {
        Ok(amount) => {
            state.deposit.deposit_returned_amount = Some(amount);
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapManualClientError::SubmissionUnknown { .. }) => {
            require_operator(state, IcpswapStep::DepositPending, error.to_string());
            persist(store, execution_id, state).await?;
            reconcile_deposit(client, state).await?;
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
        Err(error) => {
            state.step = IcpswapStep::Deposit;
            state.deposit.deposit_args = None;
            state.deposit.deposit_submitted_at = None;
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    reconcile_deposit(client, state).await?;
    persist(store, execution_id, state).await
}

async fn reconcile_deposit(client: &dyn IcpswapManualClient, state: &mut IcpswapState) -> Result<bool, String> {
    let before = state
        .deposit
        .input_pool_balance_before
        .clone()
        .ok_or_else(|| "manual deposit is missing its input-pool baseline".to_string())?;
    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let current = input_balance(&state.plan, &unused);
    if current >= before + state.plan.amount_in.value.clone() {
        state.trade.input_pool_balance_before = Some(current);
        state.trade.output_pool_balance_before = Some(output_balance(&state.plan, &unused));
        state.step = IcpswapStep::Trade;
        state.operator_pending_step = None;
        state.last_error = None;
        return Ok(true);
    }
    Ok(false)
}

async fn trade(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    if state.trade.next_retry_at_nanos.is_some_and(|ready| now_nanos < ready) {
        persist(store, execution_id, state).await?;
        return Ok(());
    }

    let quote = if state.trade.retry_count == 0 {
        state.plan.gross_quoted_out.value.clone()
    } else {
        client
            .quote_manual(
                state.plan.pool,
                &IcpswapSwapArgs {
                    zero_for_one: state.plan.zero_for_one,
                    amount_in: nat_to_decimal_text(&state.plan.amount_in.value),
                    amount_out_minimum: "0".to_string(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
    };
    state.trade.current_quote = Some(ChainTokenAmount::from_raw(
        state.plan.gross_quoted_out.token.clone(),
        quote.clone(),
    ));

    if quote < state.trade.original_hard_minimum_out.value {
        consume_retry_without_submission(state, now_nanos, "fresh quote is below the original hard output floor");
        persist(store, execution_id, state).await?;
        return Ok(());
    }

    let effective = retry_slippage_bps(state.plan.max_slippage_bps, state.trade.retry_count);
    let fresh_minimum = amount_out_minimum(&quote, effective).map_err(|error| error.to_string())?;
    let minimum = std::cmp::max(fresh_minimum, state.trade.original_hard_minimum_out.value.clone());
    state.trade.effective_slippage_bps = effective;
    state.trade.current_amount_out_minimum =
        ChainTokenAmount::from_raw(state.plan.gross_quoted_out.token.clone(), minimum.clone());

    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.trade.input_pool_balance_before = Some(input_balance(&state.plan, &unused));
    state.trade.output_pool_balance_before = Some(output_balance(&state.plan, &unused));
    let args = IcpswapSwapArgs {
        zero_for_one: state.plan.zero_for_one,
        amount_in: nat_to_decimal_text(&state.plan.amount_in.value),
        amount_out_minimum: nat_to_decimal_text(&minimum),
    };
    state.trade.swap_args = Some(args.clone());
    state.trade.swap_returned_amount = None;
    state.trade.swap_protocol_error = None;
    state.trade.swap_submitted_at = Some(now_nanos);
    state.trade.next_retry_at_nanos = None;
    state.step = IcpswapStep::TradePending;
    state.last_error = None;
    persist(store, execution_id, state).await?;

    match client.swap_manual(state.plan.pool, &args).await {
        Ok(amount) => {
            state.trade.swap_returned_amount = Some(amount);
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapManualClientError::SubmissionUnknown { .. }) => {
            require_operator(state, IcpswapStep::TradePending, error.to_string());
            persist(store, execution_id, state).await?;
            reconcile_trade(client, state).await?;
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
        Err(error @ IcpswapManualClientError::Protocol { .. }) => {
            if let IcpswapManualClientError::Protocol { error: protocol, .. } = &error {
                state.trade.swap_protocol_error = Some(protocol.clone());
            }
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            return handle_swap_protocol_error(client, store, execution_id, state, error, now_nanos).await;
        }
        Err(error) => {
            state.step = IcpswapStep::Trade;
            state.trade.swap_args = None;
            state.trade.swap_submitted_at = None;
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    reconcile_trade(client, state).await?;
    persist(store, execution_id, state).await
}

async fn handle_swap_protocol_error(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    error: IcpswapManualClientError,
    now_nanos: u64,
) -> Result<(), String> {
    let protocol = match &error {
        IcpswapManualClientError::Protocol { error, .. } => error.clone(),
        _ => unreachable!("only protocol errors reach this handler"),
    };
    handle_persisted_swap_protocol_error(client, state, protocol, now_nanos).await?;
    persist(store, execution_id, state).await?;
    Err(error.to_string())
}

async fn handle_persisted_swap_protocol_error(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
    error: IcpswapError,
    now_nanos: u64,
) -> Result<(), String> {
    let unchanged = trade_balances_unchanged(client, state).await?;
    let retryable_slippage = matches!(&error, IcpswapError::InternalError(message) if is_slippage_error(message));
    let message = format!("ICPSwap swap returned an error: {error:?}");
    if !unchanged {
        require_operator(state, IcpswapStep::TradePending, message);
    } else if retryable_slippage {
        schedule_retry_or_recovery(state, now_nanos, message);
    } else {
        state.step = IcpswapStep::Recover;
        state.last_error = Some(message);
    }
    Ok(())
}

async fn reconcile_trade(client: &dyn IcpswapManualClient, state: &mut IcpswapState) -> Result<bool, String> {
    let input_before = state
        .trade
        .input_pool_balance_before
        .clone()
        .ok_or_else(|| "manual swap is missing its input-pool baseline".to_string())?;
    let output_before = state
        .trade
        .output_pool_balance_before
        .clone()
        .ok_or_else(|| "manual swap is missing its output-pool baseline".to_string())?;
    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let input_now = input_balance(&state.plan, &unused);
    let output_now = output_balance(&state.plan, &unused);
    let expected_input_max = nat_saturating_sub(&input_before, &state.plan.amount_in.value);
    let output_delta = nat_saturating_sub(&output_now, &output_before);
    if input_now <= expected_input_max && output_delta >= state.trade.current_amount_out_minimum.value {
        state.trade.swap_returned_amount = Some(output_delta.clone());
        state.withdraw.pool_balance_before = Some(output_now);
        state.step = IcpswapStep::Withdraw;
        state.trade.swap_protocol_error = None;
        state.operator_pending_step = None;
        state.last_error = None;
        return Ok(true);
    }
    Ok(false)
}

async fn trade_balances_unchanged(client: &dyn IcpswapManualClient, state: &IcpswapState) -> Result<bool, String> {
    let input_before = state
        .trade
        .input_pool_balance_before
        .as_ref()
        .ok_or_else(|| "manual swap is missing its input-pool baseline".to_string())?;
    let output_before = state
        .trade
        .output_pool_balance_before
        .as_ref()
        .ok_or_else(|| "manual swap is missing its output-pool baseline".to_string())?;
    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    Ok(input_balance(&state.plan, &unused) == *input_before && output_balance(&state.plan, &unused) == *output_before)
}

async fn withdraw_output(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let amount = state
        .trade
        .swap_returned_amount
        .clone()
        .ok_or_else(|| "manual swap has no confirmed gross output".to_string())?;
    if amount <= state.plan.output_ledger_fee.value {
        state.step = IcpswapStep::Failed;
        state.last_error = Some("gross swap output cannot cover the ckUSDC ledger fee".to_string());
        return persist(store, execution_id, state).await;
    }
    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.withdraw.pool_balance_before = Some(output_balance(&state.plan, &unused));
    state.withdraw.wallet_balance_before = Some(
        client
            .ledger_balance(state.plan.token_out, &state.owner)
            .await
            .map_err(|error| error.to_string())?,
    );
    let args = IcpswapWithdrawArgs {
        token: state.plan.token_out.to_text(),
        fee: state.plan.output_ledger_fee.value.clone(),
        amount: amount.clone(),
    };
    state.withdraw.withdraw_args = Some(args.clone());
    state.withdraw.withdraw_submitted_at = Some(now_nanos);
    state.step = IcpswapStep::WithdrawPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;
    match client.withdraw_manual(state.plan.pool, &args).await {
        Ok(returned) => {
            state.withdraw.withdraw_returned_amount = Some(returned);
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapManualClientError::SubmissionUnknown { .. }) => {
            require_operator(state, IcpswapStep::WithdrawPending, error.to_string());
            persist(store, execution_id, state).await?;
            reconcile_output_withdrawal(client, state).await?;
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
        Err(error) => {
            require_operator(state, IcpswapStep::WithdrawPending, error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    reconcile_output_withdrawal(client, state).await?;
    persist(store, execution_id, state).await
}

async fn reconcile_output_withdrawal(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
) -> Result<bool, String> {
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
        client.manual_unused_balance(state.plan.pool, state.owner.owner),
        client.ledger_balance(state.plan.token_out, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = output_balance(&state.plan, &unused);
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        state.withdraw.wallet_credited_amount = Some(expected_credit);
        state.step = IcpswapStep::Completed;
        state.operator_pending_step = None;
        state.last_error = None;
        return Ok(true);
    }
    Ok(false)
}

async fn recover_input(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let unused = client
        .manual_unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let amount = std::cmp::min(input_balance(&state.plan, &unused), state.plan.amount_in.value.clone());
    if amount <= state.plan.input_ledger_fee.value {
        state.step = IcpswapStep::Failed;
        state.last_error = Some(format!(
            "recoverable ICP {amount} cannot cover ledger fee {}",
            state.plan.input_ledger_fee.value
        ));
        return persist(store, execution_id, state).await;
    }
    state.recovery.pool_balance_before = Some(input_balance(&state.plan, &unused));
    state.recovery.wallet_balance_before = Some(
        client
            .ledger_balance(state.plan.token_in, &state.owner)
            .await
            .map_err(|error| error.to_string())?,
    );
    let args = IcpswapWithdrawArgs {
        token: state.plan.token_in.to_text(),
        fee: state.plan.input_ledger_fee.value.clone(),
        amount,
    };
    state.recovery.withdraw_args = Some(args.clone());
    state.recovery.withdraw_submitted_at = Some(now_nanos);
    state.step = IcpswapStep::RecoverPending;
    persist(store, execution_id, state).await?;
    match client.withdraw_manual(state.plan.pool, &args).await {
        Ok(returned) => {
            state.recovery.withdraw_returned_amount = Some(returned);
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapManualClientError::SubmissionUnknown { .. }) => {
            require_operator(state, IcpswapStep::RecoverPending, error.to_string());
            persist(store, execution_id, state).await?;
            reconcile_recovery(client, state).await?;
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
        Err(error) => {
            require_operator(state, IcpswapStep::RecoverPending, error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    reconcile_recovery(client, state).await?;
    persist(store, execution_id, state).await
}

async fn reconcile_recovery(client: &dyn IcpswapManualClient, state: &mut IcpswapState) -> Result<bool, String> {
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
        client.manual_unused_balance(state.plan.pool, state.owner.owner),
        client.ledger_balance(state.plan.token_in, &state.owner),
    )
    .map_err(|error| error.to_string())?;
    let pool_now = input_balance(&state.plan, &unused);
    let expected_credit = nat_saturating_sub(&args.amount, &args.fee);
    let wallet_delta = nat_saturating_sub(&wallet_now, wallet_before);
    if pool_now <= nat_saturating_sub(pool_before, &args.amount) && wallet_delta >= expected_credit {
        state.recovery.wallet_credited_amount = Some(expected_credit);
        state.step = IcpswapStep::Refunded;
        state.operator_pending_step = None;
        state.last_error = None;
        return Ok(true);
    }
    Ok(false)
}

async fn reconcile_operator_required(client: &dyn IcpswapManualClient, state: &mut IcpswapState) -> Result<(), String> {
    match state.operator_pending_step {
        Some(IcpswapStep::DepositPending) => {
            reconcile_deposit(client, state).await?;
        }
        Some(IcpswapStep::TradePending) => {
            reconcile_trade(client, state).await?;
        }
        Some(IcpswapStep::WithdrawPending) => {
            reconcile_output_withdrawal(client, state).await?;
        }
        Some(IcpswapStep::RecoverPending) => {
            reconcile_recovery(client, state).await?;
        }
        _ => {}
    }
    Ok(())
}

fn schedule_retry_or_recovery(state: &mut IcpswapState, now_nanos: u64, message: String) {
    if state.trade.retry_count >= MAX_MANUAL_SLIPPAGE_RETRIES {
        state.step = IcpswapStep::Recover;
        state.trade.next_retry_at_nanos = None;
    } else {
        state.trade.retry_count += 1;
        state.trade.next_retry_at_nanos = Some(now_nanos.saturating_add(retry_backoff_nanos(state.trade.retry_count)));
        state.step = IcpswapStep::Trade;
    }
    state.last_error = Some(message);
}

fn consume_retry_without_submission(state: &mut IcpswapState, now_nanos: u64, message: &str) {
    if state.trade.retry_count >= MAX_MANUAL_SLIPPAGE_RETRIES {
        state.step = IcpswapStep::Recover;
        state.trade.next_retry_at_nanos = None;
    } else {
        state.trade.retry_count += 1;
        state.trade.next_retry_at_nanos = Some(now_nanos.saturating_add(retry_backoff_nanos(state.trade.retry_count)));
    }
    state.last_error = Some(message.to_string());
}

fn require_operator(state: &mut IcpswapState, pending: IcpswapStep, message: String) {
    state.operator_pending_step = Some(pending);
    state.step = IcpswapStep::OperatorRequired;
    state.last_error = Some(message);
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &IcpswapState,
) -> Result<(), String> {
    store.persist(execution_id, state).await
}

#[cfg(test)]
fn validate_state(state: &IcpswapExecutionState, execution_id: &str, owner: Account) -> Result<(), String> {
    if state.execution_id != execution_id {
        return Err(format!(
            "ICPSwap execution ID {} differs from state-store key {execution_id}",
            state.execution_id
        ));
    }
    if state.owner != owner {
        return Err(format!(
            "ICPSwap execution owner {} differs from configured owner {owner}",
            state.owner
        ));
    }
    Ok(())
}

fn input_balance(plan: &crate::swappers::icpswap::types::IcpswapExecutionPlan, unused: &IcpswapUnusedBalance) -> Nat {
    if plan.token_in == plan.token0 {
        unused.balance0.clone()
    } else {
        unused.balance1.clone()
    }
}

fn output_balance(plan: &crate::swappers::icpswap::types::IcpswapExecutionPlan, unused: &IcpswapUnusedBalance) -> Nat {
    if plan.token_out == plan.token0 {
        unused.balance0.clone()
    } else {
        unused.balance1.clone()
    }
}

fn nat_saturating_sub(left: &Nat, right: &Nat) -> Nat {
    if left > right {
        left.clone() - right.clone()
    } else {
        Nat::from(0u8)
    }
}

pub fn is_slippage_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("slippage")
        || normalized.contains("amountoutminimum")
        || normalized.contains("amount out minimum")
        || normalized.contains("too little received")
        || normalized.contains("price limit")
}
