//! Durable implementation of ICPSwap's official
//! `transfer -> deposit -> swap -> withdraw` workflow.

use candid::Principal;
#[cfg(test)]
use icrc_ledger_types::icrc1::account::Account;
use icrc_ledger_types::icrc1::{
    account::{Account as IcrcAccount, principal_to_subaccount},
    transfer::TransferArg,
};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::swappers::icpswap::{
    client::IcpswapManualClient,
    execution::IcpswapExecutionStateStore,
    plan::{amount_out_minimum, nat_to_decimal_text},
    reconciliation as recon,
    state::{
        DEPOSIT_OBSERVATION_RETRY_NANOS, MAX_DEPOSIT_OBSERVATION_ATTEMPTS, MAX_DEPOSIT_SUBMISSION_RETRIES,
        retry_slippage_bps,
    },
    types::{
        IcpswapClientError, IcpswapDepositArgs, IcpswapError, IcpswapState, IcpswapStep, IcpswapSwapArgs,
        IcpswapWithdrawArgs,
    },
};

#[cfg(test)]
use crate::swappers::icpswap::state::validate_execution_state;
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
    validate_execution_state(&state, execution_id, owner)?;

    match state.step {
        IcpswapStep::Transfer | IcpswapStep::TransferPending => {
            transfer_step(client, store, execution_id, &mut state, now_nanos).await?
        }
        IcpswapStep::Deposit | IcpswapStep::DepositPending => {
            deposit_step(client, store, execution_id, &mut state, now_nanos).await?
        }
        IcpswapStep::Trade | IcpswapStep::TradePending => {
            trade_step(client, store, execution_id, &mut state, now_nanos).await?
        }
        IcpswapStep::Withdraw | IcpswapStep::WithdrawPending => {
            withdraw_step(client, store, execution_id, &mut state, now_nanos).await?
        }
        IcpswapStep::Recover | IcpswapStep::RecoverPending => {
            recover_step(client, store, execution_id, &mut state, now_nanos).await?
        }
        IcpswapStep::OperatorRequired => operator_step(client, store, execution_id, &mut state).await?,
        IcpswapStep::Completed | IcpswapStep::Refunded | IcpswapStep::Failed => {}
    }

    Ok(state)
}

pub(crate) async fn transfer_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Transfer => transfer_input(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::TransferPending => resume_transfer(client, store, execution_id, state).await,
        _ => Err(format!("transfer cannot handle ICPSwap step {:?}", state.step)),
    }
}

pub(crate) async fn deposit_step(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Deposit => {
            if state.next_attempt_at_nanos.is_some_and(|ready_at| now_nanos < ready_at) {
                return persist(store, execution_id, state).await;
            }
            state.next_attempt_at_nanos = None;
            deposit(client, store, execution_id, state, now_nanos).await
        }
        IcpswapStep::DepositPending => {
            if state.next_attempt_at_nanos.is_some_and(|ready_at| now_nanos < ready_at) {
                return persist(store, execution_id, state).await;
            }
            state.next_attempt_at_nanos = None;
            if state.deposit.ready_to_submit {
                return submit_prepared_deposit(client, store, execution_id, state, now_nanos).await;
            }
            match recon::observe_deposit(client, state).await {
                Ok(true) => {
                    recon::complete_deposit(state);
                    persist(store, execution_id, state).await
                }
                Ok(false) => {
                    schedule_deposit_observation_retry(
                        client,
                        store,
                        execution_id,
                        state,
                        now_nanos,
                        "pool balance has not reached the required deposit credit".to_string(),
                    )
                    .await
                }
                Err(error) => {
                    schedule_deposit_observation_retry(client, store, execution_id, state, now_nanos, error).await
                }
            }
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
            match recon::observe_trade(client, state).await? {
                recon::TradeObservation::Succeeded(gross_output) => recon::complete_trade(state, gross_output),
                recon::TradeObservation::Unchanged => {
                    if let Some(error) = state.trade.swap_protocol_error.clone() {
                        recon::resolve_swap_protocol_error(state, error, recon::TradeObservation::Unchanged, now_nanos);
                    } else {
                        recon::handle_unconfirmed_trade(
                            state,
                            now_nanos,
                            recon::UnconfirmedTradeObservation::Unchanged,
                        );
                    }
                }
                recon::TradeObservation::Inconsistent => {
                    recon::handle_unconfirmed_trade(state, now_nanos, recon::UnconfirmedTradeObservation::Inconsistent)
                }
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
            if let Some(wallet_credit) = recon::observe_output_withdrawal(client, state).await? {
                recon::complete_output_withdrawal(state, wallet_credit);
            }
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
            if let Some(wallet_credit) = recon::observe_recovery(client, state).await? {
                recon::complete_recovery(state, wallet_credit);
            }
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
    recon::reconcile_operator_required(client, state).await?;
    persist(store, execution_id, state).await
}

async fn transfer_input(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let destination = pool_user_account(state.plan.pool, state.owner.owner);
    let args = TransferArg {
        from_subaccount: state.owner.subaccount,
        to: destination,
        fee: Some(state.plan.input_ledger_fee.value.clone()),
        created_at_time: Some(now_nanos),
        memo: None,
        amount: state.plan.amount_in.value.clone() + state.plan.input_ledger_fee.value.clone(),
    };
    state.transfer.args = Some(args.clone());
    state.step = IcpswapStep::TransferPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;
    submit_transfer(client, store, execution_id, state, args).await
}

async fn resume_transfer(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
) -> Result<(), String> {
    let Some(args) = state.transfer.args.clone() else {
        // Without the original arguments there is nothing to deduplicate against,
        // so a fresh transfer could move the input a second time. Returning the
        // error alone would repeat this cycle forever; park it for an operator.
        let message = "pending ICPSwap transfer is missing its persisted arguments".to_string();
        recon::require_operator(state, IcpswapStep::TransferPending, message.clone());
        persist(store, execution_id, state).await?;
        return Err(message);
    };
    submit_transfer(client, store, execution_id, state, args).await
}

async fn submit_transfer(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    args: TransferArg,
) -> Result<(), String> {
    match client.ledger_transfer(state.plan.token_in, args).await {
        // A ledger-deduplicated replay also lands here: the connector reports the
        // original block index rather than a `Duplicate` error, so a resumed
        // transfer completes exactly like a first-time submission.
        Ok(block_index) => {
            state.transfer.block_index = Some(block_index);
            state.step = IcpswapStep::Deposit;
            state.last_error = None;
            persist(store, execution_id, state).await
        }
        Err(error @ IcpswapClientError::LedgerTransferCreatedInFuture { .. }) => {
            // Ledger time only moves forward, so a timestamp the ledger reads as
            // future-dated cannot have been accepted by an earlier identical
            // submission either: nothing moved. Discarding the arguments is
            // therefore safe, and it is the only way this transfer is ever
            // accepted -- replaying the same future timestamp stays rejected.
            state.step = IcpswapStep::Transfer;
            state.transfer.args = None;
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            Err(error.to_string())
        }
        Err(error @ IcpswapClientError::LedgerTransferTooOld { .. }) => {
            // The transaction window that made replay safe has closed: the ledger
            // refuses these arguments permanently, and deduplication can no longer
            // say whether an earlier attempt moved the input. Re-timestamping here
            // would risk transferring twice, so park for reconciliation instead of
            // retrying a call that can never succeed again.
            recon::require_operator(state, IcpswapStep::TransferPending, error.to_string());
            persist(store, execution_id, state).await?;
            Err(error.to_string())
        }
        Err(error) => {
            // Deliberately left at `TransferPending` with its arguments intact:
            // the outcome is ambiguous, and replaying the identical arguments is
            // what lets ledger deduplication settle it on a later cycle.
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            Err(error.to_string())
        }
    }
}

async fn deposit(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.deposit.input_pool_balance_before = Some(recon::input_balance(&state.plan, &unused));
    let deposit_account = state
        .transfer
        .args
        .as_ref()
        .map(|args| args.to)
        .ok_or_else(|| "ICPSwap deposit is missing its persisted ledger destination".to_string())?;
    state.deposit.input_ledger_balance_before = Some(
        client
            .ledger_balance(state.plan.token_in, &deposit_account)
            .await
            .map_err(|error| error.to_string())?,
    );
    state.deposit.ready_to_submit = true;
    state.deposit.observation_attempts = 0;
    let args = IcpswapDepositArgs {
        token: state.plan.token_in.to_text(),
        // ICPSwap credits `amount - fee` when it sweeps this subaccount.
        // Include the sweep fee so the credited balance equals the swap input.
        amount: state.plan.amount_in.value.clone() + state.plan.input_ledger_fee.value.clone(),
        fee: state.plan.input_ledger_fee.value.clone(),
    };
    state.step = IcpswapStep::DepositPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;

    state.deposit.ready_to_submit = false;
    submit_deposit(client, store, execution_id, state, args, now_nanos).await
}

/// Resumes a deposit intent that was durably prepared before its side effect.
/// A prior process may have crashed while submitting it, so balances are
/// reconciled before the call is allowed to run.
async fn submit_prepared_deposit(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    if recon::observe_deposit(client, state).await? {
        recon::complete_deposit(state);
        return persist(store, execution_id, state).await;
    }

    let deposit_account = state
        .transfer
        .args
        .as_ref()
        .map(|args| args.to)
        .ok_or_else(|| "prepared ICPSwap deposit is missing its persisted ledger destination".to_string())?;
    let ledger_before = state
        .deposit
        .input_ledger_balance_before
        .clone()
        .ok_or_else(|| "prepared ICPSwap deposit is missing its ledger-balance baseline".to_string())?;
    let ledger_current = client
        .ledger_balance(state.plan.token_in, &deposit_account)
        .await
        .map_err(|error| error.to_string())?;
    if ledger_current != ledger_before {
        state.deposit.ready_to_submit = false;
        state.next_attempt_at_nanos = Some(now_nanos.saturating_add(DEPOSIT_OBSERVATION_RETRY_NANOS));
        state.last_error = Some(format!(
            "deposit subaccount balance moved from {ledger_before} to {ledger_current}; observing for delayed pool credit"
        ));
        return persist(store, execution_id, state).await;
    }

    let args = IcpswapDepositArgs {
        token: state.plan.token_in.to_text(),
        amount: state.plan.amount_in.value.clone() + state.plan.input_ledger_fee.value.clone(),
        fee: state.plan.input_ledger_fee.value.clone(),
    };
    state.deposit.ready_to_submit = false;
    submit_deposit(client, store, execution_id, state, args, now_nanos).await
}

async fn submit_deposit(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    args: IcpswapDepositArgs,
    now_nanos: u64,
) -> Result<(), String> {
    match client.deposit(state.plan.pool, &args).await {
        Ok(credited) if credited == state.plan.amount_in.value => {
            recon::complete_deposit(state);
            return persist(store, execution_id, state).await;
        }
        Ok(credited) => {
            state.step = IcpswapStep::Recover;
            state.last_error = Some(format!(
                "ICPSwap deposit credited {credited}, expected {}; recovering the deposited input instead of submitting an oversized swap",
                state.plan.amount_in.value
            ));
            return persist(store, execution_id, state).await;
        }
        Err(error @ IcpswapClientError::SubmissionUnknown { .. }) => {
            state.deposit.observation_attempts = 0;
            state.next_attempt_at_nanos = Some(now_nanos.saturating_add(DEPOSIT_OBSERVATION_RETRY_NANOS));
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await
        }
        Err(error) => {
            // Deliberately left at `DepositPending`. Only `Encode` and `Protocol`
            // reach this arm -- every transport failure is classified
            // `SubmissionUnknown` above -- and a `Protocol` error is not a clean
            // rollback: `classify_update_failure` notes that deposit "can perform
            // awaited ledger calls before it fails", so the subaccount sweep may
            // already have happened. Resetting to `Deposit` would resubmit on top
            // of that. Reconciliation by balance is the safe resolution.
            state.deposit.observation_attempts = 0;
            state.next_attempt_at_nanos = Some(now_nanos.saturating_add(DEPOSIT_OBSERVATION_RETRY_NANOS));
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await
        }
    }
}

/// Records one unsuccessful read-only deposit observation. After four spaced
/// observations, the deposit call is retried only when the ledger proves that
/// the pool has not swept the deposit subaccount. Retries are bounded; any
/// balance movement or inability to prove safety parks the leg.
async fn schedule_deposit_observation_retry(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
    detail: String,
) -> Result<(), String> {
    state.deposit.observation_attempts = state.deposit.observation_attempts.saturating_add(1);
    if state.deposit.observation_attempts >= MAX_DEPOSIT_OBSERVATION_ATTEMPTS {
        let deposit_account = state.transfer.args.as_ref().map(|args| args.to);
        let ledger_before = state.deposit.input_ledger_balance_before.clone();
        if let (Some(deposit_account), Some(ledger_before)) = (deposit_account, ledger_before)
            && state.deposit.submission_retry_count < MAX_DEPOSIT_SUBMISSION_RETRIES
        {
            match client.ledger_balance(state.plan.token_in, &deposit_account).await {
                Ok(ledger_current) if ledger_current == ledger_before => {
                    state.deposit.submission_retry_count += 1;
                    state.deposit.observation_attempts = 0;
                    state.deposit.ready_to_submit = true;
                    state.step = IcpswapStep::DepositPending;
                    state.operator_pending_step = None;
                    state.next_attempt_at_nanos =
                        Some(now_nanos.saturating_add(DEPOSIT_OBSERVATION_RETRY_NANOS));
                    state.last_error = Some(format!(
                        "deposit remained unswept after {MAX_DEPOSIT_OBSERVATION_ATTEMPTS} observations; scheduling safe deposit resubmission {}/{}",
                        state.deposit.submission_retry_count, MAX_DEPOSIT_SUBMISSION_RETRIES
                    ));
                    return persist(store, execution_id, state).await;
                }
                Ok(ledger_current) => {
                    state.deposit.ready_to_submit = false;
                    let message = format!(
                        "deposit subaccount balance moved from {ledger_before} to {ledger_current}; automatic deposit replay is unsafe"
                    );
                    recon::require_operator(state, IcpswapStep::DepositPending, message.clone());
                    persist(store, execution_id, state).await?;
                    return Err(message);
                }
                Err(error) => {
                    state.deposit.ready_to_submit = false;
                    let message = format!(
                        "could not verify the deposit subaccount before replay: {error}"
                    );
                    recon::require_operator(state, IcpswapStep::DepositPending, message.clone());
                    persist(store, execution_id, state).await?;
                    return Err(message);
                }
            }
        }

        state.deposit.ready_to_submit = false;
        let message = format!(
            "pending deposit could not be proven after {} balance observations; automatic replay is unsafe: {detail}",
            state.deposit.observation_attempts
        );
        recon::require_operator(state, IcpswapStep::DepositPending, message.clone());
        persist(store, execution_id, state).await?;
        return Err(message);
    }

    state.next_attempt_at_nanos = Some(now_nanos.saturating_add(DEPOSIT_OBSERVATION_RETRY_NANOS));
    state.last_error = Some(format!(
        "deposit observation {}/{} was not confirmed; retrying after 2 seconds: {detail}",
        state.deposit.observation_attempts, MAX_DEPOSIT_OBSERVATION_ATTEMPTS
    ));
    persist(store, execution_id, state).await
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

    let Some(args) = prepare_trade_attempt(client, state, now_nanos).await? else {
        persist(store, execution_id, state).await?;
        return Ok(());
    };

    // The pending attempt and its exact arguments must be durable before the
    // non-idempotent pool call is submitted.
    persist(store, execution_id, state).await?;

    match client.swap(state.plan.pool, &args).await {
        Ok(amount) => {
            recon::complete_trade(state, amount);
            persist(store, execution_id, state).await
        }
        Err(error @ IcpswapClientError::SubmissionUnknown { .. }) => {
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            let settled = match recon::observe_trade(client, state).await? {
                recon::TradeObservation::Succeeded(gross_output) => {
                    recon::complete_trade(state, gross_output);
                    true
                }
                recon::TradeObservation::Unchanged => {
                    recon::handle_unconfirmed_trade(state, now_nanos, recon::UnconfirmedTradeObservation::Unchanged);
                    false
                }
                recon::TradeObservation::Inconsistent => {
                    recon::handle_unconfirmed_trade(state, now_nanos, recon::UnconfirmedTradeObservation::Inconsistent);
                    false
                }
            };
            persist(store, execution_id, state).await?;
            if settled {
                return Ok(());
            }
            return Err(error.to_string());
        }
        Err(IcpswapClientError::Protocol {
            method,
            error: protocol,
        }) => {
            let error = IcpswapClientError::Protocol {
                method,
                error: protocol.clone(),
            };
            state.trade.swap_protocol_error = Some(protocol.clone());
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            return handle_swap_protocol_error(client, store, execution_id, state, error, protocol, now_nanos).await;
        }
        Err(error @ IcpswapClientError::Encode { .. }) => {
            state.step = IcpswapStep::Trade;
            state.trade.swap_args = None;
            state.trade.pending_since_nanos = None;
            state.last_error = Some(error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
        Err(error) => {
            recon::require_operator(state, IcpswapStep::TradePending, error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
}

/// Prepares one swap attempt without submitting it.
///
/// `None` means the current quote violated the immutable output floor and the
/// state was moved to either a delayed retry or recovery. `Some(args)` means
/// all reconciliation baselines and the exact call arguments are recorded in
/// `state` as `TradePending`, ready to be persisted before submission.
async fn prepare_trade_attempt(
    client: &dyn IcpswapManualClient,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<Option<IcpswapSwapArgs>, String> {
    let quote = if state.trade.retry_evaluation_count == 0 {
        state.plan.gross_quoted_out.value.clone()
    } else {
        client
            .requote(
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
    if quote < state.plan.amount_out_minimum.value {
        recon::schedule_trade_retry_or_recovery(
            state,
            now_nanos,
            recon::TradeRetryCause::QuoteBelowHardFloor,
            "fresh quote is below the original hard output floor".to_string(),
        );
        return Ok(None);
    }

    let effective = retry_slippage_bps(state.plan.max_slippage_bps, state.trade.slippage_retry_count);
    let fresh_minimum = amount_out_minimum(&quote, effective).map_err(|error| error.to_string())?;
    let minimum = std::cmp::max(fresh_minimum, state.plan.amount_out_minimum.value.clone());
    state.trade.current_amount_out_minimum =
        ChainTokenAmount::from_raw(state.plan.gross_quoted_out.token.clone(), minimum.clone());

    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.trade.input_pool_balance_before = Some(recon::input_balance(&state.plan, &unused));
    state.trade.output_pool_balance_before = Some(recon::output_balance(&state.plan, &unused));
    let args = IcpswapSwapArgs {
        zero_for_one: state.plan.zero_for_one,
        amount_in: nat_to_decimal_text(&state.plan.amount_in.value),
        amount_out_minimum: nat_to_decimal_text(&minimum),
    };
    state.trade.swap_args = Some(args.clone());
    state.trade.gross_output_amount = None;
    state.trade.swap_protocol_error = None;
    state.trade.next_retry_at_nanos = None;
    state.trade.pending_since_nanos = Some(now_nanos);
    state.trade.unchanged_observations = 0;
    state.step = IcpswapStep::TradePending;
    state.last_error = None;
    Ok(Some(args))
}

async fn handle_swap_protocol_error(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    error: IcpswapClientError,
    protocol: IcpswapError,
    now_nanos: u64,
) -> Result<(), String> {
    let observation = recon::observe_trade(client, state).await?;
    recon::resolve_swap_protocol_error(state, protocol, observation, now_nanos);
    persist(store, execution_id, state).await?;
    Err(error.to_string())
}

async fn withdraw_output(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    _now_nanos: u64,
) -> Result<(), String> {
    let amount = state
        .trade
        .gross_output_amount
        .clone()
        .ok_or_else(|| "manual swap has no confirmed gross output".to_string())?;
    if amount <= state.plan.output_ledger_fee.value {
        state.step = IcpswapStep::Failed;
        state.last_error = Some("gross swap output cannot cover the ckUSDC ledger fee".to_string());
        return persist(store, execution_id, state).await;
    }
    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    state.withdraw.pool_balance_before = Some(recon::output_balance(&state.plan, &unused));
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
    state.step = IcpswapStep::WithdrawPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;
    match client.withdraw(state.plan.pool, &args).await {
        Ok(_) => {
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapClientError::SubmissionUnknown { .. }) => {
            recon::require_operator(state, IcpswapStep::WithdrawPending, error.to_string());
            persist(store, execution_id, state).await?;
            if let Some(wallet_credit) = recon::observe_output_withdrawal(client, state).await? {
                recon::complete_output_withdrawal(state, wallet_credit);
            }
            persist(store, execution_id, state).await?;
            if state.step == IcpswapStep::Completed {
                return Ok(());
            }
            return Err(error.to_string());
        }
        Err(error) => {
            recon::require_operator(state, IcpswapStep::WithdrawPending, error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    if let Some(wallet_credit) = recon::observe_output_withdrawal(client, state).await? {
        recon::complete_output_withdrawal(state, wallet_credit);
    }
    persist(store, execution_id, state).await
}

async fn recover_input(
    client: &dyn IcpswapManualClient,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    _now_nanos: u64,
) -> Result<(), String> {
    let unused = client
        .unused_balance(state.plan.pool, state.owner.owner)
        .await
        .map_err(|error| error.to_string())?;
    let amount = std::cmp::min(
        recon::input_balance(&state.plan, &unused),
        state.plan.amount_in.value.clone(),
    );
    if amount <= state.plan.input_ledger_fee.value {
        state.step = IcpswapStep::Failed;
        state.last_error = Some(format!(
            "recoverable ICP {amount} cannot cover ledger fee {}",
            state.plan.input_ledger_fee.value
        ));
        return persist(store, execution_id, state).await;
    }
    state.recovery.pool_balance_before = Some(recon::input_balance(&state.plan, &unused));
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
    state.step = IcpswapStep::RecoverPending;
    persist(store, execution_id, state).await?;
    match client.withdraw(state.plan.pool, &args).await {
        Ok(_) => {
            persist(store, execution_id, state).await?;
        }
        Err(error @ IcpswapClientError::SubmissionUnknown { .. }) => {
            recon::require_operator(state, IcpswapStep::RecoverPending, error.to_string());
            persist(store, execution_id, state).await?;
            if let Some(wallet_credit) = recon::observe_recovery(client, state).await? {
                recon::complete_recovery(state, wallet_credit);
            }
            persist(store, execution_id, state).await?;
            if state.step == IcpswapStep::Refunded {
                return Ok(());
            }
            return Err(error.to_string());
        }
        Err(error) => {
            recon::require_operator(state, IcpswapStep::RecoverPending, error.to_string());
            persist(store, execution_id, state).await?;
            return Err(error.to_string());
        }
    }
    if let Some(wallet_credit) = recon::observe_recovery(client, state).await? {
        recon::complete_recovery(state, wallet_credit);
    }
    persist(store, execution_id, state).await
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &IcpswapState,
) -> Result<(), String> {
    store.persist(execution_id, state).await
}

pub(crate) fn pool_user_account(pool: Principal, owner: Principal) -> IcrcAccount {
    IcrcAccount {
        owner: pool,
        subaccount: Some(principal_to_subaccount(owner)),
    }
}
