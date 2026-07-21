//! Durable transfer of confirmed ICPSwap refunds into the recovery account.
//!
//! The pool sends `gross - fee` to the trader account. Moving that credit to
//! the recovery account costs the same ledger fee once more, so the final
//! transfer amount is `gross - pool_fee - transfer_fee`.
//!
//! Before the ICRC-1 update call, this module persists the destination,
//! amount, `created_at_time`, and `RecoveryTransferSubmitted`. Retrying the
//! exact same request is safe because ICRC-1 deduplication returns the
//! original block as `Duplicate { duplicate_of }`.

use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use super::{
    client::IcpswapRecoveryTransferClient,
    execution::IcpswapExecutionStateStore,
    types::{
        IcpswapExecutionPhase, IcpswapExecutionState, IcpswapRecoveryTransferClientError, IcpswapRecoveryTransferError,
        IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferRequest,
    },
};

/// Advances the confirmed-refund transfer by at most one ICRC-1 call.
///
/// `Refunded` calculates and persists the two-fee result. `Pending` persists
/// the submission marker before calling the ledger. `Submitted` may repeat
/// only the byte-equivalent transfer identified by its persisted timestamp.
pub async fn transfer_to_recovery(
    client: &dyn IcpswapRecoveryTransferClient,
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    trader: Account,
    configured_destination: Account,
    now_nanos: u64,
    timeout_nanos: u64,
) -> Result<IcpswapExecutionState, IcpswapRecoveryTransferError> {
    if trader.subaccount.is_some() {
        return Err(IcpswapRecoveryTransferError::NonRootTraderAccount);
    }

    let mut state = load_state(store, liquidation_id).await?;
    enforce_destination(&state, configured_destination)?;

    match state.phase {
        IcpswapExecutionPhase::Refunded => {
            prepare_transfer(&mut state, configured_destination, now_nanos)?;
            persist(store, liquidation_id, &state).await?;
            if state.phase == IcpswapExecutionPhase::Recovered {
                return Ok(state);
            }
        }
        IcpswapExecutionPhase::RecoveryTransferPending => {}
        IcpswapExecutionPhase::RecoveryTransferSubmitted => {
            if transfer_timed_out(&state, now_nanos, timeout_nanos)? {
                fail_terminal(
                    &mut state,
                    "recovery-account transfer was not confirmed by the ledger before timeout",
                );
                persist(store, liquidation_id, &state).await?;
                return Ok(state);
            }
        }
        IcpswapExecutionPhase::Recovered => return Ok(state),
        phase => return Err(IcpswapRecoveryTransferError::InvalidPhase(phase)),
    }

    let request = transfer_request(&state, trader)?;
    if state.phase == IcpswapExecutionPhase::RecoveryTransferPending {
        // This is the durable intent boundary. If persistence succeeds and the
        // process later stops, the same timestamp makes resubmission deduplicated.
        state.phase = IcpswapExecutionPhase::RecoveryTransferSubmitted;
        state.last_error = None;
        persist(store, liquidation_id, &state).await?;
    }

    match client.transfer_recovered_funds(&request).await {
        Ok(IcpswapRecoveryTransferOutcome::Completed(block_index))
        | Ok(IcpswapRecoveryTransferOutcome::Duplicate(block_index)) => {
            state.recovery_transfer_block_index = Some(block_index);
            state.phase = IcpswapExecutionPhase::Recovered;
            state.last_error = None;
            persist(store, liquidation_id, &state).await?;
            Ok(state)
        }
        Err(error @ IcpswapRecoveryTransferClientError::SubmissionUnknown { .. }) => {
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, &state).await?;
            Err(error.into())
        }
        Err(
            error @ (IcpswapRecoveryTransferClientError::Encode(_)
            | IcpswapRecoveryTransferClientError::Rejected { .. }),
        ) => {
            fail_terminal(&mut state, &error.to_string());
            persist(store, liquidation_id, &state).await?;
            Err(error.into())
        }
    }
}

fn prepare_transfer(
    state: &mut IcpswapExecutionState,
    destination: Account,
    now_nanos: u64,
) -> Result<(), IcpswapRecoveryTransferError> {
    let gross = state
        .returned_gross_amount
        .as_ref()
        .ok_or(IcpswapRecoveryTransferError::MissingReturnedAmount)?;
    if gross.token != state.plan.amount_in.token {
        return Err(IcpswapRecoveryTransferError::InvalidPersistedState(
            "returned amount token differs from the input token",
        ));
    }

    let fee = &state.plan.input_ledger_fee.value;
    let trader_credit = subtract_or_zero(&gross.value, fee);
    let transfer_amount = subtract_or_zero(&trader_credit, fee);

    state.recovery_destination = Some(destination);
    state.recovery_transfer_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        transfer_amount.clone(),
    ));
    state.recovery_transfer_created_at = Some(now_nanos);
    state.recovery_transfer_block_index = None;
    state.last_error = None;
    state.phase = if transfer_amount == Nat::from(0u8) {
        IcpswapExecutionPhase::Recovered
    } else {
        IcpswapExecutionPhase::RecoveryTransferPending
    };
    Ok(())
}

fn transfer_request(
    state: &IcpswapExecutionState,
    trader: Account,
) -> Result<IcpswapRecoveryTransferRequest, IcpswapRecoveryTransferError> {
    let destination = state
        .recovery_destination
        .ok_or(IcpswapRecoveryTransferError::InvalidPersistedState(
            "recovery destination is missing",
        ))?;
    let amount = state
        .recovery_transfer_amount
        .as_ref()
        .ok_or(IcpswapRecoveryTransferError::InvalidPersistedState(
            "recovery transfer amount is missing",
        ))?;
    if amount.token != state.plan.amount_in.token || amount.value == Nat::from(0u8) {
        return Err(IcpswapRecoveryTransferError::InvalidPersistedState(
            "recovery transfer amount is zero or has the wrong token",
        ));
    }
    let created_at_time =
        state
            .recovery_transfer_created_at
            .ok_or(IcpswapRecoveryTransferError::InvalidPersistedState(
                "recovery transfer timestamp is missing",
            ))?;

    Ok(IcpswapRecoveryTransferRequest {
        ledger: state.plan.token_in,
        from: trader,
        to: destination,
        amount: amount.value.clone(),
        fee: state.plan.input_ledger_fee.value.clone(),
        created_at_time,
    })
}

fn enforce_destination(state: &IcpswapExecutionState, configured: Account) -> Result<(), IcpswapRecoveryTransferError> {
    if let Some(persisted) = state.recovery_destination
        && persisted != configured
    {
        return Err(IcpswapRecoveryTransferError::DestinationMismatch { persisted, configured });
    }
    Ok(())
}

fn transfer_timed_out(
    state: &IcpswapExecutionState,
    now_nanos: u64,
    timeout_nanos: u64,
) -> Result<bool, IcpswapRecoveryTransferError> {
    let submitted_at =
        state
            .recovery_transfer_created_at
            .ok_or(IcpswapRecoveryTransferError::InvalidPersistedState(
                "recovery transfer timestamp is missing",
            ))?;
    Ok(now_nanos.saturating_sub(submitted_at) >= timeout_nanos)
}

fn subtract_or_zero(amount: &Nat, fee: &Nat) -> Nat {
    if amount > fee {
        amount.clone() - fee.clone()
    } else {
        Nat::from(0u8)
    }
}

fn fail_terminal(state: &mut IcpswapExecutionState, message: &str) {
    state.phase = IcpswapExecutionPhase::FailedTerminal;
    state.last_error = Some(message.to_string());
}

async fn load_state(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
) -> Result<IcpswapExecutionState, IcpswapRecoveryTransferError> {
    store
        .load(liquidation_id)
        .await
        .map_err(IcpswapRecoveryTransferError::Persistence)?
        .ok_or(IcpswapRecoveryTransferError::MissingState)
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    state: &IcpswapExecutionState,
) -> Result<(), IcpswapRecoveryTransferError> {
    store
        .persist(liquidation_id, state)
        .await
        .map_err(IcpswapRecoveryTransferError::Persistence)
}
