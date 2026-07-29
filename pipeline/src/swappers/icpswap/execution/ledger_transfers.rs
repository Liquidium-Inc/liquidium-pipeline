use candid::Nat;
use icrc_ledger_types::icrc1::transfer::TransferArg;
use liquidium_pipeline_connectors::backend::icp_backend::{IcpBackend, IcrcTransferError};

use super::{
    execution::IcpswapExecutionStateStore,
    reconciliation,
    transfer_state::{IcpswapLedgerTransferState, IcpswapSettlementKind},
    types::{IcpswapState, IcpswapStep},
};
const FUTURE_TIMESTAMP_RETRY_NANOS: u64 = 2_000_000_000;

#[derive(Clone, Copy)]
enum DurableTransferKind {
    Funding,
    Settlement(IcpswapSettlementKind),
}

/// Funds the isolated principal, persisting balances and exact deduplication
/// arguments before the shared trader submits the transfer.
pub(crate) async fn funding_step(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    match state.step {
        IcpswapStep::Funding => prepare_funding(client, store, execution_id, state, now_nanos).await,
        IcpswapStep::FundingPending => {
            submit_transfer(
                client,
                store,
                execution_id,
                state,
                DurableTransferKind::Funding,
                now_nanos,
            )
            .await
        }
        _ => Err(format!("funding cannot handle ICPSwap step {:?}", state.step)),
    }
}

/// Forwards only the execution-specific withdrawal credit. Unrelated child
/// balances are never read as the amount to transfer.
pub(crate) async fn forward_step(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let kind = state
        .settlement
        .kind
        .ok_or_else(|| "ICPSwap forwarding is missing its settlement kind".to_string())?;
    match state.step {
        IcpswapStep::Forward => prepare_forward(client, store, execution_id, state, kind, now_nanos).await,
        IcpswapStep::ForwardPending => {
            submit_transfer(
                client,
                store,
                execution_id,
                state,
                DurableTransferKind::Settlement(kind),
                now_nanos,
            )
            .await
        }
        _ => Err(format!("forwarding cannot handle ICPSwap step {:?}", state.step)),
    }
}

async fn prepare_funding(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let funding = &state.funding;
    let ledger = state.plan.token_in;
    let mut args = funding
        .transfer
        .args
        .clone()
        .ok_or_else(|| "ICPSwap funding is missing its transfer template".to_string())?;
    let (source_before, destination_before) = tokio::try_join!(
        client.icrc1_balance(ledger, &funding.source),
        client.icrc1_balance(ledger, &funding.destination),
    )
    .map_err(|error| error.to_string())?;
    args.created_at_time = Some(now_nanos);
    let transfer = &mut state.funding.transfer;
    transfer.args = Some(args);
    transfer.source_balance_before = Some(source_before);
    transfer.destination_balance_before = Some(destination_before);
    state.step = IcpswapStep::FundingPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;
    submit_transfer(
        client,
        store,
        execution_id,
        state,
        DurableTransferKind::Funding,
        now_nanos,
    )
    .await
}

async fn prepare_forward(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    kind: IcpswapSettlementKind,
    now_nanos: u64,
) -> Result<(), String> {
    let settlement = &state.settlement;
    let destination = settlement.destination;
    let fee = &settlement.fee;
    let (ledger, execution_credit) = match kind {
        IcpswapSettlementKind::Output => (
            state.plan.token_out,
            state
                .withdraw
                .wallet_credited_amount
                .clone()
                .ok_or_else(|| "ICPSwap output settlement has no withdrawal credit".to_string())?,
        ),
        IcpswapSettlementKind::Recovery => (
            state.plan.token_in,
            state
                .recovery
                .wallet_credited_amount
                .clone()
                .ok_or_else(|| "ICPSwap recovery settlement has no withdrawal credit".to_string())?,
        ),
    };
    if execution_credit <= fee.value {
        let message = "ICPSwap settlement credit cannot cover its forwarding fee".to_string();
        reconciliation::require_operator(state, IcpswapStep::Forward, message.clone());
        persist(store, execution_id, state).await?;
        return Err(message);
    }
    let args = TransferArg {
        from_subaccount: state.owner.subaccount,
        to: destination,
        fee: Some(fee.value.clone()),
        created_at_time: Some(now_nanos),
        memo: None,
        amount: execution_credit - fee.value.clone(),
    };
    let (source_before, destination_before) = tokio::try_join!(
        client.icrc1_balance(ledger, &state.owner),
        client.icrc1_balance(ledger, &destination),
    )
    .map_err(|error| error.to_string())?;
    let transfer = &mut state.settlement.transfer;
    transfer.args = Some(args);
    transfer.source_balance_before = Some(source_before);
    transfer.destination_balance_before = Some(destination_before);
    state.step = IcpswapStep::ForwardPending;
    state.last_error = None;
    persist(store, execution_id, state).await?;
    submit_transfer(
        client,
        store,
        execution_id,
        state,
        DurableTransferKind::Settlement(kind),
        now_nanos,
    )
    .await
}

async fn submit_transfer(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    kind: DurableTransferKind,
    now_nanos: u64,
) -> Result<(), String> {
    let ledger = transfer_ledger(state, kind);
    let args = transfer_state(state, kind)
        .args
        .clone()
        .ok_or_else(|| "ICPSwap pending ledger transfer is missing its persisted arguments".to_string())?;
    
    match client.icrc1_transfer_with_args(ledger, args.clone()).await {
        Ok(block_index) => {
            let transfer = transfer_state_mut(state, kind);
            transfer.block_index = Some(block_index);
            transfer.credited_amount = Some(args.amount);
            complete_transfer(state, kind);
            persist(store, execution_id, state).await
        }
        Err(error @ IcrcTransferError::CreatedInFuture { .. }) => {
            let error = format!("ICRC-1 transfer on ledger {ledger} failed: {error}");
            reset_for_fresh_timestamp(state, kind);
            state.next_attempt_at_nanos = Some(now_nanos.saturating_add(FUTURE_TIMESTAMP_RETRY_NANOS));
            state.last_error = Some(error.clone());
            persist(store, execution_id, state).await?;
            Err(error)
        }
        Err(error @ IcrcTransferError::TooOld) => {
            let error = format!("ICRC-1 transfer on ledger {ledger} failed: {error}");
            reconcile_aged_transfer(client, store, execution_id, state, kind, error).await
        }
        Err(error) => {
            let error = format!("ICRC-1 transfer on ledger {ledger} failed: {error}");
            state.last_error = Some(error.clone());
            persist(store, execution_id, state).await?;
            Err(error)
        }
    }
}

async fn reconcile_aged_transfer(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    kind: DurableTransferKind,
    error: String,
) -> Result<(), String> {
    let ledger = transfer_ledger(state, kind);
    let (source, destination) = transfer_accounts(state, kind)?;
    let transfer = transfer_state(state, kind).clone();
    let args = transfer
        .args
        .ok_or_else(|| "aged ICPSwap transfer is missing its arguments".to_string())?;
    let source_before = transfer
        .source_balance_before
        .ok_or_else(|| "aged ICPSwap transfer is missing its source baseline".to_string())?;
    let destination_before = transfer
        .destination_balance_before
        .ok_or_else(|| "aged ICPSwap transfer is missing its destination baseline".to_string())?;
    let (source_now, destination_now) = tokio::try_join!(
        client.icrc1_balance(ledger, &source),
        client.icrc1_balance(ledger, &destination),
    )
    .map_err(|balance_error| balance_error.to_string())?;
    let expected_debit = args.amount.clone() + args.fee.clone().unwrap_or_else(|| Nat::from(0u8));
    let source_debit = saturating_sub(&source_before, &source_now);
    let destination_credit = saturating_sub(&destination_now, &destination_before);

    if source_debit == expected_debit && destination_credit == args.amount {
        let transfer = transfer_state_mut(state, kind);
        transfer.credited_amount = Some(destination_credit);
        complete_transfer(state, kind);
        return persist(store, execution_id, state).await;
    }
    if source_now == source_before && destination_now == destination_before {
        reset_for_fresh_timestamp(state, kind);
        state.last_error = Some(error);
        return persist(store, execution_id, state).await;
    }

    let message = format!(
        "aged ICPSwap transfer has inconsistent balance deltas: source debit {source_debit}, destination credit {destination_credit}"
    );
    reconciliation::require_operator(state, pending_step(kind), message.clone());
    persist(store, execution_id, state).await?;
    Err(message)
}

fn transfer_accounts(
    state: &IcpswapState,
    kind: DurableTransferKind,
) -> Result<
    (
        icrc_ledger_types::icrc1::account::Account,
        icrc_ledger_types::icrc1::account::Account,
    ),
    String,
> {
    match kind {
        DurableTransferKind::Funding => Ok((state.funding.source, state.funding.destination)),
        DurableTransferKind::Settlement(_) => Ok((state.owner, state.settlement.destination)),
    }
}

fn transfer_ledger(state: &IcpswapState, kind: DurableTransferKind) -> candid::Principal {
    match kind {
        DurableTransferKind::Funding | DurableTransferKind::Settlement(IcpswapSettlementKind::Recovery) => {
            state.plan.token_in
        }
        DurableTransferKind::Settlement(IcpswapSettlementKind::Output) => state.plan.token_out,
    }
}

fn transfer_state(state: &IcpswapState, kind: DurableTransferKind) -> &IcpswapLedgerTransferState {
    match kind {
        DurableTransferKind::Funding => &state.funding.transfer,
        DurableTransferKind::Settlement(_) => &state.settlement.transfer,
    }
}

fn transfer_state_mut(state: &mut IcpswapState, kind: DurableTransferKind) -> &mut IcpswapLedgerTransferState {
    match kind {
        DurableTransferKind::Funding => &mut state.funding.transfer,
        DurableTransferKind::Settlement(_) => &mut state.settlement.transfer,
    }
}

fn complete_transfer(state: &mut IcpswapState, kind: DurableTransferKind) {
    state.next_attempt_at_nanos = None;
    state.operator_pending_step = None;
    state.last_error = None;
    state.step = match kind {
        DurableTransferKind::Funding => IcpswapStep::Transfer,
        DurableTransferKind::Settlement(IcpswapSettlementKind::Output) => IcpswapStep::Completed,
        DurableTransferKind::Settlement(IcpswapSettlementKind::Recovery) => IcpswapStep::Refunded,
    };
}

fn reset_for_fresh_timestamp(state: &mut IcpswapState, kind: DurableTransferKind) {
    let transfer = transfer_state_mut(state, kind);
    if let Some(args) = &mut transfer.args {
        args.created_at_time = None;
    }
    transfer.source_balance_before = None;
    transfer.destination_balance_before = None;
    state.step = match kind {
        DurableTransferKind::Funding => IcpswapStep::Funding,
        DurableTransferKind::Settlement(_) => IcpswapStep::Forward,
    };
}

fn pending_step(kind: DurableTransferKind) -> IcpswapStep {
    match kind {
        DurableTransferKind::Funding => IcpswapStep::FundingPending,
        DurableTransferKind::Settlement(_) => IcpswapStep::ForwardPending,
    }
}

fn saturating_sub(left: &Nat, right: &Nat) -> Nat {
    if left > right {
        left.clone() - right.clone()
    } else {
        Nat::from(0u8)
    }
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &IcpswapState,
) -> Result<(), String> {
    store.persist(execution_id, state).await
}
