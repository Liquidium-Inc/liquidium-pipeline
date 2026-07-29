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
    FundingSurplus,
    Settlement(IcpswapSettlementKind),
}

enum IsolatedTransferObservation {
    Confirmed,
    Unchanged,
    Unexpected { observed: Nat, expected: Nat },
}

/// The one action needed to normalize the isolated child account before it can
/// submit the immutable child-to-pool transfer.
enum FundingBalanceIntent {
    /// The child holds exactly the committed pool budget.
    Ready,
    /// Transfer only the missing input from the shared trader.
    TopUp { transfer_amount: Nat },
    /// Return surplus to the trader recovery account. `transfer_amount`
    /// excludes the ledger fee paid by the child.
    RecoverSurplus { transfer_amount: Nat },
    /// The surplus cannot pay its own recovery transfer fee, so record it and
    /// continue without changing the committed pool transfer.
    RetainDust { dust_amount: Nat },
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
        IcpswapStep::Funding => normalize_funding_balance(client, store, execution_id, state, now_nanos).await,
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
        IcpswapStep::FundingSurplusPending => {
            submit_transfer(
                client,
                store,
                execution_id,
                state,
                DurableTransferKind::FundingSurplus,
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

/// Observes the isolated child and executes the one normalization intent needed
/// to reach the committed pool budget. Transfer intents are persisted by their
/// respective helpers before submission.
async fn normalize_funding_balance(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    now_nanos: u64,
) -> Result<(), String> {
    let ledger = state.plan.token_in;
    let child = state.funding.destination;
    let child_balance = client
        .icrc1_balance(ledger, &child)
        .await
        .map_err(|error| format!("failed reading isolated funding balance: {error}"))?;
    let target = required_child_balance(state);

    match funding_balance_intent(&child_balance, &target, &state.funding.fee.value) {
        FundingBalanceIntent::Ready => mark_funding_ready(store, execution_id, state, None).await,
        FundingBalanceIntent::RetainDust { dust_amount } => {
            mark_funding_ready(store, execution_id, state, Some(dust_amount)).await
        }
        FundingBalanceIntent::TopUp { transfer_amount } => {
            persist_and_submit_top_up(
                client,
                store,
                execution_id,
                state,
                child_balance,
                transfer_amount,
                now_nanos,
            )
            .await
        }
        FundingBalanceIntent::RecoverSurplus { transfer_amount } => {
            persist_and_submit_surplus_recovery(
                client,
                store,
                execution_id,
                state,
                child_balance,
                transfer_amount,
                now_nanos,
            )
            .await
        }
    }
}

/// The child must hold the executable pool input plus two fees: one for its
/// transfer into the pool deposit account and one consumed when the pool sweeps
/// that deposit. The separate trader-to-child fee is paid outside this balance.
fn required_child_balance(state: &IcpswapState) -> Nat {
    state.plan.amount_in.value.clone() + state.funding.fee.value.clone() * Nat::from(2u8)
}

/// Purely classifies the observed balance. Keeping this decision separate from
/// ledger calls makes the intended money movement explicit and easy to test.
fn funding_balance_intent(child_balance: &Nat, target: &Nat, fee: &Nat) -> FundingBalanceIntent {
    if child_balance == target {
        return FundingBalanceIntent::Ready;
    }
    if child_balance < target {
        return FundingBalanceIntent::TopUp {
            transfer_amount: target.clone() - child_balance.clone(),
        };
    }

    let surplus = child_balance.clone() - target.clone();
    if surplus <= fee.clone() {
        FundingBalanceIntent::RetainDust { dust_amount: surplus }
    } else {
        FundingBalanceIntent::RecoverSurplus {
            transfer_amount: surplus - fee.clone(),
        }
    }
}

/// Records that balance normalization is complete before allowing the existing
/// child-to-pool workflow to run.
async fn mark_funding_ready(
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    residual_dust: Option<Nat>,
) -> Result<(), String> {
    state.funding.residual_dust = residual_dust;
    state.step = IcpswapStep::Transfer;
    state.last_error = None;
    persist(store, execution_id, state).await
}

/// Builds and persists the exact trader-to-child top-up intent, then submits
/// it. A retry therefore reuses identical deduplication arguments.
async fn persist_and_submit_top_up(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    child_balance: Nat,
    amount: Nat,
    now_nanos: u64,
) -> Result<(), String> {
    let ledger = state.plan.token_in;
    let source = state.funding.source;
    let source_balance = client
        .icrc1_balance(ledger, &source)
        .await
        .map_err(|error| format!("failed reading shared trader balance before isolated funding: {error}"))?;
    state.funding.transfer = IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: source.subaccount,
            to: state.funding.destination,
            amount,
            fee: Some(state.funding.fee.value.clone()),
            memo: None,
            created_at_time: Some(now_nanos),
        }),
        source_balance_before: Some(source_balance),
        destination_balance_before: Some(child_balance),
        ..Default::default()
    };
    state.step = IcpswapStep::FundingPending;
    state.last_error = None;

    // This is the WAL boundary: the exact intent and both reconciliation
    // baselines must be durable before the ledger sees the transfer.
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

/// Builds and persists a child-to-recovery intent, then submits it. The intent
/// removes only usable surplus; its total child debit is `amount + fee`, leaving
/// the exact target.
async fn persist_and_submit_surplus_recovery(
    client: &dyn IcpBackend,
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    child_balance: Nat,
    amount: Nat,
    now_nanos: u64,
) -> Result<(), String> {
    let ledger = state.plan.token_in;
    let child = state.funding.destination;
    let recovery = state.funding.surplus_destination;
    let recovery_balance = client
        .icrc1_balance(ledger, &recovery)
        .await
        .map_err(|error| format!("failed reading recovery account before ICPSwap surplus transfer: {error}"))?;
    state.funding.surplus_transfer = IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: child.subaccount,
            to: recovery,
            amount,
            fee: Some(state.funding.fee.value.clone()),
            memo: None,
            created_at_time: Some(now_nanos),
        }),
        source_balance_before: Some(child_balance),
        destination_balance_before: Some(recovery_balance),
        ..Default::default()
    };
    state.funding.residual_dust = None;
    state.step = IcpswapStep::FundingSurplusPending;
    state.last_error = None;

    // Persist the recovery intent before submission for the same lost-response
    // and restart guarantees as ordinary funding.
    persist(store, execution_id, state).await?;
    submit_transfer(
        client,
        store,
        execution_id,
        state,
        DurableTransferKind::FundingSurplus,
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
            settlement
                .recovery_credit
                .clone()
                .or_else(|| state.recovery.wallet_credited_amount.clone())
                .ok_or_else(|| "ICPSwap recovery settlement has no withdrawal credit".to_string())?,
        ),
        IcpswapSettlementKind::OutputRecovery => (
            state.plan.token_out,
            settlement
                .recovery_credit
                .clone()
                .ok_or_else(|| "ICPSwap output recovery has no remaining credit".to_string())?,
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
    let (source_now, destination_now) = tokio::join!(
        client.icrc1_balance(ledger, &source),
        client.icrc1_balance(ledger, &destination),
    );
    let expected_debit = args.amount.clone() + args.fee.clone().unwrap_or_else(|| Nat::from(0u8));
    let (observation, shared_delta) = match kind {
        DurableTransferKind::Funding => {
            let destination_now = destination_now
                .map_err(|error| format!("failed reading isolated funding destination balance: {error}"))?;
            let destination_credit = saturating_sub(&destination_now, &destination_before);
            let destination_unchanged = destination_now == destination_before;
            let source_debit = source_now
                .ok()
                .map(|source_now| saturating_sub(&source_before, &source_now));
            (
                classify_isolated_delta(destination_credit, args.amount.clone(), destination_unchanged),
                source_debit,
            )
        }
        DurableTransferKind::FundingSurplus | DurableTransferKind::Settlement(_) => {
            let source_now =
                source_now.map_err(|error| format!("failed reading isolated settlement source balance: {error}"))?;
            let source_debit = saturating_sub(&source_before, &source_now);
            let source_unchanged = source_now == source_before;
            let destination_credit = destination_now
                .ok()
                .map(|destination_now| saturating_sub(&destination_now, &destination_before));
            (
                classify_isolated_delta(source_debit, expected_debit, source_unchanged),
                destination_credit,
            )
        }
    };

    match observation {
        IsolatedTransferObservation::Confirmed => {
            tracing::debug!(execution_id, shared_delta = ?shared_delta, "Confirmed aged ICPSwap transfer");
            let transfer = transfer_state_mut(state, kind);
            transfer.credited_amount = Some(args.amount);
            complete_transfer(state, kind);
            persist(store, execution_id, state).await
        }
        IsolatedTransferObservation::Unchanged => {
            reset_for_fresh_timestamp(state, kind);
            state.last_error = Some(error);
            persist(store, execution_id, state).await
        }
        IsolatedTransferObservation::Unexpected { observed, expected }
            if matches!(kind, DurableTransferKind::Funding | DurableTransferKind::FundingSurplus) =>
        {
            // This principal belongs only to this liquidation. Re-entering
            // Funding safely normalizes its actual balance by topping up a
            // shortage or sweeping an excess before the immutable pool trade.
            reset_for_fresh_timestamp(state, kind);
            state.last_error = Some(format!(
                "aged ICPSwap funding observed isolated delta {observed}, expected {expected}; re-normalizing child balance"
            ));
            persist(store, execution_id, state).await
        }
        IsolatedTransferObservation::Unexpected { observed, expected } => {
            recover_unsettled_credit(store, execution_id, state, kind, observed, expected, shared_delta).await
        }
    }
}

/// An isolated child can safely recover the unspent portion of an ambiguous
/// settlement. The original intent and observed debit remain in the WAL while
/// only the mathematically remaining execution credit is redirected.
async fn recover_unsettled_credit(
    store: &dyn IcpswapExecutionStateStore,
    execution_id: &str,
    state: &mut IcpswapState,
    kind: DurableTransferKind,
    observed: Nat,
    expected: Nat,
    shared_delta: Option<Nat>,
) -> Result<(), String> {
    let DurableTransferKind::Settlement(settlement_kind) = kind else {
        return Err("ICPSwap funding discrepancy reached settlement recovery".to_string());
    };
    let remaining = saturating_sub(&expected, &observed);
    let message = format!(
        "aged ICPSwap settlement observed isolated debit {observed}, expected {expected}; shared delta {shared_delta:?}; redirecting remaining credit {remaining} to recovery"
    );

    // Keep the first interrupted intent: it contains the original requested
    // destination. A later ambiguous recovery attempt must not erase it.
    if state.settlement.interrupted_transfer.is_none() {
        state.settlement.interrupted_transfer = Some(state.settlement.transfer.clone());
        state.settlement.interrupted_observed_debit = Some(observed.clone());
    }
    state.settlement.recovery_credit = Some(remaining.clone());
    state.settlement.transfer = IcpswapLedgerTransferState::default();
    state.settlement.destination = state.funding.surplus_destination;
    state.settlement.kind = Some(match settlement_kind {
        IcpswapSettlementKind::Output | IcpswapSettlementKind::OutputRecovery => IcpswapSettlementKind::OutputRecovery,
        IcpswapSettlementKind::Recovery => IcpswapSettlementKind::Recovery,
    });
    state.operator_pending_step = None;
    state.next_attempt_at_nanos = None;
    state.last_error = Some(message);

    if remaining <= state.settlement.fee.value {
        state.settlement.residual_dust = Some(remaining);
        state.step = IcpswapStep::Refunded;
    } else {
        state.settlement.residual_dust = None;
        state.step = IcpswapStep::Forward;
    }
    persist(store, execution_id, state).await
}

fn classify_isolated_delta(observed: Nat, expected: Nat, unchanged: bool) -> IsolatedTransferObservation {
    if observed == expected {
        IsolatedTransferObservation::Confirmed
    } else if unchanged {
        IsolatedTransferObservation::Unchanged
    } else {
        IsolatedTransferObservation::Unexpected { observed, expected }
    }
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
        DurableTransferKind::FundingSurplus => Ok((state.owner, state.funding.surplus_destination)),
        DurableTransferKind::Settlement(_) => Ok((state.owner, state.settlement.destination)),
    }
}

fn transfer_ledger(state: &IcpswapState, kind: DurableTransferKind) -> candid::Principal {
    match kind {
        DurableTransferKind::Funding
        | DurableTransferKind::FundingSurplus
        | DurableTransferKind::Settlement(IcpswapSettlementKind::Recovery) => state.plan.token_in,
        DurableTransferKind::Settlement(IcpswapSettlementKind::Output | IcpswapSettlementKind::OutputRecovery) => {
            state.plan.token_out
        }
    }
}

fn transfer_state(state: &IcpswapState, kind: DurableTransferKind) -> &IcpswapLedgerTransferState {
    match kind {
        DurableTransferKind::Funding => &state.funding.transfer,
        DurableTransferKind::FundingSurplus => &state.funding.surplus_transfer,
        DurableTransferKind::Settlement(_) => &state.settlement.transfer,
    }
}

fn transfer_state_mut(state: &mut IcpswapState, kind: DurableTransferKind) -> &mut IcpswapLedgerTransferState {
    match kind {
        DurableTransferKind::Funding => &mut state.funding.transfer,
        DurableTransferKind::FundingSurplus => &mut state.funding.surplus_transfer,
        DurableTransferKind::Settlement(_) => &mut state.settlement.transfer,
    }
}

fn complete_transfer(state: &mut IcpswapState, kind: DurableTransferKind) {
    state.next_attempt_at_nanos = None;
    state.operator_pending_step = None;
    state.last_error = None;
    state.step = match kind {
        DurableTransferKind::Funding => IcpswapStep::Transfer,
        DurableTransferKind::FundingSurplus => IcpswapStep::Funding,
        DurableTransferKind::Settlement(IcpswapSettlementKind::Output) => IcpswapStep::Completed,
        DurableTransferKind::Settlement(IcpswapSettlementKind::Recovery | IcpswapSettlementKind::OutputRecovery) => {
            IcpswapStep::Refunded
        }
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
        DurableTransferKind::Funding | DurableTransferKind::FundingSurplus => IcpswapStep::Funding,
        DurableTransferKind::Settlement(_) => IcpswapStep::Forward,
    };
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
