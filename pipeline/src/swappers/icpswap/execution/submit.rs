use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use crate::{
    persistance::{VenueExecutionState, WalStore},
    wal::{decode_receipt_wrapper, encode_meta},
};

use super::{
    client::IcpswapExecutionClient,
    plan::{nat_to_decimal_text, required_allowance},
    types::{
        IcpswapApprovalRequest, IcpswapDepositAndSwapArgs, IcpswapExecutionClientError, IcpswapExecutionError,
        IcpswapExecutionPhase, IcpswapExecutionPlan, IcpswapExecutionState,
    },
};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapExecutionStateStore: Send + Sync {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String>;
    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String>;
}

pub struct WalIcpswapExecutionStateStore<'a> {
    wal: &'a dyn WalStore,
}

impl<'a> WalIcpswapExecutionStateStore<'a> {
    pub fn new(wal: &'a dyn WalStore) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for WalIcpswapExecutionStateStore<'_> {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String> {
        let Some(row) = self
            .wal
            .get_result(liquidation_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Err(format!("missing WAL row for liquidation {liquidation_id}"));
        };
        let wrapper = decode_receipt_wrapper(&row)?;
        Ok(wrapper.and_then(|wrapper| match wrapper.venue_execution {
            Some(VenueExecutionState::Icpswap(state)) => Some(state),
            None => None,
        }))
    }

    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        let mut row = self
            .wal
            .get_result(liquidation_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt metadata for liquidation {liquidation_id}"))?;

        if let Some(VenueExecutionState::Icpswap(existing)) = &wrapper.venue_execution
            && existing.plan != state.plan
        {
            return Err(format!(
                "refusing to replace persisted ICPSwap plan for liquidation {liquidation_id}"
            ));
        }

        wrapper.venue_execution = Some(VenueExecutionState::Icpswap(state.clone()));
        encode_meta(&mut row, &wrapper)?;
        self.wal.upsert_result(row).await.map_err(|error| error.to_string())
    }
}

/// Starts a new execution only when no state exists. On retry, the persisted
/// plan and phase win and `new_plan` is ignored.
pub async fn approve_and_submit(
    client: &dyn IcpswapExecutionClient,
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    owner: Account,
    new_plan: Option<IcpswapExecutionPlan>,
    now_nanos: u64,
) -> Result<IcpswapExecutionState, IcpswapExecutionError> {
    let mut state = match store
        .load(liquidation_id)
        .await
        .map_err(IcpswapExecutionError::Persistence)?
    {
        Some(state) => state,
        None => {
            let state = IcpswapExecutionState::planned(new_plan.ok_or(IcpswapExecutionError::MissingPlan)?);
            persist(store, liquidation_id, &state).await?;
            state
        }
    };

    match state.phase {
        IcpswapExecutionPhase::Planned => {
            let spender = pool_spender(state.plan.pool);
            let required = required_allowance(&state.plan);
            let current = client.allowance(state.plan.token_in, &owner, &spender).await?;
            if current < required {
                let approval_created_at = match state.approval_created_at {
                    Some(timestamp) => timestamp,
                    None => {
                        state.approval_created_at = Some(now_nanos);
                        persist(store, liquidation_id, &state).await?;
                        now_nanos
                    }
                };
                let approval = client
                    .approve(IcpswapApprovalRequest {
                        ledger: state.plan.token_in,
                        owner,
                        spender,
                        current_allowance: current,
                        required_allowance: required.clone(),
                        created_at_time: approval_created_at,
                    })
                    .await;
                match approval {
                    Ok(block_index) => state.approval_block_index = Some(block_index),
                    Err(error) => {
                        // An update-call error can be ambiguous. Re-read the
                        // allowance before deciding whether approval failed.
                        let refreshed = client.allowance(state.plan.token_in, &owner, &spender).await?;
                        if refreshed < required {
                            return Err(error.into());
                        }
                    }
                }
            }
            state.phase = IcpswapExecutionPhase::Approved;
            state.last_error = None;
            persist(store, liquidation_id, &state).await?;
        }
        IcpswapExecutionPhase::Approved => {}
        phase => return Err(IcpswapExecutionError::SubmissionAlreadyStarted(phase)),
    }

    let args = IcpswapDepositAndSwapArgs {
        zero_for_one: state.plan.zero_for_one,
        token_in_fee: state.plan.input_ledger_fee.value.clone(),
        token_out_fee: state.plan.output_ledger_fee.value.clone(),
        amount_in: nat_to_decimal_text(&state.plan.amount_in.value),
        amount_out_minimum: nat_to_decimal_text(&state.plan.amount_out_minimum.value),
    };

    if state.pool_transaction_start.is_none() {
        let latest = client.latest_transaction_id(state.plan.pool, owner.owner).await?;
        state.pool_transaction_start = Some(match latest {
            Some(id) => id + candid::Nat::from(1u8),
            None => candid::Nat::from(0u8),
        });
        persist(store, liquidation_id, &state).await?;
    }

    // This write-ahead marker is deliberately persisted before the update
    // call. If the process stops after submission, retries reconcile rather
    // than submitting the swap a second time.
    state.phase = IcpswapExecutionPhase::SubmissionUnknown;
    state.submitted_at = Some(now_nanos);
    state.last_error = None;
    persist(store, liquidation_id, &state).await?;

    match client.deposit_from_and_swap(state.plan.pool, &args).await {
        Ok(gross_output) => {
            state.phase = IcpswapExecutionPhase::AwaitingOutput;
            state.gross_swap_output = Some(ChainTokenAmount::from_raw(
                state.plan.gross_quoted_out.token.clone(),
                gross_output,
            ));
            persist(store, liquidation_id, &state).await?;
            Ok(state)
        }
        Err(error @ IcpswapExecutionClientError::SubmissionUnknown { .. }) => {
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, &state).await?;
            Err(error.into())
        }
        Err(error @ IcpswapExecutionClientError::Protocol { .. }) => {
            // The pool may have deposited funds before returning a swap error;
            // its official flow queues an automatic refund in that case.
            state.phase = IcpswapExecutionPhase::RefundPending;
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, &state).await?;
            Err(error.into())
        }
        Err(error @ IcpswapExecutionClientError::Encode { .. }) => {
            // Encoding failed before the update call was issued, so retrying is safe.
            state.phase = IcpswapExecutionPhase::Approved;
            state.submitted_at = None;
            state.last_error = Some(error.to_string());
            persist(store, liquidation_id, &state).await?;
            Err(error.into())
        }
        Err(error) => Err(error.into()),
    }
}

async fn persist(
    store: &dyn IcpswapExecutionStateStore,
    liquidation_id: &str,
    state: &IcpswapExecutionState,
) -> Result<(), IcpswapExecutionError> {
    store
        .persist(liquidation_id, state)
        .await
        .map_err(IcpswapExecutionError::Persistence)
}

pub fn pool_spender(pool: Principal) -> Account {
    Account {
        owner: pool,
        subaccount: None,
    }
}
