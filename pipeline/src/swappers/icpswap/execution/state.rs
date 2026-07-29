use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::account::icp_account::RECOVERY_ACCOUNT;

use super::{
    identity::IcpswapExecutionIdentity,
    plan::amount_out_minimum,
    transfer_state::{IcpswapFundingState, IcpswapSettlementState},
    types::{
        ICPSWAP_STATE_VERSION, IcpswapDepositState, IcpswapExecutionPlan, IcpswapRecoveryState, IcpswapState,
        IcpswapStep, IcpswapTradeState, IcpswapTransferState, IcpswapWithdrawState,
    },
};

impl IcpswapState {
    /// Constructs the sole supported isolated-principal workflow.
    pub fn prepare(
        execution_id: impl Into<String>,
        plan: IcpswapExecutionPlan,
        identity: IcpswapExecutionIdentity,
        funding: IcpswapFundingState,
        settlement: IcpswapSettlementState,
    ) -> Result<Self, String> {
        identity.validate_descriptor()?;
        let owner = Account {
            owner: identity.principal,
            subaccount: None,
        };
        if funding.destination != owner {
            return Err("ICPSwap funding destination does not match the derived execution principal".to_string());
        }
        if funding.source.subaccount.is_some() || funding.destination.subaccount.is_some() {
            return Err("ICPSwap funding requires default ledger accounts".to_string());
        }
        let initial_slippage = initial_slippage_bps(plan.max_slippage_bps);
        let initial_minimum_value = amount_out_minimum(&plan.gross_quoted_out.value, initial_slippage)
            .expect("validated plan slippage must remain valid");
        let initial_minimum = liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
            plan.gross_quoted_out.token.clone(),
            std::cmp::max(initial_minimum_value, plan.amount_out_minimum.value.clone()),
        );
        Ok(Self {
            execution_id: execution_id.into(),
            owner,
            schema_version: ICPSWAP_STATE_VERSION,
            step: IcpswapStep::Funding,
            operator_pending_step: None,
            last_error: None,
            next_attempt_at_nanos: None,
            plan,
            identity,
            funding,
            settlement,
            transfer: IcpswapTransferState::default(),
            deposit: IcpswapDepositState::default(),
            trade: IcpswapTradeState {
                retry_evaluation_count: 0,
                slippage_retry_count: 0,
                current_amount_out_minimum: initial_minimum,
                next_retry_at_nanos: None,
                pending_since_nanos: None,
                unchanged_observations: 0,
                input_pool_balance_before: None,
                output_pool_balance_before: None,
                swap_args: None,
                gross_output_amount: None,
                swap_protocol_error: None,
            },
            withdraw: IcpswapWithdrawState::default(),
            recovery: IcpswapRecoveryState::default(),
        })
    }
}

pub fn validate_execution_state(state: &IcpswapState, execution_id: &str) -> Result<(), String> {
    if state.execution_id != execution_id {
        return Err(format!(
            "ICPSwap execution ID {} differs from state-store key {execution_id}",
            state.execution_id
        ));
    }
    if state.owner.subaccount.is_some() {
        return Err("ICPSwap execution requires the owner's default ledger account".to_string());
    }

    if state.schema_version != ICPSWAP_STATE_VERSION {
        return Err(format!(
            "unsupported ICPSwap state version {}; expected {ICPSWAP_STATE_VERSION}",
            state.schema_version
        ));
    }
    validate_state_fields(state)
}

fn validate_state_fields(state: &IcpswapState) -> Result<(), String> {
    state.identity.validate_descriptor()?;
    if state.owner.owner != state.identity.principal {
        return Err("ICPSwap owner does not match its derived execution principal".to_string());
    }
    if state.funding.destination != state.owner {
        return Err("ICPSwap funding destination does not match its execution owner".to_string());
    }
    if state.funding.source.subaccount.is_some() || state.funding.destination.subaccount.is_some() {
        return Err("ICPSwap funding requires default ledger accounts".to_string());
    }
    if state.funding.surplus_destination
        != (Account {
            owner: state.funding.source.owner,
            subaccount: Some(*RECOVERY_ACCOUNT),
        })
    {
        return Err("ICPSwap funding surplus destination is not the trader recovery account".to_string());
    }
    if state.settlement.kind == Some(super::transfer_state::IcpswapSettlementKind::OutputRecovery)
        && state.settlement.destination != state.funding.surplus_destination
    {
        return Err("ICPSwap output recovery is not addressed to the trader recovery account".to_string());
    }
    Ok(())
}

/// Tight slippage tolerance used by the initial swap attempt. If the configured
/// hard cap is lower than 125 bps, the configured cap wins.
pub const INITIAL_MANUAL_SLIPPAGE_BPS: u32 = 125;

/// Maximum number of automatic trade retry evaluations after the initial
/// attempt. A retry can follow confirmed slippage, an unchanged ambiguous
/// submission, or a fresh quote that cannot satisfy the original hard floor.
pub const MAX_MANUAL_TRADE_RETRIES: u32 = 3;

/// Read-only pool-balance observations allowed after an ambiguous deposit.
pub const MAX_DEPOSIT_OBSERVATION_ATTEMPTS: u32 = 4;

/// Deposit-only resubmissions allowed when the ledger subaccount proves that
/// the preceding canister call did not sweep any funds.
pub const MAX_DEPOSIT_SUBMISSION_RETRIES: u32 = 3;

/// Delay between ambiguous-deposit balance observations.
pub const DEPOSIT_OBSERVATION_RETRY_NANOS: u64 = 2_000_000_000;

/// Number of confirmed-slippage widening steps between the initial tolerance
/// and the configured hard cap. This is independent of the general retry
/// budget so non-slippage retries cannot widen the accepted price range.
pub const MAX_MANUAL_SLIPPAGE_STEPS: u32 = 3;

/// How long an ambiguous swap may show no pool-balance movement before it can
/// be considered for a safe replay at the existing slippage tolerance.
pub const PENDING_TRADE_RECONCILIATION_TIMEOUT_NANOS: u64 = 120_000_000_000;

/// Independent unchanged-balance reads required before an ambiguous swap can
/// be retried, preventing a single stale query from triggering resubmission.
pub const MIN_UNCHANGED_TRADE_OBSERVATIONS: u32 = 2;

pub fn initial_slippage_bps(cap: u32) -> u32 {
    cap.min(INITIAL_MANUAL_SLIPPAGE_BPS)
}

pub fn retry_slippage_bps(cap: u32, slippage_retry_count: u32) -> u32 {
    let slippage_retry_count = slippage_retry_count.min(MAX_MANUAL_SLIPPAGE_STEPS);
    let initial = initial_slippage_bps(cap);
    if cap <= initial || slippage_retry_count == 0 {
        return initial;
    }
    let width = cap - initial;
    initial
        + width
            .saturating_mul(slippage_retry_count)
            .div_ceil(MAX_MANUAL_SLIPPAGE_STEPS)
}

pub fn retry_backoff_nanos(retry_evaluation_count: u32) -> u64 {
    match retry_evaluation_count {
        0 => 0,
        1 => 2_000_000_000,
        2 => 4_000_000_000,
        _ => 8_000_000_000,
    }
}
