//! The pure decisions of the withdraw step: how much to withdraw, and what to do after a failed call.
use super::*;

/// What the withdraw step does with a row that has no open withdraw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WithdrawStep {
    /// The row has nothing more to withdraw. It goes to the swap.
    Release,
    /// The pool cannot pay now. The step tries again later, for this reason.
    Wait(&'static str),
    /// The position is empty, but the row still expects tokens. An operator must look at the row.
    ParkEmptyPosition,
    /// The step withdraws this amount.
    Withdraw(Nat),
}

/// The facts that `next_withdraw` decides from. All amounts are in native units of the collateral.
pub(super) struct WithdrawInputs {
    /// What the row still has in the position.
    pub(super) remaining: Nat,
    /// The least withdraw that the step sends.
    pub(super) min_withdraw: Nat,
    /// What the pool can pay now.
    pub(super) liquidity: Nat,
    /// The value of the whole position now. Other held rows of the pool can have shares in it too.
    pub(super) deposited: Nat,
    /// True when no other held row of the pool has shares left.
    pub(super) only_row: bool,
}

pub(super) const NO_FREE_LIQUIDITY: &str = "the pool has no free liquidity";
pub(super) const BELOW_LEAST_WITHDRAW: &str = "the free liquidity is below the least withdraw";

/// Returns the next step for a row without an open withdraw.
/// The rules go in order, and the first rule that matches decides.
pub(super) fn next_withdraw(input: &WithdrawInputs) -> WithdrawStep {
    let WithdrawInputs {
        remaining,
        min_withdraw,
        liquidity,
        deposited,
        only_row,
    } = input;

    // Rule 1: what is left is not more than the least withdraw. The row goes to the swap.
    if remaining <= min_withdraw {
        return WithdrawStep::Release;
    }
    // Rule 2: the pool cannot pay the least withdraw.
    if liquidity <= min_withdraw {
        return WithdrawStep::Wait(NO_FREE_LIQUIDITY);
    }
    // Rule 3: the position is empty, but the row still expects tokens.
    if *deposited == 0u8 {
        return WithdrawStep::ParkEmptyPosition;
    }
    // Rule 4: the last held row of the pool takes the whole position, with the supply interest.
    // The position must be at most 5% above the row amount, because a larger surplus belongs to other rows.
    let interest_limit = remaining.clone() * Nat::from(10_000 + MAX_INTEREST_BPS) / Nat::from(10_000u32);
    if *only_row && liquidity >= deposited && *deposited <= interest_limit {
        return WithdrawStep::Withdraw(deposited.clone());
    }
    // Rule 5: other rows take their own amount, as far as the pool and the position allow.
    let part = remaining.clone().min(liquidity.clone()).min(deposited.clone());
    // Rule 6: a part that is not more than the least withdraw waits.
    if part <= *min_withdraw {
        return WithdrawStep::Wait(BELOW_LEAST_WITHDRAW);
    }
    WithdrawStep::Withdraw(part)
}

/// What the withdraw step does after a call that the canister did not accept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AfterFailure {
    /// The call did not run. The step tries again after `secs`, for the reason in `note`.
    Retry { secs: i64, note: String },
    /// The call did not run, and the reason does not pass. An operator must look at the row.
    Park(String),
    /// The step does not know if the call ran.
    Unknown,
}

/// Returns what the step does after a failed withdraw call.
/// The rules go in order, and the first rule that matches decides.
pub(super) fn after_failure(failure: &WithdrawFailure) -> AfterFailure {
    match failure {
        // Rule 1: a certified reject, for example no liquidity at the time of the call. The position did not change.
        WithdrawFailure::NotExecuted(reason) => AfterFailure::Retry {
            secs: RETRY_SECS,
            note: format!("the canister rejected the withdraw: {reason}"),
        },
        // Rule 2: a refusal that can pass. The position did not change.
        WithdrawFailure::Refused(
            error @ (ProtocolError::NoLiquidity | ProtocolError::PoolFrozen | ProtocolError::HealthFactorTooLow),
        ) => AfterFailure::Retry {
            secs: REFUSED_RETRY_SECS,
            note: format!("the canister refused the withdraw: {error:?}"),
        },
        // Rule 3: any other refusal needs an operator. The position did not change.
        WithdrawFailure::Refused(error) => AfterFailure::Park(format!("the canister refused the withdraw: {error:?}")),
        // Rule 4: a transport error or an unreadable reply. The call possibly ran.
        WithdrawFailure::Ambiguous(_) => AfterFailure::Unknown,
    }
}
