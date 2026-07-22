use icrc_ledger_types::icrc1::account::Account;

use super::{
    plan::amount_out_minimum,
    types::{
        IcpswapDepositState, IcpswapExecutionPlan, IcpswapManualRecoveryState, IcpswapState, IcpswapStep,
        IcpswapTradeState, IcpswapWithdrawState,
    },
};

impl IcpswapState {
    pub fn prepare(execution_id: impl Into<String>, plan: IcpswapExecutionPlan, owner: Account) -> Self {
        let initial_slippage = initial_slippage_bps(plan.max_slippage_bps);
        let initial_minimum_value = amount_out_minimum(&plan.gross_quoted_out.value, initial_slippage)
            .expect("validated plan slippage must remain valid");
        let initial_minimum = liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
            plan.gross_quoted_out.token.clone(),
            std::cmp::max(initial_minimum_value, plan.amount_out_minimum.value.clone()),
        );
        Self {
            execution_id: execution_id.into(),
            owner,
            step: IcpswapStep::Deposit,
            operator_pending_step: None,
            last_error: None,
            plan: plan.clone(),
            deposit: IcpswapDepositState::default(),
            trade: IcpswapTradeState {
                original_hard_minimum_out: plan.amount_out_minimum.clone(),
                retry_count: 0,
                effective_slippage_bps: initial_slippage,
                current_quote: Some(plan.gross_quoted_out.clone()),
                current_amount_out_minimum: initial_minimum,
                next_retry_at_nanos: None,
                input_pool_balance_before: None,
                output_pool_balance_before: None,
                swap_args: None,
                swap_returned_amount: None,
                swap_protocol_error: None,
                swap_submitted_at: None,
            },
            withdraw: IcpswapWithdrawState::default(),
            recovery: IcpswapManualRecoveryState::default(),
        }
    }
}

pub const INITIAL_MANUAL_SLIPPAGE_BPS: u32 = 125;
pub const MAX_MANUAL_SLIPPAGE_RETRIES: u32 = 3;

pub fn initial_slippage_bps(cap: u32) -> u32 {
    cap.min(INITIAL_MANUAL_SLIPPAGE_BPS)
}

pub fn retry_slippage_bps(cap: u32, retry_count: u32) -> u32 {
    let retry_count = retry_count.min(MAX_MANUAL_SLIPPAGE_RETRIES);
    let initial = initial_slippage_bps(cap);
    if cap <= initial || retry_count == 0 {
        return initial;
    }
    let width = cap - initial;
    initial + width.saturating_mul(retry_count).div_ceil(MAX_MANUAL_SLIPPAGE_RETRIES)
}

pub fn retry_backoff_nanos(retry_count: u32) -> u64 {
    match retry_count {
        0 => 0,
        1 => 2_000_000_000,
        2 => 4_000_000_000,
        _ => 8_000_000_000,
    }
}
