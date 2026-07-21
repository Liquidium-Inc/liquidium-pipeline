use super::types::{IcpswapExecutionPhase, IcpswapExecutionPlan, IcpswapExecutionState};

impl IcpswapExecutionState {
    pub fn planned(plan: IcpswapExecutionPlan) -> Self {
        Self {
            plan,
            phase: IcpswapExecutionPhase::Planned,
            gross_swap_output: None,
            input_balance_before: None,
            output_balance_before: None,
            approval_block_index: None,
            submitted_at: None,
            recovery_attempted: false,
            last_error: None,
        }
    }
}
