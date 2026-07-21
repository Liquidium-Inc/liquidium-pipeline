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
            approval_created_at: None,
            pool_transaction_start: None,
            pool_transaction_id: None,
            settlement_ledger_block_index: None,
            refund_transaction_id: None,
            refund_ledger_block_index: None,
            recovery_amount: None,
            recovery_transaction_start: None,
            recovery_transaction_id: None,
            recovery_ledger_block_index: None,
            recovery_submitted_at: None,
            returned_gross_amount: None,
            recovery_destination: None,
            recovery_transfer_amount: None,
            recovery_transfer_created_at: None,
            recovery_transfer_block_index: None,
            submitted_at: None,
            recovery_attempted: false,
            last_error: None,
        }
    }
}
