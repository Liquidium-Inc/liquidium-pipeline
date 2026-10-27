//! What a parked ICPSwap leg asks an operator, and where an answer is written.
//! The leg checks its balances again before it uses an answer.
use candid::Nat;

use crate::{
    finalizers::multi_venue::OperatorQuestion,
    outbound::driver::OperatorDecision,
    swappers::icpswap::types::{IcpswapOperatorAnswer, IcpswapState, IcpswapStep},
};

fn question(pending: Option<IcpswapStep>) -> Result<&'static str, String> {
    match pending {
        Some(IcpswapStep::TransferPending) => Ok(
            "did the input transfer to the pool deposit account land? --landed <block index> | --absent (a new transfer follows)",
        ),
        Some(IcpswapStep::DepositPending) => {
            Ok("the pool shows no deposit: --release checks again | --absent (the deposit runs again)")
        }
        Some(IcpswapStep::TradePending) => Ok(
            "did the swap run? --release checks again | --landed <gross output units> | --absent (the input comes back)",
        ),
        Some(IcpswapStep::WithdrawPending) => Ok(
            "did the output withdrawal reach the leg account? --release checks again | --absent (the withdrawal runs again)",
        ),
        Some(IcpswapStep::RecoverPending) => Ok(
            "did the input recovery reach the leg account? --release checks again | --absent (the recovery runs again)",
        ),
        Some(IcpswapStep::Forward) => {
            Ok("the leg account cannot pay its forwarding fee: --absent writes the balance off")
        }
        other => Err(format!("ICPSwap leg parked at {other:?} has no operator answer")),
    }
}

pub(super) fn operator_questions(state: &IcpswapState) -> Result<Vec<OperatorQuestion>, String> {
    if state.step != IcpswapStep::OperatorRequired {
        return Ok(Vec::new());
    }
    Ok(vec![OperatorQuestion {
        detail: question(state.operator_pending_step)?.to_string(),
    }])
}

/// Writes the answer where the next check reads it. A release without an
/// answer only makes the leg check the pool again.
pub(super) fn release(state: &mut IcpswapState, answer: Option<&OperatorDecision>) -> Result<(), String> {
    if state.step != IcpswapStep::OperatorRequired {
        return Err(format!("ICPSwap leg at {:?} is not parked", state.step));
    }
    let pending = state.operator_pending_step;
    let question = question(pending)?;
    state.operator_answer = match (pending, answer) {
        (_, Some(OperatorDecision::Absent)) => Some(IcpswapOperatorAnswer::Absent),
        (Some(IcpswapStep::TransferPending), Some(OperatorDecision::Landed { receipt })) => {
            Some(IcpswapOperatorAnswer::TransferLanded {
                block_index: parse_units(receipt)?,
            })
        }
        (Some(IcpswapStep::TradePending), Some(OperatorDecision::Landed { receipt })) => {
            let output = parse_units(receipt)?;
            if output == 0u8 {
                return Err("a landed swap needs a gross output above zero".to_string());
            }
            Some(IcpswapOperatorAnswer::SwapLanded { output })
        }
        (_, Some(OperatorDecision::Landed { .. })) => {
            return Err(format!("--landed does not answer this park: {question}"));
        }
        // These parks have no balance check to run again.
        (Some(IcpswapStep::TransferPending | IcpswapStep::Forward), None) => {
            return Err(format!("leg waits for an answer: {question}"));
        }
        (_, None) => None,
    };
    // The check runs on the next cycle, not after the old retry wait.
    state.next_attempt_at_nanos = None;
    Ok(())
}

fn parse_units(receipt: &str) -> Result<Nat, String> {
    receipt
        .trim()
        .parse::<Nat>()
        .map_err(|_| format!("`{receipt}` is not a whole number of ledger units"))
}
