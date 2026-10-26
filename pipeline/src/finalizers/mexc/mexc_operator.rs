//! What a parked MEXC leg asks an operator, and where an answer is written.
//! Each park keeps its undecided action in one place of the leg state.
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use super::mexc_finalizer::MexcFinalizer;
use crate::{
    finalizers::{cex_finalizer::CexState, multi_venue::OperatorQuestion},
    outbound::driver::{OperatorDecision, OutboundRecord},
};

/// Where the undecided action of a parked leg lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Undecided {
    /// The send that funds the MEXC deposit.
    DepositFunding,
    /// The send to a bridge provider's deposit address.
    BridgeFunding,
    /// The MEXC withdrawal request.
    Withdrawal,
    /// The provider call that bridges the deposit.
    BridgeCall,
}

/// A saved send only an operator can decide: it parked, or it may have run
/// before a restart and running it again is not safe.
fn undecided(record: &Option<OutboundRecord>) -> Option<&OutboundRecord> {
    record.as_ref().filter(|record| record.needs_operator())
}

fn send_question(purpose: &str, record: &OutboundRecord, receipt: &str) -> OperatorQuestion {
    OperatorQuestion {
        detail: format!(
            "did the {purpose} ({} send {}) happen? --landed <{receipt}> | --absent (a new send follows)",
            record.prepared.sender, record.prepared.identity
        ),
    }
}

fn open_questions(state: &CexState) -> Vec<(Undecided, OperatorQuestion)> {
    let mut questions = Vec::new();
    if let Some(record) = undecided(&state.deposit.deposit_funding) {
        questions.push((
            Undecided::DepositFunding,
            send_question("MEXC deposit funding", record, "receipt"),
        ));
    }
    if let Some(record) = undecided(&state.withdraw.bridge.withdraw_bridge_funding) {
        questions.push((
            Undecided::BridgeFunding,
            send_question("bridge deposit", record, "receipt"),
        ));
    }
    if let Some(record) = undecided(&state.withdraw.withdraw_send) {
        questions.push((
            Undecided::Withdrawal,
            send_question("MEXC withdrawal", record, "MEXC withdrawal id"),
        ));
    }
    if let Some(record) = undecided(&state.deposit.bridge.deposit_bridge_call) {
        questions.push((
            Undecided::BridgeCall,
            send_question("deposit bridge call", record, "bridge id"),
        ));
    }
    questions
}

impl<C: CexBackend> MexcFinalizer<C> {
    pub(super) fn operator_questions_for(state: &CexState) -> Vec<OperatorQuestion> {
        open_questions(state)
            .into_iter()
            .map(|(_, question)| question)
            .collect()
    }

    /// Writes the answer where the leg reads it on its next run. A leg with
    /// an open question needs an answer; a leg without one takes a release only.
    pub(super) fn answer_operator(state: &mut CexState, answer: Option<&OperatorDecision>) -> Result<(), String> {
        let mut questions = open_questions(state);
        if questions.len() > 1 {
            let details: Vec<String> = questions.into_iter().map(|(_, question)| question.detail).collect();
            return Err(format!(
                "leg has {} open questions: {}",
                details.len(),
                details.join("; ")
            ));
        }
        let (undecided, question) = match (questions.pop(), answer) {
            (None, None) => return Ok(()),
            (None, Some(_)) => return Err("leg has no open question; release it with --release".to_string()),
            (Some((_, question)), None) => {
                return Err(format!("leg waits for an answer: {}", question.detail));
            }
            (Some(open), Some(_)) => open,
        };
        let answer = answer.expect("matched above");
        let missing = || format!("open question has no saved state: {}", question.detail);
        let send = match undecided {
            Undecided::DepositFunding => &mut state.deposit.deposit_funding,
            Undecided::BridgeFunding => &mut state.withdraw.bridge.withdraw_bridge_funding,
            Undecided::Withdrawal => &mut state.withdraw.withdraw_send,
            Undecided::BridgeCall => &mut state.deposit.bridge.deposit_bridge_call,
        };
        // The next run asks the chain or MEXC first, so the decision is used
        // only while that answer stays open.
        send.as_mut().ok_or_else(missing)?.decide(answer.clone());
        Ok(())
    }
}
