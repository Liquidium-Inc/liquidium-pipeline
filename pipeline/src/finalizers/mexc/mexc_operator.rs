//! What a parked MEXC leg asks an operator, and where an answer is written.
//! Each park keeps its undecided action in one place of the leg state.
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use super::mexc_finalizer::MexcFinalizer;
use crate::finalizers::{
    cex_finalizer::{BridgeSubmitPhase, CexState, UncertainWithdraw},
    multi_venue::OperatorQuestion,
    outbound::{OperatorDecision, OutboundPhase, OutboundRecord},
};

/// Where the undecided action of a parked leg lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Undecided {
    /// The send that funds the MEXC deposit.
    DepositFunding,
    /// The send to a bridge provider's deposit address.
    BridgeFunding,
    /// A MEXC withdrawal request without a decided answer.
    Withdrawal,
    /// A bridge call whose provider record was not found.
    BridgeAttempt,
}

fn parked(record: &Option<OutboundRecord>) -> Option<&OutboundRecord> {
    record
        .as_ref()
        .filter(|record| matches!(record.phase, OutboundPhase::Parked { .. }))
}

fn send_question(purpose: &str, record: &OutboundRecord) -> OperatorQuestion {
    OperatorQuestion {
        detail: format!(
            "did the {purpose} ({} send {}) land? --landed <receipt> | --absent (a new send follows)",
            record.prepared.sender, record.prepared.identity
        ),
    }
}

fn open_questions(state: &CexState) -> Vec<(Undecided, OperatorQuestion)> {
    let mut questions = Vec::new();
    if let Some(record) = parked(&state.deposit.deposit_funding) {
        questions.push((Undecided::DepositFunding, send_question("MEXC deposit funding", record)));
    }
    if let Some(record) = parked(&state.withdraw.bridge.withdraw_bridge_funding) {
        questions.push((Undecided::BridgeFunding, send_question("bridge deposit", record)));
    }
    if state.withdraw.withdraw_uncertain == Some(UncertainWithdraw::Parked) {
        questions.push((
            Undecided::Withdrawal,
            OperatorQuestion {
                detail: format!(
                    "did MEXC withdrawal order {} of {} to {} happen? --landed <MEXC withdrawal id> | --absent (a new request follows)",
                    state.withdraw.withdraw_id.as_deref().unwrap_or("?"),
                    state.withdraw.withdraw_asset.symbol(),
                    state.withdraw.withdraw_address
                ),
            },
        ));
    }
    if let Some(BridgeSubmitPhase::Submitting(attempt)) = &state.deposit.bridge.deposit_bridge_submit_phase
        && attempt.parked_for_operator
    {
        questions.push((
            Undecided::BridgeAttempt,
            OperatorQuestion {
                detail: format!(
                    "did bridge attempt {} ({} {} to {}) reach the provider? --landed <bridge id> | --absent (the provider is searched again, then a new call follows)",
                    attempt.id, attempt.amount, attempt.asset, attempt.destination
                ),
            },
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
        match (undecided, answer) {
            // A parked send asks the chain first, so this decision is used
            // only while the chain stays undecided.
            (Undecided::DepositFunding, _) => {
                state.deposit.deposit_funding.as_mut().ok_or_else(missing)?.decision = Some(answer.clone());
            }
            (Undecided::BridgeFunding, _) => {
                state
                    .withdraw
                    .bridge
                    .withdraw_bridge_funding
                    .as_mut()
                    .ok_or_else(missing)?
                    .decision = Some(answer.clone());
            }
            // The leg then polls this MEXC id in the withdrawal history.
            (Undecided::Withdrawal, OperatorDecision::Landed { receipt }) => {
                state.withdraw.withdraw_id = Some(receipt.clone());
                state.withdraw.withdraw_uncertain = None;
            }
            // A released park submits again only while MEXC still shows nothing.
            (Undecided::Withdrawal, OperatorDecision::Absent) => {}
            (Undecided::BridgeAttempt, OperatorDecision::Landed { receipt }) => {
                Self::attach_bridge_handle(state, receipt.clone());
            }
            // A released park searches the provider again before a new call.
            (Undecided::BridgeAttempt, OperatorDecision::Absent) => {}
        }
        Ok(())
    }
}
