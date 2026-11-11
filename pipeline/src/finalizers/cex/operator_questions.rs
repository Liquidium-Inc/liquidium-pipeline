//! What a parked exchange leg asks an operator, and where an answer is written.
//! Each park keeps its undecided action in one place of the leg state.
use liquidium_pipeline_connectors::backend::{bridge_backend::BridgeRouteKind, cex_backend::CexBackend};

use super::finalizer::CexVenueFinalizer;
use crate::{
    finalizers::{cex::state::CexState, multi_venue::OperatorQuestion},
    outbound::driver::{OperatorDecision, OutboundRecord},
};

/// Where the undecided action of a parked leg lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Undecided {
    /// The send that funds the exchange deposit.
    DepositFunding,
    /// The send to a bridge provider's deposit address.
    BridgeFunding,
    /// The exchange withdrawal request.
    Withdrawal,
    /// The provider call that bridges the deposit.
    BridgeCall,
    /// The provider call that starts the return bridge.
    WithdrawBridgeCall,
    /// The ckSOL notice after a landed send: the minter's deposit id.
    DepositId,
}

/// A saved send only an operator can decide: it parked, or it may have run
/// before a restart and running it again is not safe.
fn undecided(record: &Option<OutboundRecord>) -> Option<&OutboundRecord> {
    record.as_ref().filter(|record| record.needs_operator())
}

fn send_question(purpose: &str, record: &OutboundRecord, receipt: &str) -> OperatorQuestion {
    OperatorQuestion {
        detail: format!(
            "did the {purpose} ({} send {}) happen? --landed <{receipt}> | --absent (once it can no longer land; a new send follows)",
            record.prepared.sender, record.prepared.identity
        ),
    }
}

fn open_questions<C: CexBackend>(state: &CexState) -> Vec<(Undecided, OperatorQuestion)> {
    let mut questions = Vec::new();
    if let Some(record) = undecided(&state.deposit.deposit_funding) {
        questions.push((
            Undecided::DepositFunding,
            send_question("exchange deposit funding", record, "receipt"),
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
            send_question("exchange withdrawal", record, "exchange withdrawal id"),
        ));
    }
    if let Some(record) = undecided(&state.deposit.bridge.deposit_bridge_call) {
        questions.push((
            Undecided::BridgeCall,
            send_question("deposit bridge call", record, "bridge id"),
        ));
    }
    if let Some(record) = undecided(&state.withdraw.bridge.withdraw_bridge_call) {
        questions.push((
            Undecided::WithdrawBridgeCall,
            send_question("return bridge call", record, "bridge id"),
        ));
    }
    // The minter answers a lost notice only with `ValueTooSmall` once it has
    // minted, so the deposit id must come from an operator.
    let funds = &state.withdraw.bridge;
    if let (None, None, Some(transfer)) = (
        &funds.withdraw_bridge_id,
        &funds.withdraw_bridge_funding,
        &funds.withdraw_bridge_funding_txid,
    ) && CexVenueFinalizer::<C>::withdraw_route(state)
        .is_some_and(|route| route.route_kind == BridgeRouteKind::SolanaToIcp)
    {
        questions.push((
            Undecided::DepositId,
            OperatorQuestion {
                detail: format!(
                    "which ckSOL deposit holds SOL transfer {transfer}? --landed <deposit id> | --absent (notify the minter again)"
                ),
            },
        ));
    }
    questions
}

impl<C: CexBackend> CexVenueFinalizer<C> {
    pub(super) fn operator_questions_for(state: &CexState) -> Vec<OperatorQuestion> {
        open_questions::<C>(state)
            .into_iter()
            .map(|(_, question)| question)
            .collect()
    }

    /// Writes the answer where the leg reads it on its next run. A leg with
    /// an open question needs an answer; a leg without one takes a release only.
    pub(super) fn answer_operator(state: &mut CexState, answer: Option<&OperatorDecision>) -> Result<(), String> {
        let mut questions = open_questions::<C>(state);
        if questions.len() > 1 {
            let details: Vec<String> = questions.into_iter().map(|(_, question)| question.detail).collect();
            return Err(format!(
                "leg has {} open questions: {}",
                details.len(),
                details.join("; ")
            ));
        }
        let (undecided, question, answer) = match (questions.pop(), answer) {
            (None, None) => return Ok(()),
            (None, Some(_)) => return Err("leg has no open question; release it with --release".to_string()),
            (Some((_, question)), None) => {
                return Err(format!("leg waits for an answer: {}", question.detail));
            }
            (Some((undecided, question)), Some(answer)) => (undecided, question, answer),
        };
        let send = match undecided {
            Undecided::DepositFunding => &mut state.deposit.deposit_funding,
            Undecided::BridgeFunding => &mut state.withdraw.bridge.withdraw_bridge_funding,
            Undecided::Withdrawal => &mut state.withdraw.withdraw_send,
            Undecided::BridgeCall => &mut state.deposit.bridge.deposit_bridge_call,
            Undecided::WithdrawBridgeCall => &mut state.withdraw.bridge.withdraw_bridge_call,
            Undecided::DepositId => return record_deposit_id(state, answer),
        };
        // The next run asks the chain or the exchange first, so the decision is used
        // only while that answer stays open.
        send.as_mut()
            .ok_or_else(|| format!("open question has no saved state: {}", question.detail))?
            .decide(answer.clone());
        Ok(())
    }
}

/// The notice step reads a numeric send handle as the minter's deposit id.
/// `--absent` clears it, so the next notice asks the minter again.
fn record_deposit_id(state: &mut CexState, answer: &OperatorDecision) -> Result<(), String> {
    let handle = &mut state.withdraw.bridge.withdraw_bridge_send_handle;
    *handle = match answer {
        OperatorDecision::Landed { receipt } => {
            let deposit_id = receipt
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("a ckSOL deposit id is a number, not `{receipt}`"))?;
            Some(deposit_id.to_string())
        }
        OperatorDecision::Absent => None,
    };
    Ok(())
}
