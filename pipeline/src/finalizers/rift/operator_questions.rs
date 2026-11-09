//! What a parked Rift leg asks an operator, and where an answer is written.
//! A park on a Rift status has no question: Rift or its support holds the funds.
use liquidium_pipeline_connectors::backend::rift_backend::RiftBackend;

use super::{finalizer::RiftVenueFinalizer, state::RiftState};
use crate::{
    finalizers::{
        bridge_legs::{send_question, single_open_question, undecided_send},
        multi_venue::OperatorQuestion,
    },
    outbound::driver::{OperatorDecision, OutboundRecord},
};

/// Where the undecided send of a parked leg lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Undecided {
    /// The transfer that funds the bridge wallet.
    Funding,
    /// The minter call that pays the Rift deposit address.
    BridgeCall,
    /// The provider call that starts the return bridge.
    ReturnBridgeCall,
    /// The send to the return bridge's deposit address.
    ReturnBridgeFunding,
}

fn open_questions(state: &RiftState) -> Vec<(Undecided, OperatorQuestion)> {
    let mut questions = Vec::new();
    if let Some(record) = undecided_send(&state.funding) {
        questions.push((Undecided::Funding, send_question("bridge funding", record, "receipt")));
    }
    if let Some(record) = undecided_send(&state.bridge_out.deposit_bridge_call) {
        questions.push((
            Undecided::BridgeCall,
            send_question("bridge call that pays the Rift order", record, "bridge id"),
        ));
    }
    if let Some(record) = undecided_send(&state.bridge_in.withdraw_bridge_call) {
        questions.push((
            Undecided::ReturnBridgeCall,
            send_question("return bridge call", record, "bridge id"),
        ));
    }
    if let Some(record) = undecided_send(&state.bridge_in.withdraw_bridge_funding) {
        questions.push((
            Undecided::ReturnBridgeFunding,
            send_question("return bridge deposit", record, "receipt"),
        ));
    }
    questions
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub(super) fn operator_questions_for(state: &RiftState) -> Vec<OperatorQuestion> {
        open_questions(state)
            .into_iter()
            .map(|(_, question)| question)
            .collect()
    }

    /// Writes the answer where the leg reads it on its next run. A leg with
    /// an open question needs an answer; a leg without one takes a release only.
    pub(super) fn answer_operator(state: &mut RiftState, answer: Option<&OperatorDecision>) -> Result<(), String> {
        let Some((undecided, answer)) = single_open_question(open_questions(state), answer)? else {
            return Ok(());
        };
        let send: &mut Option<OutboundRecord> = match undecided {
            Undecided::Funding => &mut state.funding,
            Undecided::BridgeCall => &mut state.bridge_out.deposit_bridge_call,
            Undecided::ReturnBridgeCall => &mut state.bridge_in.withdraw_bridge_call,
            Undecided::ReturnBridgeFunding => &mut state.bridge_in.withdraw_bridge_funding,
        };
        // The next run asks the chain or the provider first, so the decision
        // is used only while that answer stays open.
        send.as_mut()
            .ok_or_else(|| format!("open question {undecided:?} has no saved state"))?
            .decide(answer);
        Ok(())
    }
}
