//! Moves one outbound send through prepare, submit and settle.
//!
//! The record travels with the step's saved state, so the identity of a send
//! is durable before the send runs, and a restart resumes the same send.
use liquidium_pipeline_core::outbound::{
    OutboundIntent, OutboundSender, PreparedSend, SendOutcome, side_effect_ambiguous,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboundPhase {
    /// Saved and not sent yet.
    Prepared,
    /// Sent without a decided answer, at this unix time in seconds.
    Submitted { at: i64 },
    /// The open window ran out while the chain stayed undecided.
    Parked { reason: String },
}

/// What an operator found. A parked send moves on only with one of these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorDecision {
    /// The send happened; `receipt` is its proof.
    Landed { receipt: String },
    /// The send did not happen.
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundRecord {
    pub prepared: PreparedSend,
    pub phase: OutboundPhase,
    #[serde(default)]
    pub decision: Option<OperatorDecision>,
}

impl OutboundRecord {
    /// A send saved for the next cycle.
    pub fn prepared(prepared: PreparedSend) -> Self {
        Self {
            prepared,
            phase: OutboundPhase::Prepared,
            decision: None,
        }
    }

    /// True when a restart may resume this send without an operator: it was
    /// sent already, or sending it again cannot move funds twice.
    pub fn resumable(&self) -> bool {
        self.prepared.replay_safe || self.phase != OutboundPhase::Prepared
    }

    /// True when only an operator can decide the send: it parked, or it may
    /// have run before a restart and a second run is not safe.
    pub fn needs_operator(&self) -> bool {
        matches!(self.phase, OutboundPhase::Parked { .. }) || !self.resumable()
    }

    /// Records an operator's decision. The next run asks the chain first and
    /// uses the decision only while the chain stays undecided.
    pub fn decide(&mut self, decision: OperatorDecision) {
        if !matches!(self.phase, OutboundPhase::Parked { .. }) {
            self.phase = OutboundPhase::Parked {
                reason: "an operator decided a send that may have run before a restart".to_string(),
            };
        }
        self.decision = Some(decision);
    }
}

/// What the step does after one drive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drive {
    /// The send happened. The record is cleared.
    Done { receipt: String },
    /// Come back on the next cycle.
    Wait,
    /// The send did not happen. The record is cleared, and the next cycle
    /// prepares a new send.
    Retry { reason: String },
    /// An operator must decide. The reason carries the ambiguity prefix.
    Park { reason: String },
}

/// Prepares a send and saves it. The send itself runs on the next cycle,
/// after the record is durable.
pub async fn start_outbound(
    record: &mut Option<OutboundRecord>,
    sender: &dyn OutboundSender,
    intent: &OutboundIntent,
) -> Drive {
    match sender.prepare(intent).await {
        Ok(prepared) => {
            *record = Some(OutboundRecord::prepared(prepared));
            Drive::Wait
        }
        Err(reason) => Drive::Retry { reason },
    }
}

/// Advances a saved send by one step. A parked send asks the chain first, so
/// an operator decision is used only while the chain stays undecided.
pub async fn advance_outbound(record: &mut Option<OutboundRecord>, sender: &dyn OutboundSender, now: i64) -> Drive {
    let Some(current) = record.clone() else {
        return Drive::Retry {
            reason: "no outbound send is saved".to_string(),
        };
    };
    let (outcome, submitted_at) = match &current.phase {
        OutboundPhase::Prepared => (sender.submit(&current.prepared).await, now),
        OutboundPhase::Submitted { at } => (sender.settle(&current.prepared).await, *at),
        OutboundPhase::Parked { reason } => {
            let outcome = sender.settle(&current.prepared).await;
            if matches!(outcome, SendOutcome::Open { .. }) {
                return decide_parked(record, &current, reason);
            }
            (outcome, now)
        }
    };
    match outcome {
        SendOutcome::Landed { receipt } => {
            *record = None;
            Drive::Done { receipt }
        }
        SendOutcome::NeverLanded { reason } => {
            *record = None;
            Drive::Retry { reason }
        }
        SendOutcome::Open { reason } => {
            let window = sender.open_window_secs(&current.prepared);
            if now.saturating_sub(submitted_at) > window {
                let reason = side_effect_ambiguous(format!(
                    "{} send {} has no decided answer after {window}s: {reason}; record an operator decision",
                    current.prepared.sender, current.prepared.identity
                ));
                if let Some(saved) = record.as_mut() {
                    saved.phase = OutboundPhase::Parked { reason: reason.clone() };
                }
                return Drive::Park { reason };
            }
            if let Some(saved) = record.as_mut() {
                saved.phase = OutboundPhase::Submitted { at: submitted_at };
            }
            Drive::Wait
        }
    }
}

/// One drive for a step that reports through `Result`. `intent` starts a new
/// send. It returns the receipt once the send landed and `None` while it waits.
/// An error means a retry with a new send, or a park when it has the prefix.
pub async fn step_outbound(
    record: &mut Option<OutboundRecord>,
    sender: &dyn OutboundSender,
    intent: Option<&OutboundIntent>,
    now: i64,
) -> Result<Option<String>, String> {
    let drive = match intent {
        Some(intent) => start_outbound(record, sender, intent).await,
        None => advance_outbound(record, sender, now).await,
    };
    match drive {
        Drive::Done { receipt } => Ok(Some(receipt)),
        Drive::Wait => Ok(None),
        Drive::Retry { reason } | Drive::Park { reason } => Err(reason),
    }
}

fn decide_parked(record: &mut Option<OutboundRecord>, current: &OutboundRecord, reason: &str) -> Drive {
    match &current.decision {
        Some(OperatorDecision::Landed { receipt }) => {
            let receipt = receipt.clone();
            *record = None;
            Drive::Done { receipt }
        }
        Some(OperatorDecision::Absent) => {
            *record = None;
            Drive::Retry {
                reason: format!(
                    "operator confirmed {} send {} did not happen",
                    current.prepared.sender, current.prepared.identity
                ),
            }
        }
        None => Drive::Park {
            reason: reason.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::account::model::ChainAccount;
    use liquidium_pipeline_core::outbound::{MockOutboundSender, is_side_effect_ambiguous};
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;

    fn intent() -> OutboundIntent {
        OutboundIntent {
            token: ChainToken::Icp {
                ledger: Principal::anonymous(),
                symbol: "ckUSDC".into(),
                decimals: 6,
                fee: Nat::from(10_000u64),
            },
            to: ChainAccount::IcpLedger("00".into()),
            amount_native: Nat::from(5u64),
        }
    }

    fn prepared() -> PreparedSend {
        PreparedSend {
            sender: "test".into(),
            identity: "id-1".into(),
            payload: "{}".into(),
            replay_safe: true,
        }
    }

    fn sender(submit: SendOutcome, settle: Vec<SendOutcome>) -> MockOutboundSender {
        let mut sender = MockOutboundSender::new();
        sender.expect_prepare().returning(|_| Ok(prepared()));
        sender.expect_submit().returning(move |_| submit.clone());
        let mut settle = settle.into_iter();
        sender
            .expect_settle()
            .returning(move |_| settle.next().expect("no further settle expected"));
        sender.expect_open_window_secs().returning(|_| 60);
        sender
    }

    fn open() -> SendOutcome {
        SendOutcome::Open { reason: "rpc timeout".into() }
    }

    #[tokio::test]
    async fn the_send_runs_only_after_its_record_is_saved() {
        let sender = sender(SendOutcome::Landed { receipt: "block-7".into() }, vec![]);
        let mut record = None;

        assert_eq!(start_outbound(&mut record, &sender, &intent()).await, Drive::Wait);
        assert_eq!(record.as_ref().unwrap().phase, OutboundPhase::Prepared);

        assert_eq!(
            advance_outbound(&mut record, &sender, 100).await,
            Drive::Done { receipt: "block-7".into() }
        );
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn an_open_send_is_settled_each_cycle_then_parks_after_its_window() {
        let sender = sender(open(), vec![open(), open()]);
        let mut record = None;
        start_outbound(&mut record, &sender, &intent()).await;

        assert_eq!(advance_outbound(&mut record, &sender, 100).await, Drive::Wait);
        assert_eq!(record.as_ref().unwrap().phase, OutboundPhase::Submitted { at: 100 });
        assert_eq!(advance_outbound(&mut record, &sender, 150).await, Drive::Wait);

        let Drive::Park { reason } = advance_outbound(&mut record, &sender, 161).await else {
            panic!("the window ran out");
        };
        assert!(is_side_effect_ambiguous(&reason), "{reason}");
        assert!(matches!(record.as_ref().unwrap().phase, OutboundPhase::Parked { .. }));
    }

    #[tokio::test]
    async fn a_proven_failure_clears_the_record_for_a_new_send() {
        let sender = sender(open(), vec![SendOutcome::NeverLanded { reason: "expired".into() }]);
        let mut record = None;
        start_outbound(&mut record, &sender, &intent()).await;
        advance_outbound(&mut record, &sender, 100).await;

        assert_eq!(
            advance_outbound(&mut record, &sender, 120).await,
            Drive::Retry { reason: "expired".into() }
        );
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn a_parked_send_moves_only_on_the_chain_or_a_recorded_decision() {
        let parked = |decision| {
            Some(OutboundRecord {
                prepared: prepared(),
                phase: OutboundPhase::Parked {
                    reason: "side_effect_ambiguous: undecided".into(),
                },
                decision,
            })
        };

        // A release without a decision parks again.
        let mut record = parked(None);
        let Drive::Park { .. } = advance_outbound(&mut record, &sender(open(), vec![open()]), 500).await else {
            panic!("no decision recorded");
        };
        assert!(record.is_some());

        // Each decision moves it on.
        let mut record = parked(Some(OperatorDecision::Landed { receipt: "block-9".into() }));
        assert_eq!(
            advance_outbound(&mut record, &sender(open(), vec![open()]), 500).await,
            Drive::Done { receipt: "block-9".into() }
        );
        let mut record = parked(Some(OperatorDecision::Absent));
        assert!(matches!(
            advance_outbound(&mut record, &sender(open(), vec![open()]), 500).await,
            Drive::Retry { .. }
        ));
        assert!(record.is_none());

        // The chain outranks the operator: a landed send is never sent again.
        let mut record = parked(Some(OperatorDecision::Absent));
        assert_eq!(
            advance_outbound(
                &mut record,
                &sender(open(), vec![SendOutcome::Landed { receipt: "block-3".into() }]),
                500
            )
            .await,
            Drive::Done { receipt: "block-3".into() }
        );
    }

    #[tokio::test]
    async fn a_refused_prepare_is_a_plain_retry_with_nothing_saved() {
        let mut sender = MockOutboundSender::new();
        sender.expect_prepare().returning(|_| Err("unsupported token".into()));
        let mut record = None;

        assert_eq!(
            start_outbound(&mut record, &sender, &intent()).await,
            Drive::Retry { reason: "unsupported token".into() }
        );
        assert!(record.is_none());
    }

    #[test]
    fn a_saved_record_round_trips() {
        let record = OutboundRecord {
            prepared: prepared(),
            phase: OutboundPhase::Submitted { at: 42 },
            decision: Some(OperatorDecision::Absent),
        };
        let restored: OutboundRecord = serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
        assert_eq!(restored, record);
    }
}
