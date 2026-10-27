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

    /// After a restart, a prepared send that is not safe to repeat may have
    /// gone out. From then on the driver only settles it and never sends it.
    pub fn mark_uncertain(&mut self, now: i64) {
        if self.phase == OutboundPhase::Prepared && !self.prepared.replay_safe {
            self.phase = OutboundPhase::Submitted { at: now };
        }
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
enum Drive {
    /// The send happened. The record is cleared.
    Done { receipt: String },
    /// Come back on the next cycle.
    Wait,
    /// The send did not happen. The record is cleared, and the next cycle
    /// prepares a new send.
    Retry { reason: String },
    /// The receiver refuses the amount as too small. The record is cleared,
    /// and the caller must not send the same amount again.
    BelowMinimum { reason: String },
    /// An operator must decide. The reason carries the ambiguity prefix.
    Park { reason: String },
}

/// Prepares a send and saves it. The send itself runs on the next cycle,
/// after the record is durable.
async fn start_outbound(record: &mut Option<OutboundRecord>, sender: &dyn OutboundSender, intent: &OutboundIntent) -> Drive {
    match sender.prepare(intent).await {
        Ok(prepared) => {
            *record = Some(OutboundRecord::prepared(prepared));
            Drive::Wait
        }
        Err(reason) => Drive::Retry { reason },
    }
}

/// Advances a saved send by one step, and clears it once it is decided.
async fn advance_outbound(record: &mut Option<OutboundRecord>, sender: &dyn OutboundSender, now: i64) -> Drive {
    let Some(saved) = record.as_mut() else {
        return Drive::Retry {
            reason: "no outbound send is saved".to_string(),
        };
    };
    let drive = advance_saved(saved, sender, now).await;
    if matches!(
        drive,
        Drive::Done { .. } | Drive::Retry { .. } | Drive::BelowMinimum { .. }
    ) {
        *record = None;
    }
    drive
}

/// A parked send asks the chain first, so an operator decision is used only
/// while the chain stays undecided.
async fn advance_saved(saved: &mut OutboundRecord, sender: &dyn OutboundSender, now: i64) -> Drive {
    let (outcome, submitted_at) = match &saved.phase {
        OutboundPhase::Prepared => (sender.submit(&saved.prepared).await, now),
        OutboundPhase::Submitted { at } => (sender.settle(&saved.prepared).await, *at),
        OutboundPhase::Parked { reason } => {
            let outcome = sender.settle(&saved.prepared).await;
            if matches!(outcome, SendOutcome::Open { .. }) {
                return decide_parked(saved, reason);
            }
            (outcome, now)
        }
    };
    match outcome {
        SendOutcome::Landed { receipt } => Drive::Done { receipt },
        SendOutcome::NeverLanded { reason } => Drive::Retry { reason },
        SendOutcome::BelowMinimum { reason } => Drive::BelowMinimum { reason },
        SendOutcome::Open { reason } => {
            let window = sender.open_window_secs(&saved.prepared);
            if now.saturating_sub(submitted_at) > window {
                let reason = side_effect_ambiguous(format!(
                    "{} send {} has no decided answer after {window}s: {reason}; record an operator decision",
                    saved.prepared.sender, saved.prepared.identity
                ));
                saved.phase = OutboundPhase::Parked { reason: reason.clone() };
                return Drive::Park { reason };
            }
            saved.phase = OutboundPhase::Submitted { at: submitted_at };
            Drive::Wait
        }
    }
}

/// Why one step did not finish its send.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutboundError {
    /// The send did not happen; the next cycle prepares a new one.
    #[error("{0}")]
    Retry(String),
    /// The send may have run; an operator must decide. The reason carries
    /// the ambiguity prefix for the layers that still read text.
    #[error("{0}")]
    Park(String),
    /// The receiver refuses the amount as too small, so a new send of it fails too.
    #[error("{0}")]
    BelowMinimum(String),
}

impl From<OutboundError> for String {
    fn from(error: OutboundError) -> Self {
        error.to_string()
    }
}

/// One drive of a saved send; `intent` starts a new one.
pub async fn step_outbound(
    record: &mut Option<OutboundRecord>,
    sender: &dyn OutboundSender,
    intent: Option<&OutboundIntent>,
    now: i64,
) -> Result<Option<String>, OutboundError> {
    let drive = match intent {
        Some(intent) => start_outbound(record, sender, intent).await,
        None => advance_outbound(record, sender, now).await,
    };
    match drive {
        Drive::Done { receipt } => Ok(Some(receipt)),
        Drive::Wait => Ok(None),
        Drive::Retry { reason } => Err(OutboundError::Retry(reason)),
        Drive::Park { reason } => Err(OutboundError::Park(reason)),
        Drive::BelowMinimum { reason } => Err(OutboundError::BelowMinimum(reason)),
    }
}

fn decide_parked(saved: &OutboundRecord, reason: &str) -> Drive {
    match &saved.decision {
        Some(OperatorDecision::Landed { receipt }) => Drive::Done {
            receipt: receipt.clone(),
        },
        Some(OperatorDecision::Absent) => Drive::Retry {
            reason: format!(
                "operator confirmed {} send {} did not happen",
                saved.prepared.sender, saved.prepared.identity
            ),
        },
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
    async fn a_too_small_send_clears_its_record_and_says_so() {
        let reason = "under the receiver minimum".to_string();
        let too_small = SendOutcome::BelowMinimum { reason: reason.clone() };
        let sender = sender(too_small, vec![]);
        let mut record = None;

        assert_eq!(
            step_outbound(&mut record, &sender, Some(&intent()), 100).await,
            Ok(None)
        );
        assert_eq!(
            step_outbound(&mut record, &sender, None, 100).await,
            Err(OutboundError::BelowMinimum(reason))
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
