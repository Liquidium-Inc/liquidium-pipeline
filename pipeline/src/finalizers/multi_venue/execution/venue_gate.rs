//! The persistence gate every multi-step venue keeps beside its state, and the
//! verdict on one step. A venue call that may move funds takes two orchestrator
//! cycles: first the persisted intent, then the call.
use std::collections::HashSet;
use std::sync::Mutex;

use log::warn;
use serde::{Deserialize, Serialize};

use crate::utils::now_ts;

/// How long a leg waits for a venue it cannot reach before each failed cycle
/// spends the retry budget. A lasting outage then parks with an alert.
pub const UNREACHABLE_WAIT_SECS: i64 = 40 * 60;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenueGate {
    #[serde(default)]
    pub ready_to_advance: bool,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub operator_required: bool,
    /// When the venue first became unreachable in the current outage.
    #[serde(default)]
    pub unreachable_since_ts: Option<i64>,
}

/// What the gate found when a step asked to run.
pub enum GateOpening {
    /// A new intent is armed. The caller persists it and calls nothing.
    Armed,
    /// The persisted intent is taken. `armed_here` is false after a restart:
    /// another process armed it, and the call may already have run.
    Open { intent_id: String, armed_here: bool },
}

/// Why one step did not complete, in the four kinds every venue shares.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VenueStepError {
    /// The amount is under a route minimum, so the same leg fails again.
    #[error("{0}")]
    BelowMinimum(String),
    /// Money may have moved and nobody can say where it is. An operator decides.
    #[error("{0}")]
    Ambiguous(String),
    /// The request never reached the venue, so the same request later is safe.
    #[error("{0}")]
    Unreachable(String),
    #[error("{0}")]
    Other(String),
}

impl From<String> for VenueStepError {
    fn from(reason: String) -> Self {
        Self::Other(reason)
    }
}

impl From<&str> for VenueStepError {
    fn from(reason: &str) -> Self {
        Self::Other(reason.to_string())
    }
}

/// What the leg does after one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepVerdict {
    /// The step completed, or it waits with no error.
    Continue,
    /// The venue is unreachable within the free wait. No retry budget is spent.
    Wait,
    /// The step failed. The orchestrator retries, and the error spends budget.
    Retry(String),
    /// Only an operator can move the leg on.
    Park(String),
    /// The amount is under a route minimum before any funds moved: the leg ends.
    EndBelowMinimum(String),
}

impl VenueGate {
    /// Clears a park. The next cycle persists a new intent before any call.
    pub fn release_park(&mut self) {
        self.operator_required = false;
        self.ready_to_advance = false;
        self.intent_id = None;
    }

    /// Arms a new intent, or takes the persisted one. The caller closes the
    /// gate once it decides to run the step.
    pub fn open(&mut self, venue_id: &str, armed: &Mutex<HashSet<String>>) -> Result<GateOpening, String> {
        if !self.ready_to_advance {
            self.ready_to_advance = true;
            let intent_id = crate::utils::new_venue_execution_id(&format!("{venue_id}-intent"));
            self.intent_id = Some(intent_id.clone());
            armed
                .lock()
                .map_err(|_| "multi-venue intent lock poisoned".to_string())?
                .insert(intent_id);
            return Ok(GateOpening::Armed);
        }
        let intent_id = self
            .intent_id
            .clone()
            .ok_or_else(|| format!("persisted {venue_id} execution gate has no intent ID"))?;
        let armed_here = armed
            .lock()
            .map_err(|_| "multi-venue intent lock poisoned".to_string())?
            .remove(&intent_id);
        Ok(GateOpening::Open { intent_id, armed_here })
    }

    /// The intent is spent: the step runs now.
    pub fn close(&mut self) {
        self.ready_to_advance = false;
        self.intent_id = None;
    }

    /// The gate as the checkpoint of a saved send writes it: still held by
    /// the intent, so a crash during the send restarts into the saved record.
    pub fn held(&self, intent_id: String) -> Self {
        Self {
            ready_to_advance: true,
            intent_id: Some(intent_id),
            ..self.clone()
        }
    }

    /// Judges one step. `funds_left` says whether a route minimum can still
    /// end the leg cleanly; `venue` and `context` name the leg in the outage log.
    pub fn judge(
        &mut self,
        outcome: Result<(), VenueStepError>,
        funds_left: bool,
        venue: &str,
        context: &str,
    ) -> StepVerdict {
        self.unreachable_since_ts = match &outcome {
            Err(VenueStepError::Unreachable(_)) => self.unreachable_since_ts.or(Some(now_ts())),
            _ => None,
        };
        match outcome {
            Ok(()) => StepVerdict::Continue,
            // No later cycle can raise the amount, so the leg ends here with
            // its funds still in the trader account.
            Err(VenueStepError::BelowMinimum(message)) if !funds_left => StepVerdict::EndBelowMinimum(message),
            // Moved funds must stay tracked. The error spends the retry budget,
            // so the row parks with an alert.
            Err(VenueStepError::BelowMinimum(message)) => StepVerdict::Retry(format!(
                "{message}; the leg's funds already left the trader account, so an operator must settle them"
            )),
            // The request never reached the venue, so a short outage spends no
            // retry budget. A long one does, so it cannot wait without an alert.
            Err(VenueStepError::Unreachable(message)) => {
                let waited = self
                    .unreachable_since_ts
                    .map_or(0, |since| now_ts().saturating_sub(since));
                warn!("[{venue}] {context} venue unreachable waited={waited}s: {message}");
                if waited >= UNREACHABLE_WAIT_SECS {
                    StepVerdict::Retry(message)
                } else {
                    StepVerdict::Wait
                }
            }
            // Only an operator can say which row owns the provider record, or
            // whether a call without an answer moved funds.
            Err(VenueStepError::Ambiguous(message)) => StepVerdict::Park(message),
            Err(VenueStepError::Other(message)) => StepVerdict::Retry(message),
        }
    }
}
