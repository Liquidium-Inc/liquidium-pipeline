//! One operator request about a parked row, and the console that serves it.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::outbound::driver::OperatorDecision;

/// One operator request about one row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileRequest {
    pub liq_id: String,
    /// Needed only when more than one leg of the row is parked.
    #[serde(default)]
    pub leg_id: Option<String>,
    pub action: ReconcileAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileAction {
    /// Lists the legs and their open questions. It changes nothing.
    Show,
    /// Releases a parked leg that has no open question.
    Release,
    /// Answers the open question of a parked leg and releases it.
    Answer(OperatorDecision),
}

/// Serves operator requests inside the daemon.
#[async_trait]
pub trait OperatorConsole: Send + Sync {
    async fn reconcile(&self, request: ReconcileRequest) -> Result<String, String>;
}
