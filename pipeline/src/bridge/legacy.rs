//! Bridge journal types from before the outbound driver. Saved rows are
//! read with them; nothing writes them again.
use serde::Deserialize;

/// The bridge journal's phase from before the outbound driver. It is read
/// so saved rows keep their call, and it is never written again.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum LegacyBridgeSubmitPhase {
    WaitingFunds,
    Submitting(LegacyBridgeSubmitAttempt),
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct LegacyBridgeSubmitAttempt {
    #[serde(default)]
    pub id: String,
    pub asset: String,
    pub destination: String,
    pub amount: f64,
    pub cursor: Option<u64>,
    pub started_at_ts: i64,
    pub called_at_ts: Option<i64>,
    #[serde(default)]
    pub parked_for_operator: bool,
}

impl LegacyBridgeSubmitAttempt {
    /// The call as the claim registry compares it. The fields a lookup does
    /// not compare stay empty.
    pub fn open_call(&self) -> crate::bridge::call_sender::OpenBridgeCall {
        crate::bridge::call_sender::OpenBridgeCall {
            id: self.id.clone(),
            call: crate::bridge::call_sender::BridgeCall {
                asset: self.asset.clone(),
                source_chain: String::new(),
                source_address: String::new(),
                target_asset: String::new(),
                destination: self.destination.clone(),
                amount: self.amount,
                provider_fee_budget_native_units: None,
                cursor: self.cursor,
                started_at_ts: self.started_at_ts,
            },
        }
    }
}
