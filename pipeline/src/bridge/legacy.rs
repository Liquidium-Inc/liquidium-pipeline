//! Bridge journal types from before the outbound driver. A saved phase stays
//! in the row until the bridge step moves it into a saved call.
use serde::{Deserialize, Serialize};

use crate::bridge::call_sender::{BridgeCall, OpenBridgeCall};

/// The serde default of `deposit_bridge_call_unrecorded`: a row without the
/// field comes from a build that called the provider without a record.
pub fn saved_before_driver() -> bool {
    true
}

/// The bridge journal's phase from before the outbound driver. A save before
/// the upgrade keeps it, so the row does not lose its call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LegacyBridgeSubmitPhase {
    WaitingFunds,
    Submitting(LegacyBridgeSubmitAttempt),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    /// The fields the journal kept. The route fields, which a lookup does not
    /// compare, stay empty until the caller fills them.
    pub fn bridge_call(&self) -> BridgeCall {
        BridgeCall {
            asset: self.asset.clone(),
            source_chain: String::new(),
            source_address: String::new(),
            target_asset: String::new(),
            destination: self.destination.clone(),
            amount: self.amount,
            provider_fee_budget_native_units: None,
            cursor: self.cursor,
            started_at_ts: self.started_at_ts,
        }
    }

    /// The call as the claim registry compares it.
    pub fn open_call(&self) -> OpenBridgeCall {
        OpenBridgeCall {
            id: self.id.clone(),
            call: self.bridge_call(),
        }
    }
}
