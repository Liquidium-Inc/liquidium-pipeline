//! The persisted state of each bridge leg. A venue keeps these inside its own
//! state with `#[serde(flatten)]`, so the field names carry their historical
//! `deposit_` and `withdraw_` prefixes and saved rows keep loading.
use candid::Nat;
use liquidium_pipeline_connectors::backend::bridge_backend::BridgeMintReceipt;
use serde::{Deserialize, Serialize};

use crate::{bridge::legacy::LegacyBridgeSubmitPhase, outbound::driver::OutboundRecord};

/// The leg out: an ICP asset to the bridge wallet, then the provider call.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BridgeOutState {
    /// The provider call that bridges the deposit, saved before it runs.
    #[serde(default)]
    pub deposit_bridge_call: Option<OutboundRecord>,
    /// From rows saved before the outbound driver. It stays until the bridge
    /// step upgrades it, so no save between can drop the call.
    #[serde(
        default,
        rename = "deposit_bridge_submit_phase",
        skip_serializing_if = "Option::is_none"
    )]
    pub legacy_bridge_submit_phase: Option<LegacyBridgeSubmitPhase>,
    /// True on rows saved before the outbound driver, whose build called the
    /// provider without a record. The bridge step clears it.
    #[serde(default = "crate::bridge::legacy::saved_before_driver")]
    pub deposit_bridge_call_unrecorded: bool,
    #[serde(default)]
    pub deposit_planned_asset: Option<String>,
    #[serde(default)]
    pub deposit_planned_network: Option<String>,
    #[serde(default)]
    pub deposit_bridge_required: bool,
    #[serde(default)]
    pub deposit_bridge_id: Option<String>,
    #[serde(default)]
    pub deposit_bridge_submitted_at_ts: Option<i64>,
    #[serde(default)]
    pub deposit_bridge_polled_at_ts: Option<i64>,
    #[serde(default)]
    pub deposit_bridge_destination_snapshot: Option<String>,
    #[serde(default)]
    pub deposit_bridge_submit_amount: Option<f64>,
    #[serde(default)]
    pub deposit_bridge_expected_amount: Option<f64>,
    #[serde(default)]
    pub deposit_bridge_provider_fee_budget_native_units: Option<Nat>,
}

/// The leg back: a foreign asset at the bridge wallet to its ck form on ICP.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BridgeInState {
    #[serde(default)]
    pub withdraw_bridge_mint_receipt: Option<BridgeMintReceipt>,
    #[serde(default)]
    pub withdraw_planned_asset: Option<String>,
    #[serde(default)]
    pub withdraw_planned_network: Option<String>,
    #[serde(default)]
    pub withdraw_bridge_required: bool,
    #[serde(default)]
    pub withdraw_bridge_id: Option<String>,
    #[serde(default)]
    pub withdraw_bridge_submitted_at_ts: Option<i64>,
    #[serde(default)]
    pub withdraw_bridge_polled_at_ts: Option<i64>,
    #[serde(default)]
    pub withdraw_bridge_destination_snapshot: Option<String>,
    /// The provider call that starts the return bridge, saved before it runs.
    /// A lost answer settles from the provider history, never with a new call.
    #[serde(default)]
    pub withdraw_bridge_call: Option<OutboundRecord>,
    /// The send that funds the return bridge, saved before it is sent.
    #[serde(default)]
    pub withdraw_bridge_funding: Option<OutboundRecord>,
    /// The provider handle saved with that send. It turns the receipt into
    /// a bridge id.
    #[serde(default)]
    pub withdraw_bridge_send_handle: Option<String>,
    /// The chain receipt of that send, once it landed.
    #[serde(default)]
    pub withdraw_bridge_funding_txid: Option<String>,
    /// When the send landed. The provider must accept it within a window.
    #[serde(default)]
    pub withdraw_bridge_funded_at_ts: Option<i64>,
    /// How often that send reverted and was made again.
    #[serde(default)]
    pub withdraw_bridge_reverts: u32,
}

impl BridgeInState {
    /// True once a deposit send exists. The source balance then no longer
    /// holds the bridge amount, so the step must not size a new send.
    pub fn deposit_send_started(&self) -> bool {
        self.withdraw_bridge_funding.is_some() || self.withdraw_bridge_funding_txid.is_some()
    }

    /// True while nothing has been saved or sent yet, so the next cycle sizes
    /// the bridge from the funds at the wallet.
    pub fn awaits_funds(&self) -> bool {
        self.withdraw_bridge_id.is_none() && !self.deposit_send_started() && self.withdraw_bridge_call.is_none()
    }
}
