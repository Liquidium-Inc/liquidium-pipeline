use std::collections::HashSet;

use candid::Nat;
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    persistance::VenueExecutionState,
    swappers::model::{SwapExecution, SwapRequest},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiVenueExecutionState {
    pub plan: MultiVenueExecutionPlan,
    pub legs: Vec<VenueLegState>,
    pub outcome: MultiVenueExecutionOutcome,
}

impl MultiVenueExecutionState {
    pub fn validate(&self) -> Result<(), String> {
        let mut leg_ids = HashSet::new();
        let mut venue_ids = HashSet::new();
        let mut allocated = Nat::from(0u8);

        for leg in &self.legs {
            if leg.leg_id.trim().is_empty() {
                return Err("venue leg_id must not be empty".to_string());
            }
            if !leg_ids.insert(leg.leg_id.as_str()) {
                return Err(format!("duplicate venue leg_id `{}`", leg.leg_id));
            }
            if leg.venue_id.trim().is_empty() {
                return Err(format!("venue_id must not be empty for leg `{}`", leg.leg_id));
            }
            if leg.venue_id != leg.execution.venue {
                return Err(format!(
                    "venue mismatch for leg `{}`: leg venue `{}` does not match execution venue `{}`",
                    leg.leg_id, leg.venue_id, leg.execution.venue
                ));
            }
            if leg.request.pay_amount.token != self.plan.total_pay.token {
                return Err(format!(
                    "pay token mismatch for leg `{}`: allocation token does not match plan total_pay token",
                    leg.leg_id
                ));
            }

            allocated = allocated + leg.request.pay_amount.value.clone();

            if self.plan.strategy_id == "icpswap_first" && !venue_ids.insert(leg.venue_id.as_str()) {
                return Err(format!(
                    "strategy `icpswap_first` does not allow multiple legs for venue `{}`",
                    leg.venue_id
                ));
            }
        }

        if allocated != self.plan.total_pay.value {
            return Err(format!(
                "venue leg allocations sum to {allocated}, expected {}",
                self.plan.total_pay.value
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiVenueExecutionPlan {
    pub strategy_id: String,
    pub total_pay: ChainTokenAmount,
    pub receive_asset: AssetId,
    pub debt_repaid: ChainTokenAmount,
    pub allocation_reason: MultiVenueAllocationReason,
    pub min_net_edge_bps: u32,
    /// The floor this plan was actually held to, which for a bad-debt row is
    /// the deliberately negative one rather than `min_net_edge_bps`. Kept as a
    /// separate optional field rather than widening `min_net_edge_bps` to `i32`
    /// so a binary that predates it still reads these rows: a negative value in
    /// the old field would fail to decode, and an undecodable committed row is
    /// far more dangerous than an imprecise one. `None` on rows written before
    /// this field existed.
    #[serde(default)]
    pub enforced_min_net_edge_bps: Option<i32>,
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    pub combined_net_edge_bps: f64,
    pub quoted_at: i64,
}

/// Durable state for one independently executable allocation in a multi-venue plan.
///
/// The planner creates one leg for each selected venue—for example ICPSwap,
/// MEXC, and Kraken in the ordered liquidation waterfall. The complete leg is
/// written to the WAL before execution begins. On every subsequent advance or
/// recovery attempt, the orchestrator reloads this record, dispatches it to the
/// adapter identified by `venue_id`, and atomically checkpoints the returned
/// state back into the parent multi-venue plan.
///
/// `request` and `quote` are the immutable planning commitment: they describe
/// exactly how much collateral belongs to this venue and the output on which
/// route selection was based. `execution`, `status`, `result`, and `last_error`
/// form the mutable lifecycle journal. Venue-specific API details remain inside
/// the opaque `execution` payload rather than leaking into shared persistence
/// types.
///
/// Serialization keeps the leg restart-safe, cloning supports checkpointed
/// state transitions, and equality is used by persistence and recovery tests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VenueLegState {
    /// Stable identifier for this leg within its parent liquidation plan.
    pub leg_id: String,
    /// Stable adapter identifier used to dispatch execution and recovery.
    pub venue_id: String,
    /// Exact amount-scoped swap request committed by the planner.
    pub request: SwapRequest,
    /// Validated planning-time quote and conservative receive commitment.
    pub quote: VenueLegQuote,
    /// Opaque, venue-owned resumable state persisted after every checkpoint.
    pub execution: VenueExecutionState,
    /// Parent-owned lifecycle status controlling advance and recovery behavior.
    pub status: VenueLegStatus,

    /// Actual terminal execution result, populated after completion or recovery.
    /// Older persisted legs deserialize this as `None`.
    #[serde(default)]
    pub result: Option<SwapExecution>,

    /// Most recent venue-specific diagnostic retained for retries and operators.
    /// Older persisted legs deserialize this as `None`.
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VenueLegQuote {
    pub pay_amount: ChainTokenAmount,
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    #[serde(default, alias = "estimated_slippage_bps")]
    pub estimated_price_impact_bps: f64,
    pub route_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VenueLegStatus {
    Planned,
    Running,
    Completed,
    Recovered,
    OperatorRequired,
    FailedPermanent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum MultiVenueAllocationReason {
    SingleVenue {
        venue_id: String,
    },
    PriceImpactSplit,
    RemainderBelowMinimum {
        #[serde(alias = "skipped_venue_id", deserialize_with = "deserialize_venue_ids")]
        skipped_venue_ids: Vec<String>,
        selected_venue_id: String,
    },
    VenueUnavailable {
        selected_venue_id: String,
        unavailable_venue_ids: Vec<String>,
    },
}

fn deserialize_venue_ids<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    Ok(match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(venue_id) => vec![venue_id],
        OneOrMany::Many(venue_ids) => venue_ids,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MultiVenueExecutionOutcome {
    Running,
    Completed,
    Recovered,
    PartialRecovered { failed_leg_ids: Vec<String> },
    OperatorRequired { leg_ids: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiVenueAllocationSnapshot {
    pub strategy_id: String,
    pub total_pay: ChainTokenAmount,
    pub allocations: Vec<VenueAllocationSnapshot>,
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    pub combined_net_edge_bps: f64,
    pub reason: MultiVenueAllocationReason,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VenueAllocationSnapshot {
    pub leg_id: String,
    pub venue_id: String,
    pub pay_amount: ChainTokenAmount,
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    #[serde(default, alias = "estimated_slippage_bps")]
    pub estimated_price_impact_bps: Option<f64>,
    pub route_id: String,
}
