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
    #[allow(dead_code)]
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
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    pub combined_net_edge_bps: f64,
    pub quoted_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VenueLegState {
    pub leg_id: String,
    pub venue_id: String,
    pub request: SwapRequest,
    pub quote: VenueLegQuote,
    pub execution: VenueExecutionState,
    pub status: VenueLegStatus,

    #[serde(default)]
    pub result: Option<SwapExecution>,

    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VenueLegQuote {
    pub pay_amount: ChainTokenAmount,
    pub estimated_receive: ChainTokenAmount,
    pub conservative_receive: ChainTokenAmount,
    pub estimated_slippage_bps: f64,
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
    pub estimated_slippage_bps: Option<f64>,
    pub route_id: String,
}
