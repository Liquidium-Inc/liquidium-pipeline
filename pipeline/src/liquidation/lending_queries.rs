//! Read-only queries to the lending canister about one liquidation.
use candid::{Encode, Principal};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{LiquidationInflowRecord, LiquidationResult, ProtocolError};

/// The pool's current view of a liquidation, with the status of each transfer.
pub async fn get_liquidation<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidation_id: u128,
) -> Result<LiquidationResult, String> {
    let args = Encode!(&liquidation_id).map_err(|e| format!("get_liquidation encode error: {e}"))?;
    agent
        .call_query::<Result<LiquidationResult, ProtocolError>>(lending_canister, "get_liquidation", args)
        .await?
        .map_err(|err| format!("get_liquidation error: {err:?}"))
}

/// What the pool still owes for a liquidation. `None` means it owes nothing.
pub async fn get_liquidation_inflow<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidation_id: u128,
) -> Result<Option<LiquidationInflowRecord>, String> {
    let args = Encode!(&liquidation_id).map_err(|e| format!("get_liquidation_inflow encode error: {e}"))?;
    agent
        .call_query::<Option<LiquidationInflowRecord>>(lending_canister, "get_liquidation_inflow", args)
        .await
}
