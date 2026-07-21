// Keep the public module names stable while grouping implementation files by
// lifecycle boundary for auditability.
#[path = "protocol/client.rs"]
pub mod client;
#[path = "execution/submit.rs"]
pub mod execution;
#[path = "planning/plan.rs"]
pub mod plan;
#[path = "recovery/pool.rs"]
pub mod recovery;
#[path = "recovery/transfer.rs"]
pub mod recovery_transfer;
#[path = "settlement/reconcile.rs"]
pub mod settlement;
#[path = "execution/state.rs"]
pub mod state;
#[path = "protocol/types.rs"]
pub mod types;
#[path = "planning/venue.rs"]
pub mod venue;

#[cfg(test)]
#[path = "tests/execution.rs"]
mod execution_tests;
#[cfg(test)]
#[path = "tests/recovery.rs"]
mod recovery_tests;
#[cfg(test)]
#[path = "tests/recovery_transfer.rs"]
mod recovery_transfer_tests;
#[cfg(test)]
#[path = "tests/settlement.rs"]
mod settlement_tests;
#[cfg(test)]
#[path = "tests/protocol.rs"]
mod tests;
#[cfg(test)]
#[path = "tests/planning.rs"]
mod venue_tests;
