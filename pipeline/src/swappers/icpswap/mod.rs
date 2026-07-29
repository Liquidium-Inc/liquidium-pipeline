// Keep the public module names stable while grouping implementation files by
// lifecycle boundary for auditability.
#[path = "protocol/client.rs"]
pub mod client;
#[path = "execution/store.rs"]
pub mod execution;
#[path = "execution/identity.rs"]
pub mod identity;
#[path = "execution/ledger_transfers.rs"]
pub mod ledger_transfers;
#[path = "execution/session.rs"]
pub mod session;
#[path = "execution/transfer_state.rs"]
pub mod transfer_state;
#[path = "execution/manual.rs"]
pub mod manual;
#[path = "planning/plan.rs"]
pub mod plan;
#[path = "execution/reconciliation.rs"]
pub(crate) mod reconciliation;
#[path = "execution/state.rs"]
pub mod state;
#[path = "protocol/types.rs"]
pub mod types;
#[path = "planning/venue.rs"]
pub mod venue;

pub const VENUE_ID: &str = "icpswap";

#[cfg(test)]
#[path = "tests/manual.rs"]
mod manual_tests;
#[cfg(test)]
#[path = "tests/protocol.rs"]
mod tests;
#[cfg(test)]
#[path = "tests/planning.rs"]
mod venue_tests;
#[cfg(test)]
#[path = "tests/state_versioning.rs"]
mod state_versioning_tests;
#[cfg(test)]
#[path = "tests/ledger_transfers.rs"]
mod ledger_transfer_tests;
