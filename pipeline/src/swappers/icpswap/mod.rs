// Keep the public module names stable while grouping implementation files by
// lifecycle boundary for auditability.
#[path = "protocol/client.rs"]
pub mod client;
#[path = "execution/store.rs"]
pub mod execution;
#[path = "execution/manual.rs"]
pub mod manual;
#[path = "planning/plan.rs"]
pub mod plan;
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
