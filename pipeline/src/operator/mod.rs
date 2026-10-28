//! The daemon's operator API: requests about parked rows that the control
//! socket carries, and the console inside the daemon that serves them.
mod desk;
mod request;

pub use desk::OperatorDesk;
pub use request::{OperatorConsole, ReconcileAction, ReconcileRequest};
