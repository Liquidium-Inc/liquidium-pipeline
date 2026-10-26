//! Parent WAL orchestration for committed multi-venue plans.

mod multi_venue_finalizer;
mod operator_desk;
mod parent_leg_checkpoint;

pub use multi_venue_finalizer::*;
pub use operator_desk::OperatorDesk;
