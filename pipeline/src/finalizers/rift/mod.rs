//! Rift as a venue: an ICP asset bridges straight to a Rift order's deposit
//! address, Rift swaps it on Ethereum, and the shared leg back mints the ck
//! form of what it delivered.
pub mod finalizer;
mod multi_venue_adapter;
mod operator_questions;
mod preview;
pub mod runtime;
pub mod state;
