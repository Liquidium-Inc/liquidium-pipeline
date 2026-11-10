//! What is left of the bridge journal from before the outbound driver.
//! The journal phase itself is gone; no open row carried one on 2026-11-10.

/// The serde default of `deposit_bridge_call_unrecorded`: a row without the
/// field comes from a build that called the provider without a record.
pub fn saved_before_driver() -> bool {
    true
}
