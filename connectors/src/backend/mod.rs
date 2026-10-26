/// Error prefix for a call that may have moved funds without a decided
/// answer. Callers must not repeat the call; a provider record or an
/// operator decides what happened.
pub const SIDE_EFFECT_AMBIGUOUS_PREFIX: &str = "side_effect_ambiguous";

pub mod amount_utils;
pub mod bridge_backend;
pub mod cex_backend;
pub mod evm_backend;
pub mod icp_backend;
pub mod icp_backend_helpers;
pub mod solana_backend;
