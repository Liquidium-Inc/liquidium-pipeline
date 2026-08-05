pub mod runtime;

/// Compatibility exports for callers that still use the historical MEXC name.
/// The implementation now lives in the shared CEX finalizer.
pub mod mexc_finalizer {
    pub use crate::finalizers::cex_finalizer::{
        CexBridgeConfig as MexcBridgeConfig, CexBridgeDependencies as MexcBridgeDependencies,
        CexFinalizer as MexcFinalizer,
    };
}
