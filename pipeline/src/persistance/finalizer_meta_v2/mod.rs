use serde::{Deserialize, Serialize};

mod multi_venue;
mod recovery_sweep;

pub use multi_venue::*;
pub use recovery_sweep::*;

pub const FINALIZER_META_V2_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalizerMetaV2 {
    pub version: u32,

    #[serde(flatten)]
    pub payload: FinalizerMetaPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "state", rename_all = "snake_case")]
pub enum FinalizerMetaPayload {
    MultiVenueSwap(MultiVenueExecutionState),
    RecoverySweep(RecoverySweepState),
}

impl FinalizerMetaV2 {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != FINALIZER_META_V2_VERSION {
            return Err(format!(
                "unsupported finalizer meta version {}; expected {}",
                self.version, FINALIZER_META_V2_VERSION
            ));
        }

        match &self.payload {
            FinalizerMetaPayload::MultiVenueSwap(state) => state.validate(),
            FinalizerMetaPayload::RecoverySweep(state) => state.validate(),
        }
    }
}

#[cfg(test)]
mod tests;
