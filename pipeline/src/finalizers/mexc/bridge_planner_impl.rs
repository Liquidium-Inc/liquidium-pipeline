use super::*;
use crate::finalizers::bridge_planner::BridgePlanner;

impl<C> BridgePlanner for MexcFinalizer<C>
where
    C: CexBackend,
{
    fn bridge_enabled(&self) -> bool {
        self.bridge.is_some()
    }

    fn resolve_bridge_source_address(&self, source_chain: &str) -> Result<String, String> {
        self.bridge_runtime()?.sources.for_chain(source_chain)
    }
}
