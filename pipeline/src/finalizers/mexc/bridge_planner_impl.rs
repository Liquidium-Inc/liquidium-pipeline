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
        let deps = self
            .bridge
            .as_ref()
            .ok_or_else(|| "bridge dependencies are not configured".to_string())?;

        if source_chain.eq_ignore_ascii_case("ICP") {
            return Ok(deps.config.bridge_ic_source_account.to_string());
        }

        if source_chain.eq_ignore_ascii_case("ETH") {
            return Ok(deps.config.bridge_evm_source_address.clone());
        }

        if source_chain.eq_ignore_ascii_case("BTC") {
            return Ok(deps.config.bridge_btc_source_address.clone());
        }

        Err(format!("unsupported bridge source chain '{}'", source_chain))
    }

    fn bridge_liquidator_principal(&self) -> Principal {
        self.liquidator_principal
    }
}
