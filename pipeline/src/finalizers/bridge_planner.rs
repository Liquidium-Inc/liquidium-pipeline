use std::str::FromStr;

use alloy::primitives::Address as EvmAddress;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeDestinationKind, resolve_forward_route_by_target, resolve_reverse_route_by_source,
};
use liquidium_pipeline_core::{account::model::ChainAccount, tokens::chain_token::ChainToken};

use crate::finalizers::cex::state::CexState;

/// Reads an ICRC account text or a bare principal.
pub(crate) fn parse_icp_account_like(address: &str) -> Result<Account, String> {
    if let Ok(account) = Account::from_str(address) {
        return Ok(account);
    }
    if let Ok(owner) = Principal::from_text(address) {
        return Ok(Account {
            owner,
            subaccount: None,
        });
    }
    Err(format!(
        "invalid ICP account-like address '{}'; expected Account or principal",
        address
    ))
}

/// The bridge destination for a CEX deposit address of the given kind.
/// Reads an address as a bridge destination of the given kind.
pub(crate) fn bridge_destination(kind: BridgeDestinationKind, address: &str) -> Result<BridgeDestination, String> {
    match kind {
        BridgeDestinationKind::EvmAddress => address
            .parse::<EvmAddress>()
            .map(BridgeDestination::EvmAddress)
            .map_err(|error| format!("invalid EVM address '{address}': {error}")),
        BridgeDestinationKind::IcpAccount => parse_icp_account_like(address).map(BridgeDestination::IcpAccount),
        BridgeDestinationKind::BtcAddress => Ok(BridgeDestination::BtcAddress(address.to_string())),
        BridgeDestinationKind::SolanaAddress => solana_sdk::pubkey::Pubkey::from_str(address)
            .map(|address| BridgeDestination::SolanaAddress(address.to_string()))
            .map_err(|error| format!("invalid Solana address '{address}': {error}")),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BridgeTransportPlan {
    pub cex_asset: String,
    pub cex_network: String,
    pub bridge_required: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct BridgePlan {
    pub deposit: BridgeTransportPlan,
    pub withdraw: BridgeTransportPlan,
}

/// Maps a bridge route destination kind to the CEX network label used by backends.
pub(crate) fn cex_network_from_destination_kind(kind: BridgeDestinationKind) -> String {
    match kind {
        BridgeDestinationKind::IcpAccount => "ICP".to_string(),
        BridgeDestinationKind::EvmAddress => "ETH".to_string(),
        BridgeDestinationKind::BtcAddress => "BTC".to_string(),
        BridgeDestinationKind::SolanaAddress => "SOL".to_string(),
    }
}

pub(crate) trait BridgePlanner {
    /// Returns whether bridge-aware planning should be enabled for this finalizer instance.
    fn bridge_enabled(&self) -> bool;
    /// Resolves the configured bridge source address/account string for a given source chain.
    fn resolve_bridge_source_address(&self, source_chain: &str) -> Result<String, String>;
    /// Whether the exchange lists this ICP asset itself. The bridge route is then not used.
    fn takes_directly(&self, asset: &str) -> bool;
    /// Builds a direct (non-bridged) transport plan from the provided asset.
    fn plan_from_direct_asset(asset: &ChainToken) -> BridgeTransportPlan {
        BridgeTransportPlan {
            cex_asset: asset.symbol(),
            cex_network: asset.chain(),
            bridge_required: false,
        }
    }

    /// Resolves per-leg transport plans (deposit and withdraw), including bridge symbol/network translation.
    fn resolve_bridge_plan_for_assets(&self, deposit_asset: &ChainToken, withdraw_asset: &ChainToken) -> BridgePlan {
        let mut deposit = Self::plan_from_direct_asset(deposit_asset);
        let mut withdraw = Self::plan_from_direct_asset(withdraw_asset);

        if !self.bridge_enabled() {
            return BridgePlan { deposit, withdraw };
        }

        // Deposit side: the ICP asset leaves through its reverse route
        // (ckUSDC@ICP -> USDC@ETH, ckSOL@ICP -> SOL@SOL).
        if !self.takes_directly(&deposit.cex_asset)
            && let Some(route) = resolve_reverse_route_by_source(&deposit.cex_asset, &deposit.cex_network)
        {
            deposit = BridgeTransportPlan {
                cex_asset: route.target_asset.to_string(),
                cex_network: cex_network_from_destination_kind(route.destination_kind),
                bridge_required: true,
            };
        }

        // Withdraw side: the exchange delivers the forward route's source asset.
        if !self.takes_directly(&withdraw.cex_asset)
            && let Some(route) = resolve_forward_route_by_target(&withdraw.cex_asset)
        {
            withdraw = BridgeTransportPlan {
                cex_asset: route.source_asset.to_string(),
                cex_network: route.source_chain.to_string(),
                bridge_required: true,
            };
        }

        BridgePlan { deposit, withdraw }
    }

    /// The bridge destination of the final liquidator for the return bridge.
    fn bridge_destination_for_final_liquidator(
        &self,
        destination_kind: BridgeDestinationKind,
        direct_withdraw_address: &str,
    ) -> Result<BridgeDestination, String> {
        bridge_destination(destination_kind, direct_withdraw_address)
            .map_err(|error| format!("invalid final destination for bridge withdraw: {error}"))
    }

    /// Persists resolved bridge plan fields onto mutable CEX state.
    fn apply_plan_to_state(state: &mut CexState, plan: &BridgePlan) {
        state.deposit.bridge.deposit_planned_asset = Some(plan.deposit.cex_asset.clone());
        state.deposit.bridge.deposit_planned_network = Some(plan.deposit.cex_network.clone());
        state.deposit.bridge.deposit_bridge_required = plan.deposit.bridge_required;

        state.withdraw.bridge.withdraw_planned_asset = Some(plan.withdraw.cex_asset.clone());
        state.withdraw.bridge.withdraw_planned_network = Some(plan.withdraw.cex_network.clone());
        state.withdraw.bridge.withdraw_bridge_required = plan.withdraw.bridge_required;
    }

    /// Ensures state has planned asset/network fields populated, backfilling from current assets when absent.
    fn ensure_plan_on_state(&self, state: &mut CexState) {
        if state.deposit.bridge.deposit_planned_asset.is_some()
            && state.deposit.bridge.deposit_planned_network.is_some()
            && state.withdraw.bridge.withdraw_planned_asset.is_some()
            && state.withdraw.bridge.withdraw_planned_network.is_some()
        {
            return;
        }

        let plan = self.resolve_bridge_plan_for_assets(&state.deposit.deposit_asset, &state.withdraw.withdraw_asset);
        Self::apply_plan_to_state(state, &plan);
    }

    /// Returns the planned deposit symbol, falling back to the original deposit asset symbol.
    fn planned_deposit_asset(state: &CexState) -> String {
        state
            .deposit
            .bridge
            .deposit_planned_asset
            .clone()
            .unwrap_or_else(|| state.deposit.deposit_asset.symbol())
    }

    /// Returns the planned deposit network, falling back to the original deposit asset chain.
    fn planned_deposit_network(state: &CexState) -> String {
        state
            .deposit
            .bridge
            .deposit_planned_network
            .clone()
            .unwrap_or_else(|| state.deposit.deposit_asset.chain())
    }

    /// Returns the planned withdraw symbol, falling back to the original withdraw asset symbol.
    fn planned_withdraw_asset(state: &CexState) -> String {
        state
            .withdraw
            .bridge
            .withdraw_planned_asset
            .clone()
            .unwrap_or_else(|| state.withdraw.withdraw_asset.symbol())
    }

    /// Returns the planned withdraw network, falling back to the original withdraw asset chain.
    fn planned_withdraw_network(state: &CexState) -> String {
        state
            .withdraw
            .bridge
            .withdraw_planned_network
            .clone()
            .unwrap_or_else(|| state.withdraw.withdraw_asset.chain())
    }

    /// Builds a transfer destination account for pre-bridge funding on the given source chain.
    fn bridge_source_transfer_destination(source_chain: &str, source_address: &str) -> Result<ChainAccount, String> {
        if source_chain.eq_ignore_ascii_case("ICP") {
            let account = parse_icp_account_like(source_address)?;
            return Ok(ChainAccount::Icp(account));
        }
        if source_chain.eq_ignore_ascii_case("ETH") {
            return Ok(ChainAccount::Evm(source_address.to_string()));
        }
        Err(format!(
            "unsupported bridge source chain '{}' for transfer destination",
            source_chain
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestBridgePlanner {
        direct_assets: &'static [&'static str],
    }

    impl BridgePlanner for TestBridgePlanner {
        fn bridge_enabled(&self) -> bool {
            true
        }

        fn resolve_bridge_source_address(&self, _source_chain: &str) -> Result<String, String> {
            unreachable!("not needed by destination test")
        }

        fn takes_directly(&self, asset: &str) -> bool {
            self.direct_assets.contains(&asset)
        }
    }

    fn icp_token(symbol: &str) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: symbol.to_string(),
            decimals: 6,
            fee: candid::Nat::from(10u64),
        }
    }

    /// ckUSDC stands in for any asset with a bridge route: the exchange that
    /// lists it keeps it on ICP, and the other one bridges it.
    #[test]
    fn an_asset_the_exchange_lists_skips_the_bridge_on_both_sides() {
        let token = icp_token("ckUSDC");

        let lists = TestBridgePlanner {
            direct_assets: &["ckUSDC"],
        }
        .resolve_bridge_plan_for_assets(&token, &token);
        for side in [&lists.deposit, &lists.withdraw] {
            assert_eq!((side.cex_asset.as_str(), side.cex_network.as_str()), ("ckUSDC", "ICP"));
            assert!(!side.bridge_required);
        }

        let bridges = TestBridgePlanner::default().resolve_bridge_plan_for_assets(&token, &token);
        for side in [&bridges.deposit, &bridges.withdraw] {
            assert_eq!((side.cex_asset.as_str(), side.cex_network.as_str()), ("USDC", "ETH"));
            assert!(side.bridge_required);
        }
    }

    #[test]
    fn bridged_icp_destination_uses_the_persisted_per_leg_account() {
        let expected = Account {
            owner: Principal::from_slice(&[42]),
            subaccount: Some([7; 32]),
        };
        let destination = TestBridgePlanner::default()
            .bridge_destination_for_final_liquidator(BridgeDestinationKind::IcpAccount, &expected.to_string())
            .expect("valid ICP destination");

        assert_eq!(destination, BridgeDestination::IcpAccount(expected));
    }
}
