use std::str::FromStr;

use alloy::primitives::Address as EvmAddress;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeDestination, BridgeDestinationKind, resolve_cketh_forward_route_by_target,
    resolve_cketh_reverse_route_by_source,
};
use liquidium_pipeline_core::{account::model::ChainAccount, tokens::chain_token::ChainToken};

use crate::finalizers::cex_finalizer::CexState;

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

pub(crate) trait BridgePlanner {
    /// Returns whether bridge-aware planning should be enabled for this finalizer instance.
    fn bridge_enabled(&self) -> bool;
    /// Resolves the configured bridge source address/account string for a given source chain.
    fn resolve_bridge_source_address(&self, source_chain: &str) -> Result<String, String>;
    /// Returns the liquidator principal used when a bridge leg targets an ICP account.
    fn bridge_liquidator_principal(&self) -> Principal;

    /// Builds a direct (non-bridged) transport plan from the provided asset.
    fn plan_from_direct_asset(asset: &ChainToken) -> BridgeTransportPlan {
        BridgeTransportPlan {
            cex_asset: asset.symbol(),
            cex_network: asset.chain(),
            bridge_required: false,
        }
    }

    /// Maps a bridge route destination kind to the CEX network label used by backends.
    fn cex_network_from_destination_kind(kind: BridgeDestinationKind) -> String {
        match kind {
            BridgeDestinationKind::IcpAccount => "ICP".to_string(),
            BridgeDestinationKind::EvmAddress => "ETH".to_string(),
            BridgeDestinationKind::BtcAddress => "BTC".to_string(),
        }
    }

    /// Resolves per-leg transport plans (deposit and withdraw), including bridge symbol/network translation.
    fn resolve_bridge_plan_for_assets(&self, deposit_asset: &ChainToken, withdraw_asset: &ChainToken) -> BridgePlan {
        let mut deposit = Self::plan_from_direct_asset(deposit_asset);
        let mut withdraw = Self::plan_from_direct_asset(withdraw_asset);

        if !self.bridge_enabled() {
            return BridgePlan { deposit, withdraw };
        }

        // Deposit-side translation: ckERC20 reverse route (e.g. ckUSDC@ICP -> USDC@ETH).
        if let Some(route) = resolve_cketh_reverse_route_by_source(&deposit.cex_asset, &deposit.cex_network) {
            deposit = BridgeTransportPlan {
                cex_asset: route.target_asset.to_string(),
                cex_network: Self::cex_network_from_destination_kind(route.destination_kind),
                bridge_required: true,
            };
        }

        // Withdraw-side translation: ckERC20 forward route (e.g. USDC@ETH -> ckUSDC@ICP).
        if let Some(route) = resolve_cketh_forward_route_by_target(&withdraw.cex_asset)
            && route.destination_kind == BridgeDestinationKind::IcpAccount
        {
            withdraw = BridgeTransportPlan {
                cex_asset: route.source_asset.to_string(),
                cex_network: route.source_chain.to_string(),
                bridge_required: true,
            };
        }

        BridgePlan { deposit, withdraw }
    }

    /// Converts a CEX deposit address into a typed bridge destination based on route destination kind.
    fn route_destination_for_deposit(
        route_destination_kind: BridgeDestinationKind,
        cex_address: &str,
    ) -> Result<BridgeDestination, String> {
        match route_destination_kind {
            BridgeDestinationKind::EvmAddress => {
                let evm = cex_address
                    .parse::<EvmAddress>()
                    .map_err(|e| format!("invalid CEX deposit EVM address '{}': {e}", cex_address))?;
                Ok(BridgeDestination::EvmAddress(evm))
            }
            BridgeDestinationKind::IcpAccount => Self::parse_icp_account_like(cex_address)
                .map(BridgeDestination::IcpAccount)
                .map_err(|e| format!("invalid CEX deposit ICP destination '{}': {e}", cex_address)),
            BridgeDestinationKind::BtcAddress => Ok(BridgeDestination::BtcAddress(cex_address.to_string())),
        }
    }

    /// Converts the final liquidator target into a bridge destination for the post-withdraw bridge leg.
    fn bridge_destination_for_final_liquidator(
        &self,
        destination_kind: BridgeDestinationKind,
        direct_withdraw_address: &str,
    ) -> Result<BridgeDestination, String> {
        match destination_kind {
            BridgeDestinationKind::IcpAccount => Ok(BridgeDestination::IcpAccount(Account {
                owner: self.bridge_liquidator_principal(),
                subaccount: None,
            })),

            BridgeDestinationKind::EvmAddress => {
                let evm = direct_withdraw_address.parse::<EvmAddress>().map_err(|e| {
                    format!(
                        "invalid final EVM destination '{}' for bridge withdraw: {e}",
                        direct_withdraw_address
                    )
                })?;
                Ok(BridgeDestination::EvmAddress(evm))
            }
            BridgeDestinationKind::BtcAddress => Ok(BridgeDestination::BtcAddress(direct_withdraw_address.to_string())),
        }
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

    /// Parses an ICP destination string as either full `Account` text or a principal (with empty subaccount).
    fn parse_icp_account_like(address: &str) -> Result<Account, String> {
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

    /// Builds a transfer destination account for pre-bridge funding on the given source chain.
    fn bridge_source_transfer_destination(source_chain: &str, source_address: &str) -> Result<ChainAccount, String> {
        if source_chain.eq_ignore_ascii_case("ICP") {
            let account = Self::parse_icp_account_like(source_address)?;
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
