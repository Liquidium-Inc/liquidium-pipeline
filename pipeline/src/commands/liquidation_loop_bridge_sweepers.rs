use alloy::{
    network::AnyNetwork, primitives::Address as EvmAddress, providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use std::{sync::Arc, time::Duration};
use tracing::info;

use crate::{
    context::PipelineContext,
    stages::bridge_sweeper::{BridgeSweeper, evm_destination_resolver, liquidator_destination_resolver},
};
use liquidium_pipeline_connectors::backend::bridge_backend::{
    CkErc20BridgeBackend, cketh_forward_routes, cketh_reverse_routes,
};
use liquidium_pipeline_connectors::backend::{evm_backend::EvmBackendImpl, icp_backend::IcpBackendImpl};

pub(crate) const BRIDGE_SWEEPER_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) fn spawn_bridge_sweepers(
    ctx: &Arc<PipelineContext>,
    reverse_destination: Option<EvmAddress>,
) -> Result<(), String> {
    // Bridge sweeper is intentionally independent from pause/resume.
    // Even while paused, it can continue sweeping bridge wallet balances.
    let config = &ctx.config;
    let bridge_signer: PrivateKeySigner = config
        .bridge_evm_private_key
        .parse()
        .map_err(|err| format!("Failed to parse EVM private key for bridge backend: {err}"))?;
    let bridge_rpc_url = config
        .evm_rpc_url
        .parse()
        .map_err(|err| format!("Invalid EVM RPC URL for bridge backend: {err}"))?;

    let bridge_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(bridge_signer)
        .connect_http(bridge_rpc_url);
    let icp_backend = Arc::new(IcpBackendImpl::new(ctx.agent.clone()));
    let evm_backend = Arc::new(EvmBackendImpl::new(bridge_provider));

    let bridge_backend = Arc::new(CkErc20BridgeBackend::new(
        ctx.agent.clone(),
        icp_backend,
        evm_backend,
        config.bridge_cketh_minter_canister,
        config.bridge_ic_owner_principal,
    ));
    let forward_routes = cketh_forward_routes();
    let reverse_routes = cketh_reverse_routes();

    if forward_routes.is_empty() {
        return Err("No ckETH forward bridge routes configured".to_string());
    }
    if reverse_routes.is_empty() {
        return Err("No ckETH reverse bridge routes configured".to_string());
    }

    let reverse_destination = match reverse_destination {
        Some(address) => address,
        None => ctx
            .evm_address
            .parse::<EvmAddress>()
            .map_err(|e| format!("invalid default reverse bridge destination '{}': {e}", ctx.evm_address))?,
    };

    let forward_sweeper = BridgeSweeper::new(
        bridge_backend.clone(),
        config.bridge_evm_address.clone(),
        forward_routes.clone(),
        liquidator_destination_resolver(config.liquidator_principal),
        BRIDGE_SWEEPER_POLL_INTERVAL,
    );
    let reverse_source = config.bridge_ic_account().to_string();
    let reverse_sweeper = BridgeSweeper::new(
        bridge_backend,
        reverse_source.clone(),
        reverse_routes.clone(),
        evm_destination_resolver(reverse_destination),
        BRIDGE_SWEEPER_POLL_INTERVAL,
    );

    info!(
        bridge_source_evm_address = %config.bridge_evm_address,
        bridge_cketh_minter_canister = %config.bridge_cketh_minter_canister.to_text(),
        bridge_route_count = forward_routes.len(),
        bridge_destination_account = %config.liquidator_principal.to_text(),
        "Bridge forward sweeper started"
    );
    info!(
        bridge_source_ic_account = %reverse_source,
        bridge_cketh_minter_canister = %config.bridge_cketh_minter_canister.to_text(),
        bridge_route_count = reverse_routes.len(),
        bridge_destination_evm_address = %reverse_destination,
        "Bridge reverse sweeper started"
    );
    tokio::spawn(async move { forward_sweeper.run().await });
    tokio::spawn(async move { reverse_sweeper.run().await });

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::BRIDGE_SWEEPER_POLL_INTERVAL;
    use liquidium_pipeline_connectors::backend::bridge_backend::{cketh_forward_routes, cketh_reverse_routes};

    #[test]
    fn bridge_sweeper_poll_interval_is_fixed_to_five_seconds() {
        assert_eq!(BRIDGE_SWEEPER_POLL_INTERVAL, Duration::from_secs(5));
    }

    #[test]
    fn bridge_route_catalog_boots_with_usdc_forward_route() {
        let routes = cketh_forward_routes();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].source_asset, "USDC");
        assert_eq!(routes[0].source_chain, "ETH");
        assert_eq!(routes[0].target_asset, "ckUSDC");
    }

    #[test]
    fn bridge_route_catalog_boots_with_ckusdc_reverse_route() {
        let routes = cketh_reverse_routes();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].source_asset, "ckUSDC");
        assert_eq!(routes[0].source_chain, "ICP");
        assert_eq!(routes[0].target_asset, "USDC");
    }
}
