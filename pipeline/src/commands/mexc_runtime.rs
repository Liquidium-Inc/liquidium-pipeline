use std::collections::BTreeSet;
use std::sync::Arc;

use alloy::{
    network::AnyNetwork,
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};
use ic_agent::Agent;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::{CkErc20BridgeBackend, cketh_forward_routes},
    evm_backend::EvmBackendImpl,
    icp_backend::IcpBackendImpl,
};

use crate::{
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::mexc::mexc_finalizer::{MexcBridgeConfig, MexcBridgeDependencies, MexcFinalizer},
    swappers::mexc::mexc_adapter::MexcClient,
};

fn expected_chain_id_for_route_source_chain(source_chain: &str) -> Result<u64, String> {
    match source_chain.trim().to_ascii_uppercase().as_str() {
        "ETH" | "ETHEREUM" => Ok(1),
        other => Err(format!(
            "unsupported EVM source chain '{}' for single-provider mode; configure per-chain bridge providers",
            other
        )),
    }
}

fn required_cketh_route_chain_ids() -> Result<BTreeSet<u64>, String> {
    let mut chain_ids = BTreeSet::new();
    for route in cketh_forward_routes() {
        chain_ids.insert(expected_chain_id_for_route_source_chain(&route.source_chain)?);
    }
    Ok(chain_ids)
}

pub(crate) async fn build_mexc_finalizer(
    ctx: &PipelineContext,
) -> Result<Arc<MexcFinalizer<MexcClient>>, String> {
    let config = ctx.config.clone();
    let (api_key, secret) = config
        .get_cex_credentials("mexc")
        .map_err(|err| format!("Cex credentials not found: {err}"))?;

    let mexc_client = Arc::new(MexcClient::new(&api_key, &secret));
    let required_chain_ids = required_cketh_route_chain_ids()?;
    if required_chain_ids.len() > 1 {
        return Err(format!(
            "bridge route catalog requires multiple EVM chains {:?}, but runtime configures a single bridge provider from EVM_RPC_URL; configure per-chain bridge providers",
            required_chain_ids
        ));
    }

    let bridge_signer: PrivateKeySigner = config
        .bridge_evm_private_key
        .parse()
        .map_err(|err| format!("Failed to parse bridge EVM private key for MEXC bridge finalizer: {err}"))?;
    let bridge_rpc_url = config
        .evm_rpc_url
        .parse()
        .map_err(|err| format!("Invalid EVM RPC URL for MEXC bridge finalizer: {err}"))?;
    let bridge_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(bridge_signer)
        .connect_http(bridge_rpc_url);

    if let Some(expected_chain_id) = required_chain_ids.first().copied() {
        let rpc_chain_id = bridge_provider.get_chain_id().await.map_err(|err| {
            format!(
                "failed to read chain id from EVM_RPC_URL '{}' for MEXC bridge finalizer: {err}",
                config.evm_rpc_url
            )
        })?;
        if rpc_chain_id != expected_chain_id {
            return Err(format!(
                "EVM_RPC_URL '{}' resolved chain id {}, but ckETH forward bridge routes require chain id {}; configure the correct RPC endpoint or per-chain bridge providers",
                config.evm_rpc_url, rpc_chain_id, expected_chain_id
            ));
        }
    }

    let bridge_agent = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.bridge_ic_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(bridge finalizer) build: {e}"))?,
    );
    let bridge_backend = Arc::new(CkErc20BridgeBackend::new(
        bridge_agent.clone(),
        Arc::new(IcpBackendImpl::new(bridge_agent)),
        Arc::new(EvmBackendImpl::new(bridge_provider)),
        config.bridge_cketh_minter_canister,
        config.bridge_ic_owner_principal,
    ));
    let bridge_dependencies = MexcBridgeDependencies {
        backend: bridge_backend,
        config: MexcBridgeConfig {
            bridge_ic_source_account: config.bridge_ic_account(),
            bridge_evm_source_address: config.bridge_evm_address.clone(),
            bridge_btc_source_address: config.bridge_btc_address.clone(),
        },
    };

    Ok(Arc::new(
        MexcFinalizer::new_with_tunables(
            mexc_client,
            ctx.trader_transfers.actions(),
            config.liquidator_principal,
            config.max_allowed_cex_slippage_bps as f64,
            config.cex_min_exec_usd,
            config.cex_slice_target_ratio,
            config.cex_buy_truncation_trigger_ratio,
            config.cex_buy_inverse_overspend_bps,
            config.cex_buy_inverse_max_retries,
            config.cex_buy_inverse_enabled,
        )
        .with_bridge_dependencies(bridge_dependencies)
        .with_route_config(
            config.cex_mexc_available_pairs.clone(),
            config.cex_mexc_max_hops as usize,
        ),
    ))
}
