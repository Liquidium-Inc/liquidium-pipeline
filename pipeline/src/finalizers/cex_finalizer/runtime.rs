use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    sync::Arc,
};

use alloy::{providers::Provider, signers::local::PrivateKeySigner};
use ic_agent::Agent;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::{CkErc20BridgeBackend, cketh_forward_routes},
    evm_backend::EvmBackendImpl,
    evm_nonce::build_evm_provider,
    icp_backend::IcpBackendImpl,
};

use crate::{
    context::PipelineContext,
    finalizers::cex_finalizer::{CexBridgeConfig, CexBridgeDependencies},
};

const ROUTE_CHAIN_MAP_ENV: &str = "CEX_ROUTE_CHAIN_MAP";
const DEFAULT_ROUTE_CHAIN_ID_ENV: &str = "CEX_DEFAULT_ROUTE_CHAIN_ID";
const LEGACY_ROUTE_CHAIN_MAP_ENV: &str = "MEXC_ROUTE_CHAIN_MAP";
const LEGACY_DEFAULT_ROUTE_CHAIN_ID_ENV: &str = "MEXC_DEFAULT_ROUTE_CHAIN_ID";

#[derive(Debug, Clone, Default)]
struct RouteChainIdConfig {
    overrides: BTreeMap<String, u64>,
    default_chain_id: Option<u64>,
}

fn normalize_route_chain_key(source_chain: &str) -> String {
    source_chain.trim().replace(['-', ' '], "_").to_ascii_uppercase()
}

fn builtin_route_chain_ids() -> BTreeMap<String, u64> {
    [
        ("ETH", 1),
        ("ETHEREUM", 1),
        ("ARB", 42161),
        ("ARBITRUM", 42161),
        ("ARBITRUM_ONE", 42161),
    ]
    .into_iter()
    .map(|(alias, chain_id)| (alias.to_string(), chain_id))
    .collect()
}

fn parse_route_chain_map(raw: &str) -> Result<BTreeMap<String, u64>, String> {
    let mut map = BTreeMap::new();
    for entry in raw.split([',', ';']) {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (alias, chain_id) = entry
            .split_once('=')
            .or_else(|| entry.split_once(':'))
            .ok_or_else(|| format!("invalid {ROUTE_CHAIN_MAP_ENV} entry '{entry}'; expected ALIAS=CHAIN_ID"))?;
        let alias = normalize_route_chain_key(alias);
        if alias.is_empty() {
            return Err(format!("invalid {ROUTE_CHAIN_MAP_ENV} entry '{entry}'; alias is empty"));
        }
        let chain_id = chain_id
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("invalid {ROUTE_CHAIN_MAP_ENV} chain id in entry '{entry}': {error}"))?;
        map.insert(alias, chain_id);
    }
    Ok(map)
}

fn env_value_with_legacy(primary: &str, legacy: &str) -> Option<String> {
    env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| env::var(legacy).ok().filter(|value| !value.trim().is_empty()))
}

fn route_chain_id_config_from_env() -> Result<RouteChainIdConfig, String> {
    let overrides = env_value_with_legacy(ROUTE_CHAIN_MAP_ENV, LEGACY_ROUTE_CHAIN_MAP_ENV)
        .map(|raw| parse_route_chain_map(&raw))
        .transpose()?
        .unwrap_or_default();
    let default_chain_id = match env_value_with_legacy(DEFAULT_ROUTE_CHAIN_ID_ENV, LEGACY_DEFAULT_ROUTE_CHAIN_ID_ENV) {
        Some(raw) => Some(
            raw.trim()
                .parse::<u64>()
                .map_err(|error| format!("invalid {DEFAULT_ROUTE_CHAIN_ID_ENV} '{}': {error}", raw.trim()))?,
        ),
        None => None,
    };
    Ok(RouteChainIdConfig {
        overrides,
        default_chain_id,
    })
}

fn expected_chain_id_for_route_source_chain(
    source_chain: &str,
    chain_id_config: &RouteChainIdConfig,
) -> Result<u64, String> {
    let key = normalize_route_chain_key(source_chain);
    if let Some(chain_id) = chain_id_config.overrides.get(&key) {
        return Ok(*chain_id);
    }
    if let Some(chain_id) = chain_id_config.default_chain_id {
        return Ok(chain_id);
    }
    if let Some(chain_id) = builtin_route_chain_ids().get(&key) {
        return Ok(*chain_id);
    }
    Err(format!(
        "unsupported EVM source chain '{}' for single-provider mode; configure {ROUTE_CHAIN_MAP_ENV}, {DEFAULT_ROUTE_CHAIN_ID_ENV}, or per-chain bridge providers",
        source_chain.trim()
    ))
}

fn required_cketh_route_chain_ids(chain_id_config: &RouteChainIdConfig) -> Result<BTreeSet<u64>, String> {
    let mut chain_ids = BTreeSet::new();
    for route in cketh_forward_routes() {
        chain_ids.insert(expected_chain_id_for_route_source_chain(
            &route.source_chain,
            chain_id_config,
        )?);
    }
    Ok(chain_ids)
}

pub(crate) async fn build_cex_bridge_dependencies(ctx: &PipelineContext) -> Result<CexBridgeDependencies, String> {
    let config = ctx.config.clone();
    let route_chain_id_config = route_chain_id_config_from_env()?;
    let required_chain_ids = required_cketh_route_chain_ids(&route_chain_id_config)?;
    if required_chain_ids.len() > 1 {
        return Err(format!(
            "bridge route catalog requires multiple EVM chains {:?}, but runtime configures a single bridge provider from EVM_RPC_URL; configure per-chain bridge providers",
            required_chain_ids
        ));
    }

    let bridge_signer: PrivateKeySigner = config
        .bridge_evm_private_key
        .parse()
        .map_err(|error| format!("failed to parse bridge EVM private key for CEX finalizer: {error}"))?;
    let bridge_provider = build_evm_provider(&config.evm_rpc_url, bridge_signer)
        .map_err(|error| format!("bridge provider for CEX finalizer: {error}"))?;

    if let Some(expected_chain_id) = required_chain_ids.first().copied() {
        let rpc_chain_id = bridge_provider.get_chain_id().await.map_err(|error| {
            format!(
                "failed to read chain id from EVM_RPC_URL '{}' for CEX finalizer: {error}",
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
            .map_err(|error| format!("ic agent(bridge finalizer) build: {error}"))?,
    );
    let bridge_backend = Arc::new(CkErc20BridgeBackend::new(
        bridge_agent.clone(),
        Arc::new(IcpBackendImpl::new(bridge_agent)),
        Arc::new(EvmBackendImpl::new(bridge_provider)),
        config.bridge_cketh_minter_canister,
        config.bridge_ic_owner_principal,
    ));
    Ok(CexBridgeDependencies {
        backend: bridge_backend,
        config: CexBridgeConfig {
            bridge_ic_source_account: config.bridge_ic_account(),
            bridge_evm_source_address: config.bridge_evm_address.clone(),
            bridge_btc_source_address: config.bridge_btc_address.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_route_chain_map_normalizes_aliases() {
        let map = parse_route_chain_map("eth=42161, arbitrum-one:42161").expect("map should parse");
        assert_eq!(map.get("ETH"), Some(&42161));
        assert_eq!(map.get("ARBITRUM_ONE"), Some(&42161));
    }

    #[test]
    fn expected_chain_id_prefers_explicit_override() {
        let config = RouteChainIdConfig {
            overrides: parse_route_chain_map("ETH=42161").expect("map should parse"),
            default_chain_id: Some(1),
        };
        assert_eq!(
            expected_chain_id_for_route_source_chain("ETH", &config).expect("chain should resolve"),
            42161
        );
    }

    #[test]
    fn expected_chain_id_uses_default_before_builtin() {
        let config = RouteChainIdConfig {
            overrides: BTreeMap::new(),
            default_chain_id: Some(42161),
        };
        assert_eq!(
            expected_chain_id_for_route_source_chain("ETH", &config).expect("chain should resolve"),
            42161
        );
    }

    #[test]
    fn expected_chain_id_falls_back_to_builtin_aliases() {
        let config = RouteChainIdConfig::default();
        assert_eq!(
            expected_chain_id_for_route_source_chain("ETH", &config).expect("eth should resolve"),
            1
        );
        assert_eq!(
            expected_chain_id_for_route_source_chain("arbitrum", &config).expect("arb should resolve"),
            42161
        );
    }
}
