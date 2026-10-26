//! The shared bridge: provider backends, the accounts every venue bridges
//! from, and the registry of handles rows hold on them. Built once per process
//! and handed to every exchange venue, because the bridge account is shared.
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::Arc;

use alloy::{
    network::AnyNetwork,
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::{
        BridgeBackend, CkErc20BridgeBackend, DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS, RoutedBridgeBackend,
        SolanaBridgeBackend, cketh_forward_routes,
    },
    evm_backend::EvmBackendImpl,
    icp_backend::IcpBackendImpl,
    solana_backend::SolanaBackendImpl,
};
use liquidium_pipeline_connectors::outbound::{EvmSender, SignedSendRouter, SolanaSender};
use liquidium_pipeline_core::outbound::OutboundSender;
use solana_sdk::{signature::Keypair, signer::Signer};

use crate::{config::Config, finalizers::bridge_claims::BridgeClaimLookup};

/// The account the bridge spends from on each chain.
#[derive(Debug, Clone)]
pub struct BridgeSourceAccounts {
    pub ic: Account,
    pub evm: String,
    pub btc: String,
    pub solana: String,
}

impl BridgeSourceAccounts {
    pub fn for_chain(&self, chain: &str) -> Result<String, String> {
        match chain.to_ascii_uppercase().as_str() {
            "ICP" => Ok(self.ic.to_string()),
            "ETH" => Ok(self.evm.clone()),
            "BTC" => Ok(self.btc.clone()),
            "SOL" => Ok(self.solana.clone()),
            _ => Err(format!("unsupported bridge source chain '{chain}'")),
        }
    }
}

pub struct BridgeRuntime {
    pub backend: Arc<dyn BridgeBackend>,
    pub sources: BridgeSourceAccounts,
    pub claims: Arc<dyn BridgeClaimLookup>,
    /// Sends what the bridge backends sign from the bridge source accounts.
    pub outbound: Arc<dyn OutboundSender>,
}

impl BridgeRuntime {
    /// Connects the EVM and Solana providers and checks the EVM endpoint
    /// against the chains the route catalog needs.
    pub async fn build(config: &Config, claims: Arc<dyn BridgeClaimLookup>) -> Result<Arc<Self>, String> {
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
            .map_err(|err| format!("Failed to parse bridge EVM private key for the bridge runtime: {err}"))?;
        let bridge_rpc_url = config
            .evm_rpc_url
            .parse()
            .map_err(|err| format!("Invalid EVM RPC URL for the bridge runtime: {err}"))?;
        let bridge_provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .wallet(bridge_signer)
            .connect_http(bridge_rpc_url);

        if let Some(expected_chain_id) = required_chain_ids.first().copied() {
            let rpc_chain_id = bridge_provider.get_chain_id().await.map_err(|err| {
                format!(
                    "failed to read chain id from EVM_RPC_URL '{}' for the bridge runtime: {err}",
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
                .map_err(|e| format!("ic agent(bridge runtime) build: {e}"))?,
        );
        let bridge_evm_wallet = Arc::new(EvmBackendImpl::new(bridge_provider));
        let bridge_backend = Arc::new(CkErc20BridgeBackend::new(
            bridge_agent.clone(),
            Arc::new(IcpBackendImpl::new(bridge_agent.clone())),
            bridge_evm_wallet.clone(),
            config.bridge_cketh_minter_canister,
            config.bridge_ic_owner_principal,
        ));
        let bridge_solana_address = Keypair::new_from_array(config.bridge_solana_private_key_bytes)
            .pubkey()
            .to_string();
        let bridge_solana_wallet = Arc::new(SolanaBackendImpl::from_url_and_secret_key(
            config.solana_rpc_url.clone(),
            config.bridge_solana_private_key_bytes,
        ));
        let solana_backend = Arc::new(
            SolanaBridgeBackend::new(
                bridge_agent.clone(),
                Arc::new(IcpBackendImpl::new(bridge_agent)),
                bridge_solana_wallet.clone(),
                config.bridge_cksol_minter_canister,
                config.bridge_cksol_ledger_canister,
                config.bridge_ic_owner_principal,
                config.bridge_ic_proxy_canister,
                bridge_solana_address.clone(),
                DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS,
            )
            .with_repayment_owner(config.liquidator_principal),
        );
        Ok(Arc::new(Self {
            backend: Arc::new(RoutedBridgeBackend {
                evm: bridge_backend,
                solana: solana_backend,
            }),
            sources: BridgeSourceAccounts {
                ic: config.bridge_ic_account(),
                evm: config.bridge_evm_address.clone(),
                btc: config.bridge_btc_address.clone(),
                solana: bridge_solana_address,
            },
            claims,
            outbound: Arc::new(SignedSendRouter {
                solana: Arc::new(SolanaSender::new(bridge_solana_wallet)),
                evm: Arc::new(EvmSender::new(bridge_evm_wallet)),
            }),
        }))
    }

    /// A runtime over a mock backend. The claim registry has no expectations,
    /// so a test that reaches the lookup fails unless it sets one.
    #[cfg(test)]
    pub fn for_tests(backend: Arc<dyn BridgeBackend>) -> Self {
        use candid::Principal;
        Self {
            backend,
            sources: BridgeSourceAccounts {
                ic: Account {
                    owner: Principal::anonymous(),
                    subaccount: None,
                },
                evm: "0x2222222222222222222222222222222222222222".to_string(),
                btc: "bc1qbridge000000000000000000000000000000000".to_string(),
                solana: "So11111111111111111111111111111111111111112".to_string(),
            },
            claims: Arc::new(crate::finalizers::bridge_claims::MockBridgeClaimLookup::new()),
            outbound: Arc::new(liquidium_pipeline_core::outbound::MockOutboundSender::new()),
        }
    }

    #[cfg(test)]
    pub fn with_outbound(mut self, outbound: Arc<dyn OutboundSender>) -> Self {
        self.outbound = outbound;
        self
    }

    #[cfg(test)]
    pub fn with_claims(mut self, claims: Arc<dyn BridgeClaimLookup>) -> Self {
        self.claims = claims;
        self
    }
}

const MEXC_ROUTE_CHAIN_MAP_ENV: &str = "MEXC_ROUTE_CHAIN_MAP";
const MEXC_DEFAULT_ROUTE_CHAIN_ID_ENV: &str = "MEXC_DEFAULT_ROUTE_CHAIN_ID";

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
            .ok_or_else(|| format!("invalid {MEXC_ROUTE_CHAIN_MAP_ENV} entry '{entry}'; expected ALIAS=CHAIN_ID"))?;
        let alias = normalize_route_chain_key(alias);
        if alias.is_empty() {
            return Err(format!(
                "invalid {MEXC_ROUTE_CHAIN_MAP_ENV} entry '{entry}'; alias is empty"
            ));
        }
        let chain_id = chain_id
            .trim()
            .parse::<u64>()
            .map_err(|err| format!("invalid {MEXC_ROUTE_CHAIN_MAP_ENV} chain id in entry '{entry}': {err}"))?;
        map.insert(alias, chain_id);
    }
    Ok(map)
}

fn route_chain_id_config_from_env() -> Result<RouteChainIdConfig, String> {
    let overrides = match env::var(MEXC_ROUTE_CHAIN_MAP_ENV) {
        Ok(raw) if !raw.trim().is_empty() => parse_route_chain_map(&raw)?,
        _ => BTreeMap::new(),
    };
    let default_chain_id = match env::var(MEXC_DEFAULT_ROUTE_CHAIN_ID_ENV) {
        Ok(raw) if !raw.trim().is_empty() => Some(
            raw.trim()
                .parse::<u64>()
                .map_err(|err| format!("invalid {MEXC_DEFAULT_ROUTE_CHAIN_ID_ENV} '{}': {err}", raw.trim()))?,
        ),
        _ => None,
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
        "unsupported EVM source chain '{}' for single-provider mode; configure {MEXC_ROUTE_CHAIN_MAP_ENV}, {MEXC_DEFAULT_ROUTE_CHAIN_ID_ENV}, or per-chain bridge providers",
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
