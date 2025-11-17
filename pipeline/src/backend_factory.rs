use std::str::FromStr;
use std::sync::Arc;

use alloy::network::AnyNetwork;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use ic_agent::Agent;
use liquidium_pipeline_connectors::backend::evm_backend::{EvmBackend, EvmBackendImpl};
use liquidium_pipeline_connectors::backend::icp_backend::{IcpBackend, IcpBackendImpl};

use crate::config::Config;

pub struct BackendSet {
    pub icp_main: Arc<dyn IcpBackend>,
    pub icp_trader: Arc<dyn IcpBackend>,
    pub evm_main: Arc<dyn EvmBackend>,
}
pub async fn build_backends(config: &Config) -> Result<BackendSet, String> {
    // ICP main
    let agent_icp_main = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.liquidator_identity.clone())
        .with_max_tcp_error_retries(3)
        .build()
        .map_err(|e| format!("ic agent(main) build: {e}"))?;

    // ICP trader
    let agent_icp_trader = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(config.trader_identity.clone())
        .with_max_tcp_error_retries(3)
        .build()
        .map_err(|e| format!("ic agent(trader) build: {e}"))?;

    let agent_icp_main = Arc::new(agent_icp_main);
    let agent_icp_trader = Arc::new(agent_icp_trader);

    let icp_main: Arc<dyn IcpBackend> = Arc::new(IcpBackendImpl::new(agent_icp_main.clone()));
    let icp_trader: Arc<dyn IcpBackend> = Arc::new(IcpBackendImpl::new(agent_icp_trader.clone()));

    // EVM main: build signed provider
    let rpc_url = config.evm_rpc_url.clone();

    let signer =
        PrivateKeySigner::from_str(&config.evm_private_key).map_err(|e| format!("invalid EVM private key: {e}"))?;

    let provider = ProviderBuilder::new()
        .wallet(signer.clone())
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse().unwrap());

    // provider has some crazy FillProvider<..., ...> type, but that is fine
    let evm_main: Arc<dyn EvmBackend> = Arc::new(EvmBackendImpl::new(provider));

    Ok(BackendSet {
        icp_main,
        icp_trader,
        evm_main,
    })
}
