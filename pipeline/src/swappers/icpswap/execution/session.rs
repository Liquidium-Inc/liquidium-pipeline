use std::sync::Arc;

use candid::Principal;
use ic_agent::{Agent, Identity};
use liquidium_pipeline_connectors::backend::icp_backend::{IcpBackend, IcpBackendImpl};

use super::{
    client::{IcpswapClient, IcpswapManualClient},
    identity::IcpswapExecutionIdentity,
};

/// Identity-bound clients used while advancing one persisted ICPSwap leg.
pub struct IcpswapExecutionSession {
    /// Shared trader-signed ledger backend used only for child funding.
    pub funder: Arc<dyn IcpBackend>,
    /// Derived-principal backend used for final durable ledger settlement.
    pub child_ledger: Arc<dyn IcpBackend>,
    /// Derived-principal client used for every child-signed ledger and pool call.
    pub child: Arc<dyn IcpswapManualClient>,
}

pub trait IcpswapExecutionSessionFactory: Send + Sync {
    fn descriptor(&self, liquidation_id: &str) -> Result<IcpswapExecutionIdentity, String>;

    fn derive_identity(&self, descriptor: &IcpswapExecutionIdentity) -> Result<Arc<dyn Identity>, String>;

    fn open(&self, descriptor: &IcpswapExecutionIdentity) -> Result<IcpswapExecutionSession, String>;
}

/// Production factory that re-derives the child signer and builds its IC agent
/// on demand. The shared quote client remains independent of this session.
pub struct IcpswapAgentSessionFactory {
    mnemonic: Arc<str>,
    ic_url: String,
    factory_canister: Principal,
    funder: Arc<dyn IcpBackend>,
}

impl IcpswapAgentSessionFactory {
    pub fn new(mnemonic: Arc<str>, ic_url: String, factory_canister: Principal, funder: Arc<dyn IcpBackend>) -> Self {
        Self {
            mnemonic,
            ic_url,
            factory_canister,
            funder,
        }
    }
}

impl IcpswapExecutionSessionFactory for IcpswapAgentSessionFactory {
    fn descriptor(&self, liquidation_id: &str) -> Result<IcpswapExecutionIdentity, String> {
        IcpswapExecutionIdentity::derive(&self.mnemonic, liquidation_id).map(|(descriptor, _)| descriptor)
    }

    fn derive_identity(&self, descriptor: &IcpswapExecutionIdentity) -> Result<Arc<dyn Identity>, String> {
        descriptor
            .validate_and_derive(&self.mnemonic)
            .map(|identity| Arc::new(identity) as Arc<dyn Identity>)
    }

    fn open(&self, descriptor: &IcpswapExecutionIdentity) -> Result<IcpswapExecutionSession, String> {
        let identity = self.derive_identity(descriptor)?;
        let agent = Arc::new(
            Agent::builder()
                .with_url(self.ic_url.clone())
                .with_identity(identity)
                .with_max_tcp_error_retries(3)
                .build()
                .map_err(|error| format!("failed to build ICPSwap execution agent: {error}"))?,
        );
        let backend = Arc::new(IcpBackendImpl::new(agent.clone()));
        let child = Arc::new(IcpswapClient::new(agent, backend.clone(), self.factory_canister));
        Ok(IcpswapExecutionSession {
            funder: self.funder.clone(),
            child_ledger: backend,
            child,
        })
    }
}
