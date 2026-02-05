use std::collections::HashMap;
use std::sync::Arc;

use alloy::network::AnyNetwork;
use alloy::providers::{Provider, ProviderBuilder, WalletProvider};
use alloy::signers::local::PrivateKeySigner;

use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::account::evm_account::EvmAccountInfoAdapter;
use liquidium_pipeline_connectors::account::icp_account::IcpAccountInfoAdapter;
use liquidium_pipeline_connectors::backend::evm_backend::EvmBackendImpl;
use liquidium_pipeline_core::balance_service::BalanceService;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::transfer_service::TransferService;
use log::{info, warn};

use liquidium_pipeline_core::{account::actions::AccountInfo, tokens::token_registry::TokenRegistry};

use liquidium_pipeline_connectors::{
    account::router::MultiChainAccountInfoRouter,
    backend::icp_backend::IcpBackendImpl,
    token_registry_loader::load_token_registry,
    transfer::{evm_transfer::EvmTransferAdapter, icp_transfer::IcpTransferAdapter, router::MultiChainTransferRouter},
};

use crate::approval_state::ApprovalState;
use crate::config::{Config, ConfigTrait};
use crate::swappers::kong::kong_swapper::KongSwapSwapper;
use crate::swappers::kong::kong_venue::KongVenue;
use crate::swappers::router::{SwapRouter, SwapVenue};

pub struct PipelineContext {
    pub config: Arc<Config>,
    pub registry: Arc<TokenRegistry>,
    pub main_service: Arc<BalanceService>,
    pub trader_service: Arc<BalanceService>,
    pub recovery_service: Arc<BalanceService>,
    pub agent: Arc<Agent>,
    pub main_transfers: Arc<TransferService>,
    pub trader_transfers: Arc<TransferService>,
    pub swap_router: Arc<SwapRouter>,
    pub recovery_transfers: Arc<TransferService>,
    pub evm_address: String,
    pub approval_state: Arc<ApprovalState>,
}

pub struct PipelineContextBuilder<P: Provider<AnyNetwork>> {
    config: Arc<Config>,
    evm_provider_main: Option<P>,
    ic_agent_main: Option<Arc<Agent>>,
    ic_agent_trader: Option<Arc<Agent>>,
}

impl<P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + 'static> PipelineContextBuilder<P> {
    pub async fn new() -> Result<Self, String> {
        let config = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;
        Ok(Self {
            config,
            evm_provider_main: None,
            ic_agent_main: None,
            ic_agent_trader: None,
        })
    }

    pub fn with_main_evm_provider(mut self, provider: P) -> Self {
        self.evm_provider_main = Some(provider);
        self
    }

    pub fn with_main_ic_agent(mut self, agent: Arc<Agent>) -> Self {
        self.ic_agent_main = Some(agent);
        self
    }

    pub fn with_trader_ic_agent(mut self, agent: Arc<Agent>) -> Self {
        self.ic_agent_trader = Some(agent);
        self
    }

    pub async fn build(self) -> Result<PipelineContext, String> {
        self.build_with_registry_override(None).await
    }

    pub async fn build_with_registry_override(
        self,
        registry_override: Option<TokenRegistry>,
    ) -> Result<PipelineContext, String> {
        let config = self.config;

        let main_agent = self.ic_agent_main.ok_or("missing IC Main Agent")?;
        let icp_backend_main = Arc::new(IcpBackendImpl::new(main_agent.clone()));
        let icp_backend_trader = Arc::new(IcpBackendImpl::new(self.ic_agent_trader.ok_or("missing IC Trader")?));

        // Use provided EVM providers or fail
        let main_provider = self.evm_provider_main.ok_or("missing main EVM provider")?;
        let evm_address = format!("{}", main_provider.default_signer_address());

        let evm_backend_main = Arc::new(EvmBackendImpl::new(main_provider));
        let evm_backend_trader = evm_backend_main.clone();

        let registry = Arc::new(match registry_override {
            Some(registry) => registry,
            None => load_token_registry(icp_backend_main.clone(), evm_backend_main.clone()).await?,
        });
        for (id, token) in registry.tokens.iter() {
            if let ChainToken::Icp { fee, .. } = token {
                info!("Loaded ICRC fee: {} {} (ledger={})", token.symbol(), fee, id.address);
            }
        }

        let main_icp_account = Account {
            owner: config.liquidator_principal,
            subaccount: None,
        };

        let trader_icp_account = Account {
            owner: config.trader_principal,
            subaccount: None,
        };

        let main_trader_account = Account {
            owner: config.trader_principal,
            subaccount: None,
        };

        let recovery_icp_account = config.get_recovery_account();

        let icp_info_main = Arc::new(IcpAccountInfoAdapter::new(icp_backend_main.clone(), main_icp_account));
        let evm_info_main = Arc::new(EvmAccountInfoAdapter::new(evm_backend_main.clone()));
        let main_accounts: Arc<dyn AccountInfo + Send + Sync> =
            Arc::new(MultiChainAccountInfoRouter::new(icp_info_main, evm_info_main));

        let icp_info_trader = Arc::new(IcpAccountInfoAdapter::new(
            icp_backend_trader.clone(),
            trader_icp_account,
        ));
        let evm_info_trader = Arc::new(EvmAccountInfoAdapter::new(evm_backend_trader.clone()));
        let trader_accounts: Arc<dyn AccountInfo + Send + Sync> =
            Arc::new(MultiChainAccountInfoRouter::new(icp_info_trader, evm_info_trader));

        let icp_info_recovery = Arc::new(IcpAccountInfoAdapter::new(
            icp_backend_trader.clone(),
            recovery_icp_account,
        ));
        let evm_info_recovery = Arc::new(EvmAccountInfoAdapter::new(evm_backend_trader.clone()));
        let recovery_accounts: Arc<dyn AccountInfo + Send + Sync> =
            Arc::new(MultiChainAccountInfoRouter::new(icp_info_recovery, evm_info_recovery));

        let main_service = BalanceService::new(registry.clone(), main_accounts);
        let trader_service = BalanceService::new(registry.clone(), trader_accounts);
        let recovery_service = BalanceService::new(registry.clone(), recovery_accounts);

        // Build transfer adapters and routers
        let icp_transfer_main = Arc::new(IcpTransferAdapter::new(icp_backend_main.clone(), main_icp_account));
        let evm_transfer_main = Arc::new(EvmTransferAdapter::new(evm_backend_main.clone()));
        let transfer_router_main = Arc::new(MultiChainTransferRouter::new(icp_transfer_main, evm_transfer_main));
        let main_transfers = TransferService::new(registry.clone(), transfer_router_main);

        let icp_transfer_trader = Arc::new(IcpTransferAdapter::new(icp_backend_trader.clone(), trader_icp_account));
        let evm_transfer_trader = Arc::new(EvmTransferAdapter::new(evm_backend_trader.clone()));
        let transfer_router_trader = Arc::new(MultiChainTransferRouter::new(icp_transfer_trader, evm_transfer_trader));
        let trader_transfers = TransferService::new(registry.clone(), transfer_router_trader);

        let icp_transfer_recovery = Arc::new(IcpTransferAdapter::new(
            icp_backend_trader.clone(),
            recovery_icp_account,
        ));

        let evm_transfer_recovery = Arc::new(EvmTransferAdapter::new(evm_backend_trader.clone()));
        let transfer_router_recovery = Arc::new(MultiChainTransferRouter::new(
            icp_transfer_recovery,
            evm_transfer_recovery,
        ));
        let recovery_transfers = TransferService::new(registry.clone(), transfer_router_recovery);

        let approval_state = Arc::new(ApprovalState::new());

        // Setup swap venue

        // Kong swapper uses the trader agent/backend, adjust ctor args to your real API
        let kong_swapper = KongSwapSwapper::new(
            icp_backend_trader.agent.clone(),
            main_trader_account,
            approval_state.clone(),
        );
        // Build Kong venue (ICP)
        let icp_tokens: Vec<ChainToken> = registry
            .tokens
            .values()
            .filter_map(|t| match t {
                ChainToken::Icp { .. } => Some(t.clone()),
                _ => None,
            })
            .collect();

        let kong_swapper = Arc::new(kong_swapper);
        let kong_venue: Arc<dyn SwapVenue> = Arc::new(KongVenue::new(kong_swapper, icp_tokens));

        // let mexc_client = Arc::new(MexcClient::from_env()?);
        // let mexc_venue: Arc<dyn SwapVenue> = Arc::new(MexcSwapVenue::new(mexc_client));

        // Build router
        let swap_router = SwapRouter::new().with_venue("kong", kong_venue);

        let swap_router = Arc::new(swap_router);

        Ok(PipelineContext {
            config: config.clone(),
            swap_router,
            registry,
            main_service: Arc::new(main_service),
            trader_service: Arc::new(trader_service),
            recovery_service: Arc::new(recovery_service),
            main_transfers: Arc::new(main_transfers),
            trader_transfers: Arc::new(trader_transfers),
            recovery_transfers: Arc::new(recovery_transfers),
            evm_address,
            approval_state,
            agent: main_agent.clone(),
        })
    }
}

pub async fn init_context() -> Result<PipelineContext, String> {
    // Start from the builder so we keep one place that owns Config.
    let mut builder = PipelineContextBuilder::new().await?;
    let config = builder.config.clone();

    // Build ICP agents for main and trader accounts.
    let agent_main = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.liquidator_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(main) build: {e}"))?,
    );

    let agent_trader = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.trader_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(trader) build: {e}"))?,
    );

    // Build a wallet-backed EVM RootProvider from config.evm_url and config.evm_private_key.
    let signer: PrivateKeySigner = config
        .evm_private_key
        .parse()
        .map_err(|e| format!("failed parsing evm private key: {e}"))?;

    let main_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(
            config
                .evm_rpc_url
                .parse()
                .map_err(|e| format!("invalid evm rpc url: {e}"))?,
        );

    // Feed everything into the builder and let it wire backends, routers, and services.
    builder = builder
        .with_main_ic_agent(agent_main)
        .with_trader_ic_agent(agent_trader)
        .with_main_evm_provider(main_provider);

    builder.build().await
}

pub async fn init_context_best_effort() -> Result<PipelineContext, String> {
    let mut builder = PipelineContextBuilder::new().await?;
    let config = builder.config.clone();

    let agent_main = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.liquidator_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(main) build: {e}"))?,
    );

    let agent_trader = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.trader_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(trader) build: {e}"))?,
    );

    let signer: PrivateKeySigner = config
        .evm_private_key
        .parse()
        .map_err(|e| format!("failed parsing evm private key: {e}"))?;

    let main_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(
            config
                .evm_rpc_url
                .parse()
                .map_err(|e| format!("invalid evm rpc url: {e}"))?,
        );

    builder = builder
        .with_main_ic_agent(agent_main)
        .with_trader_ic_agent(agent_trader)
        .with_main_evm_provider(main_provider);

    match builder.build_with_registry_override(None).await {
        Ok(ctx) => Ok(ctx),
        Err(err) => {
            if err.contains("icp decimals") {
                warn!("Token registry load failed ({}). Starting TUI with empty registry.", err);
                let mut fallback = PipelineContextBuilder::new().await?;
                let config = fallback.config.clone();

                let agent_main = Arc::new(
                    Agent::builder()
                        .with_url(config.ic_url.clone())
                        .with_identity(config.liquidator_identity.clone())
                        .with_max_tcp_error_retries(3)
                        .build()
                        .map_err(|e| format!("ic agent(main) build: {e}"))?,
                );

                let agent_trader = Arc::new(
                    Agent::builder()
                        .with_url(config.ic_url.clone())
                        .with_identity(config.trader_identity.clone())
                        .with_max_tcp_error_retries(3)
                        .build()
                        .map_err(|e| format!("ic agent(trader) build: {e}"))?,
                );

                let signer: PrivateKeySigner = config
                    .evm_private_key
                    .parse()
                    .map_err(|e| format!("failed parsing evm private key: {e}"))?;

                let main_provider = ProviderBuilder::new()
                    .network::<AnyNetwork>()
                    .wallet(signer)
                    .connect_http(
                        config
                            .evm_rpc_url
                            .parse()
                            .map_err(|e| format!("invalid evm rpc url: {e}"))?,
                    );

                fallback = fallback
                    .with_main_ic_agent(agent_main)
                    .with_trader_ic_agent(agent_trader)
                    .with_main_evm_provider(main_provider);

                let empty_registry = TokenRegistry::new(HashMap::new());
                fallback.build_with_registry_override(Some(empty_registry)).await
            } else {
                Err(err)
            }
        }
    }
}
