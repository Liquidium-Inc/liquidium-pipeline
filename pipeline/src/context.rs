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
use liquidium_pipeline_connectors::error_format::format_with_code;
use liquidium_pipeline_core::balance_service::BalanceService;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::transfer_service::TransferService;
use log::{info, warn};

use liquidium_pipeline_core::{account::actions::AccountInfo, tokens::token_registry::TokenRegistry};

use liquidium_pipeline_connectors::{
    account::router::MultiChainAccountInfoRouter,
    backend::icp_backend::IcpBackendImpl,
    token_registry_loader::{RegistryLoadError, load_token_registry},
    transfer::{evm_transfer::EvmTransferAdapter, icp_transfer::IcpTransferAdapter, router::MultiChainTransferRouter},
};

use crate::approval_state::ApprovalState;
use crate::config::{Config, ConfigTrait};
use crate::error::{AppError, error_codes};
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

#[derive(Debug)]
pub enum PipelineContextError {
    RegistryLoad(RegistryLoadError),
    Other(String),
}

impl std::fmt::Display for PipelineContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineContextError::RegistryLoad(err) => write!(f, "{err}"),
            PipelineContextError::Other(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for PipelineContextError {}

fn should_fallback_to_empty_registry(err: &PipelineContextError) -> bool {
    matches!(
        err,
        PipelineContextError::RegistryLoad(RegistryLoadError::MissingIcpDecimals { .. })
            | PipelineContextError::RegistryLoad(RegistryLoadError::MissingIcpFee { .. })
    )
}

impl<P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + 'static> PipelineContextBuilder<P> {
    pub async fn new() -> Result<Self, AppError> {
        let config = Config::load().await.map_err(|e| {
            AppError::from_def(error_codes::CONFIG_ERROR).with_context(format!("config load failed: {e}"))
        })?;
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

    pub async fn build(self) -> Result<PipelineContext, AppError> {
        self.build_with_registry_override(None)
            .await
            .map_err(|e| AppError::from_def(error_codes::INTERNAL_ERROR).with_context(format_with_code(&e)))
    }

    pub async fn build_with_registry_override(
        self,
        registry_override: Option<TokenRegistry>,
    ) -> Result<PipelineContext, PipelineContextError> {
        let config = self.config;

        let main_agent = self
            .ic_agent_main
            .ok_or_else(|| PipelineContextError::Other("missing IC Main Agent".to_string()))?;
        let icp_backend_main = Arc::new(IcpBackendImpl::new(main_agent.clone()));
        let icp_backend_trader =
            Arc::new(IcpBackendImpl::new(self.ic_agent_trader.ok_or_else(|| {
                PipelineContextError::Other("missing IC Trader".to_string())
            })?));

        // Use provided EVM providers or fail
        let main_provider = self
            .evm_provider_main
            .ok_or_else(|| PipelineContextError::Other("missing main EVM provider".to_string()))?;
        let evm_address = format!("{}", main_provider.default_signer_address());

        let evm_backend_main = Arc::new(EvmBackendImpl::new(main_provider));
        let evm_backend_trader = evm_backend_main.clone();

        let registry = Arc::new(match registry_override {
            Some(registry) => registry,
            None => load_token_registry(icp_backend_main.clone(), evm_backend_main.clone())
                .await
                .map_err(PipelineContextError::RegistryLoad)?,
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

fn setup_agents_and_provider(
    config: &Config,
) -> Result<
    (
        Arc<Agent>,
        Arc<Agent>,
        impl Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + 'static,
    ),
    AppError,
> {
    let agent_main = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.liquidator_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| {
                AppError::from_def(error_codes::CONFIG_ERROR).with_context(format!("ic agent(main) build failed: {e}"))
            })?,
    );

    let agent_trader = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.trader_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| {
                AppError::from_def(error_codes::CONFIG_ERROR)
                    .with_context(format!("ic agent(trader) build failed: {e}"))
            })?,
    );

    // Build a wallet-backed EVM RootProvider from config.evm_url and config.evm_private_key.
    let signer: PrivateKeySigner = config.evm_private_key.parse().map_err(|e| {
        AppError::from_def(error_codes::CONFIG_ERROR).with_context(format!("failed parsing evm private key: {e}"))
    })?;

    let main_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(config.evm_rpc_url.parse().map_err(|e| {
            AppError::from_def(error_codes::CONFIG_ERROR).with_context(format!("invalid evm rpc url: {e}"))
        })?);

    Ok((agent_main, agent_trader, main_provider))
}

pub async fn init_context() -> Result<PipelineContext, AppError> {
    // Start from the builder so we keep one place that owns Config.
    let mut builder = PipelineContextBuilder::new().await?;
    let config = builder.config.clone();
    let (agent_main, agent_trader, main_provider) = setup_agents_and_provider(&config)?;

    // Feed everything into the builder and let it wire backends, routers, and services.
    builder = builder
        .with_main_ic_agent(agent_main)
        .with_trader_ic_agent(agent_trader)
        .with_main_evm_provider(main_provider);

    builder.build().await
}

pub async fn init_context_best_effort() -> Result<PipelineContext, AppError> {
    let mut builder = PipelineContextBuilder::new().await?;
    let config = builder.config.clone();
    let (agent_main, agent_trader, main_provider) = setup_agents_and_provider(&config)?;

    builder = builder
        .with_main_ic_agent(agent_main)
        .with_trader_ic_agent(agent_trader)
        .with_main_evm_provider(main_provider);

    match builder.build_with_registry_override(None).await {
        Ok(ctx) => Ok(ctx),
        Err(err) if should_fallback_to_empty_registry(&err) => {
            warn!(
                "Token registry load failed ({}). Starting TUI with empty registry.",
                err
            );
            let mut fallback = PipelineContextBuilder::new().await?;
            let config = fallback.config.clone();
            let (agent_main, agent_trader, main_provider) = setup_agents_and_provider(&config)?;

            fallback = fallback
                .with_main_ic_agent(agent_main)
                .with_trader_ic_agent(agent_trader)
                .with_main_evm_provider(main_provider);

            let empty_registry = TokenRegistry::new(HashMap::new());
            fallback
                .build_with_registry_override(Some(empty_registry))
                .await
                .map_err(|err| AppError::from_def(error_codes::INTERNAL_ERROR).with_context(err.to_string()))
        }
        Err(err) => Err(AppError::from_def(error_codes::INTERNAL_ERROR).with_context(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy::signers::local::PrivateKeySigner;
    use candid::Principal;
    use ic_agent::identity::AnonymousIdentity;

    use super::*;

    fn test_config() -> Arc<Config> {
        let evm_test_private_key = std::env::var("EVM_TEST_PRIVATE_KEY").unwrap_or_default();

        Arc::new(Config {
            liquidator_identity: Arc::new(AnonymousIdentity {}),
            trader_identity: Arc::new(AnonymousIdentity {}),
            liquidator_principal: Principal::from_text("aaaaa-aa").expect("principal"),
            trader_principal: Principal::from_text("2vxsx-fae").expect("principal"),
            ic_url: "http://localhost:4943".to_string(),
            evm_rpc_url: "http://localhost:8545".to_string(),
            evm_private_key: evm_test_private_key,
            lending_canister: Principal::from_text("aaaaa-aa").expect("principal"),
            export_path: "executions.csv".to_string(),
            buy_bad_debt: false,
            db_path: "wal.db".to_string(),
            max_allowed_dex_slippage: 125,
            max_allowed_cex_slippage_bps: 200,
            cex_min_exec_usd: 5.0,
            cex_slice_target_ratio: 0.85,
            cex_buy_truncation_trigger_ratio: 0.25,
            cex_buy_inverse_overspend_bps: 10,
            cex_buy_inverse_max_retries: 1,
            cex_buy_inverse_enabled: true,
            cex_retry_base_secs: 5,
            cex_retry_max_secs: 120,
            cex_min_net_edge_bps: 25,
            cex_delay_buffer_bps: 15,
            cex_route_fee_bps: 12,
            cex_force_over_usd_threshold: 0.0,
            swapper: crate::config::SwapperMode::Hybrid,
            cex_credentials: HashMap::new(),
            opportunity_account_filter: vec![],
        })
    }

    #[test]
    fn fallback_classifies_registry_errors() {
        let missing_decimals = PipelineContextError::RegistryLoad(RegistryLoadError::MissingIcpDecimals {
            spec: "icp:aaaaa-aa:ICP".to_string(),
            source: "boom".to_string(),
        });
        let missing_fee = PipelineContextError::RegistryLoad(RegistryLoadError::MissingIcpFee {
            spec: "icp:aaaaa-aa:ICP".to_string(),
            source: "boom".to_string(),
        });
        let other_registry = PipelineContextError::RegistryLoad(RegistryLoadError::Other("other".to_string()));
        let other = PipelineContextError::Other("other".to_string());

        assert!(should_fallback_to_empty_registry(&missing_decimals));
        assert!(should_fallback_to_empty_registry(&missing_fee));
        assert!(!should_fallback_to_empty_registry(&other_registry));
        assert!(!should_fallback_to_empty_registry(&other));
    }

    #[tokio::test]
    async fn build_stringifies_typed_internal_errors() {
        let signer = PrivateKeySigner::from_slice(&[1u8; 32]).expect("signer");
        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .wallet(signer)
            .connect_mocked_client(alloy::transports::mock::Asserter::new());

        let builder = PipelineContextBuilder {
            config: test_config(),
            evm_provider_main: Some(provider),
            ic_agent_main: None,
            ic_agent_trader: None,
        };

        let err = match builder.build().await {
            Ok(_) => panic!("expected error"),
            Err(err) => err,
        };
        assert_eq!(err, "missing IC Main Agent");
    }
}
