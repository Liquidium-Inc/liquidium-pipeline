use alloy::primitives::Address;
use candid::Principal;
use ic_agent::{Agent, Identity};
use icrc_ledger_types::icrc1::account::Account;
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use crate::account::account::RECOVERY_ACCOUNT;
use crate::bridge::minter_client::fetch_minter_info;
use crate::bridge::types::MinterInfo;
use crate::icrc_token::icrc_token::IcrcToken;
use crate::utils::create_identity_from_pem_file;

pub struct Config {
    pub liquidator_identity: Arc<dyn Identity>,
    pub trader_identity: Arc<dyn Identity>,
    pub liquidator_principal: Principal,
    pub trader_principal: Principal,
    pub debt_assets: HashMap<String, IcrcToken>,
    pub collateral_assets: HashMap<String, IcrcToken>,
    pub ic_url: String,
    pub lending_canister: Principal,
    pub export_path: String,
    pub buy_bad_debt: bool,
    pub db_path: String,

    // Hyperliquid configuration
    pub finalizer_type: String, // "kong" or "hyperliquid"
    pub hyperliquid_rpc_url: Option<String>,
    pub hyperliquid_core_api_url: Option<String>,
    pub hyperliquid_wallet_key: Option<String>,
    pub hyperliquid_dex_router: Option<String>,
    pub hyperliquid_chain_id: Option<u64>,
    // Minter canister for fetching ERC20 addresses dynamically
    pub hyperliquid_minter_canister: Option<Principal>,
    // Cached minter info with token address mappings
    pub minter_info: Option<Arc<MinterInfo>>,
}

#[cfg_attr(test, mockall::automock)]
pub trait ConfigTrait: Send + Sync {
    fn get_collateral_assets(&self) -> HashMap<String, IcrcToken>;
    fn get_debt_assets(&self) -> HashMap<String, IcrcToken>;
    fn get_liquidator_principal(&self) -> Principal;
    fn get_trader_principal(&self) -> Principal;
    fn should_buy_bad_debt(&self) -> bool;
    fn get_lending_canister(&self) -> Principal;
    fn get_recovery_account(&self) -> Account;

    // Hyperliquid configuration methods
    fn get_hyperliquid_bridge_address(&self) -> Result<Address, String>;
    fn get_hyperliquid_minter_principal(&self) -> Result<Principal, String>;
    fn get_erc20_address_by_ledger(&self, ledger_id: &Principal) -> Result<Address, String>;
    fn get_symbol_by_erc20_address(&self, address: &Address) -> Result<String, String>;
    fn get_erc20_address_by_symbol(&self, symbol: &str) -> Result<Address, String>;
    fn get_hyperliquid_wallet_key(&self) -> Option<String>;
    fn get_hyperliquid_rpc_url(&self) -> Option<String>;
    fn get_hyperliquid_core_api_url(&self) -> Option<String>;
    fn get_hyperliquid_chain_id(&self) -> Option<u64>;
}

impl ConfigTrait for Config {
    fn get_collateral_assets(&self) -> HashMap<String, IcrcToken> {
        self.collateral_assets.clone()
    }

    fn get_debt_assets(&self) -> HashMap<String, IcrcToken> {
        self.debt_assets.clone()
    }

    fn get_liquidator_principal(&self) -> Principal {
        self.liquidator_principal
    }

    fn should_buy_bad_debt(&self) -> bool {
        self.buy_bad_debt
    }

    fn get_trader_principal(&self) -> Principal {
        self.trader_principal
    }

    fn get_recovery_account(&self) -> Account {
        Account {
            owner: self.trader_principal,
            subaccount: Some(*RECOVERY_ACCOUNT),
        }
    }

    fn get_lending_canister(&self) -> Principal {
        self.lending_canister
    }

    fn get_hyperliquid_bridge_address(&self) -> Result<Address, String> {
        self.minter_info
            .as_ref()
            .ok_or_else(|| "Minter info not loaded".to_string())
            .map(|info| info.bridge_address)
    }

    fn get_hyperliquid_minter_principal(&self) -> Result<Principal, String> {
        self.hyperliquid_minter_canister
            .ok_or_else(|| "Hyperliquid minter canister not configured".to_string())
    }

    fn get_erc20_address_by_ledger(&self, ledger_id: &Principal) -> Result<Address, String> {
        self.minter_info
            .as_ref()
            .ok_or_else(|| "Minter info not loaded".to_string())?
            .get_erc20_address(ledger_id)
            .ok_or_else(|| format!("No ERC20 address found for ledger {}", ledger_id))
    }

    fn get_symbol_by_erc20_address(&self, address: &Address) -> Result<String, String> {
        self.minter_info
            .as_ref()
            .ok_or_else(|| "Minter info not loaded".to_string())?
            .get_symbol_by_address(address)
            .ok_or_else(|| format!("No symbol found for ERC20 address {:?}", address))
    }

    fn get_erc20_address_by_symbol(&self, symbol: &str) -> Result<Address, String> {
        let minter_info = self.minter_info
            .as_ref()
            .ok_or_else(|| "Minter info not loaded".to_string())?;

        // Find address by matching symbol (Core symbol like "USDT")
        minter_info.address_to_symbol
            .iter()
            .find(|(_, sym)| sym.as_str() == symbol)
            .map(|(addr, _)| *addr)
            .ok_or_else(|| format!("No ERC20 address found for symbol '{}'", symbol))
    }

    fn get_hyperliquid_wallet_key(&self) -> Option<String> {
        self.hyperliquid_wallet_key.clone()
    }

    fn get_hyperliquid_rpc_url(&self) -> Option<String> {
        self.hyperliquid_rpc_url.clone()
    }

    fn get_hyperliquid_core_api_url(&self) -> Option<String> {
        self.hyperliquid_core_api_url.clone()
    }

    fn get_hyperliquid_chain_id(&self) -> Option<u64> {
        self.hyperliquid_chain_id
    }
}

impl Config {
    pub async fn load() -> Result<Arc<Self>, String> {
        // Then load local .env
        let _ = dotenv::dotenv();

        // Load $HOME/.liquidium-pipeline/config.env
        let home = if let Ok(home) = env::var("HOME") {
            let config_path = format!("{}/.liquidium-pipeline/config.env", home);
            let _ = dotenv::from_filename(config_path);
            format!("{}/.liquidium-pipeline", home)
        } else {
            ".liquidium-pipeline".to_string()
        };

        let logger = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_target(false)
            .build();
        let multi = MultiProgress::new();

        LogWrapper::new(multi.clone(), logger).try_init().unwrap();

        let ic_url = env::var("IC_URL").unwrap();
        let export_path = env::var("EXPORT_PATH").unwrap_or("executions.csv".to_string());
        // Load the liquidator identity
        let pem_path = env::var("IDENTITY_PEM").expect("Account missing `run account new`");
        let trader_pem_path = env::var("TRADER_IDENTITY_PEM").expect("Account missing `run account new`");
        debug!("Path {}", pem_path);

        let identity = create_identity_from_pem_file(&pem_path).expect("could not create identity");
        let liquidator_principal = identity.sender().expect("could not decode liquidator principal");

        let trader_identity = create_identity_from_pem_file(&trader_pem_path).expect("could not create identity");
        let trader_principal = trader_identity.sender().expect("could not decode liquidator principal");

        let buy_bad_debt = env::var("BUY_BAD_DEBT")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false);

        debug!("Liquidator ID {}", liquidator_principal);
        debug!("Trader ID {}", trader_principal);

        // Load the asset maps
        let (debt, coll) = load_asset_maps().await;
        let lending_canister = Principal::from_text(env::var("LENDING_CANISTER").unwrap()).unwrap();

        // The db path
        let db_path = env::var("DB_PATH").unwrap_or(format!("{}/wal.db", home));

        // Hyperliquid configuration
        let finalizer_type = env::var("FINALIZER_TYPE").unwrap_or("kong".to_string());
        let hyperliquid_rpc_url = env::var("HYPERLIQUID_RPC_URL").ok();
        let hyperliquid_core_api_url = env::var("HYPERLIQUID_CORE_API_URL").ok();
        let hyperliquid_wallet_key = env::var("HYPERLIQUID_WALLET_KEY").ok();
        let hyperliquid_dex_router = env::var("HYPERLIQUID_DEX_ROUTER").ok();
        let hyperliquid_chain_id = env::var("HYPERLIQUID_CHAIN_ID").ok().and_then(|s| s.parse().ok());

        // Get minter canister for Hyperliquid integration
        let hyperliquid_minter_canister = env::var("HYPERLIQUID_MINTER_CANISTER")
            .ok()
            .and_then(|s| Principal::from_text(s).ok());

        // Fetch minter info if minter canister is configured
        let minter_info = if let Some(minter_canister) = hyperliquid_minter_canister {
            log::info!("Fetching minter info from canister: {}", minter_canister);
            let agent = Agent::builder()
                .with_url(ic_url.clone())
                .with_max_tcp_error_retries(3)
                .build()
                .expect("Failed to initialize agent for minter query");

            match fetch_minter_info(&agent, minter_canister).await {
                Ok(info) => {
                    log::info!(
                        "Successfully loaded minter info with {} tokens",
                        info.token_addresses.len()
                    );
                    Some(Arc::new(info))
                }
                Err(e) => {
                    log::error!("Failed to fetch minter info: {}", e);
                    return Err(format!("Failed to fetch minter info: {}", e));
                }
            }
        } else {
            log::warn!("HYPERLIQUID_MINTER_CANISTER not configured, minter info not loaded");
            None
        };

        Ok(Arc::new(Config {
            debt_assets: debt,
            collateral_assets: coll,
            liquidator_identity: identity.into(),
            ic_url,
            liquidator_principal,
            trader_identity: trader_identity.into(),
            trader_principal,
            lending_canister,
            export_path,
            buy_bad_debt,
            db_path,
            finalizer_type,
            hyperliquid_rpc_url,
            hyperliquid_core_api_url,
            hyperliquid_wallet_key,
            hyperliquid_dex_router,
            hyperliquid_chain_id,
            hyperliquid_minter_canister,
            minter_info,
        }))
    }
}

async fn load_asset_maps() -> (HashMap<String, IcrcToken>, HashMap<String, IcrcToken>) {
    let ic_url = env::var("IC_URL").unwrap();
    let agent = Arc::new(
        Agent::builder()
            .with_url(ic_url.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .expect("could not initialize client"),
    );
    let mut debt = HashMap::new();

    for p in parse_principals("DEBT_ASSETS") {
        let token = IcrcToken::from_principal(p.0, agent.clone()).await;
        assert_eq!(token.symbol, p.1, "token mismatch detected");
        debt.insert(p.0.to_string(), token);
    }

    let mut coll = HashMap::new();
    for p in parse_principals("COLLATERAL_ASSETS") {
        let mut token = debt.get(&p.0.to_text()).cloned();
        if token.is_none() {
            token = Some(IcrcToken::from_principal(p.0, agent.clone()).await);
        }
        let token = token.unwrap();
        assert_eq!(token.symbol, p.1, "token mismatch detected");
        coll.insert(p.0.to_string(), token.clone());
    }
    (debt, coll)
}

// helper return token Principal,Name
fn parse_principals(var: &str) -> Vec<(Principal, String)> {
    std::env::var(var)
        .unwrap_or_default()
        .split(',')
        .map(|s| {
            let s: Vec<&str> = s.trim().split(":").collect();
            (Principal::from_text(s[0]).unwrap(), s[1].to_string())
        })
        .collect::<Vec<(Principal, String)>>()
}
