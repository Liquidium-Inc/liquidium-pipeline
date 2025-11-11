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
    // Token addresses on Hyperliquid EVM
    pub hyperliquid_btc_address: Option<String>,
    pub hyperliquid_usdc_address: Option<String>,
    pub hyperliquid_usdt_address: Option<String>,
    // Bridge contract addresses
    pub hyperliquid_bridge_address: Option<String>,
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
    fn get_hyperliquid_bridge_address(&self) -> Option<String>;
    fn get_hyperliquid_btc_address(&self) -> Option<String>;
    fn get_hyperliquid_usdc_address(&self) -> Option<String>;
    fn get_hyperliquid_usdt_address(&self) -> Option<String>;
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

    fn get_hyperliquid_bridge_address(&self) -> Option<String> {
        self.hyperliquid_bridge_address.clone()
    }

    fn get_hyperliquid_btc_address(&self) -> Option<String> {
        self.hyperliquid_btc_address.clone()
    }

    fn get_hyperliquid_usdc_address(&self) -> Option<String> {
        self.hyperliquid_usdc_address.clone()
    }

    fn get_hyperliquid_usdt_address(&self) -> Option<String> {
        self.hyperliquid_usdt_address.clone()
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
        self.hyperliquid_chain_id.clone()
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
        let hyperliquid_btc_address = env::var("HYPERLIQUID_BTC_ADDRESS").ok();
        let hyperliquid_usdc_address = env::var("HYPERLIQUID_USDC_ADDRESS").ok();
        let hyperliquid_usdt_address = env::var("HYPERLIQUID_USDT_ADDRESS").ok();
        let hyperliquid_bridge_address = env::var("HYPERLIQUID_BRIDGE_ADDRESS").ok();

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
            hyperliquid_btc_address,
            hyperliquid_usdc_address,
            hyperliquid_usdt_address,
            hyperliquid_bridge_address,
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
