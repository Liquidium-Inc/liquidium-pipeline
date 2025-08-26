use candid::Principal;
use ic_agent::{Agent, Identity};
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use crate::icrc_token::icrc_token::IcrcToken;
use crate::utils::create_identity_from_pem_file;

pub struct Config {
    pub liquidator_identity: Arc<dyn Identity>,
    pub liquidator_principal: Principal,
    pub debt_assets: HashMap<String, IcrcToken>,
    pub collateral_assets: HashMap<String, IcrcToken>,
    pub ic_url: String,
    pub lending_canister: Principal,
    pub export_path: String,
    pub buy_bad_debt: bool,
}

#[cfg_attr(test, mockall::automock)]
pub trait ConfigTrait: Send + Sync {
    fn get_collateral_assets(&self) -> HashMap<String, IcrcToken>;
    fn get_debt_assets(&self) -> HashMap<String, IcrcToken>;
    fn get_liquidator_principal(&self) -> Principal;
    fn should_buy_bad_debt(&self) -> bool;
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
}

impl Config {
    pub async fn load() -> Result<Arc<Self>, String> {
        // Then load local .env
        let _ = dotenv::dotenv();

        // Load $HOME/.liquidium-pipeline/config.env
        if let Ok(home) = env::var("HOME") {
            let config_path = format!("{}/.liquidium-pipeline/config.env", home);
            let _ = dotenv::from_filename(config_path);
        }

        let logger = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_target(false)
            .build();
        let multi = MultiProgress::new();

        LogWrapper::new(multi.clone(), logger).try_init().unwrap();

        let ic_url = env::var("IC_URL").unwrap();
        let export_path = env::var("EXPORT_PATH").unwrap_or("executions.csv".to_string());
        // Load the liquidator identity
        let pem_path = env::var("IDENTITY_PEM").unwrap();
        debug!("Path {}", pem_path);
        let identity = create_identity_from_pem_file(&pem_path).expect("could not create identity");
        let liquidator_principal = identity.sender().expect("could not decode liquidator principal");
        let buy_bad_debt = env::var("BUY_BAD_DEBT")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false);

        debug!("Using identity {}", liquidator_principal);

        // Load the asset maps
        let (debt, coll) = load_asset_maps().await;

        let liquidator_principal = identity.sender().unwrap();
        let lending_canister = Principal::from_text(env::var("LENDING_CANISTER").unwrap()).unwrap();

        Ok(Arc::new(Config {
            debt_assets: debt,
            collateral_assets: coll,
            liquidator_identity: identity.into(),
            ic_url,
            liquidator_principal,
            lending_canister,
            export_path,
            buy_bad_debt,
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
