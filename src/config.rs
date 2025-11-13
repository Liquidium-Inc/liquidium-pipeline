use candid::Principal;
use ic_agent::{Agent, Identity};
use icrc_ledger_types::icrc1::account::Account;
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use crate::account::icp::adapter::{RECOVERY_ACCOUNT, derive_icp_identity};
use crate::icrc_token::icrc_token::IcrcToken;

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
    pub mnemonic: String,
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
    fn icrc_token_for_symbol(&self, symbol: &str) -> Result<IcrcToken, String>;
    fn exchange_deposit_account_for(&self, chain: &str) -> Result<Account, String>;
    fn exchange_withdraw_address_for(&self, chain: &str) -> Result<String, String>;
    fn mnemonic(&self) -> String;
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

    fn icrc_token_for_symbol(&self, symbol: &str) -> Result<IcrcToken, String> {
        // search both maps by token symbol
        if let Some(token) = self.debt_assets.values().find(|t| t.symbol == symbol).cloned() {
            return Ok(token);
        }
        if let Some(token) = self.collateral_assets.values().find(|t| t.symbol == symbol).cloned() {
            return Ok(token);
        }
        Err(format!("Unknown ICRC token symbol: {}", symbol))
    }

    fn exchange_deposit_account_for(&self, _chain: &str) -> Result<Account, String> {
        // For now, reuse the recovery account as the MEXC IC inbox.
        // This can be refined later per-asset if needed.
        Ok(self.get_recovery_account())
    }

    fn exchange_withdraw_address_for(&self, chain: &str) -> Result<String, String> {
        let var = format!("EXCHANGE_WITHDRAW_{}", chain);
        match std::env::var(&var).or_else(|_| std::env::var("EXCHANGE_WITHDRAW_DEFAULT")) {
            Ok(addr) => Ok(addr),
            Err(_) => Err(format!(
                "No withdraw address configured for chain {} (tried {} and EXCHANGE_WITHDRAW_DEFAULT)",
                chain, var
            )),
        }
    }

    fn mnemonic(&self) -> String {
        self.mnemonic.clone()
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
        // Load base mnemonic and derive child identities
        let mnemonic = env::var("MNEMONIC").expect("MNEMONIC not configured");

        let liquidator_identity =
            derive_icp_identity(&mnemonic, 0, 0).expect("could not create liquidator identity from mnemonic");
        let liquidator_principal = liquidator_identity
            .sender()
            .expect("could not decode liquidator principal");

        let trader_identity =
            derive_icp_identity(&mnemonic, 0, 1).expect("could not create trader identity from mnemonic");
        let trader_principal = trader_identity.sender().expect("could not decode trader principal");

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
        Ok(Arc::new(Config {
            debt_assets: debt,
            collateral_assets: coll,
            liquidator_identity: Arc::new(liquidator_identity),
            ic_url,
            liquidator_principal,
            trader_identity: Arc::new(trader_identity),
            trader_principal,
            lending_canister,
            export_path,
            buy_bad_debt,
            db_path,
            mnemonic,
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
