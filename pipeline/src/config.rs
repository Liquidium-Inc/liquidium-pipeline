use alloy::hex::ToHexExt;
use candid::Principal;

use ic_agent::Identity;
use icrc_ledger_types::icrc1::account::Account;
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use liquidium_pipeline_connectors::account::icp_account::{RECOVERY_ACCOUNT, derive_icp_identity};
use liquidium_pipeline_connectors::crypto::derivation::derive_evm_private_key;
use log::debug;

use alloy::signers::local::PrivateKeySigner;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapperMode {
    Dex,
    Cex,
    Hybrid,
}

pub struct Config {
    pub liquidator_identity: Arc<dyn Identity>,
    pub trader_identity: Arc<dyn Identity>,
    pub liquidator_principal: Principal,
    pub trader_principal: Principal,
    pub ic_url: String,
    pub evm_rpc_url: String,
    pub evm_private_key: String,
    pub lending_canister: Principal,
    pub export_path: String,
    pub buy_bad_debt: bool,
    pub db_path: String,
    pub max_allowed_dex_slippage: u32,
    pub swapper: SwapperMode,
    pub cex_credentials: HashMap<String, (String, String)>,
}

#[cfg_attr(test, mockall::automock)]
pub trait ConfigTrait: Send + Sync {
    fn get_liquidator_principal(&self) -> Principal;
    fn get_trader_principal(&self) -> Principal;
    fn should_buy_bad_debt(&self) -> bool;
    fn get_max_allowed_dex_slippage(&self) -> u32;
    fn get_lending_canister(&self) -> Principal;
    fn get_recovery_account(&self) -> Account;
    fn exchange_deposit_account_for(&self, chain: &str) -> Result<Account, String>;
    fn exchange_withdraw_address_for(&self, chain: &str) -> Result<String, String>;
    fn get_swapper_mode(&self) -> SwapperMode;
    fn get_cex_credentials(&self, cex: &str) -> Result<(String, String), String>;
}

impl ConfigTrait for Config {
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

    fn get_max_allowed_dex_slippage(&self) -> u32 {
        self.max_allowed_dex_slippage
    }

    fn get_swapper_mode(&self) -> SwapperMode {
        self.swapper
    }

    fn get_cex_credentials(&self, cex: &str) -> Result<(String, String), String> {
        self.cex_credentials
            .get(cex)
            .ok_or("Cex credentials not found".to_string())
            .cloned()
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

        let mnemonic_path = env::var("MNEMONIC_FILE").expect("MNEMONIC_FILE not configured");

        let mnemonic = std::fs::read_to_string(&mnemonic_path)
            .expect("failed to read mnemonic file")
            .trim()
            .to_string();

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
        let lending_canister = Principal::from_text(env::var("LENDING_CANISTER").unwrap()).unwrap();

        // The db path
        let db_path = env::var("DB_PATH").unwrap_or(format!("{}/wal.db", home));

        // Derive EVM private key
        let sk = derive_evm_private_key(&mnemonic, 0, 0)?;
        let evm_signer: PrivateKeySigner = PrivateKeySigner::from_slice(&sk.to_bytes()).map_err(|e| e.to_string())?;
        let hex = evm_signer.to_bytes().encode_hex();
        let evm_private_key = format!("{:#}", hex);

        let max_allowed_dex_slippage: u32 = std::env::var("MAX_ALLOWED_DEX_SLIPPAGE")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(500); // default 5%

        let swapper = match env::var("SWAPPER")
            .unwrap_or_else(|_| "hybrid".to_string())
            .to_lowercase()
            .as_str()
        {
            "dex" => SwapperMode::Dex,
            "cex" => SwapperMode::Cex,
            "hybrid" => SwapperMode::Hybrid,
            other => {
                debug!("Unknown SWAPPER value '{}', defaulting to hybrid", other);
                SwapperMode::Hybrid
            }
        };

        debug!("Loading cex credentials...");
        let cex_credentials = load_cex_credentials();
        debug!("Cex credentials loaded...");
        Ok(Arc::new(Config {
            evm_private_key,
            evm_rpc_url: env::var("EVM_RPC_URL").expect("EVM_RPC_URL not configured"),
            liquidator_identity: Arc::new(liquidator_identity),
            ic_url,
            liquidator_principal,
            trader_identity: Arc::new(trader_identity),
            trader_principal,
            lending_canister,
            export_path,
            buy_bad_debt,
            db_path,
            max_allowed_dex_slippage,
            swapper,
            cex_credentials,
        }))
    }
}

fn load_cex_credentials() -> HashMap<String, (String, String)> {
    let mut cex_credentials: HashMap<String, (String, String)> = HashMap::new();

    for (key, value) in std::env::vars() {
        // match CEX_NAME_API_KEY
        if let Some(name) = key.strip_prefix("CEX_").and_then(|s| s.strip_suffix("_API_KEY")) {
            let name_lower = name.to_lowercase();
            let secret_var = format!("CEX_{}_API_SECRET", name);

            match std::env::var(&secret_var) {
                Ok(secret) => {
                    debug!("Loaded CEX credentials for '{}'", name_lower);
                    cex_credentials.insert(name_lower, (value, secret));
                }
                Err(_) => {
                    debug!("Found {} but missing {}", key, secret_var);
                }
            }
        }
    }

    cex_credentials
}
