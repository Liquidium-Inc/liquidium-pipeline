use alloy::hex::ToHexExt;
use alloy::signers::local::PrivateKeySigner;
use candid::Principal;
use ic_agent::Identity;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_commons::{
    env::config_dir,
    error::{CodedError, ErrorCode, ExternalError},
};
use liquidium_pipeline_connectors::account::icp_account::{RECOVERY_ACCOUNT, derive_icp_identity};
use liquidium_pipeline_connectors::crypto::derivation::derive_evm_private_key;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

fn expand_tilde(p: &str) -> std::path::PathBuf {
    if let Some(stripped) = p.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return std::path::PathBuf::from(home).join(stripped);
    }
    std::path::PathBuf::from(p)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapperMode {
    Dex,
    Cex,
    Hybrid,
}

pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required env var {var}")]
    MissingEnv {
        var: &'static str,
        #[source]
        source: env::VarError,
    },
    #[error("failed to parse principal in {var}")]
    InvalidPrincipal {
        var: &'static str,
        #[source]
        source: ExternalError,
    },
    #[error("failed to read mnemonic at {path:?}")]
    ReadMnemonic {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not derive {role} identity")]
    DeriveIdentity {
        role: &'static str,
        #[source]
        source: ExternalError,
    },
    #[error("could not derive {role} principal")]
    DerivePrincipal {
        role: &'static str,
        #[source]
        source: ExternalError,
    },
    #[error("failed to derive EVM private key")]
    DeriveEvmKey {
        #[source]
        source: ExternalError,
    },
    #[error("missing CEX credentials for {cex}")]
    MissingCexCredentials { cex: String },
}

impl CodedError for ConfigError {
    fn code(&self) -> ErrorCode {
        match self {
            ConfigError::MissingEnv { .. } => ErrorCode::ConfigMissingEnv,
            ConfigError::InvalidPrincipal { .. } => ErrorCode::ConfigInvalidPrincipal,
            ConfigError::ReadMnemonic { .. } => ErrorCode::ConfigReadMnemonic,
            ConfigError::DeriveIdentity { .. } | ConfigError::DerivePrincipal { .. } => {
                ErrorCode::ConfigDeriveIdentity
            }
            ConfigError::DeriveEvmKey { .. } => ErrorCode::ConfigDeriveEvmKey,
            ConfigError::MissingCexCredentials { .. } => ErrorCode::ConfigMissingCexCredentials,
        }
    }
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
    pub max_allowed_cex_slippage_bps: u32,
    pub swapper: SwapperMode,
    pub cex_credentials: HashMap<String, (String, String)>,
    pub opportunity_account_filter: Vec<Principal>,
}

#[cfg_attr(test, mockall::automock)]
pub trait ConfigTrait: Send + Sync {
    fn get_liquidator_principal(&self) -> Principal;
    fn get_trader_principal(&self) -> Principal;
    fn should_buy_bad_debt(&self) -> bool;
    fn get_max_allowed_dex_slippage(&self) -> u32;
    fn get_max_allowed_cex_slippage_bps(&self) -> u32;
    #[allow(dead_code)]
    fn get_lending_canister(&self) -> Principal;
    #[allow(dead_code)]
    fn get_recovery_account(&self) -> Account;
    fn get_swapper_mode(&self) -> SwapperMode;
    fn get_cex_credentials(&self, cex: &str) -> ConfigResult<(String, String)>;
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

    fn get_max_allowed_dex_slippage(&self) -> u32 {
        self.max_allowed_dex_slippage
    }

    fn get_max_allowed_cex_slippage_bps(&self) -> u32 {
        self.max_allowed_cex_slippage_bps
    }

    fn get_swapper_mode(&self) -> SwapperMode {
        self.swapper
    }

    fn get_cex_credentials(&self, cex: &str) -> ConfigResult<(String, String)> {
        self.cex_credentials
            .get(cex)
            .cloned()
            .ok_or_else(|| ConfigError::MissingCexCredentials {
                cex: cex.to_string(),
            })
    }
}

impl Config {
    pub async fn load() -> ConfigResult<Arc<Self>> {
        let home = config_dir();

        let ic_url = env::var("IC_URL").map_err(|source| ConfigError::MissingEnv {
            var: "IC_URL",
            source,
        })?;
        let export_path = env::var("EXPORT_PATH").unwrap_or("executions.csv".to_string());

        let mnemonic_path = expand_tilde(
            &env::var("MNEMONIC_FILE").map_err(|source| ConfigError::MissingEnv {
                var: "MNEMONIC_FILE",
                source,
            })?,
        );

        let mnemonic = std::fs::read_to_string(&mnemonic_path)
            .map_err(|source| ConfigError::ReadMnemonic {
                path: mnemonic_path.clone(),
                source,
            })?
            .trim()
            .to_string();

        let liquidator_identity =
            derive_icp_identity(&mnemonic, 0, 0).map_err(|e| ConfigError::DeriveIdentity {
                role: "liquidator",
                source: ExternalError::from(e.to_string()),
            })?;
        let liquidator_principal = liquidator_identity.sender().map_err(|e| ConfigError::DerivePrincipal {
            role: "liquidator",
            source: ExternalError::from(e.to_string()),
        })?;

        let trader_identity =
            derive_icp_identity(&mnemonic, 0, 1).map_err(|e| ConfigError::DeriveIdentity {
                role: "trader",
                source: ExternalError::from(e.to_string()),
            })?;
        let trader_principal = trader_identity.sender().map_err(|e| ConfigError::DerivePrincipal {
            role: "trader",
            source: ExternalError::from(e.to_string()),
        })?;

        let buy_bad_debt = env::var("BUY_BAD_DEBT")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false);

        debug!("Liquidator ID {}", liquidator_principal);
        debug!("Trader ID {}", trader_principal);

        // Load the asset maps
        let lending_canister_str =
            env::var("LENDING_CANISTER").map_err(|source| ConfigError::MissingEnv {
                var: "LENDING_CANISTER",
                source,
            })?;
        let lending_canister =
            Principal::from_text(&lending_canister_str).map_err(|e| ConfigError::InvalidPrincipal {
                var: "LENDING_CANISTER",
                source: ExternalError::from(e.to_string()),
            })?;

        // The db path
        let db_path = env::var("DB_PATH").unwrap_or(format!("{}/wal.db", home));

        // Derive EVM private key
        let sk = derive_evm_private_key(&mnemonic, 0, 0).map_err(|e| ConfigError::DeriveEvmKey {
            source: ExternalError::from(e.to_string()),
        })?;
        let evm_signer: PrivateKeySigner = PrivateKeySigner::from_slice(&sk.to_bytes()).map_err(|e| {
            ConfigError::DeriveEvmKey {
                source: ExternalError::from(e.to_string()),
            }
        })?;
        let hex = evm_signer.to_bytes().encode_hex();
        let evm_private_key = format!("{:#}", hex);

        let max_allowed_dex_slippage: u32 = std::env::var("MAX_ALLOWED_DEX_SLIPPAGE")
            .or_else(|_| std::env::var("MAX_ALLOWED_SLIPPAGE_BPS"))
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(125); // default 1.25%

        let max_allowed_cex_slippage_bps: u32 = std::env::var("MAX_ALLOWED_CEX_SLIPPAGE_BPS")
            .or_else(|_| std::env::var("MAX_ALLOWED_SLIPPAGE_BPS"))
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(200);

        let swapper_raw = env::var("SWAPPER").unwrap_or_else(|_| "hybrid".to_string());
        let swapper = match swapper_raw.trim().to_lowercase().as_str() {
            "" => SwapperMode::Hybrid,
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

        let opportunity_account_filter = match env::var("OPPORTUNITY_ACCOUNT_FILTER") {
            Ok(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
                    vec![]
                } else {
                    trimmed
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .filter_map(|item| Principal::from_text(item).ok())
                        .collect()
                }
            }
            Err(_) => vec![],
        };

        let evm_rpc_url = env::var("EVM_RPC_URL").map_err(|source| ConfigError::MissingEnv {
            var: "EVM_RPC_URL",
            source,
        })?;

        Ok(Arc::new(Config {
            evm_private_key,
            evm_rpc_url,
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
            max_allowed_cex_slippage_bps,
            swapper,
            cex_credentials,
            opportunity_account_filter,
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
