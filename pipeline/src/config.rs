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
    /// Minimum USD notional allowed for a single CEX execution chunk.
    /// Chunks below this threshold are treated as dust.
    pub cex_min_exec_usd: f64,
    /// Target fraction of max allowed slippage used for slice sizing.
    /// Example: 0.85 with 200 bps cap targets ~170 bps per slice.
    pub cex_slice_target_ratio: f64,
    /// Trigger ratio for buy-side truncation before attempting inverse/base fallback.
    pub cex_buy_truncation_trigger_ratio: f64,
    /// Maximum quote overspend allowed for inverse/base buy fallback, in bps.
    pub cex_buy_inverse_overspend_bps: u32,
    /// Maximum inverse/base fallback retries per leg.
    pub cex_buy_inverse_max_retries: u32,
    /// Enables buy-side inverse/base fallback after truncation.
    pub cex_buy_inverse_enabled: bool,
    /// Base delay for retryable CEX failures, in seconds.
    pub cex_retry_base_secs: u64,
    /// Maximum retry delay cap, in seconds.
    pub cex_retry_max_secs: u64,
    /// Minimum projected net edge required to execute, in bps.
    pub cex_min_net_edge_bps: u32,
    /// Extra safety haircut for price-move risk during execution latency, in bps.
    pub cex_delay_buffer_bps: u32,
    /// Route fee estimate in bps subtracted from projected edge.
    pub cex_route_fee_bps: u32,
    /// In hybrid mode, skip route candidate comparison and force CEX when
    /// estimated swap notional is above this USD threshold.
    /// Set to `0` to disable force-over-threshold behavior.
    pub cex_force_over_usd_threshold: f64,
    pub swapper: SwapperMode,
    pub cex_credentials: HashMap<String, (String, String)>,
    pub opportunity_account_filter: Vec<Principal>,
}

#[allow(dead_code)]
#[cfg_attr(test, mockall::automock)]
pub trait ConfigTrait: Send + Sync {
    fn get_liquidator_principal(&self) -> Principal;
    fn get_trader_principal(&self) -> Principal;
    fn should_buy_bad_debt(&self) -> bool;
    fn get_max_allowed_dex_slippage(&self) -> u32;
    fn get_max_allowed_cex_slippage_bps(&self) -> u32;
    fn get_cex_min_exec_usd(&self) -> f64;
    fn get_cex_slice_target_ratio(&self) -> f64;
    fn get_cex_buy_truncation_trigger_ratio(&self) -> f64;
    fn get_cex_buy_inverse_overspend_bps(&self) -> u32;
    fn get_cex_buy_inverse_max_retries(&self) -> u32;
    fn get_cex_buy_inverse_enabled(&self) -> bool;
    fn get_cex_retry_base_secs(&self) -> u64;
    fn get_cex_retry_max_secs(&self) -> u64;
    fn get_cex_min_net_edge_bps(&self) -> u32;
    fn get_cex_delay_buffer_bps(&self) -> u32;
    fn get_cex_route_fee_bps(&self) -> u32;
    fn get_cex_force_over_usd_threshold(&self) -> f64;
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

    fn get_cex_min_exec_usd(&self) -> f64 {
        self.cex_min_exec_usd
    }

    fn get_cex_slice_target_ratio(&self) -> f64 {
        self.cex_slice_target_ratio
    }

    fn get_cex_buy_truncation_trigger_ratio(&self) -> f64 {
        self.cex_buy_truncation_trigger_ratio
    }

    fn get_cex_buy_inverse_overspend_bps(&self) -> u32 {
        self.cex_buy_inverse_overspend_bps
    }

    fn get_cex_buy_inverse_max_retries(&self) -> u32 {
        self.cex_buy_inverse_max_retries
    }

    fn get_cex_buy_inverse_enabled(&self) -> bool {
        self.cex_buy_inverse_enabled
    }

    fn get_cex_retry_base_secs(&self) -> u64 {
        self.cex_retry_base_secs
    }

    fn get_cex_retry_max_secs(&self) -> u64 {
        self.cex_retry_max_secs
    }

    fn get_cex_min_net_edge_bps(&self) -> u32 {
        self.cex_min_net_edge_bps
    }

    fn get_cex_delay_buffer_bps(&self) -> u32 {
        self.cex_delay_buffer_bps
    }

    fn get_cex_route_fee_bps(&self) -> u32 {
        self.cex_route_fee_bps
    }

    fn get_cex_force_over_usd_threshold(&self) -> f64 {
        self.cex_force_over_usd_threshold
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
        let cex_tunables = parse_cex_tunables_from_env();

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
            cex_min_exec_usd: cex_tunables.min_exec_usd,
            cex_slice_target_ratio: cex_tunables.slice_target_ratio,
            cex_buy_truncation_trigger_ratio: cex_tunables.buy_truncation_trigger_ratio,
            cex_buy_inverse_overspend_bps: cex_tunables.buy_inverse_overspend_bps,
            cex_buy_inverse_max_retries: cex_tunables.buy_inverse_max_retries,
            cex_buy_inverse_enabled: cex_tunables.buy_inverse_enabled,
            cex_retry_base_secs: cex_tunables.retry_base_secs,
            cex_retry_max_secs: cex_tunables.retry_max_secs,
            cex_min_net_edge_bps: cex_tunables.min_net_edge_bps,
            cex_delay_buffer_bps: cex_tunables.delay_buffer_bps,
            cex_route_fee_bps: cex_tunables.route_fee_bps,
            cex_force_over_usd_threshold: cex_tunables.force_over_usd_threshold,
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

#[derive(Clone, Copy, Debug, PartialEq)]
struct CexTunables {
    min_exec_usd: f64,
    slice_target_ratio: f64,
    buy_truncation_trigger_ratio: f64,
    buy_inverse_overspend_bps: u32,
    buy_inverse_max_retries: u32,
    buy_inverse_enabled: bool,
    retry_base_secs: u64,
    retry_max_secs: u64,
    min_net_edge_bps: u32,
    delay_buffer_bps: u32,
    route_fee_bps: u32,
    force_over_usd_threshold: f64,
}

const DEFAULT_CEX_MIN_EXEC_USD: f64 = 2.0;
const DEFAULT_CEX_SLICE_TARGET_RATIO: f64 = 0.7;
const DEFAULT_CEX_BUY_TRUNCATION_TRIGGER_RATIO: f64 = 0.25;
const DEFAULT_CEX_BUY_INVERSE_OVERSPEND_BPS: u32 = 10;
const MAX_CEX_BUY_INVERSE_OVERSPEND_BPS: u32 = 100;
const DEFAULT_CEX_BUY_INVERSE_MAX_RETRIES: u32 = 1;
const MAX_CEX_BUY_INVERSE_MAX_RETRIES: u32 = 3;
const DEFAULT_CEX_BUY_INVERSE_ENABLED: bool = true;
const DEFAULT_CEX_RETRY_BASE_SECS: u64 = 5;
const DEFAULT_CEX_RETRY_MAX_SECS: u64 = 120;
const DEFAULT_CEX_MIN_NET_EDGE_BPS: u32 = 150;
const DEFAULT_CEX_DELAY_BUFFER_BPS: u32 = 75;
const DEFAULT_CEX_ROUTE_FEE_BPS: u32 = 25;
const DEFAULT_CEX_FORCE_OVER_USD_THRESHOLD: f64 = 12.5;
const MIN_RATIO: f64 = 0.0;
const MAX_RATIO: f64 = 1.0;
const MIN_SLICE_TARGET_RATIO: f64 = 0.1;

fn parse_cex_tunables_from_env() -> CexTunables {
    // Defaults are conservative; env overrides are expected in .env.
    let min_exec_usd = env::var("CEX_MIN_EXEC_USD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(DEFAULT_CEX_MIN_EXEC_USD);

    let slice_target_ratio = env::var("CEX_SLICE_TARGET_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(MIN_SLICE_TARGET_RATIO, MAX_RATIO))
        .unwrap_or(DEFAULT_CEX_SLICE_TARGET_RATIO);

    let buy_truncation_trigger_ratio = env::var("CEX_BUY_TRUNCATION_TRIGGER_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(MIN_RATIO, MAX_RATIO))
        .unwrap_or(DEFAULT_CEX_BUY_TRUNCATION_TRIGGER_RATIO);

    let buy_inverse_overspend_bps = env::var("CEX_BUY_INVERSE_OVERSPEND_BPS")
        .or_else(|_| env::var("CEX_BUY_INVERSE_OVESPEND_BPS"))
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .map(|v| v.min(MAX_CEX_BUY_INVERSE_OVERSPEND_BPS))
        .unwrap_or(DEFAULT_CEX_BUY_INVERSE_OVERSPEND_BPS);

    let buy_inverse_max_retries = env::var("CEX_BUY_INVERSE_MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .map(|v| v.min(MAX_CEX_BUY_INVERSE_MAX_RETRIES))
        .unwrap_or(DEFAULT_CEX_BUY_INVERSE_MAX_RETRIES);

    let buy_inverse_enabled = env::var("CEX_BUY_INVERSE_ENABLED")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(DEFAULT_CEX_BUY_INVERSE_ENABLED);

    let retry_base_secs = env::var("CEX_RETRY_BASE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CEX_RETRY_BASE_SECS);

    let retry_max_secs = env::var("CEX_RETRY_MAX_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v >= retry_base_secs)
        .unwrap_or(DEFAULT_CEX_RETRY_MAX_SECS)
        .max(retry_base_secs);

    let min_net_edge_bps = env::var("CEX_MIN_NET_EDGE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CEX_MIN_NET_EDGE_BPS);

    let delay_buffer_bps = env::var("CEX_DELAY_BUFFER_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CEX_DELAY_BUFFER_BPS);

    let route_fee_bps = env::var("CEX_ROUTE_FEE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CEX_ROUTE_FEE_BPS);

    let force_over_usd_threshold = env::var("CEX_FORCE_OVER_USD_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v >= MIN_RATIO)
        .unwrap_or(DEFAULT_CEX_FORCE_OVER_USD_THRESHOLD);

    CexTunables {
        min_exec_usd,
        slice_target_ratio,
        buy_truncation_trigger_ratio,
        buy_inverse_overspend_bps,
        buy_inverse_max_retries,
        buy_inverse_enabled,
        retry_base_secs,
        retry_max_secs,
        min_net_edge_bps,
        delay_buffer_bps,
        route_fee_bps,
        force_over_usd_threshold,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn parse_cex_tunables_uses_defaults() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let vars = [
            "CEX_MIN_EXEC_USD",
            "CEX_SLICE_TARGET_RATIO",
            "CEX_BUY_TRUNCATION_TRIGGER_RATIO",
            "CEX_BUY_INVERSE_OVERSPEND_BPS",
            "CEX_BUY_INVERSE_OVESPEND_BPS",
            "CEX_BUY_INVERSE_MAX_RETRIES",
            "CEX_BUY_INVERSE_ENABLED",
            "CEX_RETRY_BASE_SECS",
            "CEX_RETRY_MAX_SECS",
            "CEX_MIN_NET_EDGE_BPS",
            "CEX_DELAY_BUFFER_BPS",
            "CEX_ROUTE_FEE_BPS",
            "CEX_FORCE_OVER_USD_THRESHOLD",
        ];
        for key in vars {
            unsafe { env::remove_var(key) };
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(
            parsed,
            CexTunables {
                min_exec_usd: 2.0,
                slice_target_ratio: 0.7,
                buy_truncation_trigger_ratio: 0.25,
                buy_inverse_overspend_bps: 10,
                buy_inverse_max_retries: 1,
                buy_inverse_enabled: true,
                retry_base_secs: 5,
                retry_max_secs: 120,
                min_net_edge_bps: 150,
                delay_buffer_bps: 75,
                route_fee_bps: 25,
                force_over_usd_threshold: 12.5,
            }
        );
    }

    #[test]
    fn parse_cex_tunables_respects_overrides_and_guards() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_MIN_EXEC_USD", "1.05");
            env::set_var("CEX_SLICE_TARGET_RATIO", "0.85");
            env::set_var("CEX_BUY_TRUNCATION_TRIGGER_RATIO", "0.4");
            env::set_var("CEX_BUY_INVERSE_OVERSPEND_BPS", "20");
            env::set_var("CEX_BUY_INVERSE_MAX_RETRIES", "2");
            env::set_var("CEX_BUY_INVERSE_ENABLED", "false");
            env::set_var("CEX_RETRY_BASE_SECS", "7");
            env::set_var("CEX_RETRY_MAX_SECS", "240");
            env::set_var("CEX_MIN_NET_EDGE_BPS", "160");
            env::set_var("CEX_DELAY_BUFFER_BPS", "90");
            env::set_var("CEX_ROUTE_FEE_BPS", "0");
            env::set_var("CEX_FORCE_OVER_USD_THRESHOLD", "5.75");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(
            parsed,
            CexTunables {
                min_exec_usd: 1.05,
                slice_target_ratio: 0.85,
                buy_truncation_trigger_ratio: 0.4,
                buy_inverse_overspend_bps: 20,
                buy_inverse_max_retries: 2,
                buy_inverse_enabled: false,
                retry_base_secs: 7,
                retry_max_secs: 240,
                min_net_edge_bps: 160,
                delay_buffer_bps: 90,
                route_fee_bps: 0,
                force_over_usd_threshold: 5.75,
            }
        );
    }

    #[test]
    fn parse_cex_tunables_clamps_invalid_values() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_MIN_EXEC_USD", "0");
            env::set_var("CEX_SLICE_TARGET_RATIO", "99");
            env::set_var("CEX_BUY_TRUNCATION_TRIGGER_RATIO", "-1");
            env::set_var("CEX_BUY_INVERSE_OVERSPEND_BPS", "999");
            env::set_var("CEX_BUY_INVERSE_MAX_RETRIES", "9");
            env::set_var("CEX_BUY_INVERSE_ENABLED", "not-a-bool");
            env::set_var("CEX_RETRY_BASE_SECS", "10");
            env::set_var("CEX_RETRY_MAX_SECS", "1");
            env::set_var("CEX_FORCE_OVER_USD_THRESHOLD", "-1");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.min_exec_usd, 2.0);
        assert_eq!(parsed.slice_target_ratio, 1.0);
        assert_eq!(parsed.buy_truncation_trigger_ratio, 0.0);
        assert_eq!(parsed.buy_inverse_overspend_bps, 100);
        assert_eq!(parsed.buy_inverse_max_retries, 3);
        assert!(parsed.buy_inverse_enabled);
        assert_eq!(parsed.retry_base_secs, 10);
        assert_eq!(parsed.retry_max_secs, 120);
        assert_eq!(parsed.force_over_usd_threshold, 12.5);
    }

    #[test]
    fn parse_cex_tunables_allows_zero_force_threshold_for_disable() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_FORCE_OVER_USD_THRESHOLD", "0");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.force_over_usd_threshold, 0.0);
    }

    #[test]
    fn parse_cex_tunables_accepts_legacy_buy_inverse_overspend_key() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("CEX_BUY_INVERSE_OVERSPEND_BPS");
            env::set_var("CEX_BUY_INVERSE_OVESPEND_BPS", "33");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.buy_inverse_overspend_bps, 33);
    }

    #[test]
    fn parse_cex_tunables_prefers_new_buy_inverse_overspend_key() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_BUY_INVERSE_OVERSPEND_BPS", "44");
            env::set_var("CEX_BUY_INVERSE_OVESPEND_BPS", "55");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.buy_inverse_overspend_bps, 44);
    }

    #[test]
    fn parse_cex_tunables_keeps_retry_max_at_least_retry_base() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_RETRY_BASE_SECS", "300");
            env::remove_var("CEX_RETRY_MAX_SECS");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.retry_base_secs, 300);
        assert_eq!(parsed.retry_max_secs, 300);
    }
}
