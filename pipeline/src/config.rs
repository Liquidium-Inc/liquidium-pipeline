use alloy::hex::ToHexExt;
use alloy::signers::local::PrivateKeySigner;
use candid::Principal;
use ic_agent::Identity;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_commons::env::config_dir;
use liquidium_pipeline_connectors::account::icp_account::{RECOVERY_ACCOUNT, derive_icp_identity};
use liquidium_pipeline_connectors::crypto::derivation::derive_evm_private_key;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

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
    fn get_cex_min_exec_usd(&self) -> f64;
    fn get_cex_slice_target_ratio(&self) -> f64;
    fn get_cex_retry_base_secs(&self) -> u64;
    fn get_cex_retry_max_secs(&self) -> u64;
    fn get_cex_min_net_edge_bps(&self) -> u32;
    fn get_cex_delay_buffer_bps(&self) -> u32;
    fn get_cex_route_fee_bps(&self) -> u32;
    #[allow(dead_code)]
    fn get_lending_canister(&self) -> Principal;
    #[allow(dead_code)]
    fn get_recovery_account(&self) -> Account;
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
        let home = config_dir();

        let ic_url = env::var("IC_URL").map_err(|_| "IC_URL not configured".to_string())?;
        let export_path = env::var("EXPORT_PATH").unwrap_or("executions.csv".to_string());

        let mnemonic_path =
            expand_tilde(&env::var("MNEMONIC_FILE").map_err(|_| "MNEMONIC_FILE not configured".to_string())?);

        let mnemonic = std::fs::read_to_string(&mnemonic_path)
            .map_err(|e| format!("failed to read mnemonic file: {e}"))?
            .trim()
            .to_string();

        let liquidator_identity =
            derive_icp_identity(&mnemonic, 0, 0).map_err(|e| format!("could not create liquidator identity: {e}"))?;
        let liquidator_principal = liquidator_identity
            .sender()
            .map_err(|e| format!("could not decode liquidator principal: {e}"))?;

        let trader_identity =
            derive_icp_identity(&mnemonic, 0, 1).map_err(|e| format!("could not create trader identity: {e}"))?;
        let trader_principal = trader_identity
            .sender()
            .map_err(|e| format!("could not decode trader principal: {e}"))?;

        let buy_bad_debt = env::var("BUY_BAD_DEBT")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false);

        debug!("Liquidator ID {}", liquidator_principal);
        debug!("Trader ID {}", trader_principal);

        // Load the asset maps
        let lending_canister_str =
            env::var("LENDING_CANISTER").map_err(|_| "LENDING_CANISTER not configured".to_string())?;
        let lending_canister = Principal::from_text(&lending_canister_str)
            .map_err(|e| format!("invalid LENDING_CANISTER principal: {e}"))?;

        // The db path
        let db_path = env::var("DB_PATH").unwrap_or(format!("{}/wal.db", home));

        // Derive EVM private key
        let sk = derive_evm_private_key(&mnemonic, 0, 0)?;
        let evm_signer: PrivateKeySigner = PrivateKeySigner::from_slice(&sk.to_bytes()).map_err(|e| e.to_string())?;
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

        let evm_rpc_url = env::var("EVM_RPC_URL").map_err(|_| "EVM_RPC_URL not configured".to_string())?;

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
            cex_retry_base_secs: cex_tunables.retry_base_secs,
            cex_retry_max_secs: cex_tunables.retry_max_secs,
            cex_min_net_edge_bps: cex_tunables.min_net_edge_bps,
            cex_delay_buffer_bps: cex_tunables.delay_buffer_bps,
            cex_route_fee_bps: cex_tunables.route_fee_bps,
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
    retry_base_secs: u64,
    retry_max_secs: u64,
    min_net_edge_bps: u32,
    delay_buffer_bps: u32,
    route_fee_bps: u32,
}

fn parse_cex_tunables_from_env() -> CexTunables {
    // Defaults are conservative; env overrides are expected in .env.
    let min_exec_usd = env::var("CEX_MIN_EXEC_USD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(2.0);

    let slice_target_ratio = env::var("CEX_SLICE_TARGET_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.1, 1.0))
        .unwrap_or(0.7);

    let retry_base_secs = env::var("CEX_RETRY_BASE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(5);

    let retry_max_secs = env::var("CEX_RETRY_MAX_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v >= retry_base_secs)
        .unwrap_or(120);

    let min_net_edge_bps = env::var("CEX_MIN_NET_EDGE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(150);

    let delay_buffer_bps = env::var("CEX_DELAY_BUFFER_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(75);

    let route_fee_bps = env::var("CEX_ROUTE_FEE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(25);

    CexTunables {
        min_exec_usd,
        slice_target_ratio,
        retry_base_secs,
        retry_max_secs,
        min_net_edge_bps,
        delay_buffer_bps,
        route_fee_bps,
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
            "CEX_RETRY_BASE_SECS",
            "CEX_RETRY_MAX_SECS",
            "CEX_MIN_NET_EDGE_BPS",
            "CEX_DELAY_BUFFER_BPS",
            "CEX_ROUTE_FEE_BPS",
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
                retry_base_secs: 5,
                retry_max_secs: 120,
                min_net_edge_bps: 150,
                delay_buffer_bps: 75,
                route_fee_bps: 25,
            }
        );
    }

    #[test]
    fn parse_cex_tunables_respects_overrides_and_guards() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_MIN_EXEC_USD", "1.05");
            env::set_var("CEX_SLICE_TARGET_RATIO", "0.85");
            env::set_var("CEX_RETRY_BASE_SECS", "7");
            env::set_var("CEX_RETRY_MAX_SECS", "240");
            env::set_var("CEX_MIN_NET_EDGE_BPS", "160");
            env::set_var("CEX_DELAY_BUFFER_BPS", "90");
            env::set_var("CEX_ROUTE_FEE_BPS", "0");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(
            parsed,
            CexTunables {
                min_exec_usd: 1.05,
                slice_target_ratio: 0.85,
                retry_base_secs: 7,
                retry_max_secs: 240,
                min_net_edge_bps: 160,
                delay_buffer_bps: 90,
                route_fee_bps: 0,
            }
        );
    }

    #[test]
    fn parse_cex_tunables_clamps_invalid_values() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_MIN_EXEC_USD", "0");
            env::set_var("CEX_SLICE_TARGET_RATIO", "99");
            env::set_var("CEX_RETRY_BASE_SECS", "10");
            env::set_var("CEX_RETRY_MAX_SECS", "1");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.min_exec_usd, 2.0);
        assert_eq!(parsed.slice_target_ratio, 1.0);
        assert_eq!(parsed.retry_base_secs, 10);
        assert_eq!(parsed.retry_max_secs, 120);
    }
}
