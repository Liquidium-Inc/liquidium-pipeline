use alloy::hex::ToHexExt;
use alloy::signers::local::PrivateKeySigner;
use candid::Principal;
use ic_agent::Identity;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_commons::env::config_dir;
use liquidium_pipeline_connectors::account::icp_account::{RECOVERY_ACCOUNT, derive_icp_identity};
use liquidium_pipeline_connectors::crypto::derivation::{derive_btc_p2tr_address, derive_evm_private_key};
use log::{debug, warn};
use std::collections::{HashMap, HashSet};
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

pub struct Config {
    /// Mnemonic retained only for deterministic, per-liquidation ICPSwap
    /// session derivation. It is never serialized or logged.
    pub(crate) icpswap_mnemonic: Arc<str>,
    pub liquidator_identity: Arc<dyn Identity>,
    pub trader_identity: Arc<dyn Identity>,
    pub bridge_ic_identity: Arc<dyn Identity>,
    pub liquidator_principal: Principal,
    pub trader_principal: Principal,
    pub ic_url: String,
    pub evm_rpc_url: String,
    pub evm_private_key: String,
    pub bridge_evm_private_key: String,
    pub bridge_evm_address: String,
    pub bridge_ic_owner_principal: Principal,
    pub bridge_btc_address: String,
    pub bridge_cketh_minter_canister: Principal,
    pub lending_canister: Principal,
    pub export_path: String,
    pub buy_bad_debt: bool,
    pub db_path: String,
    pub liquidations_db_path: String,
    pub max_allowed_dex_slippage: u32,
    pub max_allowed_cex_slippage_bps: u32,
    pub bad_debt_collateral_slippage_bps: u32,
    /// Minimum USD notional allowed for a single CEX execution chunk.
    /// Chunks below this threshold are treated as dust.
    pub cex_min_exec_usd: f64,
    /// Minimum USD a CEX leg must still be worth on the way out, before the
    /// dust the slicer may abandon is allowed for. A venue refuses to withdraw
    /// less than its own per-asset minimum, and that refusal lands after the
    /// trade, with the proceeds already on the exchange, so the floor is applied
    /// while the leg is only a plan. Zero disables the check.
    pub cex_min_leg_receive_usd: f64,
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
    /// Minimum projected net edge required for any multi-venue plan, in bps.
    pub multi_venue_min_net_edge_bps: u32,
    /// Signed edge floor used only for bad-debt liquidations. See
    /// `parse_multi_venue_bad_debt_min_net_edge_bps_from_env`.
    pub multi_venue_bad_debt_min_net_edge_bps: i32,
    /// Maximum downside from the oracle-implied output allowed for any venue
    /// quote, in bps. Applied centrally after venue quote normalization.
    pub multi_venue_max_oracle_discount_bps: u32,
    /// How old a recorded price may be, in seconds, before the venue quote guard
    /// stops using it as a fallback for a live oracle read.
    pub multi_venue_oracle_snapshot_max_age_secs: i64,
    /// Extra safety haircut for price-move risk during execution latency, in bps.
    pub cex_delay_buffer_bps: u32,
    /// Route fee estimate in bps subtracted from projected edge.
    pub cex_route_fee_bps: u32,
    /// Configured MEXC market universe for hop-based route discovery.
    /// Markets are normalized as `BASE_QUOTE`.
    pub cex_mexc_available_pairs: Vec<String>,
    /// Maximum intermediate hops allowed for MEXC route discovery.
    /// Example: `2` allows up to 3 legs total.
    pub cex_mexc_max_hops: u8,
    /// Optional Kraken market allowlist, normalized as `BASE_QUOTE`.
    pub cex_kraken_available_pairs: Vec<String>,
    /// Maximum intermediate hops allowed for Kraken route discovery.
    pub cex_kraken_max_hops: u8,
    pub icpswap_factory_canister: Principal,
    pub icpswap_fee_tiers: Vec<candid::Nat>,
    /// Highest ICPSwap price impact an allocation may carry, in bps. The limit is
    /// strict: an allocation quoted exactly at it is rejected.
    pub icpswap_max_price_impact_bps: f64,
    /// Iteration budget for the planner's binary search for the largest safe
    /// ICPSwap allocation. Each iteration costs one pool quote.
    pub icpswap_max_search_iterations: u8,
    /// Higher ICPSwap impact cap used only when an overflow remainder is below
    /// the venue minimum and the planner considers sending the full amount DEX-side.
    pub icpswap_dust_fallback_max_price_impact_bps: f64,
    /// Test-only fixed USD allocation sent to ICPSwap before routing the
    /// remainder to an overflow venue. Unset preserves the normal policy.
    pub icpswap_test_allocation_usd: Option<f64>,
    /// Test-only fixed USD allocation sent to MEXC after the forced ICPSwap
    /// leg. Requires `icpswap_test_allocation_usd`; Kraken receives the rest.
    pub mexc_test_allocation_usd: Option<f64>,
    /// Ordered venue IDs eligible for new multi-venue plans.
    pub enabled_swap_venues: Vec<String>,
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
    fn get_bad_debt_collateral_slippage_bps(&self) -> u32;
    fn get_cex_min_exec_usd(&self) -> f64;
    fn get_cex_min_leg_receive_usd(&self) -> f64;
    fn get_cex_slice_target_ratio(&self) -> f64;
    fn get_cex_buy_truncation_trigger_ratio(&self) -> f64;
    fn get_cex_buy_inverse_overspend_bps(&self) -> u32;
    fn get_cex_buy_inverse_max_retries(&self) -> u32;
    fn get_cex_buy_inverse_enabled(&self) -> bool;
    fn get_cex_retry_base_secs(&self) -> u64;
    fn get_cex_retry_max_secs(&self) -> u64;
    fn get_cex_delay_buffer_bps(&self) -> u32;
    fn get_cex_route_fee_bps(&self) -> u32;
    fn get_cex_mexc_available_pairs(&self) -> Vec<String>;
    fn get_cex_mexc_max_hops(&self) -> u8;
    fn get_cex_kraken_available_pairs(&self) -> Vec<String>;
    fn get_cex_kraken_max_hops(&self) -> u8;
    #[allow(dead_code)]
    fn get_lending_canister(&self) -> Principal;
    #[allow(dead_code)]
    fn get_recovery_account(&self) -> Account;
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

    fn get_bad_debt_collateral_slippage_bps(&self) -> u32 {
        self.bad_debt_collateral_slippage_bps
    }

    fn get_cex_min_exec_usd(&self) -> f64 {
        self.cex_min_exec_usd
    }

    fn get_cex_min_leg_receive_usd(&self) -> f64 {
        self.cex_min_leg_receive_usd
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

    fn get_cex_delay_buffer_bps(&self) -> u32 {
        self.cex_delay_buffer_bps
    }

    fn get_cex_route_fee_bps(&self) -> u32 {
        self.cex_route_fee_bps
    }

    fn get_cex_mexc_available_pairs(&self) -> Vec<String> {
        self.cex_mexc_available_pairs.clone()
    }

    fn get_cex_mexc_max_hops(&self) -> u8 {
        self.cex_mexc_max_hops
    }

    fn get_cex_kraken_available_pairs(&self) -> Vec<String> {
        self.cex_kraken_available_pairs.clone()
    }

    fn get_cex_kraken_max_hops(&self) -> u8 {
        self.cex_kraken_max_hops
    }

    fn get_cex_credentials(&self, cex: &str) -> Result<(String, String), String> {
        self.cex_credentials
            .get(cex)
            .ok_or("Cex credentials not found".to_string())
            .cloned()
    }
}

impl Config {
    pub fn bridge_ic_account(&self) -> Account {
        Account {
            owner: self.bridge_ic_owner_principal,
            subaccount: None,
        }
    }

    pub async fn load() -> Result<Arc<Self>, String> {
        let home = config_dir();

        let ic_url = env::var("IC_URL").map_err(|_| "IC_URL not configured".to_string())?;
        let export_path_raw = env::var("EXPORT_PATH").unwrap_or(format!("{}/executions.csv", home));
        let export_path = expand_tilde(&export_path_raw).to_string_lossy().into_owned();

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
        let db_path_raw = env::var("DB_PATH").unwrap_or(format!("{}/wal.db", home));
        let db_path = expand_tilde(&db_path_raw).to_string_lossy().into_owned();
        let liquidations_db_path = match env::var("LIQUIDATIONS_DB_PATH") {
            Ok(path) => expand_tilde(&path),
            Err(_) => std::path::Path::new(&db_path)
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("liquidations.db"),
        }
        .to_string_lossy()
        .into_owned();
        validate_distinct_database_paths(&db_path, &liquidations_db_path)?;

        // Derive EVM private key
        let sk = derive_evm_private_key(&mnemonic, 0, 0)?;
        let evm_signer: PrivateKeySigner = PrivateKeySigner::from_slice(&sk.to_bytes()).map_err(|e| e.to_string())?;
        let hex = evm_signer.to_bytes().encode_hex();
        let evm_private_key = format!("{:#}", hex);

        // Derive dedicated bridge namespace identities.
        let bridge_sk = derive_evm_private_key(&mnemonic, BRIDGE_NAMESPACE_ACCOUNT, BRIDGE_EVM_INDEX)?;
        let bridge_signer = PrivateKeySigner::from_slice(&bridge_sk.to_bytes())
            .map_err(|e| format!("failed to create bridge EVM signer: {e}"))?;
        let bridge_evm_private_key = format!("{:#}", bridge_signer.to_bytes().encode_hex());
        let bridge_evm_address = bridge_signer.address().to_string();

        let bridge_ic_identity = derive_icp_identity(&mnemonic, BRIDGE_NAMESPACE_ACCOUNT, BRIDGE_ICP_INDEX)
            .map_err(|e| format!("could not create bridge identity: {e}"))?;
        let bridge_ic_owner_principal = bridge_ic_identity
            .sender()
            .map_err(|e| format!("could not decode bridge principal: {e}"))?;
        let bridge_btc_address = derive_btc_p2tr_address(&mnemonic, BRIDGE_NAMESPACE_ACCOUNT, BRIDGE_BTC_INDEX)
            .map_err(|e| format!("could not derive bridge BTC address: {e}"))?;

        let bridge_cketh_minter_canister_text = env::var("BRIDGE_CKETH_MINTER_CANISTER")
            .unwrap_or_else(|_| DEFAULT_BRIDGE_CKETH_MINTER_CANISTER.to_string())
            .trim()
            .to_string();
        let bridge_cketh_minter_canister_text = if bridge_cketh_minter_canister_text.is_empty() {
            DEFAULT_BRIDGE_CKETH_MINTER_CANISTER.to_string()
        } else {
            bridge_cketh_minter_canister_text
        };

        let bridge_cketh_minter_canister = Principal::from_text(&bridge_cketh_minter_canister_text).map_err(|e| {
            format!(
                "invalid BRIDGE_CKETH_MINTER_CANISTER principal '{}': {e}",
                bridge_cketh_minter_canister_text
            )
        })?;
        let max_allowed_dex_slippage =
            parse_slippage_bps_from_env("MAX_ALLOWED_DEX_SLIPPAGE", DEFAULT_MAX_ALLOWED_DEX_SLIPPAGE_BPS)?;
        let max_allowed_cex_slippage_bps =
            parse_slippage_bps_from_env("MAX_ALLOWED_CEX_SLIPPAGE_BPS", DEFAULT_MAX_ALLOWED_CEX_SLIPPAGE_BPS)?;

        let bad_debt_collateral_slippage_bps = parse_bad_debt_collateral_slippage_bps_from_env();
        let cex_tunables = parse_cex_tunables_from_env();
        let cex_mexc_available_pairs = parse_cex_mexc_available_pairs_from_env();
        let cex_mexc_max_hops = parse_cex_mexc_max_hops_from_env();
        let cex_kraken_available_pairs = parse_cex_available_pairs_from_env("CEX_KRAKEN_AVAILABLE_PAIRS");
        let cex_kraken_max_hops = parse_cex_max_hops_from_env(
            "CEX_KRAKEN_MAX_HOPS",
            DEFAULT_CEX_KRAKEN_MAX_HOPS,
            MAX_CEX_KRAKEN_MAX_HOPS,
        );
        let icpswap_factory_canister = parse_icpswap_factory_from_env()?;
        let icpswap_fee_tiers = parse_icpswap_fee_tiers_from_env()?;
        let icpswap_test_allocation_usd = parse_icpswap_test_allocation_usd_from_env()?;
        let mexc_test_allocation_usd = parse_mexc_test_allocation_usd_from_env()?;
        match (icpswap_test_allocation_usd, mexc_test_allocation_usd) {
            (None, Some(_)) => {
                return Err("MEXC_TEST_ALLOCATION_USD requires ICPSWAP_TEST_ALLOCATION_USD".to_string());
            }
            (Some(icpswap), Some(mexc)) => {
                warn!(
                    "TEST-ONLY three-venue split is enabled: approximately ${icpswap:.2} goes to ICPSwap, ${mexc:.2} to MEXC, and the remainder to Kraken"
                );
            }
            (Some(icpswap), None) => {
                warn!(
                    "TEST-ONLY ICPSwap split is enabled: approximately ${icpswap:.2} goes to ICPSwap and the remainder follows the CEX waterfall"
                );
            }
            (None, None) => {}
        }

        let enabled_swap_venues = parse_enabled_swap_venues_from_env()?;
        if let Ok(legacy) = env::var("SWAPPER") {
            warn!("SWAPPER={} is ignored; configure ENABLED_SWAP_VENUES instead", legacy);
        }

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
            icpswap_mnemonic: Arc::from(mnemonic),
            evm_private_key,
            evm_rpc_url,
            bridge_evm_private_key,
            bridge_evm_address,
            bridge_ic_owner_principal,
            bridge_btc_address,
            bridge_cketh_minter_canister,
            liquidator_identity: Arc::new(liquidator_identity),
            bridge_ic_identity: Arc::new(bridge_ic_identity),
            ic_url,
            liquidator_principal,
            trader_identity: Arc::new(trader_identity),
            trader_principal,
            lending_canister,
            export_path,
            buy_bad_debt,
            db_path,
            liquidations_db_path,
            max_allowed_dex_slippage,
            max_allowed_cex_slippage_bps,
            bad_debt_collateral_slippage_bps,
            cex_min_exec_usd: cex_tunables.min_exec_usd,
            cex_min_leg_receive_usd: cex_tunables.min_leg_receive_usd,
            cex_slice_target_ratio: cex_tunables.slice_target_ratio,
            cex_buy_truncation_trigger_ratio: cex_tunables.buy_truncation_trigger_ratio,
            cex_buy_inverse_overspend_bps: cex_tunables.buy_inverse_overspend_bps,
            cex_buy_inverse_max_retries: cex_tunables.buy_inverse_max_retries,
            cex_buy_inverse_enabled: cex_tunables.buy_inverse_enabled,
            cex_retry_base_secs: cex_tunables.retry_base_secs,
            cex_retry_max_secs: cex_tunables.retry_max_secs,
            multi_venue_min_net_edge_bps: parse_multi_venue_min_net_edge_bps_from_env(),
            multi_venue_bad_debt_min_net_edge_bps: parse_multi_venue_bad_debt_min_net_edge_bps_from_env(),
            multi_venue_max_oracle_discount_bps: parse_multi_venue_max_oracle_discount_bps_from_env(),
            multi_venue_oracle_snapshot_max_age_secs: parse_multi_venue_oracle_snapshot_max_age_secs_from_env(),
            cex_delay_buffer_bps: cex_tunables.delay_buffer_bps,
            cex_route_fee_bps: cex_tunables.route_fee_bps,
            cex_mexc_available_pairs,
            cex_mexc_max_hops,
            cex_kraken_available_pairs,
            cex_kraken_max_hops,
            icpswap_factory_canister,
            icpswap_fee_tiers,
            icpswap_max_price_impact_bps: parse_icpswap_max_price_impact_bps_from_env(),
            icpswap_max_search_iterations: parse_icpswap_max_search_iterations_from_env(),
            icpswap_dust_fallback_max_price_impact_bps: parse_icpswap_dust_fallback_max_price_impact_bps_from_env(),
            icpswap_test_allocation_usd,
            mexc_test_allocation_usd,
            enabled_swap_venues,
            cex_credentials,
            opportunity_account_filter,
        }))
    }
}

fn validate_distinct_database_paths(wal_path: &str, liquidations_path: &str) -> Result<(), String> {
    fn comparable_path(path: &str) -> std::path::PathBuf {
        let path = std::path::Path::new(path);
        std::fs::canonicalize(path)
            .or_else(|_| std::path::absolute(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }

    if comparable_path(wal_path) == comparable_path(liquidations_path) {
        return Err("DB_PATH and LIQUIDATIONS_DB_PATH must point to different files".to_string());
    }
    Ok(())
}

fn load_cex_credentials() -> HashMap<String, (String, String)> {
    let mut cex_credentials: HashMap<String, (String, String)> = HashMap::new();

    for (key, value) in std::env::vars() {
        // match CEX_NAME_API_KEY
        if let Some(name) = key.strip_prefix("CEX_").and_then(|s| s.strip_suffix("_API_KEY")) {
            let name_lower = name.to_lowercase();
            let secret_var = format!("CEX_{}_API_SECRET", name);

            match std::env::var(&secret_var) {
                Ok(secret) if !value.trim().is_empty() && !secret.trim().is_empty() => {
                    debug!("Loaded CEX credentials for '{}'", name_lower);
                    cex_credentials.insert(name_lower, (value.trim().to_string(), secret.trim().to_string()));
                }
                Ok(_) => {
                    debug!("Ignoring blank CEX credentials for '{}'", name_lower);
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
    min_leg_receive_usd: f64,
    slice_target_ratio: f64,
    buy_truncation_trigger_ratio: f64,
    buy_inverse_overspend_bps: u32,
    buy_inverse_max_retries: u32,
    buy_inverse_enabled: bool,
    retry_base_secs: u64,
    retry_max_secs: u64,
    delay_buffer_bps: u32,
    route_fee_bps: u32,
}

const DEFAULT_CEX_MIN_EXEC_USD: f64 = 8.0;
/// Venue withdrawal minimums are dollar-sized -- MEXC refuses an ERC20 USDC
/// withdrawal under 5 -- so a leg worth less than this on the way out risks
/// stranding its proceeds on the exchange for an operator to retrieve.
const DEFAULT_CEX_MIN_LEG_RECEIVE_USD: f64 = 10.0;
const DEFAULT_CEX_SLICE_TARGET_RATIO: f64 = 0.7;
const DEFAULT_CEX_BUY_TRUNCATION_TRIGGER_RATIO: f64 = 0.25;
const DEFAULT_CEX_BUY_INVERSE_OVERSPEND_BPS: u32 = 10;
const MAX_CEX_BUY_INVERSE_OVERSPEND_BPS: u32 = 100;
const DEFAULT_CEX_BUY_INVERSE_MAX_RETRIES: u32 = 1;
const MAX_CEX_BUY_INVERSE_MAX_RETRIES: u32 = 3;
const DEFAULT_CEX_BUY_INVERSE_ENABLED: bool = true;
const DEFAULT_CEX_RETRY_BASE_SECS: u64 = 5;
const DEFAULT_CEX_RETRY_MAX_SECS: u64 = 120;
const DEFAULT_MULTI_VENUE_MIN_NET_EDGE_BPS: u32 = 150;
/// Bad debt may return this much less than it repaid and still be recycled
/// automatically; anything further underwater is escalated to an operator.
const DEFAULT_MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS: i32 = -500;
const DEFAULT_MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS: u32 = 500;
const DEFAULT_MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS: i64 = 300;
const DEFAULT_CEX_DELAY_BUFFER_BPS: u32 = 75;
const DEFAULT_CEX_ROUTE_FEE_BPS: u32 = 25;
const DEFAULT_CEX_MEXC_MAX_HOPS: u8 = 2;
const MAX_CEX_MEXC_MAX_HOPS: u8 = 4;
const DEFAULT_CEX_KRAKEN_MAX_HOPS: u8 = 2;
const MAX_CEX_KRAKEN_MAX_HOPS: u8 = 4;
const DEFAULT_BAD_DEBT_COLLATERAL_SLIPPAGE_BPS: u32 = 500;
const MAX_BPS: u32 = 10_000;
const MIN_RATIO: f64 = 0.0;
const MAX_RATIO: f64 = 1.0;
const MIN_SLICE_TARGET_RATIO: f64 = 0.1;
const BRIDGE_NAMESPACE_ACCOUNT: u32 = 1;
const BRIDGE_EVM_INDEX: u32 = 0;
const BRIDGE_ICP_INDEX: u32 = 1;
const BRIDGE_BTC_INDEX: u32 = 0;
const DEFAULT_BRIDGE_CKETH_MINTER_CANISTER: &str = "sv3dd-oaaaa-aaaar-qacoa-cai";
pub const DEFAULT_ICPSWAP_FACTORY_CANISTER: &str = "4mmnk-kiaaa-aaaag-qbllq-cai";
const DEFAULT_ICPSWAP_FEE_TIERS: &str = "100,500,3000,10000";
const DEFAULT_ICPSWAP_MAX_PRICE_IMPACT_BPS: f64 = 100.0;
const DEFAULT_ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS: f64 = 150.0;
const DEFAULT_ICPSWAP_MAX_SEARCH_ITERATIONS: u8 = 16;
const DEFAULT_ENABLED_SWAP_VENUES: &str = "icpswap,mexc";
/// Every venue `ENABLED_SWAP_VENUES` accepts.
///
/// `pub(crate)` so tests elsewhere can assert that per-venue tables stay in
/// step with it rather than silently omitting a venue added here.
pub(crate) const SUPPORTED_SWAP_VENUES: [&str; 3] = ["icpswap", "mexc", "kraken"];

fn parse_multi_venue_min_net_edge_bps_from_env() -> u32 {
    env::var("MULTI_VENUE_MIN_NET_EDGE_BPS")
        .or_else(|_| env::var("CEX_MIN_NET_EDGE_BPS"))
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_MULTI_VENUE_MIN_NET_EDGE_BPS)
        .min(MAX_BPS)
}

/// Edge floor for collateral bought as bad debt, which repays more than the
/// collateral is worth by construction.
///
/// It mirrors `MULTI_VENUE_MIN_NET_EDGE_BPS` in sign, not in value: a
/// profitable liquidation must clear the debt by 150 bps, while bad debt may
/// fall short of it by 150 bps and still be recycled without a human. Anything
/// further underwater is escalated instead of sold automatically.
///
/// Deliberately a constant rather than the negation of the profitable floor:
/// raising the profit bar is a tightening, and must not silently widen how much
/// loss the swap-back will accept. The quote is priced against the oracle
/// regardless, so this governs recycling, never fill quality.
fn parse_multi_venue_bad_debt_min_net_edge_bps_from_env() -> i32 {
    let limit = i32::try_from(MAX_BPS).unwrap_or(i32::MAX);
    env::var("MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|bps| *bps >= -limit && *bps <= limit)
        .unwrap_or(DEFAULT_MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS)
}

/// A discount of `MAX_BPS` accepts any output at all, so out-of-range values
/// fall back to the default instead of being clamped into disabling the guard.
fn parse_multi_venue_max_oracle_discount_bps_from_env() -> u32 {
    env::var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value < MAX_BPS)
        .unwrap_or(DEFAULT_MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS)
}

fn parse_multi_venue_oracle_snapshot_max_age_secs_from_env() -> i64 {
    env::var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(DEFAULT_MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS)
}

fn parse_bad_debt_collateral_slippage_bps_from_env() -> u32 {
    env::var("BAD_DEBT_COLLATERAL_SLIPPAGE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .map(|v| v.min(MAX_BPS))
        .unwrap_or(DEFAULT_BAD_DEBT_COLLATERAL_SLIPPAGE_BPS)
}

/// Ceiling for any configured slippage allowance. These values become on-chain
/// `amount_out_minimum` floors, so a fat-fingered order of magnitude is the
/// difference between a 1.25% cap and effectively none at all.
const MAX_CONFIGURABLE_SLIPPAGE_BPS: u32 = 2_000;
const DEFAULT_MAX_ALLOWED_DEX_SLIPPAGE_BPS: u32 = 125;
const DEFAULT_MAX_ALLOWED_CEX_SLIPPAGE_BPS: u32 = 200;

/// Reads a basis-point slippage allowance, falling back to the shared
/// `MAX_ALLOWED_SLIPPAGE_BPS` and then to `default_bps`.
///
/// A *set but unparseable* value is an error rather than a silent fallback:
/// `"1.25"` or `"0.5%"` are natural ways to write this, and quietly substituting
/// the default would leave an operator believing they had tightened a cap they
/// had in fact left at its default.
fn parse_slippage_bps_from_env(primary: &str, default_bps: u32) -> Result<u32, String> {
    const SHARED: &str = "MAX_ALLOWED_SLIPPAGE_BPS";
    // An empty or whitespace-only value counts as unset, so `FOO=` in an env
    // file falls through to the shared cap rather than short-circuiting to the
    // default the operator was trying to override.
    let read = |name: &str| {
        env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    let Some((name, trimmed)) = read(primary)
        .map(|value| (primary, value))
        .or_else(|| read(SHARED).map(|value| (SHARED, value)))
    else {
        return Ok(default_bps);
    };

    let parsed: u32 = trimmed
        .parse()
        .map_err(|_| format!("{name}='{trimmed}' is not a whole number of basis points (e.g. 125 for 1.25%)"))?;
    if parsed > MAX_CONFIGURABLE_SLIPPAGE_BPS {
        return Err(format!(
            "{name}={parsed} exceeds the maximum {MAX_CONFIGURABLE_SLIPPAGE_BPS} bps allowed for a slippage cap"
        ));
    }
    Ok(parsed)
}

fn parse_enabled_swap_venues_from_env() -> Result<Vec<String>, String> {
    let raw = env::var("ENABLED_SWAP_VENUES").unwrap_or_else(|_| DEFAULT_ENABLED_SWAP_VENUES.to_string());
    let mut seen = HashSet::new();
    let mut venues = Vec::new();

    for entry in raw.split(',') {
        let venue_id = entry.trim().to_ascii_lowercase();
        if venue_id.is_empty() {
            return Err("ENABLED_SWAP_VENUES contains an empty venue ID".to_string());
        }
        if !SUPPORTED_SWAP_VENUES.contains(&venue_id.as_str()) {
            return Err(format!(
                "ENABLED_SWAP_VENUES contains unsupported venue `{venue_id}`; expected one of {}",
                SUPPORTED_SWAP_VENUES.join(", ")
            ));
        }
        if !seen.insert(venue_id.clone()) {
            return Err(format!("ENABLED_SWAP_VENUES contains duplicate venue `{venue_id}`"));
        }
        venues.push(venue_id);
    }

    if venues.is_empty() {
        return Err("ENABLED_SWAP_VENUES must enable at least one venue".to_string());
    }
    Ok(venues)
}

pub(crate) fn parse_icpswap_factory_from_env() -> Result<Principal, String> {
    let raw = env::var("ICPSWAP_FACTORY_CANISTER").unwrap_or_else(|_| DEFAULT_ICPSWAP_FACTORY_CANISTER.to_string());
    let trimmed = raw.trim();
    Principal::from_text(trimmed)
        .map_err(|error| format!("invalid ICPSWAP_FACTORY_CANISTER principal '{trimmed}': {error}"))
}

/// Mirrors `IcpswapFirstPlannerConfig::validate`: a non-finite or non-positive
/// limit would be rejected by the planner constructor, so an unusable override
/// falls back to the default instead of failing startup.
fn parse_icpswap_max_price_impact_bps_from_env() -> f64 {
    env::var("ICPSWAP_MAX_PRICE_IMPACT_BPS")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(DEFAULT_ICPSWAP_MAX_PRICE_IMPACT_BPS)
}

/// The planner rejects a zero iteration budget, so zero falls back to the
/// default rather than producing an unconstructable planner.
fn parse_icpswap_max_search_iterations_from_env() -> u8 {
    env::var("ICPSWAP_MAX_SEARCH_ITERATIONS")
        .ok()
        .and_then(|value| value.trim().parse::<u8>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ICPSWAP_MAX_SEARCH_ITERATIONS)
}

fn parse_icpswap_dust_fallback_max_price_impact_bps_from_env() -> f64 {
    env::var("ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(DEFAULT_ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS)
}

fn parse_test_allocation_usd_from_env(name: &str) -> Result<Option<f64>, String> {
    let Ok(raw) = env::var(name) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value = trimmed
        .parse::<f64>()
        .map_err(|_| format!("{name}='{trimmed}' must be a positive USD amount"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{name}='{trimmed}' must be finite and positive"));
    }
    Ok(Some(value))
}

fn parse_icpswap_test_allocation_usd_from_env() -> Result<Option<f64>, String> {
    parse_test_allocation_usd_from_env("ICPSWAP_TEST_ALLOCATION_USD")
}

fn parse_mexc_test_allocation_usd_from_env() -> Result<Option<f64>, String> {
    parse_test_allocation_usd_from_env("MEXC_TEST_ALLOCATION_USD")
}

pub(crate) fn parse_icpswap_fee_tiers_from_env() -> Result<Vec<candid::Nat>, String> {
    let raw = env::var("ICPSWAP_FEE_TIERS").unwrap_or_else(|_| DEFAULT_ICPSWAP_FEE_TIERS.to_string());
    parse_icpswap_fee_tiers(&raw)
}

fn parse_icpswap_fee_tiers(raw: &str) -> Result<Vec<candid::Nat>, String> {
    let mut tiers = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|error| format!("invalid ICPSWAP fee tier '{value}': {error}"))
                .and_then(|fee| {
                    if fee == 0 {
                        Err("ICPSWAP fee tiers must be greater than zero".to_string())
                    } else {
                        Ok(candid::Nat::from(fee))
                    }
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    tiers.sort();
    tiers.dedup();
    if tiers.is_empty() {
        return Err("ICPSWAP_FEE_TIERS must contain at least one positive integer".to_string());
    }
    Ok(tiers)
}

fn normalize_cex_market_pair(raw: &str) -> Option<String> {
    let normalized = raw.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return None;
    }

    let mut parts = normalized.split('_').filter(|part| !part.is_empty());
    let base = parts.next()?;
    let quote = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some(format!("{}_{}", base, quote))
}

fn parse_cex_mexc_available_pairs_from_env() -> Vec<String> {
    parse_cex_available_pairs_from_env("CEX_MEXC_AVAILABLE_PAIRS")
}

fn parse_cex_available_pairs_from_env(name: &str) -> Vec<String> {
    let raw = match env::var(name) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let mut out = Vec::new();
    for token in raw.split(',') {
        let Some(market) = normalize_cex_market_pair(token) else {
            // Named rather than dropped in silence: a Kraken list that loses
            // every entry this way refuses to start, and the operator needs to
            // see which entry it was.
            if !token.trim().is_empty() {
                warn!("{name}: ignoring '{}', expected BASE_QUOTE", token.trim());
            }
            continue;
        };
        if !out.iter().any(|existing| existing == &market) {
            out.push(market);
        }
    }
    out
}

fn parse_cex_mexc_max_hops_from_env() -> u8 {
    parse_cex_max_hops_from_env("CEX_MEXC_MAX_HOPS", DEFAULT_CEX_MEXC_MAX_HOPS, MAX_CEX_MEXC_MAX_HOPS)
}

fn parse_cex_max_hops_from_env(name: &str, default: u8, maximum: u8) -> u8 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .map(|v| v.min(maximum))
        .unwrap_or(default)
}

fn parse_cex_tunables_from_env() -> CexTunables {
    // Defaults are conservative; env overrides are expected in .env.
    let min_exec_usd = env::var("CEX_MIN_EXEC_USD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(DEFAULT_CEX_MIN_EXEC_USD);

    let min_leg_receive_usd = env::var("CEX_MIN_LEG_RECEIVE_USD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(DEFAULT_CEX_MIN_LEG_RECEIVE_USD);

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

    let delay_buffer_bps = env::var("CEX_DELAY_BUFFER_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CEX_DELAY_BUFFER_BPS);

    let route_fee_bps = env::var("CEX_ROUTE_FEE_BPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CEX_ROUTE_FEE_BPS);

    CexTunables {
        min_exec_usd,
        min_leg_receive_usd,
        slice_target_ratio,
        buy_truncation_trigger_ratio,
        buy_inverse_overspend_bps,
        buy_inverse_max_retries,
        buy_inverse_enabled,
        retry_base_secs,
        retry_max_secs,
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
    fn blank_cex_credentials_are_treated_as_missing() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_REVIEWBLANK_API_KEY", "   ");
            env::set_var("CEX_REVIEWBLANK_API_SECRET", "");
        }

        let credentials = load_cex_credentials();
        assert!(!credentials.contains_key("reviewblank"));

        unsafe {
            env::remove_var("CEX_REVIEWBLANK_API_KEY");
            env::remove_var("CEX_REVIEWBLANK_API_SECRET");
        }
    }

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
            "MULTI_VENUE_MIN_NET_EDGE_BPS",
            "MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS",
            "CEX_MIN_NET_EDGE_BPS",
            "MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS",
            "MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS",
            "CEX_DELAY_BUFFER_BPS",
            "CEX_ROUTE_FEE_BPS",
            "CEX_MEXC_AVAILABLE_PAIRS",
            "CEX_MEXC_MAX_HOPS",
            "BAD_DEBT_COLLATERAL_SLIPPAGE_BPS",
        ];
        for key in vars {
            unsafe { env::remove_var(key) };
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(
            parsed,
            CexTunables {
                min_exec_usd: 8.0,
                min_leg_receive_usd: 10.0,
                slice_target_ratio: 0.7,
                buy_truncation_trigger_ratio: 0.25,
                buy_inverse_overspend_bps: 10,
                buy_inverse_max_retries: 1,
                buy_inverse_enabled: true,
                retry_base_secs: 5,
                retry_max_secs: 120,
                delay_buffer_bps: 75,
                route_fee_bps: 25,
            }
        );
        assert_eq!(parse_multi_venue_min_net_edge_bps_from_env(), 150);
        // The same 150 bps, mirrored: a profitable liquidation must clear the
        // debt by that much, bad debt may fall short of it by that much.
        assert_eq!(parse_multi_venue_bad_debt_min_net_edge_bps_from_env(), -500);
        assert_eq!(parse_multi_venue_oracle_snapshot_max_age_secs_from_env(), 300);
    }

    #[test]
    fn bad_debt_edge_floor_accepts_negatives_and_rejects_out_of_range() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        unsafe { env::set_var("MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS", "-6000") };
        assert_eq!(parse_multi_venue_bad_debt_min_net_edge_bps_from_env(), -6000);

        // Beyond ±10000 the floor would stop meaning anything, so a bad value
        // falls back to the default instead of silently disabling the check.
        unsafe { env::set_var("MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS", "-20000") };
        assert_eq!(parse_multi_venue_bad_debt_min_net_edge_bps_from_env(), -500);

        unsafe { env::set_var("MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS", "not-a-number") };
        assert_eq!(parse_multi_venue_bad_debt_min_net_edge_bps_from_env(), -500);

        unsafe { env::remove_var("MULTI_VENUE_BAD_DEBT_MIN_NET_EDGE_BPS") };
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
            env::set_var("MULTI_VENUE_MIN_NET_EDGE_BPS", "160");
            env::set_var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS", "175");
            env::set_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS", "45");
            env::set_var("CEX_DELAY_BUFFER_BPS", "90");
            env::set_var("CEX_ROUTE_FEE_BPS", "0");
            env::set_var("BAD_DEBT_COLLATERAL_SLIPPAGE_BPS", "350");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(
            parsed,
            CexTunables {
                min_exec_usd: 1.05,
                min_leg_receive_usd: 10.0,
                slice_target_ratio: 0.85,
                buy_truncation_trigger_ratio: 0.4,
                buy_inverse_overspend_bps: 20,
                buy_inverse_max_retries: 2,
                buy_inverse_enabled: false,
                retry_base_secs: 7,
                retry_max_secs: 240,
                delay_buffer_bps: 90,
                route_fee_bps: 0,
            }
        );
        assert_eq!(parse_bad_debt_collateral_slippage_bps_from_env(), 350);
        assert_eq!(parse_multi_venue_min_net_edge_bps_from_env(), 160);
        assert_eq!(parse_multi_venue_max_oracle_discount_bps_from_env(), 175);
        assert_eq!(parse_multi_venue_oracle_snapshot_max_age_secs_from_env(), 45);

        unsafe {
            env::remove_var("MULTI_VENUE_MIN_NET_EDGE_BPS");
            env::remove_var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS");
            env::remove_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS");
        }
    }

    #[test]
    fn legacy_cex_min_edge_key_remains_a_fallback() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("MULTI_VENUE_MIN_NET_EDGE_BPS");
            env::set_var("CEX_MIN_NET_EDGE_BPS", "165");
        }

        assert_eq!(parse_multi_venue_min_net_edge_bps_from_env(), 165);

        unsafe { env::remove_var("CEX_MIN_NET_EDGE_BPS") };
    }

    #[test]
    fn oracle_discount_defaults_to_500_and_rejects_a_guard_disabling_value() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe { env::remove_var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS") };
        assert_eq!(parse_multi_venue_max_oracle_discount_bps_from_env(), 500);

        // 10_000 bps and above would accept any output at all, so out-of-range
        // values fall back to the default instead of turning the guard off.
        for value in ["10000", "20000", "not-a-number"] {
            unsafe { env::set_var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS", value) };
            assert_eq!(
                parse_multi_venue_max_oracle_discount_bps_from_env(),
                500,
                "`{value}` must not disable the oracle guard"
            );
        }
        unsafe { env::remove_var("MULTI_VENUE_MAX_ORACLE_DISCOUNT_BPS") };
    }

    #[test]
    fn oracle_snapshot_max_age_defaults_and_rejects_negative_values() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe { env::remove_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS") };
        assert_eq!(parse_multi_venue_oracle_snapshot_max_age_secs_from_env(), 300);

        unsafe { env::set_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS", "0") };
        assert_eq!(parse_multi_venue_oracle_snapshot_max_age_secs_from_env(), 0);

        unsafe { env::set_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS", "-1") };
        assert_eq!(parse_multi_venue_oracle_snapshot_max_age_secs_from_env(), 300);
        unsafe { env::remove_var("MULTI_VENUE_ORACLE_SNAPSHOT_MAX_AGE_SECS") };
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
            env::set_var("BAD_DEBT_COLLATERAL_SLIPPAGE_BPS", "20000");
        }

        let parsed = parse_cex_tunables_from_env();
        assert_eq!(parsed.min_exec_usd, 8.0);
        assert_eq!(parsed.slice_target_ratio, 1.0);
        assert_eq!(parsed.buy_truncation_trigger_ratio, 0.0);
        assert_eq!(parsed.buy_inverse_overspend_bps, 100);
        assert_eq!(parsed.buy_inverse_max_retries, 3);
        assert!(parsed.buy_inverse_enabled);
        assert_eq!(parsed.retry_base_secs, 10);
        assert_eq!(parsed.retry_max_secs, 120);
        assert_eq!(parse_bad_debt_collateral_slippage_bps_from_env(), 10_000);
    }

    #[test]
    fn parse_bad_debt_collateral_slippage_bps_defaults_to_500() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("BAD_DEBT_COLLATERAL_SLIPPAGE_BPS");
        }
        assert_eq!(parse_bad_debt_collateral_slippage_bps_from_env(), 500);
    }

    #[test]
    fn parse_icpswap_test_allocation_is_optional_and_strictly_positive() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe { env::remove_var("ICPSWAP_TEST_ALLOCATION_USD") };
        assert_eq!(parse_icpswap_test_allocation_usd_from_env().unwrap(), None);

        unsafe { env::set_var("ICPSWAP_TEST_ALLOCATION_USD", "1") };
        assert_eq!(parse_icpswap_test_allocation_usd_from_env().unwrap(), Some(1.0));

        unsafe { env::set_var("ICPSWAP_TEST_ALLOCATION_USD", "0") };
        assert!(parse_icpswap_test_allocation_usd_from_env().is_err());

        unsafe { env::remove_var("ICPSWAP_TEST_ALLOCATION_USD") };
    }

    #[test]
    fn parse_mexc_test_allocation_is_optional_and_strictly_positive() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe { env::remove_var("MEXC_TEST_ALLOCATION_USD") };
        assert_eq!(parse_mexc_test_allocation_usd_from_env().unwrap(), None);

        unsafe { env::set_var("MEXC_TEST_ALLOCATION_USD", "10") };
        assert_eq!(parse_mexc_test_allocation_usd_from_env().unwrap(), Some(10.0));

        unsafe { env::set_var("MEXC_TEST_ALLOCATION_USD", "0") };
        assert!(parse_mexc_test_allocation_usd_from_env().is_err());

        unsafe { env::remove_var("MEXC_TEST_ALLOCATION_USD") };
    }

    #[test]
    fn parse_icpswap_dust_fallback_impact_defaults_to_150_and_accepts_override() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe { env::remove_var("ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS") };
        assert_eq!(parse_icpswap_dust_fallback_max_price_impact_bps_from_env(), 150.0);

        unsafe { env::set_var("ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS", "175") };
        assert_eq!(parse_icpswap_dust_fallback_max_price_impact_bps_from_env(), 175.0);

        unsafe { env::remove_var("ICPSWAP_DUST_FALLBACK_MAX_PRICE_IMPACT_BPS") };
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

    #[test]
    fn parse_cex_mexc_available_pairs_normalizes_and_dedupes() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var(
                "CEX_MEXC_AVAILABLE_PAIRS",
                "ckbtc/btc, BTC-USDC, invalid, USDC_USDT, CKBTC_BTC, , USDC__USDT",
            );
        }

        let parsed = parse_cex_mexc_available_pairs_from_env();
        assert_eq!(
            parsed,
            vec!["CKBTC_BTC".to_string(), "BTC_USDC".to_string(), "USDC_USDT".to_string(),]
        );
    }

    #[test]
    fn parse_cex_mexc_available_pairs_defaults_to_empty() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("CEX_MEXC_AVAILABLE_PAIRS");
        }

        assert!(parse_cex_mexc_available_pairs_from_env().is_empty());
    }

    #[test]
    fn parse_cex_mexc_max_hops_uses_default_and_clamps() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("CEX_MEXC_MAX_HOPS");
        }
        assert_eq!(parse_cex_mexc_max_hops_from_env(), DEFAULT_CEX_MEXC_MAX_HOPS);

        unsafe {
            env::set_var("CEX_MEXC_MAX_HOPS", "9");
        }
        assert_eq!(parse_cex_mexc_max_hops_from_env(), MAX_CEX_MEXC_MAX_HOPS);

        unsafe {
            env::set_var("CEX_MEXC_MAX_HOPS", "1");
        }
        assert_eq!(parse_cex_mexc_max_hops_from_env(), 1);
    }

    #[test]
    fn parse_cex_kraken_route_config_normalizes_and_defaults_to_two_hops() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("CEX_KRAKEN_AVAILABLE_PAIRS", " xbt/usd,ETH-USD,xbt_usd ");
            env::remove_var("CEX_KRAKEN_MAX_HOPS");
        }

        assert_eq!(
            parse_cex_available_pairs_from_env("CEX_KRAKEN_AVAILABLE_PAIRS"),
            vec!["XBT_USD".to_string(), "ETH_USD".to_string()]
        );
        assert_eq!(
            parse_cex_max_hops_from_env(
                "CEX_KRAKEN_MAX_HOPS",
                DEFAULT_CEX_KRAKEN_MAX_HOPS,
                MAX_CEX_KRAKEN_MAX_HOPS,
            ),
            2
        );

        unsafe {
            env::remove_var("CEX_KRAKEN_AVAILABLE_PAIRS");
        }
    }

    #[test]
    fn enabled_swap_venues_default_to_icpswap_then_mexc() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("ENABLED_SWAP_VENUES");
        }
        assert_eq!(
            parse_enabled_swap_venues_from_env().unwrap(),
            vec!["icpswap".to_string(), "mexc".to_string()]
        );
    }

    #[test]
    fn enabled_swap_venues_normalize_and_preserve_order() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("ENABLED_SWAP_VENUES", " MEXC, IcPsWaP ");
        }
        assert_eq!(
            parse_enabled_swap_venues_from_env().unwrap(),
            vec!["mexc".to_string(), "icpswap".to_string()]
        );
        unsafe {
            env::remove_var("ENABLED_SWAP_VENUES");
        }
    }

    #[test]
    fn enabled_swap_venues_reject_invalid_lists() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        for (raw, expected) in [
            ("", "empty venue ID"),
            ("mexc,", "empty venue ID"),
            ("mexc,MEXC", "duplicate venue"),
            ("binance", "unsupported venue"),
        ] {
            unsafe {
                env::set_var("ENABLED_SWAP_VENUES", raw);
            }
            let error = parse_enabled_swap_venues_from_env().expect_err("invalid venue list must fail");
            assert!(error.contains(expected), "unexpected error: {error}");
        }
        unsafe {
            env::remove_var("ENABLED_SWAP_VENUES");
        }
    }

    #[test]
    fn legacy_swapper_does_not_affect_enabled_venues() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::set_var("SWAPPER", "dex");
            env::set_var("ENABLED_SWAP_VENUES", "mexc");
        }
        assert_eq!(parse_enabled_swap_venues_from_env().unwrap(), vec!["mexc"]);
        unsafe {
            env::remove_var("SWAPPER");
            env::remove_var("ENABLED_SWAP_VENUES");
        }
    }

    #[test]
    fn parse_slippage_bps_rejects_malformed_and_oversized_values() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var("MAX_ALLOWED_SLIPPAGE_BPS");
            env::remove_var("SLIPPAGE_TEST_BPS");
        }
        assert_eq!(parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).unwrap(), 125);

        // A percentage or decimal is the natural way to get this wrong, and
        // silently falling back would hide a cap the operator believes they set.
        for bad in ["1.25", "0.5%", "abc"] {
            unsafe {
                env::set_var("SLIPPAGE_TEST_BPS", bad);
            }
            assert!(
                parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).is_err(),
                "{bad} must be rejected rather than silently defaulted"
            );
        }

        unsafe {
            env::set_var("SLIPPAGE_TEST_BPS", "9999");
        }
        assert!(parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).is_err());

        unsafe {
            env::set_var("SLIPPAGE_TEST_BPS", " 300 ");
        }
        assert_eq!(parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).unwrap(), 300);

        // `FOO=` in an env file is unset, not "use the default": the shared cap
        // still applies, and with neither set we fall back to the default.
        unsafe {
            env::set_var("SLIPPAGE_TEST_BPS", "   ");
            env::set_var("MAX_ALLOWED_SLIPPAGE_BPS", "175");
        }
        assert_eq!(parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).unwrap(), 175);
        unsafe {
            env::set_var("MAX_ALLOWED_SLIPPAGE_BPS", "");
        }
        assert_eq!(parse_slippage_bps_from_env("SLIPPAGE_TEST_BPS", 125).unwrap(), 125);

        unsafe {
            env::remove_var("SLIPPAGE_TEST_BPS");
            env::remove_var("MAX_ALLOWED_SLIPPAGE_BPS");
        }
    }

    #[test]
    fn parse_icpswap_fee_tiers_sorts_and_deduplicates() {
        assert_eq!(
            parse_icpswap_fee_tiers("3000, 500,3000,100").unwrap(),
            vec![
                candid::Nat::from(100u32),
                candid::Nat::from(500u32),
                candid::Nat::from(3000u32),
            ]
        );
    }

    #[test]
    fn parse_icpswap_fee_tiers_rejects_empty_zero_and_invalid_values() {
        assert!(parse_icpswap_fee_tiers(" , ").is_err());
        assert!(parse_icpswap_fee_tiers("0").is_err());
        assert!(parse_icpswap_fee_tiers("500,nope").is_err());
    }

    #[test]
    fn default_icpswap_factory_is_a_valid_principal() {
        assert_eq!(
            Principal::from_text(DEFAULT_ICPSWAP_FACTORY_CANISTER)
                .unwrap()
                .to_text(),
            DEFAULT_ICPSWAP_FACTORY_CANISTER
        );
    }

    #[test]
    fn expand_tilde_uses_home_prefix() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let prev_home = env::var("HOME").ok();
        unsafe { env::set_var("HOME", "/tmp/liquidator-home-test") };

        let expanded = expand_tilde("~/wal.db");
        assert_eq!(expanded, std::path::PathBuf::from("/tmp/liquidator-home-test/wal.db"));

        unsafe {
            match prev_home {
                Some(home) => env::set_var("HOME", home),
                None => env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn database_paths_must_be_distinct() {
        let temp = tempfile::tempdir().expect("temp directory");
        let wal = temp.path().join("wal.db");
        let equivalent = temp.path().join(".").join("wal.db");
        let liquidations = temp.path().join("liquidations.db");

        assert!(
            validate_distinct_database_paths(
                wal.to_str().expect("wal path"),
                equivalent.to_str().expect("equivalent path")
            )
            .is_err()
        );
        validate_distinct_database_paths(
            wal.to_str().expect("wal path"),
            liquidations.to_str().expect("liquidations path"),
        )
        .expect("different database paths");
    }
}
