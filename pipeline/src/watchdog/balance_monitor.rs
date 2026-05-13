use std::sync::Arc;
use std::time::{Duration, Instant};

use candid::Nat;
use log::{debug, warn};
use num_traits::ToPrimitive;
use tokio::sync::Mutex;

use liquidium_pipeline_core::balance_service::BalanceService;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
    token_registry::TokenRegistryTrait,
};

use super::{Watchdog, WatchdogEvent};

pub const DEFAULT_BALANCE_MONITOR_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_LOW_BALANCE_ALERT_COOLDOWN: Duration = Duration::from_secs(30 * 60);

#[derive(Clone)]
pub struct MonitoredBalanceAccount {
    pub label: &'static str,
    pub service: Arc<BalanceService>,
}

pub struct LowBalanceMonitor {
    accounts: Vec<MonitoredBalanceAccount>,
    watchdog: Arc<dyn Watchdog>,
    interval: Duration,
    last_check: Mutex<Option<Instant>>,
}

impl LowBalanceMonitor {
    pub fn new(accounts: Vec<MonitoredBalanceAccount>, watchdog: Arc<dyn Watchdog>) -> Self {
        Self::with_interval(accounts, watchdog, DEFAULT_BALANCE_MONITOR_INTERVAL)
    }

    pub fn with_interval(
        accounts: Vec<MonitoredBalanceAccount>,
        watchdog: Arc<dyn Watchdog>,
        interval: Duration,
    ) -> Self {
        Self {
            accounts,
            watchdog,
            interval,
            last_check: Mutex::new(None),
        }
    }

    pub async fn check_if_due(&self) {
        let now = Instant::now();
        {
            let mut last_check = self.last_check.lock().await;
            if let Some(last) = *last_check
                && now.duration_since(last) < self.interval
            {
                return;
            }
            *last_check = Some(now);
        }

        self.check_now().await;
    }

    pub async fn check_now(&self) {
        let Some(first_account) = self.accounts.first() else {
            return;
        };

        let asset_ids = monitored_asset_ids(first_account.service.registry().as_ref());
        if asset_ids.is_empty() {
            return;
        }

        for account in &self.accounts {
            let results = account.service.sync_assets(&asset_ids).await;
            for result in results {
                match result {
                    Ok((asset_id, balance)) => {
                        if let Some(ev) = low_balance_event(account.label, &asset_id, &balance) {
                            self.watchdog.notify(ev).await;
                        }
                    }
                    Err(err) => {
                        warn!("Low-balance monitor failed to read {} balance: {}", account.label, err);
                    }
                }
            }
        }
    }
}

pub fn monitored_asset_ids(registry: &dyn TokenRegistryTrait) -> Vec<AssetId> {
    registry
        .all()
        .into_iter()
        .filter_map(|(asset_id, token)| {
            if threshold_for_token(&token).is_some() {
                Some(asset_id)
            } else {
                debug!(
                    "Low-balance monitor skipping asset with no static threshold: {} ({})",
                    asset_id,
                    token.symbol()
                );
                None
            }
        })
        .collect()
}

pub fn low_balance_event(
    account: &'static str,
    asset_id: &AssetId,
    balance: &ChainTokenAmount,
) -> Option<WatchdogEvent<'static>> {
    let threshold = threshold_for_token(&balance.token)?;
    if balance.value >= threshold {
        return None;
    }

    let symbol = balance.token.symbol();
    let decimals = balance.token.decimals();
    Some(WatchdogEvent::LowBalance {
        account: account.to_string(),
        asset: symbol.clone(),
        asset_id: asset_id.to_string(),
        current: format_native_units(&balance.value, decimals, &symbol),
        threshold: format_native_units(&threshold, decimals, &symbol),
    })
}

pub fn threshold_for_token(token: &ChainToken) -> Option<Nat> {
    let threshold = match token.symbol().trim().to_ascii_uppercase().as_str() {
        "CKBTC" | "BTC" => "0.001",
        "CKUSDT" | "USDT" => "100",
        "CKUSDC" | "USDC" => "100",
        "ICP" => "5",
        "ETH" => "0.05",
        _ => return None,
    };

    decimal_to_native_units(threshold, token.decimals())
}

pub fn decimal_to_native_units(value: &str, decimals: u8) -> Option<Nat> {
    let value = value.trim();
    if value.is_empty() || value.starts_with('-') {
        return None;
    }

    let (whole, frac) = match value.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (value, ""),
    };
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let decimals = decimals as usize;
    let mut frac = frac.to_string();
    if frac.len() > decimals {
        if frac[decimals..].chars().any(|c| c != '0') {
            return None;
        }
        frac.truncate(decimals);
    }
    while frac.len() < decimals {
        frac.push('0');
    }

    let scale = 10u128.checked_pow(decimals as u32)?;
    let whole_units = if whole.is_empty() {
        0u128
    } else {
        whole.parse::<u128>().ok()?.checked_mul(scale)?
    };
    let frac_units = if frac.is_empty() {
        0u128
    } else {
        frac.parse::<u128>().ok()?
    };

    Some(Nat::from(whole_units.checked_add(frac_units)?))
}

pub fn format_native_units(value: &Nat, decimals: u8, symbol: &str) -> String {
    let Some(raw) = value.0.to_u128() else {
        return format!("{} {}", value, symbol);
    };

    let decimals = decimals as u32;
    if decimals == 0 {
        return format!("{} {}", raw, symbol);
    }

    let Some(scale) = 10u128.checked_pow(decimals) else {
        return format!("{} {}", value, symbol);
    };
    let display_decimals = decimals.min(6);
    let display_scale = 10u128.pow(decimals - display_decimals);
    let scaled = raw / display_scale;
    let int_part = scaled / 10u128.pow(display_decimals);
    let frac_part = scaled % 10u128.pow(display_decimals);

    let mut frac = format!("{:0>width$}", frac_part, width = display_decimals as usize);
    while frac.ends_with('0') {
        frac.pop();
    }

    if frac.is_empty() {
        format!("{} {}", raw / scale, symbol)
    } else {
        format!("{}.{} {}", int_part, frac, symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use candid::Principal;
    use liquidium_pipeline_core::account::actions::MockAccountInfo;
    use liquidium_pipeline_core::tokens::token_registry::TokenRegistry;
    use std::collections::HashMap;

    struct RecordingWatchdog {
        events: Mutex<Vec<WatchdogEvent<'static>>>,
    }

    impl RecordingWatchdog {
        fn new() -> Self {
            Self {
                events: Mutex::new(vec![]),
            }
        }

        async fn events(&self) -> Vec<WatchdogEvent<'static>> {
            self.events.lock().await.clone()
        }
    }

    #[async_trait]
    impl Watchdog for RecordingWatchdog {
        async fn notify(&self, ev: WatchdogEvent<'_>) {
            if let WatchdogEvent::LowBalance {
                account,
                asset,
                asset_id,
                current,
                threshold,
            } = ev
            {
                self.events.lock().await.push(WatchdogEvent::LowBalance {
                    account,
                    asset,
                    asset_id,
                    current,
                    threshold,
                });
            }
        }
    }

    fn icp_token(symbol: &str, decimals: u8) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").expect("principal"),
            symbol: symbol.to_string(),
            decimals,
            fee: Nat::from(10u8),
        }
    }

    fn registry_with(token: ChainToken) -> Arc<TokenRegistry> {
        let id = token.asset_id();
        Arc::new(TokenRegistry::new(HashMap::from([(id, token)])))
    }

    fn balance_service(token: ChainToken, raw_balance: u128) -> Arc<BalanceService> {
        let registry = registry_with(token);
        let mut account = MockAccountInfo::new();
        account.expect_sync_balance().returning(move |token| {
            Ok(ChainTokenAmount {
                token: token.clone(),
                value: Nat::from(raw_balance),
            })
        });
        Arc::new(BalanceService::new(registry, Arc::new(account)))
    }

    #[test]
    fn threshold_matching_uses_native_units() {
        let ckusdc = icp_token("ckUSDC", 6);
        let ckusdc_threshold = threshold_for_token(&ckusdc).expect("threshold");
        assert_eq!(ckusdc_threshold, Nat::from(100_000_000u128));

        let ckbtc = icp_token("ckBTC", 8);
        let ckbtc_threshold = threshold_for_token(&ckbtc).expect("threshold");
        assert_eq!(ckbtc_threshold, Nat::from(100_000u128));

        let eth = ChainToken::EvmNative {
            chain: "eth".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        };
        assert_eq!(
            threshold_for_token(&eth).expect("threshold"),
            Nat::from(50_000_000_000_000_000u128)
        );
    }

    #[test]
    fn unknown_symbol_is_not_monitored() {
        let token = icp_token("DOGE", 8);
        assert!(threshold_for_token(&token).is_none());
        assert!(monitored_asset_ids(registry_with(token).as_ref()).is_empty());
    }

    #[test]
    fn low_balance_event_detects_zero_immediately() {
        let token = icp_token("ICP", 8);
        let id = token.asset_id();
        let balance = ChainTokenAmount {
            token,
            value: Nat::from(0u8),
        };

        let ev = low_balance_event("main", &id, &balance).expect("zero should alert");
        match ev {
            WatchdogEvent::LowBalance {
                account,
                asset,
                current,
                threshold,
                ..
            } => {
                assert_eq!(account, "main");
                assert_eq!(asset, "ICP");
                assert_eq!(current, "0 ICP");
                assert_eq!(threshold, "5 ICP");
            }
            _ => panic!("expected low balance"),
        }
    }

    #[test]
    fn above_threshold_balance_does_not_alert() {
        let token = icp_token("ICP", 8);
        let id = token.asset_id();
        let balance = ChainTokenAmount {
            token,
            value: Nat::from(5_000_000_000u128),
        };

        assert!(low_balance_event("main", &id, &balance).is_none());
    }

    #[tokio::test]
    async fn monitor_first_due_check_sends_low_balance_alert() {
        let token = icp_token("ckUSDC", 6);
        let service = balance_service(token, 0);
        let recorder = Arc::new(RecordingWatchdog::new());
        let monitor = LowBalanceMonitor::with_interval(
            vec![MonitoredBalanceAccount { label: "main", service }],
            recorder.clone(),
            Duration::from_secs(60),
        );

        monitor.check_if_due().await;

        let events = recorder.events().await;
        assert_eq!(events.len(), 1);
        match &events[0] {
            WatchdogEvent::LowBalance {
                account,
                asset,
                current,
                threshold,
                ..
            } => {
                assert_eq!(account, "main");
                assert_eq!(asset, "ckUSDC");
                assert_eq!(current, "0 ckUSDC");
                assert_eq!(threshold, "100 ckUSDC");
            }
            _ => panic!("expected low balance"),
        }
    }
}
