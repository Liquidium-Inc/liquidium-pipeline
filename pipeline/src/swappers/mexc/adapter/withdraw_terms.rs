//! Withdrawal fees and minimums from MEXC's coin configuration, read before a
//! withdrawal so the planner can count them.
use super::*;

use std::sync::Arc;
use std::time::{Duration, Instant};

use mexc_rs::spot::v3::coin_config::{CoinConfig, CoinConfigEndpoint, CoinNetworkConfig};

/// MEXC changes withdrawal fees rarely, and one read covers every coin.
const COIN_CONFIG_TTL: Duration = Duration::from_secs(10 * 60);

pub(super) type CoinConfigCache = tokio::sync::Mutex<Option<(Instant, Arc<Vec<CoinConfig>>)>>;

impl MexcClient {
    pub(super) async fn coin_config(&self) -> Result<Arc<Vec<CoinConfig>>, String> {
        let mut cached = self.coin_config.lock().await;
        if let Some((read_at, config)) = cached.as_ref()
            && read_at.elapsed() < COIN_CONFIG_TTL
        {
            return Ok(config.clone());
        }
        let config = Arc::new(
            self.inner
                .lock()
                .await
                .get_coin_config()
                .await
                .map_err(|error| format!("MEXC coin config failed: {error}"))?,
        );
        *cached = Some((Instant::now(), config.clone()));
        Ok(config)
    }
}

/// The terms for the first coin alias and network a withdrawal would try.
pub(super) fn find_withdraw_terms(config: &[CoinConfig], asset: &str, network: &str) -> Result<WithdrawTerms, String> {
    for coin in mexc_withdraw_coin_candidates(asset) {
        for net in mexc_withdraw_network_order(asset, network) {
            if let Some(entry) = find_network(config, &coin, &net) {
                return withdraw_terms(entry);
            }
        }
    }
    Err(format!("MEXC publishes no withdrawal terms for {asset} on {network}"))
}

fn find_network<'a>(config: &'a [CoinConfig], coin: &str, network: &str) -> Option<&'a CoinNetworkConfig> {
    config
        .iter()
        .filter(|entry| entry.coin.eq_ignore_ascii_case(coin))
        .flat_map(|entry| entry.network_list.iter())
        .find(|entry| {
            [entry.net_work.as_deref(), entry.network.as_deref()]
                .into_iter()
                .flatten()
                .any(|name| name.eq_ignore_ascii_case(network))
        })
}

fn withdraw_terms(entry: &CoinNetworkConfig) -> Result<WithdrawTerms, String> {
    if !entry.withdraw_enable {
        return Err(format!("MEXC withdrawals of {} are disabled", entry.coin));
    }
    let parse = |field: &str, value: Option<&str>| -> Result<f64, String> {
        let value = value.ok_or_else(|| format!("MEXC coin config for {} has no {field}", entry.coin))?;
        value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|amount| amount.is_finite() && *amount >= 0.0)
            .ok_or_else(|| format!("MEXC coin config for {} has an invalid {field} `{value}`", entry.coin))
    };
    Ok(WithdrawTerms {
        fee: parse("withdrawFee", entry.withdraw_fee.as_deref())?,
        minimum: parse("withdrawMin", entry.withdraw_min.as_deref())?,
    })
}
