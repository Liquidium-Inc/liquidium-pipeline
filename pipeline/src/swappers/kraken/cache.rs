//! Kraken metadata that rarely changes: pairs, funding methods and addresses.
//! Each answer is kept for a few minutes, so one liquidation does not spend
//! the private rate limit on the same reads again.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::Mutex,
    time::{Duration, Instant},
};

use super::api::{KrakenApiError, KrakenDepositAddress, KrakenFundingMethod, KrakenPair, KrakenWithdrawalAddress};

const METADATA_TTL: Duration = Duration::from_secs(5 * 60);

pub(super) struct TtlCache<K, V> {
    entries: Mutex<HashMap<K, (Instant, V)>>,
}

impl<K, V> Default for TtlCache<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash, V: Clone> TtlCache<K, V> {
    /// The fresh answer for `key`, else `load`'s. A failed load is not kept.
    pub(super) async fn get_or_load(
        &self,
        key: K,
        load: impl AsyncFnOnce() -> Result<V, KrakenApiError>,
    ) -> Result<V, KrakenApiError> {
        if let Some((loaded_at, value)) = self.entries.lock().unwrap_or_else(|e| e.into_inner()).get(&key)
            && loaded_at.elapsed() < METADATA_TTL
        {
            return Ok(value.clone());
        }
        let value = load().await?;
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (Instant::now(), value.clone()));
        Ok(value)
    }
}

#[derive(Default)]
pub(super) struct MetadataCache {
    pub(super) pairs: TtlCache<(), Vec<KrakenPair>>,
    pub(super) deposit_methods: TtlCache<String, Vec<KrakenFundingMethod>>,
    pub(super) deposit_addresses: TtlCache<(String, String), Vec<KrakenDepositAddress>>,
    pub(super) withdrawal_methods: TtlCache<String, Vec<KrakenFundingMethod>>,
    pub(super) withdrawal_addresses: TtlCache<String, Vec<KrakenWithdrawalAddress>>,
}
