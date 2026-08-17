//! Process-wide nonce allocation for EVM signing keys.
//!
//! Every provider built from the same key must allocate nonces from one
//! sequence. Alloy's own managers cannot give that: the simple one re-reads the
//! node's pending count on every send, and the cached one keeps its counter
//! inside a single provider instance. This pipeline builds several providers over
//! the same bridge key, so both leave room for two transactions to receive the
//! same nonce -- and the second then replaces the first.
//!
//! Provider construction lives here rather than in `evm_backend` because
//! installing the allocator means replacing alloy's default filler stack, and
//! `EvmBackendImpl` is deliberately generic over an already-built provider.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use alloy::{
    network::{AnyNetwork, Network},
    primitives::Address,
    providers::{
        Provider, ProviderBuilder, WalletProvider,
        fillers::{BlobGasFiller, ChainIdFiller, GasFiller, NonceManager},
    },
    signers::local::PrivateKeySigner,
    transports::TransportResult,
};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// How long a locally allocated nonce outranks the node's own view.
///
/// Long enough to cover a burst the mempool has not finished propagating, short
/// enough that a nonce allocated for a transaction which never broadcast stops
/// holding back later ones.
const LOCAL_NONCE_TRUST_WINDOW: Duration = Duration::from_secs(120);

#[derive(Debug, Default)]
struct NonceTracker {
    next: HashMap<Address, (u64, Instant)>,
}

impl NonceTracker {
    /// Returns the nonce to use, taking whichever of the local high-water mark
    /// and the node's count is further ahead.
    ///
    /// Deferring to the node whenever it leads keeps this self-healing: a
    /// restart, another signer sharing the key, or an abandoned allocation all
    /// resolve on their own instead of wedging the sequence.
    fn allocate(&mut self, address: Address, node_count: u64, now: Instant) -> u64 {
        let local = self
            .next
            .get(&address)
            .filter(|(_, allocated_at)| now.duration_since(*allocated_at) < LOCAL_NONCE_TRUST_WINDOW)
            .map(|(next, _)| *next);
        let nonce = local.map_or(node_count, |local| local.max(node_count));
        self.next.insert(address, (nonce + 1, now));
        nonce
    }
}

/// Serializes nonce allocation across every provider sharing a signing key.
///
/// A load-balanced RPC can report a pending count that omits a transaction it
/// already accepted, handing the next caller a nonce that is still in flight.
/// The local high-water mark closes that window.
#[derive(Clone, Debug, Default)]
pub struct SharedNonceManager {
    tracker: Arc<Mutex<NonceTracker>>,
}

#[async_trait]
impl NonceManager for SharedNonceManager {
    async fn get_next_nonce<P, N>(&self, provider: &P, address: Address) -> TransportResult<u64>
    where
        P: Provider<N>,
        N: Network,
    {
        // The lock is deliberately held across the fetch: two callers that both
        // read the node's count before either records an allocation would
        // otherwise derive the same nonce from it.
        let mut tracker = self.tracker.lock().await;
        let node_count = provider.get_transaction_count(address).pending().await?;
        Ok(tracker.allocate(address, node_count, Instant::now()))
    }
}

/// The one allocator every provider in this process shares.
///
/// Building a provider with its own allocator reintroduces the collision this
/// module exists to prevent, so construction goes through [`build_evm_provider`]
/// rather than exposing a constructor.
fn shared_nonce_manager() -> SharedNonceManager {
    static SHARED: OnceLock<SharedNonceManager> = OnceLock::new();
    SHARED.get_or_init(SharedNonceManager::default).clone()
}

/// Builds a wallet provider whose nonces come from the process-wide allocator.
///
/// The filler stack mirrors alloy's recommended set, with the shared allocator
/// in place of its default nonce manager. `ProviderBuilder::new` cannot be used
/// here: it installs its own nonce filler, which fills first and leaves any
/// later one with nothing to do.
pub fn build_evm_provider(
    rpc_url: &str,
    signer: PrivateKeySigner,
) -> Result<impl Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + 'static, String> {
    let rpc_url = rpc_url
        .parse()
        .map_err(|error| format!("invalid EVM RPC URL '{rpc_url}': {error}"))?;
    Ok(ProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .filler(GasFiller)
        .filler(BlobGasFiller)
        .with_nonce_management(shared_nonce_manager())
        .filler(ChainIdFiller::default())
        .wallet(signer)
        .connect_http(rpc_url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    #[test]
    fn a_stale_node_count_never_reissues_an_allocated_nonce() {
        let mut tracker = NonceTracker::default();
        let now = Instant::now();
        let owner = address(1);

        assert_eq!(tracker.allocate(owner, 70, now), 70);
        // The node still reports 70: it has not propagated the transaction that
        // just took it. Reissuing 70 is what replaced a live bridge deposit.
        assert_eq!(tracker.allocate(owner, 70, now), 71);
        assert_eq!(tracker.allocate(owner, 70, now), 72);
    }

    #[test]
    fn a_node_that_leads_the_local_mark_wins() {
        let mut tracker = NonceTracker::default();
        let now = Instant::now();
        let owner = address(2);

        assert_eq!(tracker.allocate(owner, 10, now), 10);
        // Another signer moved the account on; the local mark must not drag the
        // sequence back onto nonces that are already mined.
        assert_eq!(tracker.allocate(owner, 40, now), 40);
        assert_eq!(tracker.allocate(owner, 40, now), 41);
    }

    #[test]
    fn each_address_allocates_independently() {
        let mut tracker = NonceTracker::default();
        let now = Instant::now();

        assert_eq!(tracker.allocate(address(3), 5, now), 5);
        assert_eq!(tracker.allocate(address(4), 9, now), 9);
        assert_eq!(tracker.allocate(address(3), 5, now), 6);
    }

    #[test]
    fn an_abandoned_allocation_stops_holding_back_the_sequence() {
        let mut tracker = NonceTracker::default();
        let start = Instant::now();
        let owner = address(5);

        assert_eq!(tracker.allocate(owner, 70, start), 70);
        // That transaction never broadcast, so the node still reports 70. Past
        // the trust window the node is authoritative again, otherwise the local
        // mark would leave a permanent gap nothing can fill.
        let later = start + LOCAL_NONCE_TRUST_WINDOW + Duration::from_secs(1);
        assert_eq!(tracker.allocate(owner, 70, later), 70);
    }
}
