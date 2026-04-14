use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};

type SubmitLockMap = HashMap<String, Arc<TokioMutex<()>>>;

static BRIDGE_SUBMIT_LOCKS: OnceLock<TokioMutex<SubmitLockMap>> = OnceLock::new();

fn bridge_submit_lock_key(asset: &str, source_chain: &str, source_address: &str) -> String {
    format!(
        "{}@{}#{}",
        asset.trim().to_ascii_uppercase(),
        source_chain.trim().to_ascii_uppercase(),
        source_address.trim().to_ascii_lowercase()
    )
}

/// Acquire an in-process submit lock for one route/source tuple.
///
/// This serializes bridge submissions originating from both:
/// - background bridge sweepers,
/// - liquidation-linked finalizer bridge submits.
pub async fn acquire_bridge_submit_lock(asset: &str, source_chain: &str, source_address: &str) -> OwnedMutexGuard<()> {
    let registry = BRIDGE_SUBMIT_LOCKS.get_or_init(|| TokioMutex::new(HashMap::new()));
    let key = bridge_submit_lock_key(asset, source_chain, source_address);

    let lock = {
        let mut guard = registry.lock().await;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    };

    lock.lock_owned().await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::time::sleep;

    use super::acquire_bridge_submit_lock;

    #[tokio::test]
    async fn finalizer_and_sweeper_lock_contention_serializes_submit_path() {
        let in_critical = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let key_asset = "USDC";
        let key_chain = "ETH";
        let key_source = "0x1111111111111111111111111111111111111111";

        let finalizer_task = {
            let in_critical = in_critical.clone();
            let max_seen = max_seen.clone();
            tokio::spawn(async move {
                let _guard = acquire_bridge_submit_lock(key_asset, key_chain, key_source).await;
                let now = in_critical.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                sleep(Duration::from_millis(75)).await;
                in_critical.fetch_sub(1, Ordering::SeqCst);
            })
        };

        let sweeper_task = {
            let in_critical = in_critical.clone();
            let max_seen = max_seen.clone();
            tokio::spawn(async move {
                let _guard = acquire_bridge_submit_lock(key_asset, key_chain, key_source).await;
                let now = in_critical.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                sleep(Duration::from_millis(25)).await;
                in_critical.fetch_sub(1, Ordering::SeqCst);
            })
        };

        finalizer_task.await.expect("finalizer task");
        sweeper_task.await.expect("sweeper task");

        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }
}
