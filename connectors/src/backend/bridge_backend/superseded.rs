//! Detection for a bridge transaction that can never mine.
//!
//! A transaction is replaced when another one carrying the same nonce mines
//! first. The replaced hash then stays absent from the chain forever, so polling
//! its receipt reports "pending" on every cycle and the leg waiting on it stalls
//! instead of retrying — which is how a bridged withdrawal can sit unfinished
//! while its funds are still sitting on the source account.

/// Marks a bridge failure whose transaction was replaced rather than rejected.
///
/// A replaced transaction never executed, so its funds never left the source
/// account and the caller may resubmit. This prefix is what separates that from
/// a revert, which must not be retried blindly.
pub const BRIDGE_TX_SUPERSEDED_PREFIX: &str = "bridge_tx_superseded";

/// Whether a transaction that still has no receipt can no longer mine.
///
/// `sender_next_nonce` is the sender's transaction count at the latest block.
/// Once it has passed the transaction's own nonce, that nonce was consumed by
/// some other hash, so this one is dead no matter how long it is polled.
pub fn is_superseded(tx_nonce: u64, sender_next_nonce: u64) -> bool {
    sender_next_nonce > tx_nonce
}

/// Builds the sentinel-prefixed reason a resubmitting caller recognises.
pub fn superseded_reason(bridge_id: &str, tx_nonce: u64, sender_next_nonce: u64) -> String {
    format!(
        "{BRIDGE_TX_SUPERSEDED_PREFIX}: bridge transaction {bridge_id} at nonce {tx_nonce} was replaced by \
         another transaction (sender has reached nonce {sender_next_nonce}) and can never mine"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nonce_the_sender_has_passed_is_superseded() {
        // The account moved to 72 while the tracked transaction sat at 70, so
        // some other hash took nonce 70.
        assert!(is_superseded(70, 72));
        assert!(is_superseded(70, 71));
    }

    #[test]
    fn a_nonce_still_awaiting_its_turn_is_not_superseded() {
        // Sender count equal to the nonce means this transaction is next: it is
        // simply unmined, and reporting it dead would abandon a live bridge.
        assert!(!is_superseded(70, 70));
        assert!(!is_superseded(70, 69));
    }

    #[test]
    fn the_reason_carries_the_prefix_a_resubmitting_caller_matches_on() {
        let reason = superseded_reason("0xabc", 70, 72);
        assert!(reason.starts_with(BRIDGE_TX_SUPERSEDED_PREFIX));
        assert!(reason.contains("nonce 70"));
        assert!(reason.contains("nonce 72"));
    }
}
