//! Detection for a bridge transaction that can never mine.
//!
//! A transaction is replaced when another one carrying the same nonce mines
//! first. The replaced hash then stays absent from the chain forever, so polling
//! its receipt reports "pending" on every cycle and the leg waiting on it stalls
//! instead of retrying — which is how a bridged withdrawal can sit unfinished
//! while its funds are still sitting on the source account.
//!
//! Absence of a receipt is not what proves a transaction unmined: a node can
//! still be indexing one it already mined. Minedness is therefore read from the
//! transaction's own block number before any nonce is compared.

use super::types::TxLiveness;

/// Marks a bridge failure whose transaction was replaced rather than rejected.
///
/// A replaced transaction never executed, so its funds never left the source
/// account and the caller may resubmit. This prefix is what separates that from
/// a revert, which must not be retried blindly.
pub const BRIDGE_TX_SUPERSEDED_PREFIX: &str = "bridge_tx_superseded";

/// Whether the sender has moved past this transaction's nonce.
///
/// Only meaningful for a transaction already known to be unmined. A mined
/// transaction consumes its own nonce, so the sender's count passes it and this
/// returns true for a transaction that succeeded. Use [`classify_liveness`],
/// which checks minedness first.
pub fn is_superseded(tx_nonce: u64, sender_next_nonce: u64) -> bool {
    sender_next_nonce > tx_nonce
}

/// Reads a receiptless transaction's state from what the node reports.
///
/// `block_number` comes from the transaction itself: `Some` means it mined and
/// the receipt is merely lagging. Only once it is known to be unmined does a
/// passed nonce mean some other hash took it.
pub fn classify_liveness(block_number: Option<u64>, tx_nonce: u64, sender_next_nonce: u64) -> TxLiveness {
    if block_number.is_some() {
        return TxLiveness::Mined;
    }
    if is_superseded(tx_nonce, sender_next_nonce) {
        return TxLiveness::Replaced {
            tx_nonce,
            sender_next_nonce,
        };
    }
    TxLiveness::Pending
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

    /// Liquidation 1625: its deposit mined at nonce 87, which moved the sender's
    /// count to 88, and the receipt was not served for another few seconds. Read
    /// on the nonce alone that looks exactly like a replacement, and the leg
    /// cleared its bridge id and stood ready to send the same funds twice.
    #[test]
    fn a_mined_transaction_is_never_replaced_by_its_own_nonce() {
        assert!(is_superseded(87, 88), "the nonce alone cannot tell the two apart");
        assert_eq!(classify_liveness(Some(25_774_730), 87, 88), TxLiveness::Mined);
    }

    #[test]
    fn an_unmined_transaction_that_lost_its_nonce_is_replaced() {
        assert_eq!(
            classify_liveness(None, 70, 72),
            TxLiveness::Replaced {
                tx_nonce: 70,
                sender_next_nonce: 72,
            }
        );
    }

    #[test]
    fn an_unmined_transaction_still_holding_its_nonce_is_pending() {
        assert_eq!(classify_liveness(None, 70, 70), TxLiveness::Pending);
    }

    #[test]
    fn the_reason_carries_the_prefix_a_resubmitting_caller_matches_on() {
        let reason = superseded_reason("0xabc", 70, 72);
        assert!(reason.starts_with(BRIDGE_TX_SUPERSEDED_PREFIX));
        assert!(reason.contains("nonce 70"));
        assert!(reason.contains("nonce 72"));
    }
}
