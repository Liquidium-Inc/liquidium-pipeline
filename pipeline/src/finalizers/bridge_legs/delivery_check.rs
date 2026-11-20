//! The delivery check before a leg back. The bridge wallet on Ethereum is shared
//! between rows, so the transaction of a delivery must show the funds at the wallet.
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeRouteSpec, TransactionPayment};

use super::SOURCE_BALANCE_TOLERANCE;
use crate::bridge::runtime::BridgeRuntime;

/// What the chain says about one delivery to the bridge wallet.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryCheck {
    /// The delivery is at the bridge wallet, so the leg may spend it.
    Arrived,
    /// Nothing can be said yet. The leg waits, within its bound.
    NotYet(&'static str),
    /// The chain contradicts the delivery that the sender reports. An operator decides.
    Refused(String),
}

impl DeliveryCheck {
    /// Judges what the transaction paid against the amount that the sender reports.
    /// `describe` writes what the transaction paid, for the refusal text.
    fn of(paid: TransactionPayment, expected: f64, describe: impl Fn(f64) -> String) -> Self {
        match paid {
            TransactionPayment::NotOnChain => Self::NotYet("the chain does not show the delivery transaction"),
            TransactionPayment::Reverted => Self::Refused("its delivery transaction failed on the chain".to_string()),
            TransactionPayment::Paid(amount) if amount + SOURCE_BALANCE_TOLERANCE >= expected => Self::Arrived,
            TransactionPayment::Paid(amount) => Self::Refused(describe(amount)),
            // The transaction is on the chain and did not fail. A receipt shows no more for ETH that a contract paid.
            TransactionPayment::NotVisible => Self::Arrived,
        }
    }
}

/// Returns the text only if it is an Ethereum transaction hash: `0x` and 64 hex digits.
pub fn evm_transaction_hash(text: &str) -> Option<&str> {
    let text = text.trim();
    let digits = text.strip_prefix("0x")?;
    (digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(text)
}

/// Reads from the chain what `transaction` paid to the bridge wallet of the route,
/// and judges it against `expected`. An error means that the chain could not be read.
pub async fn check_delivery(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    transaction: &str,
    expected: f64,
) -> Result<DeliveryCheck, String> {
    let wallet = bridge.sources.for_chain(route.source_chain)?;
    let paid = bridge
        .backend
        .payment_in_transaction(route.source_asset, route.source_chain, &wallet, transaction)
        .await
        .map_err(|error| format!("delivery transaction {transaction} could not be read: {error}"))?;
    Ok(DeliveryCheck::of(paid, expected, |paid| {
        format!(
            "delivery transaction {transaction} paid {paid} {} to the bridge wallet {wallet}",
            route.source_asset
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payment_is_judged_against_the_reported_amount() {
        let judge = |paid| DeliveryCheck::of(paid, 99.4, |paid| format!("paid {paid}"));
        assert_eq!(judge(TransactionPayment::Paid(99.4)), DeliveryCheck::Arrived);
        assert_eq!(judge(TransactionPayment::Paid(120.0)), DeliveryCheck::Arrived);
        assert_eq!(judge(TransactionPayment::NotVisible), DeliveryCheck::Arrived);
        assert_eq!(
            judge(TransactionPayment::Paid(99.3)),
            DeliveryCheck::Refused("paid 99.3".to_string())
        );
        assert_eq!(
            judge(TransactionPayment::Paid(0.0)),
            DeliveryCheck::Refused("paid 0".to_string())
        );
        assert!(matches!(judge(TransactionPayment::Reverted), DeliveryCheck::Refused(_)));
        assert!(matches!(judge(TransactionPayment::NotOnChain), DeliveryCheck::NotYet(_)));
    }

    #[test]
    fn only_a_full_hash_reads_as_a_transaction() {
        let hash = "0x7f3a9c1e5b2d84f60a1c7e93d5b8426f0e9a3d17c6b24f58a0de13c79b6f2e41";
        assert_eq!(evm_transaction_hash(&format!(" {hash} ")), Some(hash));
        for other in ["", "0xpayout", &hash[2..], &format!("{hash}:0"), &hash.replace('7', "g")] {
            assert_eq!(evm_transaction_hash(other), None, "{other}");
        }
    }
}
