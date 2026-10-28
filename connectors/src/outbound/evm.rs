//! A signed EVM transaction has its hash and nonce before it is broadcast,
//! and its nonce can be used once, so the chain decides every send.
use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_core::outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome};

use super::{decode_payload, encode_payload};
use crate::backend::{
    bridge_backend::ckerc20_bridge::BridgeEvmBackend,
    evm_backend::{Broadcast, SignedEvmTx},
};

pub const NAME: &str = "evm";

/// A transaction with a fee that the network does not take stays pending.
/// After this long an operator decides.
const OPEN_WINDOW_SECS: i64 = 30 * 60;

/// The saved form of a signed transaction.
pub(crate) fn prepared_send(tx: &SignedEvmTx) -> Result<PreparedSend, String> {
    Ok(PreparedSend {
        sender: NAME.to_string(),
        identity: format!("{:#x}", tx.hash),
        payload: encode_payload(tx)?,
        // The same bytes cannot land twice: their nonce is used once.
        replay_safe: true,
    })
}

/// Sends transactions that a caller signed with one wallet.
pub struct EvmSender<E: BridgeEvmBackend> {
    backend: Arc<E>,
}

impl<E: BridgeEvmBackend> EvmSender<E> {
    pub fn new(backend: Arc<E>) -> Self {
        Self { backend }
    }
}

fn saved_tx(prepared: &PreparedSend) -> Result<SignedEvmTx, SendOutcome> {
    decode_payload(&prepared.payload).map_err(|reason| SendOutcome::Open { reason })
}

#[async_trait]
impl<E: BridgeEvmBackend> OutboundSender for EvmSender<E> {
    async fn prepare(&self, _intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Err("an EVM send is signed by the bridge that builds its call".to_string())
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        let tx = match saved_tx(prepared) {
            Ok(tx) => tx,
            Err(outcome) => return outcome,
        };
        match self.backend.broadcast(&tx).await {
            Broadcast::Sent => SendOutcome::Open {
                reason: format!("{:#x} broadcast; waiting for a receipt", tx.hash),
            },
            Broadcast::Rejected(reason) => SendOutcome::NeverLanded { reason },
            Broadcast::Unknown(reason) => SendOutcome::Open { reason },
        }
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        match saved_tx(prepared) {
            Ok(tx) => self.backend.signed_outcome(&tx).await,
            Err(outcome) => outcome,
        }
    }

    fn open_window_secs(&self, _prepared: &PreparedSend) -> i64 {
        OPEN_WINDOW_SECS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::bridge_backend::ckerc20_bridge::MockBridgeEvmBackend;
    use alloy::primitives::{Address, Bytes, TxHash};

    fn signed() -> SignedEvmTx {
        SignedEvmTx {
            raw: Bytes::from_static(&[0x02, 0x01]),
            hash: TxHash::from([5; 32]),
            from: Address::ZERO,
            nonce: 3,
        }
    }

    #[tokio::test]
    async fn the_saved_bytes_are_sent_and_decided_by_the_chain() {
        let prepared = prepared_send(&signed()).unwrap();
        assert_eq!(prepared.identity, format!("{:#x}", TxHash::from([5; 32])));
        assert!(prepared.replay_safe);
        let restored: PreparedSend = serde_json::from_str(&serde_json::to_string(&prepared).unwrap()).unwrap();

        let mut backend = MockBridgeEvmBackend::new();
        let answers = std::sync::Mutex::new(vec![
            Broadcast::Unknown("connection reset".into()),
            Broadcast::Rejected("insufficient funds".into()),
            Broadcast::Sent,
        ]);
        backend
            .expect_broadcast()
            .times(3)
            .withf(|tx| *tx == signed())
            .returning(move |_| answers.lock().unwrap().pop().unwrap());
        backend
            .expect_signed_outcome()
            .times(1)
            .returning(|tx| SendOutcome::Landed {
                receipt: format!("{:#x}", tx.hash),
            });
        let sender = EvmSender::new(Arc::new(backend));

        assert!(
            matches!(sender.submit(&restored).await, SendOutcome::Open { .. }),
            "sent: wait for a receipt"
        );
        assert!(
            matches!(sender.submit(&restored).await, SendOutcome::NeverLanded { .. }),
            "refused"
        );
        assert!(
            matches!(sender.submit(&restored).await, SendOutcome::Open { .. }),
            "no answer"
        );
        assert!(matches!(sender.settle(&restored).await, SendOutcome::Landed { .. }));
    }
}
