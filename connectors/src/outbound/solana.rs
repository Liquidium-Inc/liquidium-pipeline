//! A signed Solana transaction has its signature before it is sent, and it
//! cannot land after its blockhash expires, so the cluster decides every send.
use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome},
    tokens::chain_token::ChainToken,
};

use super::{decode_payload, encode_payload};
use crate::backend::solana_backend::{SignedSolanaTx, SolanaBackend};

pub const NAME: &str = "solana";

/// Sends SOL and SPL transfers from one wallet.
pub struct SolanaSender<S: SolanaBackend> {
    backend: Arc<S>,
}

impl<S: SolanaBackend> SolanaSender<S> {
    pub fn new(backend: Arc<S>) -> Self {
        Self { backend }
    }
}

/// The saved form of a signed transfer.
pub(crate) fn prepared_send(tx: &SignedSolanaTx) -> Result<PreparedSend, String> {
    Ok(PreparedSend {
        sender: NAME.to_string(),
        identity: tx.signature.clone(),
        payload: encode_payload(tx)?,
        // The cluster accepts one transaction per signature.
        replay_safe: true,
    })
}

fn saved_tx(prepared: &PreparedSend) -> Result<SignedSolanaTx, SendOutcome> {
    decode_payload(&prepared.payload).map_err(|reason| SendOutcome::Open { reason })
}

#[async_trait]
impl<S: SolanaBackend + Send + Sync> OutboundSender for SolanaSender<S> {
    async fn prepare(&self, intent: &OutboundIntent) -> Result<PreparedSend, String> {
        let mint = match &intent.token {
            ChainToken::SolanaNative { .. } => None,
            ChainToken::SolanaSpl { mint, .. } => Some(mint.clone()),
            other => return Err(format!("Solana sender cannot send {}", other.symbol())),
        };
        let ChainAccount::Solana(to) = &intent.to else {
            return Err(format!("Solana sender cannot reach {:?}", intent.to));
        };
        let tx = self
            .backend
            .prepare_transfer(mint, to, intent.amount_native.clone())
            .await?;
        prepared_send(&tx)
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        match saved_tx(prepared) {
            Ok(tx) => self.backend.send_signed(&tx).await,
            Err(outcome) => outcome,
        }
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        match saved_tx(prepared) {
            Ok(tx) => self.backend.signed_outcome(&tx).await,
            Err(outcome) => outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::solana_backend::MockSolanaBackend;
    use candid::Nat;
    use solana_sdk::{hash::Hash, signature::Keypair, signer::Signer, transaction::Transaction};

    fn signed() -> SignedSolanaTx {
        let payer = Keypair::new();
        let transaction = Transaction::new_signed_with_payer(&[], Some(&payer.pubkey()), &[&payer], Hash::default());
        SignedSolanaTx {
            signature: transaction.signatures[0].to_string(),
            transaction,
            last_valid_block_height: 100,
        }
    }

    #[tokio::test]
    async fn the_saved_transaction_is_the_one_sent_and_read() {
        let tx = signed();
        let expected = tx.clone();
        let mut backend = MockSolanaBackend::new();
        backend
            .expect_prepare_transfer()
            .times(1)
            .withf(|mint, to, amount| mint.is_none() && to == "dest" && *amount == Nat::from(5u8))
            .return_once(move |_, _, _| Ok(tx));
        let sent = expected.clone();
        backend
            .expect_send_signed()
            .times(1)
            .withf(move |tx| *tx == sent)
            .returning(|_| SendOutcome::Open { reason: "sent".into() });
        let read = expected.clone();
        backend
            .expect_signed_outcome()
            .times(1)
            .withf(move |tx| *tx == read)
            .returning(|tx| SendOutcome::Landed { receipt: tx.signature.clone() });
        let sender = SolanaSender::new(Arc::new(backend));
        let intent = OutboundIntent {
            token: ChainToken::SolanaNative {
                symbol: "SOL".into(),
                decimals: 9,
                fee: Nat::from(5_000u64),
            },
            to: ChainAccount::Solana("dest".into()),
            amount_native: Nat::from(5u8),
        };

        let prepared = sender.prepare(&intent).await.unwrap();
        assert_eq!(prepared.identity, expected.signature);
        assert!(prepared.replay_safe);
        let restored: PreparedSend = serde_json::from_str(&serde_json::to_string(&prepared).unwrap()).unwrap();

        assert!(matches!(sender.submit(&restored).await, SendOutcome::Open { .. }));
        assert_eq!(
            sender.settle(&restored).await,
            SendOutcome::Landed {
                receipt: expected.signature.clone()
            }
        );
    }
}
