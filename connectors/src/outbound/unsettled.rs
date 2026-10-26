//! A sender for chains that cannot be read back yet. It sends once and
//! leaves an undecided answer to an operator.
use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome},
    tokens::chain_token::ChainToken,
    transfer::actions::{TransferActions, TransferFailure},
};
use serde::{Deserialize, Serialize};

use super::{decode_payload, encode_payload, now_nanos};

pub const NAME: &str = "unsettled";

#[derive(Debug, Serialize, Deserialize)]
struct PlainSend {
    token: ChainToken,
    to: ChainAccount,
    amount: Nat,
}

pub struct UnsettledSender {
    transfers: Arc<dyn TransferActions>,
}

impl UnsettledSender {
    pub fn new(transfers: Arc<dyn TransferActions>) -> Self {
        Self { transfers }
    }
}

#[async_trait]
impl OutboundSender for UnsettledSender {
    async fn prepare(&self, intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Ok(PreparedSend {
            sender: NAME.to_string(),
            identity: format!("plain-{}", now_nanos()),
            payload: encode_payload(&PlainSend {
                token: intent.token.clone(),
                to: intent.to.clone(),
                amount: intent.amount_native.clone(),
            })?,
            // Nothing identifies the send on the chain, so a replay could pay twice.
            replay_safe: false,
        })
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        let send: PlainSend = match decode_payload(&prepared.payload) {
            Ok(send) => send,
            // An unreadable record proves nothing about the chain.
            Err(reason) => return SendOutcome::Open { reason },
        };
        match self.transfers.transfer(&send.token, &send.to, send.amount).await {
            Ok(receipt) => SendOutcome::Landed { receipt },
            Err(TransferFailure::Rejected(reason)) => SendOutcome::NeverLanded { reason },
            Err(TransferFailure::Ambiguous(reason)) => SendOutcome::Open { reason },
        }
    }

    async fn settle(&self, _prepared: &PreparedSend) -> SendOutcome {
        SendOutcome::Open {
            reason: "this chain cannot be read back yet; an operator decides".to_string(),
        }
    }

    /// Nothing on the chain can decide it, so the first undecided read parks.
    fn open_window_secs(&self, _prepared: &PreparedSend) -> i64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liquidium_pipeline_core::transfer::actions::MockTransferActions;

    #[tokio::test]
    async fn a_plain_send_is_not_replay_safe_and_never_settles_by_itself() {
        let mut transfers = MockTransferActions::new();
        transfers
            .expect_transfer()
            .times(1)
            .returning(|_, _, _| Err(TransferFailure::Ambiguous("rpc timed out".into())));
        let sender = UnsettledSender::new(Arc::new(transfers));
        let intent = OutboundIntent {
            token: ChainToken::EvmNative {
                chain: "eth".into(),
                symbol: "ETH".into(),
                decimals: 18,
                fee: Nat::from(0u8),
            },
            to: ChainAccount::Evm("0x1".into()),
            amount_native: Nat::from(1u8),
        };
        let prepared = sender.prepare(&intent).await.unwrap();

        assert!(!prepared.replay_safe);
        assert!(matches!(sender.submit(&prepared).await, SendOutcome::Open { .. }));
        assert!(matches!(sender.settle(&prepared).await, SendOutcome::Open { .. }));
        assert_eq!(sender.open_window_secs(&prepared), 0);
    }
}
