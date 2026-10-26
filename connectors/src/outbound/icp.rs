//! ICP ledgers deduplicate a transfer with the same arguments and
//! `created_at_time` for 24 hours, so a resend returns the first block.
use std::sync::Arc;

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome},
    tokens::chain_token::ChainToken,
    transfer::actions::TransferFailure,
};
use serde::{Deserialize, Serialize};

use super::{decode, encode, now_nanos};
use crate::backend::icp_backend::IcpBackend;

pub const NAME: &str = "icp";

#[derive(Debug, Serialize, Deserialize)]
struct IcpSend {
    ledger: Principal,
    to: ChainAccount,
    amount: Nat,
    created_at_time_nanos: u64,
}

/// Sends ICP ledger transfers from one account.
pub struct IcpSender<B: IcpBackend> {
    backend: Arc<B>,
    account: Account,
}

impl<B: IcpBackend> IcpSender<B> {
    pub fn new(backend: Arc<B>, account: Account) -> Self {
        Self { backend, account }
    }

    /// One call serves as submit and as settle: the ledger applies the first
    /// copy and answers every later copy with that block.
    async fn send(&self, prepared: &PreparedSend) -> SendOutcome {
        let send: IcpSend = match decode(&prepared.payload) {
            Ok(send) => send,
            Err(reason) => return SendOutcome::NeverLanded { reason },
        };
        let result = match &send.to {
            ChainAccount::IcpLedger(account_id_hex) => self
                .backend
                .icp_transfer(send.ledger, account_id_hex, send.amount, Some(send.created_at_time_nanos))
                .await
                .map(Nat::from),
            ChainAccount::Icp(to) => self
                .backend
                .icrc1_transfer_with_args(
                    send.ledger,
                    TransferArg {
                        from_subaccount: self.account.subaccount,
                        to: *to,
                        amount: send.amount,
                        fee: None,
                        memo: None,
                        created_at_time: Some(send.created_at_time_nanos),
                    },
                )
                .await
                .map_err(TransferFailure::from),
            other => Err(TransferFailure::Rejected(format!("ICP sender cannot reach {other:?}"))),
        };
        match result {
            Ok(block) => SendOutcome::Landed {
                receipt: block.to_string(),
            },
            Err(TransferFailure::Rejected(reason)) => SendOutcome::NeverLanded { reason },
            Err(TransferFailure::Ambiguous(reason)) => SendOutcome::Open { reason },
        }
    }
}

#[async_trait]
impl<B: IcpBackend + Send + Sync> OutboundSender for IcpSender<B> {
    async fn prepare(&self, intent: &OutboundIntent) -> Result<PreparedSend, String> {
        let ChainToken::Icp { ledger, .. } = &intent.token else {
            return Err(format!("ICP sender cannot send {}", intent.token.symbol()));
        };
        if !matches!(intent.to, ChainAccount::Icp(_) | ChainAccount::IcpLedger(_)) {
            return Err(format!("ICP sender cannot reach {:?}", intent.to));
        }
        let created_at_time_nanos = now_nanos();
        Ok(PreparedSend {
            sender: NAME.to_string(),
            identity: created_at_time_nanos.to_string(),
            payload: encode(&IcpSend {
                ledger: *ledger,
                to: intent.to.clone(),
                amount: intent.amount_native.clone(),
                created_at_time_nanos,
            })?,
            replay_safe: true,
        })
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        self.send(prepared).await
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        self.send(prepared).await
    }

    /// Open means the replica gave no answer. The ledger window is 24 hours,
    /// so an hour of failed resends parks well inside it.
    fn open_window_secs(&self, _prepared: &PreparedSend) -> i64 {
        60 * 60
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::icp_backend::{IcrcTransferError, MockIcpBackend};

    fn token() -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_slice(&[1]),
            symbol: "ckUSDC".into(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        }
    }

    fn intent(to: ChainAccount) -> OutboundIntent {
        OutboundIntent {
            token: token(),
            to,
            amount_native: Nat::from(100u64),
        }
    }

    fn account() -> ChainAccount {
        ChainAccount::Icp(Account {
            owner: Principal::from_slice(&[2]),
            subaccount: None,
        })
    }

    fn sender(backend: MockIcpBackend) -> IcpSender<MockIcpBackend> {
        IcpSender::new(
            Arc::new(backend),
            Account {
                owner: Principal::anonymous(),
                subaccount: Some([7; 32]),
            },
        )
    }

    #[tokio::test]
    async fn every_resend_carries_the_saved_created_at_time() {
        let mut backend = MockIcpBackend::new();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = calls.clone();
        backend.expect_icrc1_transfer_with_args().times(2).returning(move |_, args| {
            assert_eq!(args.from_subaccount, Some([7; 32]));
            seen.lock().unwrap().push(args.created_at_time);
            Ok(Nat::from(9u64))
        });
        let sender = sender(backend);
        let prepared = sender.prepare(&intent(account())).await.unwrap();
        assert!(prepared.replay_safe);

        assert_eq!(sender.submit(&prepared).await, SendOutcome::Landed { receipt: "9".into() });
        assert_eq!(sender.settle(&prepared).await, SendOutcome::Landed { receipt: "9".into() });
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0], calls[1]);
        assert_eq!(calls[0].map(|t| t.to_string()), Some(prepared.identity.clone()));
    }

    #[tokio::test]
    async fn ledger_answers_map_to_outcomes() {
        for (answer, expected) in [
            (Err(IcrcTransferError::Other("replica timed out".into())), "open"),
            (Err(IcrcTransferError::TooOld), "open"),
            (Err(IcrcTransferError::Rejected("insufficient funds".into())), "never"),
            (Err(IcrcTransferError::CreatedInFuture { ledger_time: 1 }), "never"),
        ] {
            let mut backend = MockIcpBackend::new();
            backend
                .expect_icrc1_transfer_with_args()
                .return_once(move |_, _| answer);
            let sender = sender(backend);
            let prepared = sender.prepare(&intent(account())).await.unwrap();

            let outcome = sender.submit(&prepared).await;

            match expected {
                "open" => assert!(matches!(outcome, SendOutcome::Open { .. }), "{outcome:?}"),
                _ => assert!(matches!(outcome, SendOutcome::NeverLanded { .. }), "{outcome:?}"),
            }
        }
    }

    #[tokio::test]
    async fn native_icp_uses_the_account_id_with_the_same_created_at_time() {
        let mut backend = MockIcpBackend::new();
        backend
            .expect_icp_transfer()
            .times(1)
            .withf(|_, account_id, _, created_at| account_id == "00" && created_at.is_some())
            .returning(|_, _, _, _| Ok(11));
        let sender = sender(backend);
        let prepared = sender.prepare(&intent(ChainAccount::IcpLedger("00".into()))).await.unwrap();

        assert_eq!(sender.submit(&prepared).await, SendOutcome::Landed { receipt: "11".into() });
    }

    #[tokio::test]
    async fn other_tokens_are_refused_before_anything_is_saved() {
        let sender = sender(MockIcpBackend::new());
        let evm = OutboundIntent {
            token: ChainToken::EvmNative {
                chain: "eth".into(),
                symbol: "ETH".into(),
                decimals: 18,
                fee: Nat::from(0u8),
            },
            to: ChainAccount::Evm("0x1".into()),
            amount_native: Nat::from(1u8),
        };
        assert!(sender.prepare(&evm).await.is_err());
    }
}
