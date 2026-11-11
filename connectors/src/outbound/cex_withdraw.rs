//! Exchange withdrawals as outbound sends. The order id exists before the
//! request, so the withdraw history decides a request whose reply was lost.
use std::sync::Arc;

use async_trait::async_trait;
use crate::backend::cex_backend::{CexBackend, WithdrawError, WithdrawStatus};
use liquidium_pipeline_core::outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome};
use serde::{Deserialize, Serialize};

pub const NAME: &str = "mexc-withdraw";

/// An exchange lists an accepted request within seconds. A request that stays
/// absent for this long needs an operator.
const OPEN_WINDOW_SECS: i64 = 15 * 60;

/// One withdrawal request, saved before it is sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub asset: String,
    pub network: String,
    pub address: String,
    pub amount: f64,
    pub order_id: String,
}

/// What the leg keeps from an accepted request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedWithdrawal {
    /// The exchange's id, or our order id when only the history found the request.
    /// The history lookup matches either.
    pub id: String,
    #[serde(default)]
    pub txid: Option<String>,
}

impl AcceptedWithdrawal {
    /// Reads a receipt. An operator gives a bare exchange id.
    pub fn parse(receipt: &str) -> Self {
        serde_json::from_str(receipt).unwrap_or_else(|_| Self {
            id: receipt.trim().to_string(),
            txid: None,
        })
    }

    fn encode(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.id.clone())
    }
}

pub struct CexWithdrawals<B> {
    backend: Arc<B>,
    /// The exchange's name, for operator-facing messages.
    venue: &'static str,
}

impl<B: CexBackend> CexWithdrawals<B> {
    pub fn new(backend: Arc<B>, venue: &'static str) -> Self {
        Self { backend, venue }
    }
}

/// The saved form of a request. The leg builds it from its own state,
/// because each exchange names the asset and network its own way.
pub fn prepare_cex_withdraw(request: &WithdrawRequest) -> Result<PreparedSend, String> {
    Ok(PreparedSend {
        sender: NAME.to_string(),
        identity: request.order_id.clone(),
        payload: super::encode_payload(request)?,
        // An exchange is not known to refuse a reused order id, so a second
        // request could withdraw the same funds twice.
        replay_safe: false,
    })
}

fn saved_request(prepared: &PreparedSend) -> Result<WithdrawRequest, SendOutcome> {
    super::decode_payload(&prepared.payload).map_err(|reason| SendOutcome::Open { reason })
}

#[async_trait]
impl<B: CexBackend> OutboundSender for CexWithdrawals<B> {
    async fn prepare(&self, _intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Err(format!("a {} withdrawal is prepared from the leg state", self.venue))
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        let request = match saved_request(prepared) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        match self
            .backend
            .withdraw(
                &request.asset,
                &request.network,
                &request.address,
                request.amount,
                &request.order_id,
            )
            .await
        {
            Ok(receipt) => SendOutcome::Landed {
                receipt: AcceptedWithdrawal {
                    id: receipt.internal_id.unwrap_or(request.order_id),
                    txid: receipt.txid,
                }
                .encode(),
            },
            Err(WithdrawError::Ambiguous(reason)) => SendOutcome::Open { reason },
            Err(WithdrawError::BelowMinimum(reason)) => SendOutcome::BelowMinimum { reason },
            Err(WithdrawError::Refused(reason)) => SendOutcome::NeverLanded { reason },
            Err(WithdrawError::Unreachable(reason)) => SendOutcome::Deferred { reason },
        }
    }

    /// Looks the order id up in the history. It never sends again.
    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        let request = match saved_request(prepared) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        match self
            .backend
            .get_withdraw_status_snapshot_by_id(&request.asset, &request.order_id)
            .await
        {
            // The leg's next poll reads the delivery and its fee.
            Ok(snapshot) if snapshot.status != WithdrawStatus::Unknown => SendOutcome::Landed {
                receipt: AcceptedWithdrawal {
                    id: request.order_id,
                    txid: None,
                }
                .encode(),
            },
            Ok(_) => SendOutcome::Open {
                reason: format!(
                    "order id {} is not in the {} withdraw history",
                    request.order_id, self.venue
                ),
            },
            Err(error) => SendOutcome::Open {
                reason: format!("{} withdraw history read failed: {error}", self.venue),
            },
        }
    }

    fn open_window_secs(&self, _prepared: &PreparedSend) -> i64 {
        OPEN_WINDOW_SECS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::cex_backend::MockCexBackend;

    #[tokio::test]
    async fn each_withdraw_refusal_kind_becomes_its_send_outcome() {
        let prepared = prepare_cex_withdraw(&WithdrawRequest {
            asset: "USDC".into(),
            network: "ETH".into(),
            address: "0x2222222222222222222222222222222222222222".into(),
            amount: 1.5,
            order_id: "lq1w0".into(),
        })
        .expect("prepared");
        let cases = [
            (
                WithdrawError::Ambiguous("timed out".into()),
                SendOutcome::Open {
                    reason: "timed out".into(),
                },
            ),
            (
                WithdrawError::BelowMinimum("under 2 USDC".into()),
                SendOutcome::BelowMinimum {
                    reason: "under 2 USDC".into(),
                },
            ),
            (
                WithdrawError::Refused("no balance".into()),
                SendOutcome::NeverLanded {
                    reason: "no balance".into(),
                },
            ),
            (
                WithdrawError::Unreachable("dns error".into()),
                SendOutcome::Deferred {
                    reason: "dns error".into(),
                },
            ),
        ];
        for (error, expected) in cases {
            let mut backend = MockCexBackend::new();
            backend.expect_withdraw().return_once(move |_, _, _, _, _| Err(error));
            let outcome = CexWithdrawals::new(Arc::new(backend), "MEXC").submit(&prepared).await;
            assert_eq!(outcome, expected);
        }
    }
}
