//! Exchange withdrawals as outbound sends. The order id exists before the
//! request, so the withdraw history decides a request whose reply was lost.
use std::sync::Arc;

use async_trait::async_trait;
use crate::backend::cex_backend::{CexBackend, WithdrawStatus};
use liquidium_pipeline_core::outbound::{
    OutboundIntent, OutboundSender, PreparedSend, SendOutcome, is_side_effect_ambiguous,
};
use serde::{Deserialize, Serialize};

pub const NAME: &str = "mexc-withdraw";

/// MEXC lists an accepted request within seconds. A request that stays
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
    /// The MEXC id, or our order id when only the history found the request.
    /// The history lookup matches either.
    pub id: String,
    #[serde(default)]
    pub txid: Option<String>,
}

impl AcceptedWithdrawal {
    /// Reads a receipt. An operator gives a bare MEXC id.
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
}

impl<B: CexBackend> CexWithdrawals<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

/// The saved form of a request. The leg builds it from its own state,
/// because MEXC names the asset and network its own way.
pub fn prepare_cex_withdraw(request: &WithdrawRequest) -> Result<PreparedSend, String> {
    Ok(PreparedSend {
        sender: NAME.to_string(),
        identity: request.order_id.clone(),
        payload: serde_json::to_string(request).map_err(|error| format!("encode withdraw request: {error}"))?,
        // MEXC is not known to refuse a reused order id, so a second
        // request could withdraw the same funds twice.
        replay_safe: false,
    })
}

fn saved_request(prepared: &PreparedSend) -> Result<WithdrawRequest, SendOutcome> {
    serde_json::from_str(&prepared.payload).map_err(|error| SendOutcome::NeverLanded {
        reason: format!("decode withdraw request: {error}"),
    })
}

#[async_trait]
impl<B: CexBackend> OutboundSender for CexWithdrawals<B> {
    async fn prepare(&self, _intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Err("a MEXC withdrawal is prepared from the leg state".to_string())
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
            Err(error) if is_side_effect_ambiguous(&error) => SendOutcome::Open { reason: error },
            Err(error) => SendOutcome::NeverLanded { reason: error },
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
                reason: format!("order id {} is not in the MEXC withdraw history", request.order_id),
            },
            Err(error) => SendOutcome::Open {
                reason: format!("MEXC withdraw history read failed: {error}"),
            },
        }
    }

    fn open_window_secs(&self, _prepared: &PreparedSend) -> i64 {
        OPEN_WINDOW_SECS
    }
}
