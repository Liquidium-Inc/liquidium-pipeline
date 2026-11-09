//! The wait for Rift: the order status until it delivers. Every status but a
//! wait and a delivery means Rift holds or moved the funds, so an operator decides.
use liquidium_pipeline_connectors::backend::rift_backend::{RiftBackend, RiftOrderStatus};
use log::{debug, info, warn};

use super::{RiftVenueFinalizer, VenueStepError, parse_amount};
use crate::{
    finalizers::rift::state::{RiftState, RiftStep},
    utils::{now_ts, waited_secs},
};

/// How long Rift may take to see a deposit the bridge already paid.
const DEPOSIT_SEEN_TIMEOUT_SECS: i64 = 2 * 60 * 60;
/// How long a funded order may take to deliver.
const DELIVERY_TIMEOUT_SECS: i64 = 6 * 60 * 60;

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub(super) async fn wait(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        let refund_address = self.refund_address.clone();
        let liq_id = state.liq_id.clone();
        let order = state.order.as_mut().ok_or("missing Rift order while waiting")?;
        let answer = self.backend.order_state(&order.id).await?;
        order.polled_at_ts = Some(now_ts());
        order.status = Some(answer.status.as_api());
        let waited = waited_secs(&mut order.wait_since_ts);
        match answer.status {
            RiftOrderStatus::AwaitingDeposit if waited <= DEPOSIT_SEEN_TIMEOUT_SECS => {
                debug!(
                    "[rift] liq_id={} order {} awaits its deposit {}s after the bridge paid it",
                    liq_id, order.id, waited
                );
                Ok(())
            }
            RiftOrderStatus::AwaitingDeposit => Err(VenueStepError::Ambiguous(format!(
                "the bridge paid Rift order {} {waited}s ago and the order still awaits its deposit",
                order.id
            ))),
            RiftOrderStatus::Funded | RiftOrderStatus::Executing if waited <= DELIVERY_TIMEOUT_SECS => {
                debug!(
                    "[rift] liq_id={} order {} is {} after {}s",
                    liq_id,
                    order.id,
                    answer.status.as_api(),
                    waited
                );
                Ok(())
            }
            RiftOrderStatus::Funded | RiftOrderStatus::Executing => Err(VenueStepError::Ambiguous(format!(
                "Rift order {} is still {} {waited}s after the bridge paid it",
                order.id,
                answer.status.as_api()
            ))),
            RiftOrderStatus::Delivered => {
                let amount_out = answer.amount_out.ok_or_else(|| {
                    VenueStepError::Ambiguous(format!("Rift order {} delivered without an amount", order.id))
                })?;
                let delivered = parse_amount(&amount_out)?;
                if delivered <= 0.0 {
                    return Err(VenueStepError::Ambiguous(format!(
                        "Rift order {} reports a delivery of {amount_out} {}",
                        order.id, state.receive_symbol
                    )));
                }
                // The money is already at the wallet, so the leg brings it
                // back. The shortfall is visible here and in the leg's result.
                if delivered < state.min_amount_out {
                    warn!(
                        "[rift] liq_id={} order {} delivered {} {}, under the planned floor {}",
                        liq_id, order.id, delivered, state.receive_symbol, state.min_amount_out
                    );
                }
                info!(
                    "[rift] liq_id={} order {} delivered {} {} payout_tx={:?}",
                    liq_id, order.id, amount_out, state.receive_symbol, answer.payout_tx
                );
                order.amount_out = Some(amount_out);
                order.payout_tx = answer.payout_tx;
                order.delivered_at_ts = Some(now_ts());
                state.step = RiftStep::BridgeBack;
                Ok(())
            }
            RiftOrderStatus::Refunded => Err(VenueStepError::Ambiguous(format!(
                "Rift order {} was refunded; the funds are at the refund address {}",
                order.id,
                answer.refund_address.as_deref().unwrap_or(&refund_address)
            ))),
            RiftOrderStatus::Underfunded | RiftOrderStatus::Frozen | RiftOrderStatus::Expired => {
                Err(VenueStepError::Ambiguous(format!(
                    "Rift order {} is {} after the bridge paid it; Rift support holds the funds",
                    order.id,
                    answer.status.as_api()
                )))
            }
            RiftOrderStatus::Unknown(status) => Err(VenueStepError::Ambiguous(format!(
                "Rift order {} reports status `{status}`, which this build does not know",
                order.id
            ))),
        }
    }
}
