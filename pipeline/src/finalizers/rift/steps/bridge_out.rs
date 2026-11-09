//! The leg out on the Rift side: the shared leg with the order's deposit
//! address as its destination, and its outcome mapped onto the step.
use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::rift_backend::RiftBackend;
use log::warn;

use super::{RiftVenueFinalizer, VenueStepError, order::AMOUNT_TOLERANCE, parse_amount};
use crate::{
    finalizers::{
        bridge_legs::{BridgeDestinationProvider, BridgeLegOutcome, BridgeOutInputs, FundingSlot, advance_bridge_out},
        rift::state::{RiftState, RiftStep},
    },
    utils::now_ts,
};

/// How close to the order's deposit deadline the bridge may still start. The
/// minter pays in minutes, but a parked leg can be released days later.
const DEADLINE_MARGIN_SECS: i64 = 60 * 60;

/// The order's deposit address. Rift gave it when the order was created.
struct OrderDepositAddress(String);

/// Seconds until the deadline, as Rift writes it (RFC 3339).
fn secs_until(deadline: &str) -> Result<i64, VenueStepError> {
    let deadline = chrono::DateTime::parse_from_rfc3339(deadline.trim())
        .map_err(|error| VenueStepError::Other(format!("Rift deposit deadline `{deadline}` is not a date: {error}")))?;
    Ok(deadline.timestamp().saturating_sub(now_ts()))
}

#[async_trait]
impl BridgeDestinationProvider for OrderDepositAddress {
    async fn destination(&self) -> Result<String, String> {
        Ok(self.0.clone())
    }
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    /// The shared leg out, paying the order. The step stays `BridgeOut` until
    /// the bridge completes; then the order is watched.
    pub(super) async fn bridge_out(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        let order = state.order.as_ref().ok_or("missing Rift order before the bridge out")?;
        let order_id = order.id.clone();
        let order_amount = parse_amount(&order.amount_in)?;
        let destination = OrderDepositAddress(order.deposit_address.clone());
        // Until the minter call is saved, the deposit can still go to a new
        // order. Past that, the saved call names this order's address.
        let call_open = state.bridge_out.deposit_bridge_call.is_none() && state.bridge_out.deposit_bridge_id.is_none();
        if call_open && secs_until(&order.deposit_deadline)? < DEADLINE_MARGIN_SECS {
            warn!(
                "[rift] liq_id={} order {} deposit deadline {} is too close; a new order follows",
                state.liq_id, order_id, order.deposit_deadline
            );
            state.reorder();
            return Ok(());
        }
        let route = Self::leg_routes(&state.size_in.token, &state.final_asset)?.0;
        let funding_phase = state.funding_txid.is_none();
        let outcome = advance_bridge_out(
            &self.bridge,
            &mut state.bridge_out,
            FundingSlot {
                record: &mut state.funding,
                txid: &mut state.funding_txid,
                sent_at_ts: &mut state.funding_sent_at_ts,
            },
            BridgeOutInputs {
                leg_id: &state.liq_id,
                route,
                asset: &state.size_in.token,
                size_in: &state.size_in,
                outbound: self.outbound.as_ref(),
                destination: &destination,
            },
        )
        .await?;
        match outcome {
            BridgeLegOutcome::Waiting { stalled } => {
                if let Some(line) = stalled {
                    state.last_error = Some(line);
                }
            }
            BridgeLegOutcome::Saved { side_costs } => {
                // The funding was just sized. Rift refunds a deposit that is
                // not the order amount, so the unsent transfer is dropped and
                // a new order is taken for what the bridge delivers now.
                if funding_phase
                    && let Some(expected) = state.bridge_out.deposit_bridge_expected_amount
                    && (expected - order_amount).abs() > AMOUNT_TOLERANCE
                {
                    warn!(
                        "[rift] liq_id={} the bridge would deliver {expected} {}, but order {} expects {order_amount}; a new order follows",
                        state.liq_id, state.pay_symbol, order_id
                    );
                    state.amount_in = expected;
                    state.reorder();
                    return Ok(());
                }
                state.record_side_costs(&side_costs);
                state.send_saved = true;
            }
            BridgeLegOutcome::Funded { side_costs } => state.record_side_costs(&side_costs),
            BridgeLegOutcome::Submitted => {}
            BridgeLegOutcome::Completed { .. } => {
                if let Some(order) = state.order.as_mut() {
                    order.wait_since_ts = Some(now_ts());
                }
                state.step = RiftStep::Wait;
            }
        }
        Ok(())
    }
}
