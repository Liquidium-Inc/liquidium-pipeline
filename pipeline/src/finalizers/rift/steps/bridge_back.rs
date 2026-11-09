//! The leg back on the Rift side: what Rift delivered to the bridge EVM wallet
//! goes to its ck form on ICP through the shared leg.
use liquidium_pipeline_connectors::backend::rift_backend::RiftBackend;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;

use super::{RiftVenueFinalizer, VenueStepError, parse_amount};
use crate::finalizers::{
    bridge_legs::{BridgeInInputs, BridgeLegOutcome, WaitBound, advance_bridge_in},
    rift::state::{RiftState, RiftStep},
};

/// `amount` cut to `decimals` places.
fn floor_to_decimals(amount: f64, decimals: u8) -> f64 {
    let scale = 10_f64.powi(i32::from(decimals));
    (amount * scale).floor() / scale
}

/// How long the delivered funds may take to reach the bridge wallet and start
/// the bridge. Past it, each waiting cycle errors, so the row parks for an operator.
const RETURN_TIMEOUT_SECS: i64 = 60 * 60;

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub(super) async fn bridge_back(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        let route = Self::leg_routes(&state.size_in.token, &state.final_asset)?.1;
        let order = state
            .order
            .as_mut()
            .ok_or("missing Rift order before the bridge back")?;
        let delivered = order
            .amount_out
            .as_deref()
            .ok_or("Rift order delivered without an amount")?;
        // The wallet holds whole units of the asset, so a longer decimal
        // from Rift is cut, never rounded up past the balance.
        let amount = floor_to_decimals(parse_amount(delivered)?, state.final_asset.decimals());
        let outcome = advance_bridge_in(
            &self.bridge,
            &mut state.bridge_in,
            BridgeInInputs {
                leg_id: &state.liq_id,
                route,
                amount,
                final_asset: &state.final_asset,
                final_destination: &state.final_destination,
                wait: WaitBound {
                    since: &mut order.delivered_at_ts,
                    timeout_secs: RETURN_TIMEOUT_SECS,
                },
            },
        )
        .await?;
        match outcome {
            BridgeLegOutcome::Waiting { stalled } => {
                if let Some(line) = stalled {
                    state.last_error = Some(line);
                }
            }
            BridgeLegOutcome::Saved { .. } => state.send_saved = true,
            BridgeLegOutcome::Funded { .. } | BridgeLegOutcome::Submitted => {}
            BridgeLegOutcome::Completed { receipt } => {
                // The mint receipt is what reached ICP. Without one, the
                // delivered amount is the best record.
                state.size_out = Some(match receipt {
                    Some(receipt) => ChainTokenAmount::from_raw(state.final_asset.clone(), receipt.amount),
                    None => ChainTokenAmount::from_formatted(state.final_asset.clone(), amount),
                });
                state.step = RiftStep::Completed;
            }
        }
        Ok(())
    }
}
