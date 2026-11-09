//! The order: a fresh quote checked against the planned floor, saved, then the
//! order Rift creates from it. The order names the deposit address the bridge pays.
use liquidium_pipeline_connectors::backend::rift_backend::{RiftAsset, RiftBackend, RiftError, RiftQuoteRequest};
use log::info;

use super::{RIFT_CHAIN, RiftVenueFinalizer, VenueStepError, decimal_text, parse_amount};
use crate::finalizers::rift::state::{RiftOrderRecord, RiftState, RiftStep};

/// How far the order's `amount_in` may sit from what the bridge delivers.
/// Both come from the same decimal text, so only a rounding bit is allowed.
pub(in crate::finalizers::rift) const AMOUNT_TOLERANCE: f64 = 1e-9;

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    /// What to quote: the amount the bridge delivers, in Rift's names.
    pub(super) fn quote_request(&self, state: &RiftState) -> RiftQuoteRequest {
        RiftQuoteRequest {
            from: RiftAsset::new(RIFT_CHAIN, &state.pay_symbol),
            to: RiftAsset::new(RIFT_CHAIN, &state.receive_symbol),
            amount_in: decimal_text(state.amount_in, state.size_in.token.decimals()),
        }
    }

    /// Cycle one takes a quote and saves its id. Cycle two creates the order
    /// from the saved id, so a repeat after a crash gets the same order back.
    pub(super) async fn order(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        if state.order.is_some() {
            state.step = RiftStep::BridgeOut;
            return Ok(());
        }
        let Some(quote_id) = state.quote_id.clone() else {
            let quote = self.backend.quote(&self.quote_request(state)).await?;
            let amount_out = parse_amount(&quote.estimated_amount_out)?;
            // The plan committed to the conservative output. A thinner quote
            // now would make the leg pay more than the plan allowed.
            if amount_out < state.min_amount_out {
                return Err(VenueStepError::Other(format!(
                    "Rift quote {} pays {amount_out} {}, under the planned floor {}",
                    quote.id, state.receive_symbol, state.min_amount_out
                )));
            }
            info!(
                "[rift] liq_id={} quote {} saved: {} {} -> {} {} via {:?}",
                state.liq_id,
                quote.id,
                quote.amount_in,
                state.pay_symbol,
                amount_out,
                state.receive_symbol,
                quote.venues
            );
            state.quote_id = Some(quote.id);
            state.send_saved = true;
            return Ok(());
        };

        let order = match self
            .backend
            .create_order(&quote_id, self.delivery_address(), Some(self.refund_address.clone()))
            .await
        {
            Ok(order) => order,
            // Nothing was created. The next cycle takes a new quote.
            Err(RiftError::QuoteExpired(reason)) => {
                state.quote_id = None;
                return Err(VenueStepError::Other(format!(
                    "Rift quote {quote_id} expired before the order; a new quote follows: {reason}"
                )));
            }
            Err(error) => return Err(error.into()),
        };
        // The order is never saved, so nothing funds it; it expires unfunded.
        let amount_in = parse_amount(&order.amount_in)?;
        if (amount_in - state.amount_in).abs() > AMOUNT_TOLERANCE {
            return Err(VenueStepError::Other(format!(
                "Rift order {} expects {amount_in} {}, but the bridge delivers {}",
                order.id, state.pay_symbol, state.amount_in
            )));
        }
        info!(
            "[rift] liq_id={} order {} created: deposit_address={} amount_in={} {} deadline={} to={} refund={}",
            state.liq_id,
            order.id,
            order.deposit_address,
            order.amount_in,
            state.pay_symbol,
            order.deposit_deadline,
            self.delivery_address(),
            self.refund_address
        );
        state.order = Some(RiftOrderRecord {
            id: order.id,
            quote_id: order.quote_id,
            deposit_address: order.deposit_address,
            amount_in: order.amount_in,
            deposit_deadline: order.deposit_deadline,
            status: Some(order.status.as_api()),
            amount_out: None,
            payout_tx: None,
            polled_at_ts: None,
            wait_since_ts: None,
            delivered_at_ts: None,
            refund_tx: None,
        });
        state.step = RiftStep::BridgeOut;
        Ok(())
    }
}
