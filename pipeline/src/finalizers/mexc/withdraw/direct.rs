//! The MEXC withdrawal request: its order id, the saved send, and the
//! below-minimum dust completion.
use super::*;

/// The MEXC `withdrawOrderId` for one withdrawal attempt of a row.
pub fn mexc_withdraw_order_id(liq_id: &str, attempt: u32) -> String {
    let hash = super::super::mexc_utils::fnv1a_64(liq_id);
    format!("lq{hash:016x}w{attempt}")
}

pub(super) fn is_mexc_withdraw_below_min_error(err: &str) -> bool {
    // The CEX backend trait currently erases MEXC's typed error into a string.
    // Recover the stable raw code instead of matching MEXC's human-readable text.
    parse_mexc_raw_code(err) == Some(MEXC_WITHDRAW_BELOW_MIN_RAW_CODE)
}

fn parse_mexc_raw_code(err: &str) -> Option<i64> {
    let value = err.split("raw_code:").nth(1)?.trim_start();
    let end = value
        .find(|ch: char| !ch.is_ascii_digit() && ch != '-')
        .unwrap_or(value.len());
    value.get(..end)?.parse().ok()
}

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// The exchange keeps this id with the request, so history can find a
    /// request whose reply was lost. A refused request bumps the attempt.
    fn withdraw_order_id(state: &CexState) -> String {
        mexc_withdraw_order_id(&state.liq_id, state.withdraw.withdraw_attempt)
    }

    /// Moves the leg's withdrawal request through the outbound driver: one
    /// cycle saves it, the next sends it. `None` means wait, or the residual
    /// completed as dust.
    pub(super) async fn submit_withdraw(
        &self,
        state: &mut CexState,
        asset: &str,
        network: &str,
        amount: f64,
    ) -> Result<Option<AcceptedWithdrawal>, String> {
        if state.withdraw.withdraw_send.is_none() {
            let request = WithdrawRequest {
                asset: asset.to_string(),
                network: network.to_string(),
                address: state.withdraw.withdraw_address.clone(),
                amount,
                order_id: Self::withdraw_order_id(state),
            };
            state.withdraw.withdraw_send = Some(OutboundRecord::prepared(prepare_cex_withdraw(&request)?));
            state.send_saved = true;
            return Ok(None);
        }
        let sender = CexWithdrawals::new(self.backend.clone());
        match step_outbound(&mut state.withdraw.withdraw_send, &sender, None, now_ts()).await {
            Ok(landed) => Ok(landed.as_deref().map(AcceptedWithdrawal::parse)),
            Err(error) if is_mexc_withdraw_below_min_error(&error) => {
                Self::complete_withdraw_as_below_min_dust(state, asset, network, amount, &error);
                Ok(None)
            }
            Err(error) if is_side_effect_ambiguous(&error) => Err(error),
            // MEXC refused it, so the next request takes a new order id.
            Err(error) => {
                state.withdraw.withdraw_attempt += 1;
                Err(error)
            }
        }
    }

    fn complete_withdraw_as_below_min_dust(
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
        err: &str,
    ) {
        let msg = format!(
            "mexc withdraw below minimum; treating residual as CEX dust for liq_id {} asset={} network={} amount={} err={}",
            state.liq_id, planned_asset, planned_network, amount, err
        );
        warn!("{}", msg);
        state.last_error = Some(msg);
        state.withdraw.size_out = Some(ChainTokenAmount::from_raw(
            state.withdraw.withdraw_asset.clone(),
            Nat::from(0u8),
        ));
        state.step = CexStep::Completed;
    }
}
