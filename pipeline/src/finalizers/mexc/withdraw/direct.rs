//! The MEXC withdrawal request: its order id, the saved send, and the
//! below-minimum dust completion.
use super::*;

/// The MEXC `withdrawOrderId` for one withdrawal attempt of a row.
pub fn mexc_withdraw_order_id(liq_id: &str, attempt: u32) -> String {
    let hash = super::super::mexc_utils::fnv1a_64(liq_id);
    format!("lq{hash:016x}w{attempt}")
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
            Err(OutboundError::BelowMinimum(error)) => {
                Self::complete_withdraw_as_below_min_dust(state, asset, network, amount, &error);
                Ok(None)
            }
            Err(OutboundError::Park(error)) => Err(error),
            // MEXC refused it, so the next request takes a new order id.
            Err(OutboundError::Retry(error)) => {
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

    /// A withdrawal straight to the final address: fix the destination, then
    /// read the delivery of an accepted request, or send a new one.
    pub(super) async fn withdraw_direct(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
    ) -> Result<(), String> {
        let direct_destination = self.direct_withdraw_destination(state, planned_asset, planned_network)?;

        if state.withdraw.withdraw_address != direct_destination {
            state.withdraw.withdraw_address = direct_destination;
        }

        if state
            .withdraw
            .withdraw_txid
            .as_ref()
            .is_some_and(|tx| !tx.trim().is_empty())
        {
            state.step = CexStep::Completed;
            return Ok(());
        }

        if let Some(withdraw_id) = state.withdraw.withdraw_id.clone() {
            return self
                .poll_direct_withdraw(state, planned_asset, &withdraw_id, amount)
                .await;
        }

        let Some(accepted) = self
            .submit_withdraw(state, planned_asset, planned_network, amount)
            .await?
        else {
            return Ok(());
        };
        state.withdraw.withdraw_id = Some(accepted.id);
        state.withdraw.withdraw_txid = accepted.txid.clone();
        state.step = if accepted.txid.as_ref().is_some_and(|tx| !tx.trim().is_empty()) {
            CexStep::Completed
        } else {
            state.withdraw.withdraw_accepted_at_ts = Some(now_ts());
            CexStep::WithdrawPending
        };
        Ok(())
    }

    /// Reads the delivery of an accepted withdrawal. The leg completes only
    /// with a transaction id, and parks if delivery never arrives.
    async fn poll_direct_withdraw(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        withdraw_id: &str,
        amount: f64,
    ) -> Result<(), String> {
        // An accepted POST only reserves exchange funds. Require delivery
        // evidence before making this leg terminal and reporting profit.
        let snapshot = self
            .backend
            .get_withdraw_status_snapshot_by_id(planned_asset, withdraw_id)
            .await?;
        match snapshot.status {
            WithdrawStatus::Failed | WithdrawStatus::Canceled => {
                return Err(format!(
                    "cex withdraw failed for liq_id {} withdraw_id={withdraw_id}",
                    state.liq_id
                ));
            }
            WithdrawStatus::Completed if snapshot.txid.as_ref().is_some_and(|tx| !tx.trim().is_empty()) => {
                if let Some(fee) = snapshot.transaction_fee {
                    let net = Self::net_of_withdraw_fee(amount, fee)
                        .map_err(|error| format!("invalid completed withdrawal fee: {error}"))?;
                    state.withdraw.size_out = Some(ChainTokenAmount::from_formatted(
                        state.withdraw.withdraw_asset.clone(),
                        net,
                    ));
                }
                state.withdraw.withdraw_txid = snapshot.txid;
                state.step = CexStep::Completed;
            }
            _ => {
                // Rows accepted before this clock existed start it now.
                let accepted_at = *state.withdraw.withdraw_accepted_at_ts.get_or_insert_with(now_ts);
                let waited = now_ts().saturating_sub(accepted_at);
                if waited > DIRECT_WITHDRAW_DELIVERY_TIMEOUT_SECS {
                    return Err(format!(
                        "cex withdraw not delivered after {waited}s for liq_id {} withdraw_id={withdraw_id} status={:?}",
                        state.liq_id, snapshot.status
                    ));
                }
                state.step = CexStep::WithdrawPending;
            }
        }
        Ok(())
    }
}
