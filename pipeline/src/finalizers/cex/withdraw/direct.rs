//! The exchange withdrawal request: its order id, the saved send, and the
//! below-minimum dust completion.
use super::*;
use liquidium_pipeline_connectors::backend::cex_backend::WithdrawStatusSnapshot;

/// The withdrawal order id for one withdrawal attempt of a row (MEXC `withdrawOrderId`).
pub fn withdraw_order_id(liq_id: &str, attempt: u32) -> String {
    let hash = super::super::utils::fnv1a_64(liq_id);
    format!("lq{hash:016x}w{attempt}")
}

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// The exchange keeps this id with the request, so history can find a
    /// request whose reply was lost. A refused request bumps the attempt.
    fn withdraw_order_id(state: &CexState) -> String {
        withdraw_order_id(&state.liq_id, state.withdraw.withdraw_attempt)
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
    ) -> Result<Option<AcceptedWithdrawal>, CexBackendError> {
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
        let sender = CexWithdrawals::new(self.backend.clone(), self.profile.name());
        match step_outbound(&mut state.withdraw.withdraw_send, &sender, None, now_ts()).await {
            Ok(landed) => Ok(landed.as_deref().map(AcceptedWithdrawal::parse)),
            Err(OutboundError::BelowMinimum(error)) => {
                self.settle_withdraw_below_min(state, asset, network, amount, &error)
                    .await?;
                Ok(None)
            }
            Err(OutboundError::Park(error) | OutboundError::Reverted(error)) => Err(CexBackendError::Ambiguous(error)),
            // The exchange could not be reached, so the same order id is still unused.
            Err(OutboundError::Deferred(error)) => Err(CexBackendError::Unreachable(error)),
            // The exchange refused the request, so the next one takes a new order id.
            Err(OutboundError::Retry(error)) => {
                state.withdraw.withdraw_attempt += 1;
                Err(error.into())
            }
        }
    }

    /// Writes off a refused withdrawal as dust only when it prices under the
    /// dust floor. A larger or unpriceable amount stays on the exchange for an operator.
    async fn settle_withdraw_below_min(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
        refusal: &str,
    ) -> Result<(), CexBackendError> {
        let usd = self
            .amount_symbol_to_usd(planned_asset, amount)
            .await
            .map_err(|error| {
                format!(
                    "{refusal}; {amount} {planned_asset} stays on {} because it could not be priced: {error}",
                    self.profile.name()
                )
            })?;
        if usd > self.cex_min_exec_usd {
            return Err(format!(
                "{refusal}; {amount} {planned_asset} (~{usd:.2} USD) is above the {} USD dust floor, so it stays on {} for an operator",
                self.cex_min_exec_usd,
                self.profile.name()
            )
            .into());
        }
        let msg = format!(
            "{} withdraw below minimum; treating residual as CEX dust for liq_id {} asset={} network={} amount={} (~{usd:.4} USD) err={}",
            self.profile.venue_id(),
            state.liq_id,
            planned_asset,
            planned_network,
            amount,
            refusal
        );
        warn!("{}", msg);
        state.last_error = Some(msg);
        state.withdraw.size_out = Some(ChainTokenAmount::from_raw(
            state.withdraw.withdraw_asset.clone(),
            Nat::from(0u8),
        ));
        state.step = CexStep::Completed;
        Ok(())
    }

    /// A withdrawal straight to the final address: fix the destination, then
    /// read the delivery of an accepted request, or send a new one.
    pub(super) async fn withdraw_direct(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
        amount: f64,
    ) -> Result<(), CexBackendError> {
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
            return Ok(self
                .poll_direct_withdraw(state, planned_asset, &withdraw_id, amount)
                .await?);
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

    /// The exchange ended an accepted withdrawal without sending it, so the funds
    /// are back on its balance. Past the bound the error stays, and the retry budget parks the row.
    pub(in crate::finalizers::cex) fn resubmit_failed_withdrawal(
        state: &mut CexState,
        withdraw_id: &str,
        snapshot: &WithdrawStatusSnapshot,
    ) -> Result<(), String> {
        let ended = format!(
            "cex withdraw {:?} for liq_id {} withdraw_id={withdraw_id}",
            snapshot.status, state.liq_id
        );
        // A transaction id means a send went out, so a new request could pay twice.
        if let Some(txid) = snapshot.txid.as_deref().filter(|tx| !tx.trim().is_empty()) {
            return Err(format!(
                "{ended} with txid={txid}; the send may still land, so no new request"
            ));
        }
        let withdraw = &mut state.withdraw;
        if withdraw.withdraw_failures >= MAX_FAILED_WITHDRAWAL_RESUBMITS {
            return Err(format!(
                "{ended}; after {} failed withdrawals the funds stay on the exchange for an operator",
                withdraw.withdraw_failures + 1
            ));
        }
        withdraw.withdraw_failures += 1;
        // A new order id, so the exchange cannot answer with the failed request.
        withdraw.withdraw_attempt += 1;
        withdraw.withdraw_id = None;
        withdraw.withdraw_txid = None;
        withdraw.withdraw_accepted_at_ts = None;
        warn!(
            "[cex] {ended}; requesting it again ({}/{MAX_FAILED_WITHDRAWAL_RESUBMITS})",
            withdraw.withdraw_failures
        );
        state.step = CexStep::Withdraw;
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
                Self::resubmit_failed_withdrawal(state, withdraw_id, &snapshot)?;
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
