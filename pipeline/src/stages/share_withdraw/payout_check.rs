//! Looks for the payout of an accepted withdraw, and clears a withdraw with an unknown outcome.
use super::*;

impl<D: WalStore> ShareWithdrawWatcher<D> {
    pub(super) async fn check_payout(
        &self,
        entry: &mut HeldRow,
        outflow_id: String,
        amount: Nat,
        after_index: u64,
        at: i64,
    ) -> Result<(), String> {
        let token = entry.wrapper.receipt.request.collateral_asset.clone();
        let fees = token.fee() * Nat::from(MAX_PAYOUT_FEES);
        let min_amount = if amount > fees {
            amount.clone() - fees
        } else {
            Nat::from(1u8)
        };
        let found = self
            .positions
            .arrived(PayoutQuery {
                token,
                pool: entry.holding.pool,
                destination: self.destination(entry),
                outflow_id: outflow_id.clone(),
                after_index,
                min_amount,
                max_amount: amount.clone(),
            })
            .await?;
        let Some(arrived) = found else {
            if now_ts() - at > PAYOUT_PARK_SECS {
                let note = format!(
                    "withdraw {outflow_id} of {} left the position, but its payout is not on the ledger after {} minutes; \
                     release the row to wait again",
                    Self::shown(entry, &amount),
                    PAYOUT_PARK_SECS / 60
                );
                return self.park(entry, "payout", note).await;
            }
            return Ok(());
        };
        entry.holding.arrived += arrived.clone();
        entry.holding.open = None;
        entry.holding.last_note = None;
        info!(
            event = "share_withdraw_arrived",
            liq_id = %entry.row.id,
            outflow_id = %outflow_id,
            arrived = %Self::shown(entry, &arrived),
            remaining = %Self::shown(entry, &entry.holding.remaining()),
            "[shares] 📥 payout arrived"
        );
        if entry.holding.remaining() <= Self::min_withdraw(entry) {
            return self.release(entry).await;
        }
        self.save(entry).await
    }

    /// Clears a withdraw with an unknown outcome, but only when the share count of the position did not change.
    /// An equal count proves that the call did not run, because interest does not change the count.
    pub(super) async fn resolve_unknown_withdraw(
        &self,
        entry: &mut HeldRow,
        amount: Nat,
        shares_before: Nat,
    ) -> Result<(), String> {
        let position = self
            .positions
            .position(entry.holding.profile, entry.holding.pool)
            .await?;
        if position.shares == shares_before {
            entry.holding.open = None;
            entry.holding.last_note = Some("a withdraw call did not run; it is tried again".to_string());
            entry.holding.next_try_at = now_ts() + RETRY_SECS;
            info!(liq_id = %entry.row.id, "[shares] withdraw call did not run; the position is unchanged");
            return self.save(entry).await;
        }
        let note = format!(
            "the outcome of a withdraw of {} is not known, and the position changed; \
             answer `landed <withdraw id>` if it ran, or `absent` if it did not",
            Self::shown(entry, &amount)
        );
        self.park(entry, "withdraw", note).await
    }
}
