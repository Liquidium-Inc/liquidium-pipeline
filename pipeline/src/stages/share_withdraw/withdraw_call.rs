//! Starts a withdraw: reads the pool and the position, decides the amount, and calls the canister.
use super::withdraw_rules::{AfterFailure, WithdrawInputs, WithdrawStep, after_failure, next_withdraw};
use super::*;

impl<D: WalStore> ShareWithdrawWatcher<D> {
    pub(super) async fn start_withdraw(&self, entry: &mut HeldRow, only_row: bool) -> Result<(), String> {
        let now = now_ts();
        // The step checks the wait before the reads. A waiting row then costs no canister call.
        if now < entry.holding.next_try_at {
            return Ok(());
        }
        let pool = entry.holding.pool;
        let liquidity = self.positions.available_liquidity(pool).await?;
        let position = self.positions.position(entry.holding.profile, pool).await?;
        let step = next_withdraw(&WithdrawInputs {
            remaining: entry.holding.remaining(),
            min_withdraw: Self::min_withdraw(entry),
            liquidity,
            deposited: position.native.clone(),
            only_row,
        });
        let amount = match step {
            WithdrawStep::Release => return self.release(entry).await,
            WithdrawStep::Wait(reason) => return self.wait(entry, reason, RETRY_SECS).await,
            WithdrawStep::ParkEmptyPosition => {
                let note = format!(
                    "the position is empty, but the row still expects {}",
                    Self::shown(entry, &entry.holding.remaining())
                );
                return self.park(entry, "withdraw", note).await;
            }
            WithdrawStep::Withdraw(amount) => amount,
        };

        let destination = self.destination(entry);
        let token = entry.wrapper.receipt.request.collateral_asset.clone();
        let after_index = self.positions.newest_index(token, destination).await?;
        // The step saves the intent before the call. The next tick then sees a crash or a lost reply.
        entry.holding.open = Some(OpenWithdraw::Submitting {
            amount: amount.clone(),
            shares_before: position.shares,
            after_index,
            at: now,
        });
        self.save(entry).await?;

        let failure = match self
            .positions
            .withdraw(entry.holding.profile, pool, amount, destination)
            .await
        {
            Ok(accepted) => {
                return self
                    .record_accepted(entry, accepted, after_index, now, destination)
                    .await;
            }
            Err(failure) => failure,
        };
        match after_failure(&failure) {
            AfterFailure::Retry { secs, note } => {
                entry.holding.open = None;
                warn!(liq_id = %entry.row.id, "[shares] {note}");
                self.wait(entry, &note, secs).await
            }
            AfterFailure::Park(note) => {
                entry.holding.open = None;
                self.park(entry, "withdraw", note).await
            }
            // The saved intent stays, and the next tick reads the share count to decide.
            AfterFailure::Unknown => {
                warn!(liq_id = %entry.row.id, "[shares] withdraw outcome unknown: {failure:?}");
                Ok(())
            }
        }
    }

    /// Saves a withdraw that the canister accepted, with its id. The next ticks look for its payout.
    async fn record_accepted(
        &self,
        entry: &mut HeldRow,
        accepted: WithdrawAccepted,
        after_index: u64,
        at: i64,
        destination: Principal,
    ) -> Result<(), String> {
        entry.holding.withdrawn += accepted.amount.clone();
        entry.holding.open = Some(OpenWithdraw::Accepted {
            outflow_id: accepted.outflow_id.clone(),
            amount: accepted.amount.clone(),
            after_index,
            at,
        });
        entry.holding.last_note = None;
        info!(
            event = "share_withdraw_accepted",
            liq_id = %entry.row.id,
            outflow_id = %accepted.outflow_id,
            amount = %Self::shown(entry, &accepted.amount),
            remaining = %Self::shown(entry, &entry.holding.remaining()),
            destination = %destination,
            "[shares] 📤 withdraw accepted"
        );
        self.save(entry).await
    }
}
