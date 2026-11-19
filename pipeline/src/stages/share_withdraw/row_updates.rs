//! Writes the state of a held row: release to the swap, wait, save, and park for an operator.
use super::*;

impl<D: WalStore> ShareWithdrawWatcher<D> {
    /// Hands the row to the finalizer with the amount that arrived as its collateral.
    pub(super) async fn release(&self, entry: &mut HeldRow) -> Result<(), String> {
        if entry.holding.arrived == 0u8 {
            let note = format!(
                "no tokens were withdrawn: the held amount {} is below the least withdraw",
                Self::shown(entry, &entry.holding.seized)
            );
            return self.park(entry, "withdraw", note).await;
        }
        let now = now_ts();
        entry.holding.released_at = Some(now);
        entry.holding.open = None;
        let token = entry.wrapper.receipt.request.collateral_asset.clone();
        if let Some(liquidation) = entry.wrapper.receipt.liquidation_result.as_mut() {
            // The swap spends the amount that arrived, not the seized amount.
            liquidation.amounts.collateral_received = entry.holding.arrived.clone();
        }
        entry.wrapper.share_holding = Some(entry.holding.clone());
        entry.row.status = ResultStatus::Enqueued;
        entry.row.last_error = None;
        entry.row.updated_at = now;
        encode_meta(&mut entry.row, &entry.wrapper)?;
        self.wal
            .upsert_result(entry.row.clone())
            .await
            .map_err(|error| error.to_string())?;
        info!(
            event = "share_holding_released",
            liq_id = %entry.row.id,
            seized = %ChainTokenAmount::from_raw(token.clone(), entry.holding.seized.clone()).formatted(),
            arrived = %ChainTokenAmount::from_raw(token.clone(), entry.holding.arrived.clone()).formatted(),
            interest = %ChainTokenAmount::from_raw(token, entry.holding.interest()).formatted(),
            held_secs = entry.holding.held_secs(now),
            "[shares] ✅ tokens withdrawn; row enqueued for finalization"
        );
        Ok(())
    }

    /// Saves the reason and the time of the next try.
    pub(super) async fn wait(&self, entry: &mut HeldRow, note: &str, secs: i64) -> Result<(), String> {
        entry.holding.next_try_at = now_ts() + secs;
        entry.holding.last_note = Some(note.to_string());
        self.save(entry).await
    }

    pub(super) async fn save(&self, entry: &mut HeldRow) -> Result<(), String> {
        entry.wrapper.share_holding = Some(entry.holding.clone());
        entry.row.updated_at = now_ts();
        encode_meta(&mut entry.row, &entry.wrapper)?;
        self.wal
            .upsert_result(entry.row.clone())
            .await
            .map_err(|error| format!("failed to save share state for {}: {error}", entry.row.id))
    }

    /// Stops automatic work on the row and tells an operator why.
    pub(super) async fn park(&self, entry: &mut HeldRow, step: &str, note: String) -> Result<(), String> {
        entry.holding.last_note = Some(note.clone());
        entry.wrapper.share_holding = Some(entry.holding.clone());
        entry.row.status = ResultStatus::OperatorRequired;
        entry.row.last_error = Some(note.clone());
        entry.row.updated_at = now_ts();
        encode_meta(&mut entry.row, &entry.wrapper)?;
        self.wal
            .upsert_result(entry.row.clone())
            .await
            .map_err(|error| error.to_string())?;
        warn!(liq_id = %entry.row.id, "[shares] 🛑 operator required: {note}");
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: entry.row.id.clone(),
                venue: "lending shares".to_string(),
                pending_step: step.to_string(),
                owner: entry.holding.profile.to_string(),
                details: note,
            })
            .await;
        Ok(())
    }

    pub(super) async fn park_unreadable(&self, row_id: &str, reason: &str) {
        let note = format!("held row cannot be read: {reason}");
        if let Err(error) = self
            .wal
            .update_failure(row_id, ResultStatus::OperatorRequired, note, false)
            .await
        {
            warn!(liq_id = %row_id, "[shares] failed to park an unreadable row: {error}");
        }
    }
}
