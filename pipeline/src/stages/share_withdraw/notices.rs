//! The Slack notice for a new held row, and the log of the held totals.
use super::*;

impl<D: WalStore> ShareWithdrawWatcher<D> {
    /// Sends one notice per held row and process run.
    pub(super) async fn announce(&self, entry: &HeldRow) {
        // A row with nothing left to withdraw goes to the swap in this tick.
        if entry.holding.remaining() == 0u8 {
            return;
        }
        if !self
            .announced
            .lock()
            .expect("announced rows")
            .insert(entry.row.id.clone())
        {
            return;
        }
        let token = entry.wrapper.receipt.request.collateral_asset.clone();
        let details = format!(
            "liquidation {} holds {} as supply shares in pool {}; the bot withdraws it when the pool has liquidity",
            entry.row.id,
            ChainTokenAmount::from_raw(token, entry.holding.remaining()).formatted(),
            entry.holding.pool
        );
        info!(event = "share_holding_held", liq_id = %entry.row.id, "[shares] 🧾 {details}");
        self.watchdog
            .notify(WatchdogEvent::Lifecycle {
                state: "collateral held as shares".to_string(),
                details,
            })
            .await;
    }

    /// Logs the held amount and value per asset. An operator must watch this log, because no cap limits the total.
    pub(super) fn log_totals(&self, held: &[HeldRow]) {
        let now = now_ts();
        {
            let mut logged_at = self.totals_logged_at.lock().expect("totals log time");
            if now - *logged_at < TOTALS_LOG_SECS {
                return;
            }
            *logged_at = now;
        }
        let mut by_asset: HashMap<String, (ChainTokenAmount, f64, usize)> = HashMap::new();
        for entry in held {
            let request = &entry.wrapper.receipt.request;
            let amount = ChainTokenAmount::from_raw(request.collateral_asset.clone(), entry.holding.remaining());
            // The price is from the time of the liquidation. Thus the value is an estimate.
            let usd = amount.to_f64() * reference_price_usd(&request.ref_price).unwrap_or(0.0);
            let total = by_asset.entry(request.collateral_asset.symbol()).or_insert_with(|| {
                (
                    ChainTokenAmount::from_raw(request.collateral_asset.clone(), Nat::from(0u8)),
                    0.0,
                    0,
                )
            });
            total.0.value += amount.value;
            total.1 += usd;
            total.2 += 1;
        }
        let total_usd: f64 = by_asset.values().map(|total| total.1).sum();
        for (symbol, (amount, usd, rows)) in by_asset {
            info!(
                event = "share_holding_total",
                asset = %symbol,
                rows,
                held = %amount.formatted(),
                value_usd = format!("{usd:.2}"),
                "[shares] held as supply shares"
            );
        }
        info!(
            event = "share_holding_total_usd",
            rows = held.len(),
            value_usd = format!("{total_usd:.2}"),
            "[shares] total value held as supply shares"
        );
    }
}
