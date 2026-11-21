//! Withdraws collateral that a liquidation paid as supply shares. Then it gives the row to the swap.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::types::protocol_types::ProtocolError;
use tracing::{info, warn};

use crate::finalizers::multi_venue::reference_price_usd;
use crate::liquidation::share_position_adapter::{PayoutQuery, SharePositions, WithdrawAccepted, WithdrawFailure};
use crate::persistance::{LiqMetaWrapper, LiqResultRecord, OpenWithdraw, ResultStatus, ShareHolding, WalStore};
use crate::utils::now_ts;
use crate::wal::{decode_receipt_wrapper, encode_meta};
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};

#[path = "notices.rs"]
mod notices;
#[path = "payout_check.rs"]
mod payout_check;
#[path = "row_updates.rs"]
mod row_updates;
#[path = "share_withdraw_utils.rs"]
mod share_withdraw_utils;
#[path = "withdraw_call.rs"]
mod withdraw_call;
#[path = "withdraw_rules.rs"]
mod withdraw_rules;

/// The number of held rows that one tick reads. The next ticks take a larger backlog, oldest first.
const HELD_ROWS_PER_TICK: usize = 500;
/// The number of operator rows that one tick reads, to find parked share rows.
const PARKED_ROWS_PER_TICK: usize = 2_000;
/// One tick starts no new row after this time. It is half of the worker's limit for the step.
const TICK_BUDGET: Duration = Duration::from_secs(60);
/// The step sends no withdraw below this many ledger fees. The pool cannot pay an amount that is not more than its fee.
const MIN_WITHDRAW_FEES: u32 = 10;
/// The pool takes at most this many ledger fees out of a payout (two transfers, and one spare).
const MAX_PAYOUT_FEES: u32 = 3;
/// The wait before the next try, for example after the pool had no liquidity.
const RETRY_SECS: i64 = 60;
/// The wait after the canister refused a withdraw for a reason that can pass, for example a frozen pool.
const REFUSED_RETRY_SECS: i64 = 300;
/// A payout that is not on the ledger after this time goes to an operator.
const PAYOUT_PARK_SECS: i64 = 3_600;
/// The last row takes the whole position only when the position is at most this much above the row amount.
/// A larger surplus is not interest, because it belongs to other rows, for example parked rows.
const MAX_INTEREST_BPS: u32 = 500;
/// The pause between two logs of the held totals.
const TOTALS_LOG_SECS: i64 = 60;

pub struct ShareWithdrawWatcher<D: WalStore> {
    wal: Arc<D>,
    positions: Arc<dyn SharePositions>,
    /// Receives the tokens of a row that swaps.
    trader: Principal,
    /// Receives the tokens of a row without a swap.
    liquidator: Principal,
    watchdog: Arc<dyn Watchdog>,
    announced: Mutex<HashSet<String>>,
    totals_logged_at: Mutex<i64>,
}

/// One held row with its decoded state.
struct HeldRow {
    row: LiqResultRecord,
    wrapper: LiqMetaWrapper,
    holding: ShareHolding,
}

impl<D: WalStore> ShareWithdrawWatcher<D> {
    pub fn new(wal: Arc<D>, positions: Arc<dyn SharePositions>, trader: Principal, liquidator: Principal) -> Self {
        Self {
            wal,
            positions,
            trader,
            liquidator,
            watchdog: noop_watchdog(),
            announced: Mutex::new(HashSet::new()),
            totals_logged_at: Mutex::new(0),
        }
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// Moves every held row one step: checks an open payout, or starts a withdraw when the pool has liquidity.
    pub async fn tick(&self) -> Result<(), String> {
        let started = Instant::now();
        let rows = self
            .wal
            .list_by_status(ResultStatus::HeldAsShares, HELD_ROWS_PER_TICK)
            .await
            .map_err(|error| error.to_string())?;
        if rows.is_empty() {
            return Ok(());
        }

        let mut held = Vec::with_capacity(rows.len());
        for row in rows {
            match decode_receipt_wrapper(&row) {
                Ok(Some(wrapper)) if wrapper.share_holding.is_some() => {
                    let holding = wrapper.share_holding.clone().expect("checked above");
                    held.push(HeldRow { row, wrapper, holding });
                }
                Ok(_) => self.park_unreadable(&row.id, "held row has no share state").await,
                Err(error) => self.park_unreadable(&row.id, &error).await,
            }
        }

        let mut rows_by_pool: HashMap<Principal, usize> = HashMap::new();
        // A full page can hide more rows of a pool. Then no row counts as the last one.
        let page_is_full = held.len() >= HELD_ROWS_PER_TICK;
        for entry in held.iter().filter(|entry| entry.holding.remaining() != 0u8) {
            *rows_by_pool.entry(entry.holding.pool).or_insert(0) += 1;
        }
        self.log_totals(&held);

        // Each pool has at most one open withdraw, because a two-step payout has no id to tell two payouts apart.
        // A parked row with an open withdraw counts too, because its payout can still arrive.
        let mut busy_pools = self.parked_pools_with_open_withdraw().await?;
        busy_pools.extend(
            held.iter()
                .filter(|entry| entry.holding.open.is_some())
                .map(|entry| entry.holding.pool),
        );

        for mut entry in held {
            // The worker stops this step at its time limit. Thus the step stops between rows, and not during a withdraw call.
            if started.elapsed() > TICK_BUDGET {
                break;
            }
            self.announce(&entry).await;
            let pool = entry.holding.pool;
            let only_row = !page_is_full && rows_by_pool.get(&pool).copied().unwrap_or(0) <= 1;
            let result = match entry.holding.open.clone() {
                Some(OpenWithdraw::Accepted {
                    outflow_id,
                    amount,
                    after_index,
                    at,
                }) => self.check_payout(&mut entry, outflow_id, amount, after_index, at).await,
                Some(OpenWithdraw::Submitting {
                    amount, shares_before, ..
                }) => self.resolve_unknown_withdraw(&mut entry, amount, shares_before).await,
                None if busy_pools.contains(&pool) => Ok(()),
                None => {
                    busy_pools.insert(pool);
                    self.start_withdraw(&mut entry, only_row).await
                }
            };
            if let Err(error) = result {
                warn!(liq_id = %entry.row.id, "[shares] step failed: {error}");
            }
        }
        Ok(())
    }

    /// Returns the pools in which a row that waits for an operator still has an open withdraw.
    async fn parked_pools_with_open_withdraw(&self) -> Result<HashSet<Principal>, String> {
        let parked = self
            .wal
            .list_by_status(ResultStatus::OperatorRequired, PARKED_ROWS_PER_TICK)
            .await
            .map_err(|error| error.to_string())?;
        if parked.len() >= PARKED_ROWS_PER_TICK {
            warn!("[shares] more than {PARKED_ROWS_PER_TICK} operator rows; a parked share row may be missed");
        }
        Ok(parked
            .iter()
            .filter_map(|row| decode_receipt_wrapper(row).ok().flatten())
            .filter_map(|wrapper| wrapper.share_holding)
            .filter(|holding| holding.open.is_some())
            .map(|holding| holding.pool)
            .collect())
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
