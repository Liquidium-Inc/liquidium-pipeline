//! Confirms that a deposit reached the exchange account, by balance delta or,
//! after a wait, by the total free balance.
use super::*;
use crate::finalizers::bridge_planner::BridgePlanner;

/// Deposit is considered confirmed once balance delta reaches expected amount minus this epsilon.
const DEPOSIT_CONFIRMATION_DELTA_EPSILON: f64 = 0.00001;

/// After this wait window, allow a fallback confirmation from total free balance.
const DEPOSIT_TOTAL_FREE_FALLBACK_SECS: i64 = 25;

/// A Bitcoin deposit needs several confirmations, so the credit wait allows
/// hours. After it, each waiting cycle errors, so the row parks for an operator.
const DEPOSIT_CREDIT_TIMEOUT_SECS: i64 = 2 * 60 * 60;

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// Fallback confirmation path when delta-based confirmation misses due balance churn.
    ///
    /// This path is intentionally delayed to reduce false positives from pre-existing
    /// inventory: only after a short window do we allow `current_free >= expected`.
    fn maybe_confirm_deposit_via_total_free_fallback(
        &self,
        state: &mut CexState,
        current_balance: f64,
        expected_floor: f64,
        expected_deposit_amount: f64,
    ) -> bool {
        if state.deposit.deposit_txid.is_none() {
            return false;
        }

        let now = now_ts();
        let sent_at = match state.deposit.deposit_sent_at_ts {
            Some(ts) => ts,
            None => {
                state.deposit.deposit_sent_at_ts = Some(now);
                info!(
                    "[cex] liq_id={} deposit fallback timer initialized at={} (legacy state without send timestamp)",
                    state.liq_id, now
                );
                return false;
            }
        };

        let elapsed = now.saturating_sub(sent_at);
        if elapsed >= DEPOSIT_TOTAL_FREE_FALLBACK_SECS && current_balance >= expected_floor {
            if let Some(expected_amount) = state.deposit.bridge.deposit_bridge_expected_amount {
                state.trade.trade_next_amount_in = Some(expected_amount);
            }
            info!(
                "[cex] liq_id={} deposit confirmed via total-free fallback: current={} expected={} elapsed={}s",
                state.liq_id, current_balance, expected_deposit_amount, elapsed
            );
            state.step = CexStep::Trade;
            return true;
        }

        false
    }

    /// Phase-B deposit confirmation by balance delta against the captured baseline.
    pub(super) async fn check_deposit(&self, state: &mut CexState) -> Result<(), String> {
        let symbol = <Self as BridgePlanner>::planned_deposit_asset(state);
        let current_balance = self.backend.get_balance(&symbol).await?;

        match state.deposit.deposit_balance_before {
            Some(baseline_balance) => {
                let expected_deposit_amount = state
                    .deposit
                    .bridge
                    .deposit_bridge_expected_amount
                    .or(state.trade.trade_next_amount_in)
                    .unwrap_or_else(|| state.size_in.to_f64());
                let expected_floor = expected_deposit_amount - DEPOSIT_CONFIRMATION_DELTA_EPSILON;

                // Primary path: prove this deposit by balance delta from captured baseline.
                let observed_balance_delta = current_balance - baseline_balance;
                if observed_balance_delta >= expected_floor {
                    if state.deposit.bridge.deposit_bridge_required {
                        // Cap at what this deposit can actually credit: the
                        // post-fee expected amount. The bridge submit amount is
                        // the pre-fee gross and is unreachable, because the
                        // minter takes its withdrawal fee out of the transfer in
                        // transit.
                        //
                        // The delta only reaches this ceiling when it is
                        // contaminated — a concurrent deposit landing on the
                        // same account inflates it beyond what this leg sent.
                        // Capping on the gross would then size the trade above
                        // what arrived and the venue rejects it as oversold.
                        state.trade.trade_next_amount_in =
                            Some(observed_balance_delta.max(0.0).min(expected_deposit_amount));
                    }
                    info!(
                        "[cex] liq_id={} deposit confirmed: before={} after={} expected={}",
                        state.liq_id, baseline_balance, current_balance, expected_deposit_amount
                    );
                    state.step = CexStep::Trade;
                    return Ok(());
                }

                // Fallback path: after a short wait, allow progression if total free
                // balance can fund this leg even when delta evidence is noisy.
                if self.maybe_confirm_deposit_via_total_free_fallback(
                    state,
                    current_balance,
                    expected_floor,
                    expected_deposit_amount,
                ) {
                    return Ok(());
                }

                let waited = waited_secs(&mut state.deposit.deposit_credit_since_ts);
                let line = format!(
                    "deposit of {symbol} for liq_id {} is not credited after {waited}s: before={} current={} delta={} expected={}",
                    state.liq_id, baseline_balance, current_balance, observed_balance_delta, expected_deposit_amount
                );
                if waited > DEPOSIT_CREDIT_TIMEOUT_SECS {
                    return Err(line);
                }
                debug!("[cex] {line}");
            }
            None => {
                // No baseline recorded yet (should normally be set when we send the transfer),
                // so record the current balance as the baseline and stay in Deposit.
                info!(
                    "[cex] liq_id={} deposit baseline set: current={}",
                    state.liq_id, current_balance
                );
                state.deposit.deposit_balance_before = Some(current_balance);
                if state.deposit.deposit_txid.is_some() && state.deposit.deposit_sent_at_ts.is_none() {
                    state.deposit.deposit_sent_at_ts = Some(now_ts());
                }
            }
        }

        Ok(())
    }
}
