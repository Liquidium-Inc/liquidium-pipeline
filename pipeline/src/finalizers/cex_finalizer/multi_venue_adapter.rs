use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, CexSubmissionError, is_cex_venue_unreachable_error,
};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use log::{info, warn};
use serde::{Deserialize, Serialize};

use super::CexFinalizer;
use crate::{
    finalizers::{
        bridge_planner::BridgePlanner,
        cex_finalizer::{CexFinalizerLogic, CexState, CexStep},
        multi_venue::{
            MultiVenueAdapter, VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext, VenueRoutePreview,
        },
    },
    persistance::{VenueExecutionState, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
    utils::now_ts,
};

#[cfg(test)]
const VENUE_ID: &str = "mexc";

/// How long a leg keeps re-offering an order that a CEX rejects as not yet
/// tradable before an operator has to look at it.
///
/// Deposit credit and matching-engine availability can be minutes apart,
/// while the finalizer's retry budget is spent in about two, so the wait has to
/// live here rather than in the retry counter. The deadline still bounds it:
/// an account that is genuinely short never resolves and must not loop forever.
const SETTLEMENT_WAIT_TIMEOUT_SECS: i64 = 15 * 60;

/// Upper bound on phases chained inside one `advance`.
///
/// The chain covers the orchestration around a single submission and stops at
/// every pending phase, so it costs at most four: arm the gate, persist the
/// client order ID, arm again, submit. Eight leaves room for that sequence to
/// grow without tripping, while still bounding the case a phase reports
/// success without moving:
/// the finalize stage walks rows serially, so an unbounded loop here would
/// stall every other liquidation behind it rather than just this one.
const MAX_CHAINED_PHASES: usize = 8;

/// Names the step-level idempotency guard that makes a persisted step safe to
/// re-enter without the in-memory arming token, or `None` while the step still
/// owes a first submission whose outcome was never recorded.
fn durable_resume_anchor(state: &CexState) -> Option<&'static str> {
    match state.step {
        // Past the seizure transfer the step only reads balances; a bridged
        // deposit re-checks its source balance before submitting.
        CexStep::Deposit | CexStep::DepositPending => {
            if state.deposit.bridge.deposit_bridge_id.is_some() {
                Some("deposit_bridge_id")
            } else {
                state.deposit.deposit_txid.as_ref().map(|_| "deposit_txid")
            }
        }
        // A step without a client order id only prepares one and returns; with
        // one it re-offers under that same id.
        CexStep::Trade | CexStep::TradePending => Some(
            state
                .trade
                .trade_pending_client_order_id
                .as_ref()
                .map_or("trade_order_intent_unsubmitted", |_| "trade_pending_client_order_id"),
        ),
        CexStep::Withdraw | CexStep::WithdrawPending => {
            if state.withdraw.bridge.withdraw_bridge_id.is_some() {
                Some("withdraw_bridge_id")
            } else {
                state.withdraw.withdraw_id.as_ref().map(|_| "withdraw_id")
            }
        }
        CexStep::Completed | CexStep::Failed => None,
    }
}

/// Complete venue-local CEX state plus a persistence gate. The gate makes
/// every call that may submit a transfer, order, withdrawal, or bridge request
/// take two orchestrator cycles: first persist `ready_to_advance`, then execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CexVenueExecutionState {
    cex: CexState,
    #[serde(default)]
    ready_to_advance: bool,
    #[serde(default)]
    intent_id: Option<String>,
    #[serde(default)]
    operator_required: bool,
}

impl<B> CexFinalizer<B>
where
    B: CexBackend,
{
    /// Translates the amount-scoped CEX preview into the generic venue shape.
    async fn preview_leg_request(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        let execution_id = crate::utils::new_venue_execution_id(self.profile.venue_id());
        let preview = self.preview_swap_request(&execution_id, request).await?;
        let execution = CexVenueExecutionState {
            cex: preview.state,
            ready_to_advance: false,
            intent_id: None,
            operator_required: false,
        };

        Ok(VenueRoutePreview {
            venue_id: self.profile.venue_id().to_string(),
            request: request.clone(),
            quote: preview.quote,
            conservative_receive: preview.conservative_receive,
            initial_execution_state: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
        })
    }

    /// Decodes this leg only and verifies that its persisted CEX allocation
    /// still matches the immutable request committed by the parent plan.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<CexVenueExecutionState, String> {
        if leg.venue_id != self.profile.venue_id() {
            return Err(format!(
                "{} adapter cannot advance venue `{}`",
                self.profile.venue_id(),
                leg.venue_id
            ));
        }
        let state = leg
            .execution
            .decode::<CexVenueExecutionState>(self.profile.venue_id())?
            .ok_or_else(|| {
                format!(
                    "{} leg execution state decoded to the wrong venue",
                    self.profile.venue_id()
                )
            })?;
        if state.cex.size_in != leg.request.pay_amount {
            return Err(format!(
                "{} persisted allocation does not match the leg request",
                self.profile.venue_id()
            ));
        }
        if state.cex.withdraw.withdraw_asset.asset_id() != leg.request.receive_asset {
            return Err(format!(
                "{} persisted receive asset does not match the leg request",
                self.profile.venue_id()
            ));
        }
        Ok(state)
    }

    /// Drives this leg through every immediately runnable CEX phase, persisting
    /// each one through the narrow checkpoint before the next may submit
    /// anything, and stopping on a wait, an error, or a terminal state so the
    /// next daemon tick can resume safely.
    ///
    /// The orchestrator otherwise re-enters once per phase on its own poll
    /// interval, so a route that deposits, trades two hops and withdraws spent
    /// tens of seconds waiting to be asked again rather than waiting on the
    /// exchange. Only two phases wait on somebody else -- a deposit being
    /// credited and a withdrawal being processed -- and those still hand
    /// control back, because only the venue can say when they are done.
    ///
    /// This loops *around* [`Self::advance_decoded_leg`] rather than inside it
    /// on purpose. Every iteration runs the same arming, intent preparation and
    /// error classification the orchestrator drove one call at a time, so the
    /// crash-safety contract stays the existing one instead of becoming a
    /// second copy of it. `checkpoint` writes each result into the parent
    /// envelope before the next iteration begins, and that iteration re-decodes
    /// its state from what was written, so the loop only ever continues from
    /// state that is already durable.
    ///
    /// This method never receives a WAL handle and therefore cannot change the
    /// parent row status.
    async fn advance_leg(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut execution = self.decode_leg_state(leg)?;
        let mut phases = 0usize;

        loop {
            let progress = self.advance_decoded_leg(execution).await?;
            phases += 1;

            if !self.may_chain(&progress)? {
                return Ok(progress);
            }

            if phases >= MAX_CHAINED_PHASES {
                // Returning un-checkpointed is safe: the orchestrator persists
                // whatever `advance` hands back, exactly as it does for a leg
                // that stopped on its own.
                warn!(
                    "[{}] leg_id={} chained {phases} phases without reaching a waiting state; handing back to the orchestrator",
                    self.profile.venue_id(),
                    leg.leg_id
                );
                return Ok(progress);
            }

            checkpoint.checkpoint(progress.clone()).await?;
            execution = self.decode_progress_state(&progress)?;
        }
    }

    /// Whether the venue can take another phase immediately.
    ///
    /// Chaining is for the orchestration around a submission -- arming the
    /// gate, persisting a client order ID, arming again -- never for two
    /// submissions in a row. Each pending phase marks a boundary where
    /// something outside this process has to happen first:
    ///
    /// - `DepositPending` and `WithdrawPending` wait on the exchange, and only
    ///   it can say when a deposit is credited or a withdrawal has left.
    /// - `Withdraw` reached with the gate still closed means the trade just
    ///   finished. The withdrawal itself is never instant, so starting it a
    ///   poll interval later costs nothing measurable and keeps it away from
    ///   the moment the proceeds landed.
    /// - `TradePending` with no client order ID left means the leg still has
    ///   size to sell. Slices must not run back to back: each is measured
    ///   against a mid price re-read right after its predecessor, so chained
    ///   slices can each pass the per-slice cap while together walking the book
    ///   down by a multiple of it. Handing control back puts a poll interval
    ///   between slices and lets the book refill, which is what bounded the
    ///   aggregate before phases were chained at all.
    ///
    /// Beyond that the test is an error of any kind, not merely a retryable
    /// one. A venue outage and a settlement wait both report a non-retryable
    /// error while leaving the phase where it was, and both describe a leg
    /// waiting on somebody else; chaining either would spin against the venue
    /// for nothing.
    ///
    /// An armed gate is the one exception. Arming submits nothing and cannot
    /// repeat -- the next phase consumes the intent -- so a leg still carrying
    /// a previous cycle's error for diagnostics may chain through it. Without
    /// that exception any leg that had ever failed would be stuck at the old
    /// one-phase-per-cycle cadence forever, because nothing clears `last_error`
    /// until a phase actually runs.
    fn may_chain(&self, progress: &VenueLegProgress) -> Result<bool, String> {
        if !matches!(progress.status, VenueLegStatus::Running) {
            return Ok(false);
        }

        let execution = self.decode_progress_state(progress)?;
        let waits_on_someone_else = match execution.cex.step {
            CexStep::DepositPending | CexStep::WithdrawPending => true,
            // `TradePending` covers two different situations. While a client
            // order ID is still pending the intent is persisted and its
            // submission is the very next phase -- orchestration, which is
            // exactly what chaining is for. Once it is cleared the previous
            // slice has completed, and the next phase would open a *new* order
            // against a book that slice just moved.
            CexStep::TradePending => execution.cex.trade.trade_pending_client_order_id.is_none(),
            // Arriving at the withdrawal is a boundary; being armed for it is
            // not. The venue takes minutes to approve a withdrawal, so
            // submitting it in the same instant as the fill that funded it buys
            // nothing and risks refusal while the proceeds are still settling
            // -- which classifies as an ordinary rejection and spends the
            // retry budget rather than waiting.
            CexStep::Withdraw => !execution.ready_to_advance,
            _ => false,
        };
        if waits_on_someone_else {
            return Ok(false);
        }

        Ok(execution.ready_to_advance || progress.last_error.is_none())
    }

    /// Reads back the state a phase just produced, so the next phase starts
    /// from the same bytes the checkpoint persisted.
    fn decode_progress_state(&self, progress: &VenueLegProgress) -> Result<CexVenueExecutionState, String> {
        progress
            .execution
            .decode::<CexVenueExecutionState>(self.profile.venue_id())?
            .ok_or_else(|| format!("{} progress state decoded to the wrong venue", self.profile.venue_id()))
    }

    /// Advances an already validated state so recovery does not decode the
    /// same persisted JSON twice in one orchestrator cycle.
    async fn advance_decoded_leg(&self, mut execution: CexVenueExecutionState) -> Result<VenueLegProgress, String> {
        if execution.cex.step == CexStep::Completed {
            return self.progress_for(execution, None);
        }
        if execution.cex.step == CexStep::Failed {
            let last_error = execution.cex.last_error.clone();
            return self.progress_for(execution, last_error);
        }
        if execution.operator_required {
            let error = execution
                .cex
                .last_error
                .clone()
                .unwrap_or_else(|| format!("{} leg requires operator reconciliation", self.profile.venue_id()));
            return self.operator_required_progress(execution, error);
        }

        if !execution.ready_to_advance {
            execution.ready_to_advance = true;
            let intent_id = crate::utils::new_venue_execution_id(&format!("{}-intent", self.profile.venue_id()));
            execution.intent_id = Some(intent_id.clone());
            self.multi_venue_armed_intents
                .lock()
                .map_err(|_| format!("{} multi-venue intent lock poisoned", self.profile.venue_id()))?
                .insert(intent_id);
            return self.progress_for(execution, None);
        }

        let intent_id = execution
            .intent_id
            .clone()
            .ok_or_else(|| format!("persisted {} execution gate has no intent ID", self.profile.venue_id()))?;
        let armed_here = self
            .multi_venue_armed_intents
            .lock()
            .map_err(|_| format!("{} multi-venue intent lock poisoned", self.profile.venue_id()))?
            .remove(&intent_id);
        if !armed_here {
            // A restart loses the process-local token but not the step's own
            // idempotency guard; park only when there is no guard to defer to.
            match durable_resume_anchor(&execution.cex) {
                Some(anchor) => info!(
                    "[{}] liq_id={} resuming intent `{intent_id}` after restart: step={:?} guarded by {}",
                    self.profile.venue_id(),
                    execution.cex.liq_id,
                    execution.cex.step,
                    anchor
                ),
                None => {
                    return self.operator_required_progress(
                        execution,
                        format!(
                            "persisted {} intent `{intent_id}` has ambiguous submission state after restart",
                            self.profile.venue_id()
                        ),
                    );
                }
            }
        }
        execution.ready_to_advance = false;
        execution.intent_id = None;
        execution.cex.last_error = None;

        if matches!(execution.cex.step, CexStep::Trade | CexStep::TradePending)
            && execution.cex.trade.trade_pending_client_order_id.is_none()
        {
            match self.prepare_next_trade_order_intent(&mut execution.cex).await {
                Ok(_) => return self.progress_for(execution, None),
                Err(error) => {
                    execution.cex.last_error = Some(error.clone());
                    return self.progress_for(execution, Some(error));
                }
            }
        }

        let error = match self.advance_current_step(&mut execution.cex).await {
            Err(failure) => {
                let error = failure.to_string();
                execution.cex.last_error = Some(error.clone());
                if error.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX) {
                    execution.cex.step = CexStep::Failed;
                } else {
                    // A `Rejected` may only mean the step could not classify
                    // itself: everything outside the trade path converts through
                    // `From<String>` and lands here by default. Backends that can
                    // read their own failures get to re-decide that one case; a
                    // classification the backend already made is never
                    // second-guessed, so a typed pending or ambiguous result
                    // survives regardless of profile.
                    let classified = match &failure {
                        CexSubmissionError::Rejected(message)
                            if self.profile.backend_submission_classifier_required() =>
                        {
                            // The raw message, not the rendered error: a backend
                            // recognises its own failures by their sentinel
                            // prefix, which any decoration would hide.
                            self.backend.classify_submission_error(message)
                        }
                        // Unreachability is not venue-specific, so it is read
                        // back for every profile: the marker is only ever
                        // written by our own adapters, at the one place the
                        // transport error is still typed.
                        CexSubmissionError::Rejected(message) if is_cex_venue_unreachable_error(message) => {
                            CexSubmissionError::Unreachable(message.clone())
                        }
                        _ => failure.clone(),
                    };
                    match classified {
                        CexSubmissionError::PendingSettlement(message) => {
                            return self.settlement_wait_progress(execution, message).await;
                        }
                        CexSubmissionError::Unreachable(message) => {
                            return self.unreachable_venue_progress(execution, message);
                        }
                        CexSubmissionError::Ambiguous(message) => {
                            return self.operator_required_progress(execution, message);
                        }
                        CexSubmissionError::Rejected(_) => {}
                    }
                }
                Some(error)
            }
            // Any step that completes without an error proves the venue is
            // caught up, so a later wait starts from its own first refusal.
            Ok(()) => {
                execution.cex.trade.trade_settlement_waiting_since_ts = None;
                None
            }
        };
        self.progress_for(execution, error)
    }

    /// Holds a leg whose venue could not be reached at all.
    ///
    /// The request never arrived, so the leg stays `Running` with no retryable
    /// error and the next cycle tries again without spending the retry budget.
    /// No deadline: waiting out an outage is cheaper than parking a row that
    /// only a human could then unpark.
    fn unreachable_venue_progress(
        &self,
        mut execution: CexVenueExecutionState,
        error: String,
    ) -> Result<VenueLegProgress, String> {
        warn!(
            "[{}] liq_id={} venue unreachable at step={:?}; retrying without spending the retry budget: {}",
            self.profile.venue_id(),
            execution.cex.liq_id,
            execution.cex.step,
            error
        );
        execution.cex.last_error = Some(error.clone());
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
            status: VenueLegStatus::Running,
            result: None,
            last_error: Some(error),
            retryable_error: None,
        })
    }

    /// Holds a leg whose funds the venue reports as not tradable yet.
    ///
    /// Nothing failed here: the venue accepted the deposit into the account balance
    /// but its matching engine still refuses to trade it, and only the venue
    /// can say when that changes. The leg therefore keeps its `Running` status
    /// and reports no retryable error, so the orchestrator re-offers the same
    /// order under the same client id on later cycles without spending the
    /// finalization retry budget. The error stays in `last_error` for the TUI.
    async fn settlement_wait_progress(
        &self,
        mut execution: CexVenueExecutionState,
        error: String,
    ) -> Result<VenueLegProgress, String> {
        let now = now_ts();
        let waiting_since = *execution.cex.trade.trade_settlement_waiting_since_ts.get_or_insert(now);
        let waited = now.saturating_sub(waiting_since);

        if waited < SETTLEMENT_WAIT_TIMEOUT_SECS {
            info!(
                "[{}] liq_id={} waiting for venue settlement: waited={}s timeout={}s err={}",
                self.profile.venue_id(),
                execution.cex.liq_id,
                waited,
                SETTLEMENT_WAIT_TIMEOUT_SECS,
                error
            );
            return Ok(VenueLegProgress {
                execution: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
                status: VenueLegStatus::Running,
                result: None,
                last_error: Some(error),
                retryable_error: None,
            });
        }

        // The wait is over and the venue never released the funds. One balance
        // read costs nothing here and tells the operator which case they have:
        // funds present and stuck, or an account that never held them.
        let asset = <Self as BridgePlanner>::planned_deposit_asset(&execution.cex);
        let observed = match self.backend.get_balance(&asset).await {
            Ok(balance) => format!("{}", balance),
            Err(balance_error) => format!("unavailable ({})", balance_error),
        };
        warn!(
            "[{}] liq_id={} venue settlement wait expired after {}s free_{}={} err={}",
            self.profile.venue_id(),
            execution.cex.liq_id,
            waited,
            asset,
            observed,
            error
        );
        self.operator_required_progress(
            execution,
            format!(
                "{} leg not settled after {waited}s: {error} (free {asset}={observed})",
                self.profile.venue_id()
            ),
        )
    }

    /// Maps the detailed CEX state onto the generic leg status and creates a
    /// result only after this CEX leg itself reaches `Completed`.
    fn progress_for(
        &self,
        execution: CexVenueExecutionState,
        error: Option<String>,
    ) -> Result<VenueLegProgress, String> {
        let status = match execution.cex.step {
            CexStep::Completed => VenueLegStatus::Completed,
            CexStep::Failed => VenueLegStatus::FailedPermanent,
            _ => VenueLegStatus::Running,
        };
        let result = if execution.cex.step == CexStep::Completed {
            Some(self.finish_state(&execution.cex)?)
        } else {
            None
        };
        let last_error = error.clone().or_else(|| execution.cex.last_error.clone());
        // A permanently failed leg keeps its error for diagnostics but must never
        // advertise it as retryable. Both paths that reach here with an error --
        // the `CexStep::Failed` short-circuit and the amount-floor rejection --
        // describe a swap that cannot succeed on a later cycle, so the retry
        // signal has to be dropped at the source rather than relying on the
        // orchestrator to filter it back out.
        let retryable_error = match status {
            VenueLegStatus::FailedPermanent => None,
            _ => error,
        };
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
            status,
            result,
            last_error,
            retryable_error,
        })
    }

    fn operator_required_progress(
        &self,
        mut execution: CexVenueExecutionState,
        error: String,
    ) -> Result<VenueLegProgress, String> {
        execution.operator_required = true;
        execution.cex.last_error = Some(error.clone());
        Ok(VenueLegProgress {
            execution: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
            status: VenueLegStatus::OperatorRequired,
            result: None,
            last_error: Some(error),
            retryable_error: None,
        })
    }
}

#[async_trait]
impl<B> MultiVenueAdapter for CexFinalizer<B>
where
    B: CexBackend + 'static,
{
    fn venue_id(&self) -> &'static str {
        self.profile.venue_id()
    }

    fn validate_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err(format!(
                "token registry is required for amount-scoped {} previews",
                self.profile.venue_id()
            ));
        }
        Ok(())
    }

    async fn minimum_executable_amount(&self, token: &ChainToken) -> Result<Option<ChainTokenAmount>, String> {
        self.minimum_bridged_deposit_size_in(token).await
    }

    async fn preview(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        self.preview_leg_request(request).await
    }

    async fn advance(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_leg(leg, checkpoint).await
    }

    async fn recover(
        &self,
        leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut execution = self.decode_leg_state(leg)?;
        if execution.cex.step == CexStep::Completed {
            return Err(format!(
                "completed {} leg does not require recovery",
                self.profile.venue_id()
            ));
        }
        // OperatorRequired rows are excluded from automatic WAL polling. An
        // explicit re-enqueue is therefore the operator's authorization to
        // discard the old in-memory submission token and retry the persisted
        // CEX step through a newly persisted intent. This call only arms that
        // intent; the external action still happens in a later WAL cycle.
        if execution.operator_required {
            execution.operator_required = false;
            execution.ready_to_advance = false;
            execution.intent_id = None;
        }
        // The legacy CEX machine uses its Pending phases as durable restart
        // reconciliation points; unlike ICPSwap it has no separate Recover
        // variants. Recovery therefore accepts pending phases even without a
        // last error, but still passes through the persistence gate before the
        // state machine can poll or submit an idempotent follow-up action.
        if execution.cex.last_error.is_none()
            && !matches!(
                execution.cex.step,
                CexStep::DepositPending | CexStep::TradePending | CexStep::WithdrawPending | CexStep::Failed
            )
        {
            return Err(format!(
                "{} leg at {:?} is not in recovery",
                self.profile.venue_id(),
                execution.cex.step
            ));
        }
        self.advance_decoded_leg(execution).await
    }
}

#[cfg(test)]
type MexcFinalizer<B> = CexFinalizer<B>;

#[cfg(test)]
type MexcVenueExecutionState = CexVenueExecutionState;

#[cfg(test)]
#[path = "../mexc/mexc_multi_venue_adapter_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "kraken_route_tests.rs"]
mod kraken_route_tests;
