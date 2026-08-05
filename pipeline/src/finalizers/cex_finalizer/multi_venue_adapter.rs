use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, CexSubmissionError, classify_cex_submission_error,
};
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

    /// Advances exactly one venue-local state-machine phase. This method never
    /// receives a WAL handle and therefore cannot change the parent row status.
    async fn advance_leg(&self, leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        self.advance_decoded_leg(self.decode_leg_state(leg)?).await
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
            return self.operator_required_progress(
                execution,
                format!(
                    "persisted {} intent `{intent_id}` has ambiguous submission state after restart",
                    self.profile.venue_id()
                ),
            );
        }
        execution.ready_to_advance = false;
        execution.intent_id = None;
        execution.cex.last_error = None;

        let result = self.advance_current_step(&mut execution.cex).await;
        let error = result.err();
        match &error {
            Some(error) => {
                execution.cex.last_error = Some(error.clone());
                if error.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX) {
                    execution.cex.step = CexStep::Failed;
                } else {
                    let classified = if self.profile.backend_submission_classifier_required() {
                        self.backend.classify_submission_error(error)
                    } else {
                        classify_cex_submission_error(error)
                    };
                    match classified {
                        CexSubmissionError::PendingSettlement(message) => {
                            return self.settlement_wait_progress(execution, message).await;
                        }
                        CexSubmissionError::Ambiguous(message) => {
                            return self.operator_required_progress(execution, message);
                        }
                        CexSubmissionError::Rejected(_) => {}
                    }
                }
            }
            // Any step that completes without an error proves the venue is
            // caught up, so a later wait starts from its own first refusal.
            None => execution.cex.trade.trade_settlement_waiting_since_ts = None,
        }
        self.progress_for(execution, error)
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
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_leg(leg).await
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
