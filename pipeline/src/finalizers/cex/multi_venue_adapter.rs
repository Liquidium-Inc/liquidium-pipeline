use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeRouteKind, resolve_route};
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, CexBackendError};
use log::{info, warn};
use serde::{Deserialize, Serialize};

use super::finalizer::CexVenueFinalizer;
use crate::{
    finalizers::{
        bridge_planner::BridgePlanner,
        cex::state::{CexState, CexStep},
        multi_venue::{
            MultiVenueAdapter, OperatorQuestion, VenueAmountError, VenueLegCheckpoint, VenueLegProgress,
            VenuePlanningContext, VenueRoutePreview,
        },
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueExecutionState, VenueLegFailure, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
    utils::now_ts,
};

/// How long a leg keeps re-offering an order that the exchange rejects as not yet
/// tradable before an operator has to look at it.
///
/// Deposit credit and matching-engine availability are minutes apart on MEXC,
/// while the finalizer's retry budget is spent in about two, so the wait has to
/// live here rather than in the retry counter. The deadline still bounds it:
/// an account that is genuinely short never resolves and must not loop forever.
const SETTLEMENT_WAIT_TIMEOUT_SECS: i64 = 15 * 60;

/// How long a leg waits for an exchange it cannot connect to before each
/// failed cycle spends the retry budget. A lasting outage then parks with an alert.
const UNREACHABLE_WAIT_SECS: i64 = 40 * 60;

/// The exchange leg's complete venue-local state plus a persistence gate. The gate makes
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
    /// When the exchange first became unreachable in the current outage.
    #[serde(default)]
    unreachable_since_ts: Option<i64>,
}

impl CexVenueExecutionState {
    /// Clears a park. The next cycle persists a new intent before the leg
    /// makes any venue call.
    fn release_park(&mut self) {
        self.operator_required = false;
        self.ready_to_advance = false;
        self.intent_id = None;
    }
}

impl<B> CexVenueFinalizer<B>
where
    B: CexBackend,
{
    /// Translates the amount-scoped CEX preview into the generic venue shape;
    /// route preparation stays in the CEX machine.
    async fn preview_leg_request(&self, request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        let execution_id = crate::utils::new_venue_execution_id(self.profile.venue_id());
        let preview = self.preview_swap_request(&execution_id, request).await?;
        let execution = CexVenueExecutionState {
            cex: preview.state,
            ready_to_advance: false,
            intent_id: None,
            operator_required: false,
            unreachable_since_ts: None,
        };

        Ok(VenueRoutePreview {
            market_receive: Some(preview.market_receive),
            side_costs: preview.side_costs,
            ..VenueRoutePreview::new(
                self.profile.venue_id(),
                request.clone(),
                preview.quote,
                preview.conservative_receive,
                VenueExecutionState::new(self.profile.venue_id(), &execution)?,
            )
        })
    }

    /// Decodes this leg only and verifies that its persisted allocation
    /// still matches the immutable request committed by the parent plan.
    fn decode_leg_state(&self, leg: &VenueLegState) -> Result<CexVenueExecutionState, String> {
        if leg.venue_id != self.profile.venue_id() {
            return Err(format!(
                "{} adapter cannot advance venue `{}`",
                self.profile.name(),
                leg.venue_id
            ));
        }
        let state = leg
            .execution
            .decode::<CexVenueExecutionState>(self.profile.venue_id())?
            .ok_or_else(|| format!("{} leg execution state decoded to the wrong venue", self.profile.name()))?;
        if state.cex.size_in != leg.request.pay_amount {
            return Err(format!(
                "{} persisted allocation does not match the leg request",
                self.profile.name()
            ));
        }
        if state.cex.withdraw.withdraw_asset.asset_id() != leg.request.receive_asset {
            return Err(format!(
                "{} persisted receive asset does not match the leg request",
                self.profile.name()
            ));
        }
        Ok(state)
    }

    /// Advances exactly one venue-local state-machine phase. This method never
    /// receives a WAL handle and therefore cannot change the parent row status.
    async fn advance_leg(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        self.advance_decoded_leg(self.decode_leg_state(leg)?, checkpoint).await
    }

    /// Whether a restarted leg may advance without its submission gate.
    ///
    /// Legacy plan backfilling can turn a direct step into a bridged one, so
    /// the plan is resolved here before the durable-id check runs.
    fn can_resume_by_polling(&self, state: &mut CexState) -> bool {
        self.ensure_plan_on_state(state);
        state.resume_after_restart(now_ts())
    }

    /// Advances an already validated state so recovery does not decode the
    /// same persisted JSON twice in one orchestrator cycle.
    async fn advance_decoded_leg(
        &self,
        mut execution: CexVenueExecutionState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
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
                .unwrap_or_else(|| format!("{} leg requires operator reconciliation", self.profile.name()));
            return self.operator_required_progress(execution, error);
        }

        if !execution.ready_to_advance {
            execution.ready_to_advance = true;
            let intent_id = crate::utils::new_venue_execution_id(&format!("{}-intent", self.profile.venue_id()));
            execution.intent_id = Some(intent_id.clone());
            self.multi_venue_armed_intents
                .lock()
                .map_err(|_| "multi-venue intent lock poisoned".to_string())?
                .insert(intent_id);
            return self.progress_for(execution, None);
        }

        let intent_id = execution
            .intent_id
            .clone()
            .ok_or_else(|| "persisted CEX execution gate has no intent ID".to_string())?;
        let armed_here = self
            .multi_venue_armed_intents
            .lock()
            .map_err(|_| "multi-venue intent lock poisoned".to_string())?
            .remove(&intent_id);
        if !armed_here && !self.can_resume_by_polling(&mut execution.cex) {
            return self.operator_required_progress(
                execution,
                format!(
                    "persisted {} intent `{intent_id}` has ambiguous submission state after restart",
                    self.profile.name()
                ),
            );
        }
        execution.ready_to_advance = false;
        execution.intent_id = None;
        execution.cex.last_error = None;

        let mut outcome = self.advance_current_step(&mut execution.cex).await;
        // A step that saved a send writes it to the WAL with the gate still
        // set, then sends it in the same cycle. A crash during the send then
        // restarts into the saved send, which parks unless a replay is safe.
        if outcome.is_ok() && std::mem::take(&mut execution.cex.send_saved) {
            let mut saved = execution.clone();
            saved.ready_to_advance = true;
            saved.intent_id = Some(intent_id);
            checkpoint.checkpoint(self.progress_for(saved, None)?).await?;
            outcome = self.advance_current_step(&mut execution.cex).await;
            execution.cex.send_saved = false;
        }

        if let Err(error) = &outcome {
            execution.cex.last_error = Some(error.to_string());
        }
        execution.unreachable_since_ts = match &outcome {
            Err(CexBackendError::Unreachable(_)) => execution.unreachable_since_ts.or(Some(now_ts())),
            _ => None,
        };
        let error = match outcome {
            Err(error @ CexBackendError::PendingSettlement(_)) => {
                return self.settlement_wait_progress(execution, error.to_string()).await;
            }
            // No later cycle can raise the amount, so the leg ends here with
            // its funds still in the trader account.
            Err(CexBackendError::BelowMinimum(message)) if !execution.cex.funds_left_trader_account() => {
                execution.cex.step = CexStep::Failed;
                let mut progress = self.progress_for(execution, Some(message))?;
                progress.failure = Some(VenueLegFailure::BelowMinimum);
                return Ok(progress);
            }
            // Moved funds must stay tracked. The error spends the retry budget,
            // so the row parks with an alert.
            Err(CexBackendError::BelowMinimum(message)) => Some(format!(
                "{message}; the leg's funds already left the trader account, so an operator must settle them"
            )),
            // The request never reached the venue, so a short outage spends no
            // retry budget. A long one does, so it cannot wait without an alert.
            Err(CexBackendError::Unreachable(message)) => {
                let waited = execution
                    .unreachable_since_ts
                    .map_or(0, |since| now_ts().saturating_sub(since));
                warn!(
                    "[cex] liq_id={} venue unreachable at step={:?} waited={}s: {}",
                    execution.cex.liq_id, execution.cex.step, waited, message
                );
                (waited >= UNREACHABLE_WAIT_SECS).then_some(message)
            }
            // Only an operator can say which row owns the provider record, or
            // whether a call without an answer moved funds.
            Err(CexBackendError::Ambiguous(message)) => {
                return self.operator_required_progress(execution, message);
            }
            Err(error) => Some(error.to_string()),
            // Any step that completes without an error proves the venue is
            // caught up, so a later wait starts from its own first refusal.
            Ok(()) => {
                execution.cex.trade.trade_settlement_waiting_since_ts = None;
                None
            }
        };
        self.progress_for(execution, error)
    }

    /// Holds a leg whose funds the venue reports as not tradable yet.
    ///
    /// Nothing failed here: the exchange accepted the deposit into the account balance
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
                "[cex] liq_id={} waiting for venue settlement: waited={}s timeout={}s err={}",
                execution.cex.liq_id, waited, SETTLEMENT_WAIT_TIMEOUT_SECS, error
            );
            return Ok(VenueLegProgress {
                execution: VenueExecutionState::new(self.profile.venue_id(), &execution)?,
                status: VenueLegStatus::Running,
                result: None,
                last_error: Some(error),
                retryable_error: None,
                failure: None,
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
            "[cex] liq_id={} venue settlement wait expired after {}s free_{}={} err={}",
            execution.cex.liq_id, waited, asset, observed, error
        );
        self.operator_required_progress(
            execution,
            format!(
                "{} leg not settled after {waited}s: {error} (free {asset}={observed})",
                self.profile.name()
            ),
        )
    }

    /// Maps the detailed CEX state onto the generic leg status and creates a
    /// result only after this exchange leg itself reaches `Completed`.
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
            failure: None,
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
            failure: None,
        })
    }
}

#[async_trait]
impl<B> MultiVenueAdapter for CexVenueFinalizer<B>
where
    B: CexBackend + 'static,
{
    fn venue_id(&self) -> &'static str {
        self.profile.venue_id()
    }

    fn validate_configuration(&self) -> Result<(), String> {
        if self.token_registry.is_none() {
            return Err("token registry is required for amount-scoped CEX previews".to_string());
        }
        Ok(())
    }

    async fn check_amount(&self, request: &SwapRequest) -> Result<(), VenueAmountError> {
        let state = self
            .prepare_swap_request("amount-check", request)
            .map_err(VenueAmountError::Unavailable)?;
        let Some(route) = resolve_route(
            &state.deposit.deposit_asset.symbol(),
            &state.deposit.deposit_asset.chain(),
            &<Self as BridgePlanner>::planned_deposit_asset(&state),
        ) else {
            return Ok(());
        };
        // Only these minters enforce a withdrawal minimum; ckERC20 does not.
        if !matches!(
            route.route_kind,
            BridgeRouteKind::CkEthToEth | BridgeRouteKind::IcpToSolana
        ) {
            return Ok(());
        }
        let bridge = self.bridge_runtime().map_err(VenueAmountError::Unavailable)?;
        let sizing = Self::size_bridged_deposit(bridge, route, &state.deposit.deposit_asset, &state.size_in)
            .await
            .map_err(|error| match error {
                CexBackendError::BelowMinimum(message) => VenueAmountError::BelowMinimum(message),
                other => VenueAmountError::Unavailable(other.to_string()),
            })?;
        match Self::bridge_minimum_shortfall(bridge, route, &sizing.bridge_amount)
            .await
            .map_err(VenueAmountError::Unavailable)?
        {
            Some((amount, minimum)) => Err(VenueAmountError::BelowMinimum(format!(
                "{} -> {} bridge amount ({amount}) after fee reserves is below minimum ({minimum})",
                route.source_asset, route.target_asset
            ))),
            None => Ok(()),
        }
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

    fn operator_questions(&self, leg: &VenueLegState) -> Result<Vec<OperatorQuestion>, String> {
        let execution = self.decode_leg_state(leg)?;
        Ok(Self::operator_questions_for(&execution.cex))
    }

    fn operator_release(
        &self,
        leg: &VenueLegState,
        answer: Option<&OperatorDecision>,
    ) -> Result<VenueExecutionState, String> {
        let mut execution = self.decode_leg_state(leg)?;
        Self::answer_operator(&mut execution.cex, answer)?;
        execution.release_park();
        VenueExecutionState::new(self.profile.venue_id(), &execution)
    }

    async fn recover(
        &self,
        leg: &VenueLegState,
        checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        let mut execution = self.decode_leg_state(leg)?;
        if execution.cex.step == CexStep::Completed {
            return Err(format!(
                "completed {} leg does not require recovery",
                self.profile.name()
            ));
        }
        // OperatorRequired rows are excluded from automatic WAL polling. An
        // explicit re-enqueue is therefore the operator's authorization to
        // discard the old in-memory submission token and retry the persisted
        // CEX step through a newly persisted intent. This call only arms that
        // intent; the external action still happens in a later WAL cycle.
        if execution.operator_required {
            execution.release_park();
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
                self.profile.name(),
                execution.cex.step
            ));
        }
        self.advance_decoded_leg(execution, checkpoint).await
    }
}

#[cfg(test)]
#[path = "multi_venue_adapter_tests.rs"]
mod tests;
