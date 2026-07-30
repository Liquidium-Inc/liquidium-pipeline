use std::sync::Arc;

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult},
    finalizers::multi_venue::{
        IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerConfig, IcpswapFirstPlannerError,
        MultiVenueAdapter, VenueLegProgress,
    },
    persistance::{
        FINALIZER_META_V2_VERSION, FinalizerDecisionSnapshot, FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper,
        MultiVenueAllocationSnapshot, MultiVenueExecutionOutcome, MultiVenueExecutionState, VenueAllocationSnapshot,
        VenueLegState, VenueLegStatus, WalStore,
    },
    price_oracle::price_oracle::PriceOracle,
    stages::executor::ExecutionReceipt,
    utils::now_ts,
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
    watchdog::{Watchdog, WatchdogEvent, noop_watchdog},
};
use async_trait::async_trait;
use candid::Nat;
use tracing::{debug, info};

use super::parent_leg_checkpoint::ParentLegCheckpoint;
use crate::finalizers::multi_venue::planning::venue_registry::VenueRegistry;

pub(super) const MULTI_VENUE_PERMANENT_PREFIX: &str = "permanent multi-venue finalizer: ";
const MULTI_VENUE_CUSTODY_PREFIX: &str = "multi-venue venue custody: ";

/// Mutable parent state shared by one multi-venue finalization invocation.
///
/// Keeping the WAL handle, encoded wrapper, row, and decoded execution state
/// together prevents orchestration helpers from accepting mismatched pieces
/// from different liquidations.
struct MultiVenueFinalizationContext<'a> {
    wal: &'a dyn WalStore,
    row: crate::persistance::LiqResultRecord,
    wrapper: LiqMetaWrapper,
    state: MultiVenueExecutionState,
}

impl<'a> MultiVenueFinalizationContext<'a> {
    fn new(
        wal: &'a dyn WalStore,
        row: crate::persistance::LiqResultRecord,
        wrapper: LiqMetaWrapper,
        state: MultiVenueExecutionState,
    ) -> Self {
        Self {
            wal,
            row,
            wrapper,
            state,
        }
    }
}

/// WAL orchestrator for a committed generic multi-venue execution plan.
///
/// It owns parent persistence, but delegates every venue side effect to the
/// amount-scoped adapter for exactly one persisted leg.
pub struct MultiVenueFinalizer {
    planner: IcpswapFirstPlanner,
    venues: Arc<VenueRegistry>,
    watchdog: Arc<dyn Watchdog>,
}

impl MultiVenueFinalizer {
    pub fn new(
        adapters: Vec<Arc<dyn MultiVenueAdapter>>,
        planner_config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, String> {
        let venues = Arc::new(VenueRegistry::new(adapters)?);
        let planner =
            IcpswapFirstPlanner::from_registry(venues.clone(), planner_config).map_err(|error| error.to_string())?;
        Ok(Self {
            planner,
            venues,
            watchdog: noop_watchdog(),
        })
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// Lets the planner bound venue quotes against a live oracle read instead of
    /// the prices recorded when the liquidation was detected.
    pub fn with_price_oracle(mut self, price_oracle: Arc<dyn PriceOracle>) -> Self {
        self.planner = self.planner.with_price_oracle(price_oracle);
        self
    }

    async fn notify_operator_required_leg(&self, leg: &VenueLegState) {
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: leg.leg_id.clone(),
                venue: leg.venue_id.clone(),
                pending_step: "venue_reconciliation".to_string(),
                owner: leg.venue_id.clone(),
                details: leg
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "venue leg requires operator reconciliation".to_string()),
            })
            .await;
    }

    /// Loads a committed plan or creates and atomically persists a new plan
    /// with every initialized leg before any adapter is allowed to advance.
    async fn load_or_commit_plan(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
    ) -> Result<
        (
            crate::persistance::LiqResultRecord,
            LiqMetaWrapper,
            MultiVenueExecutionState,
        ),
        String,
    > {
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let mut row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for multi-venue liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt wrapper for multi-venue liquidation {liquidation_id}"))?;

        if wrapper.meta_v2.is_some() {
            let state = {
                let meta = wrapper.meta_v2.as_ref().expect("meta_v2 checked above");
                meta.validate()
                    .map_err(|error| format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"))?;
                let FinalizerMetaPayload::MultiVenueSwap(state) = &meta.payload;
                state.clone()
            };
            return Ok((row, wrapper, state));
        }

        if wrapper.venue_execution.is_some()
            || !wrapper.meta.is_empty()
            || wrapper
                .finalizer_decision
                .as_ref()
                .is_some_and(|decision| matches!(decision.chosen.as_str(), "dex" | "cex" | "multi_venue"))
        {
            return Err(format!(
                "refusing to upgrade committed legacy execution for liquidation {liquidation_id} to meta_v2"
            ));
        }

        // Planning starts here. Build the planner input from the confirmed
        // liquidation result, including the collateral amount actually received.
        let input = IcpswapFirstPlanInput::from_receipt(receipt).map_err(|error| error.to_string())?;

        // This is the pure routing decision: quote the registered venues and
        // produce the immutable allocation plan plus initialized venue legs.
        // No swap, transfer, or order side effect is submitted by `plan`.
        let state = self.planner.plan(&input, now_ts()).await.map_err(|error| match error {
            IcpswapFirstPlannerError::InvalidInput(_) => {
                format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}")
            }
            IcpswapFirstPlannerError::NoViableRoute(_) => error.to_string(),
        })?;

        // Commit the entire decision and every initialized leg atomically.
        // Venue execution may begin only after this WAL write succeeds, so a
        // restart always resumes the same allocations instead of replanning.
        wrapper.finalizer_decision = Some(decision_snapshot(&state));
        set_meta_v2(&mut wrapper, &state)?;
        row.updated_at = now_ts();
        encode_meta(&mut row, &wrapper)?;
        wal.upsert_result(row.clone())
            .await
            .map_err(|error| format!("failed to commit multi-venue plan for {liquidation_id}: {error}"))?;

        Ok((row, wrapper, state))
    }

    /// Persists the complete envelope after one leg transition. Only the
    /// supplied leg's execution fields are changed; allocations remain frozen.
    async fn persist_state(
        &self,
        wal: &dyn WalStore,
        row: &mut crate::persistance::LiqResultRecord,
        wrapper: &mut LiqMetaWrapper,
        state: &MultiVenueExecutionState,
    ) -> Result<(), String> {
        set_meta_v2(wrapper, state)?;
        row.updated_at = now_ts();
        encode_meta(row, wrapper)?;
        wal.upsert_result(row.clone())
            .await
            .map_err(|error| format!("failed to persist multi-venue state for {}: {error}", row.id))
    }

    /// Advances each runnable leg once in committed vector order and journals
    /// every transition before another venue is touched.
    async fn advance_legs(&self, context: &mut MultiVenueFinalizationContext<'_>) -> Result<(), String> {
        let wal = context.wal;
        let row = &mut context.row;
        let wrapper = &mut context.wrapper;
        let state = &mut context.state;
        let mut retryable_errors = Vec::new();
        for index in 0..state.legs.len() {
            let current = state.legs[index].clone();
            let runnable = matches!(current.status, VenueLegStatus::Planned | VenueLegStatus::Running);
            if !runnable {
                continue;
            }

            let adapter = self.venues.adapter(&current.venue_id).ok_or_else(|| {
                format!(
                    "{MULTI_VENUE_PERMANENT_PREFIX}no adapter registered for committed venue `{}`",
                    current.venue_id
                )
            })?;
            let checkpoint = ParentLegCheckpoint::new(wal, index, row, wrapper, state);
            let execution_result = adapter.advance(&current, &checkpoint).await;
            // Venue-local checkpoints may have committed several transitions
            // before the adapter returns, including when its final call fails.
            // Synchronize those durable snapshots before handling the result.
            let (checkpointed_row, checkpointed_wrapper, checkpointed_state) = checkpoint.snapshot().await;
            *row = checkpointed_row;
            *wrapper = checkpointed_wrapper;
            *state = checkpointed_state;

            let progress = execution_result?;
            let retryable_error = progress.retryable_error.clone();
            let current = state.legs[index].clone();

            apply_progress(&mut state.legs[index], progress)
                .map_err(|error| format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"))?;
            let advanced = &state.legs[index];
            if current.status != advanced.status || current.last_error != advanced.last_error {
                info!(
                    event = "multi_venue_leg_transition",
                    liquidation_id = %row.id,
                    strategy_id = %state.plan.strategy_id,
                    leg_id = %advanced.leg_id,
                    venue_id = %advanced.venue_id,
                    pay_amount = %advanced.request.pay_amount.value,
                    previous_status = ?current.status,
                    status = ?advanced.status,
                    last_error = ?advanced.last_error,
                    "Multi-venue leg advanced"
                );
            } else {
                debug!(
                    event = "multi_venue_leg_transition",
                    liquidation_id = %row.id,
                    strategy_id = %state.plan.strategy_id,
                    leg_id = %advanced.leg_id,
                    venue_id = %advanced.venue_id,
                    status = ?advanced.status,
                    "Multi-venue leg made internal progress"
                );
            }
            let previous_outcome = state.outcome.clone();
            state.outcome = derive_outcome(&state.legs);
            // The adapter's own checkpoints already journaled everything it
            // committed, so only rewrite the parent envelope when this
            // transition actually changed it. A waiting leg would otherwise
            // cost one fsync'd write per daemon tick.
            if current != state.legs[index] || previous_outcome != state.outcome {
                self.persist_state(wal, row, wrapper, state).await?;
            }

            // Alert only when entering the parked state. A direct retry of the
            // same committed row must not emit a duplicate notification.
            if current.status != VenueLegStatus::OperatorRequired
                && state.legs[index].status == VenueLegStatus::OperatorRequired
            {
                self.notify_operator_required_leg(&state.legs[index]).await;
            }

            if state.legs[index].status != VenueLegStatus::FailedPermanent
                && let Some(error) = retryable_error
            {
                retryable_errors.push(format!(
                    "{} leg `{}`: {error}",
                    state.legs[index].venue_id, state.legs[index].leg_id
                ));
            }
        }

        state.outcome = derive_outcome(&state.legs);
        if retryable_errors.is_empty() {
            Ok(())
        } else {
            Err(retryable_errors.join("; "))
        }
    }

    fn result_for_state(&self, state: &MultiVenueExecutionState) -> Result<FinalizerResult, String> {
        match &state.outcome {
            MultiVenueExecutionOutcome::Running => Ok(FinalizerResult {
                swap_result: None,
                finalized: false,
                operator_required: false,
                swapper: Some("multi_venue".to_string()),
                reason: None,
            }),
            MultiVenueExecutionOutcome::OperatorRequired { leg_ids } => Ok(FinalizerResult {
                swap_result: None,
                finalized: false,
                operator_required: true,
                swapper: Some("multi_venue".to_string()),
                reason: Some(format!("operator required for venue legs: {}", leg_ids.join(","))),
            }),
            MultiVenueExecutionOutcome::PartialRecovered { failed_leg_ids } => {
                let failed_legs = failed_leg_ids
                    .iter()
                    .map(|leg_id| {
                        state
                            .legs
                            .iter()
                            .find(|leg| &leg.leg_id == leg_id)
                            .and_then(|leg| leg.last_error.as_deref())
                            .map(|error| format!("{leg_id}: {error}"))
                            .unwrap_or_else(|| leg_id.clone())
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                let context = format!("venue legs failed permanently: {failed_legs}");
                // Whatever the surviving legs produced is real money already in
                // the wallet. Reporting only the permanent failure would hand the
                // pipeline `swap_result: None`, dropping those proceeds out of
                // profit accounting and the analytics export for good. Fail hard
                // only when there is nothing to account for.
                match aggregate_completed_legs(state)? {
                    Some(swap_result) => Ok(FinalizerResult {
                        swap_result: Some(swap_result),
                        finalized: true,
                        operator_required: false,
                        swapper: Some("multi_venue".to_string()),
                        reason: Some(context),
                    }),
                    None => Err(format!("{MULTI_VENUE_PERMANENT_PREFIX}{context}")),
                }
            }
            MultiVenueExecutionOutcome::Completed | MultiVenueExecutionOutcome::Recovered => Ok(FinalizerResult {
                swap_result: aggregate_completed_legs(state)?,
                finalized: true,
                operator_required: false,
                swapper: Some("multi_venue".to_string()),
                reason: None,
            }),
        }
    }
}

impl MultiVenueFinalizer {
    async fn finalize_multi_venue(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
    ) -> Result<FinalizerResult, String> {
        let (row, wrapper, state) = self.load_or_commit_plan(wal, &receipt).await?;
        let mut context = MultiVenueFinalizationContext::new(wal, row, wrapper, state);
        if let Err(error) = self.advance_legs(&mut context).await {
            return Err(tag_custody_error(&context.state, error));
        }
        self.result_for_state(&context.state)
    }
}

/// Collects every completed leg's execution result and folds them into the one
/// swap shape the pipeline records. A completed leg with no result is a broken
/// invariant, not an empty aggregate.
fn aggregate_completed_legs(
    state: &MultiVenueExecutionState,
) -> Result<Option<crate::swappers::model::SwapExecution>, String> {
    let completed_results = state
        .legs
        .iter()
        .filter(|leg| leg.status == VenueLegStatus::Completed)
        .map(|leg| {
            leg.result.clone().ok_or_else(|| {
                format!(
                    "{MULTI_VENUE_PERMANENT_PREFIX}completed venue leg `{}` has no execution result",
                    leg.leg_id
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    aggregate_swap_executions(&state.legs, completed_results)
}

/// Projects completed venue legs into the pipeline's existing single execution
/// shape while preserving the committed venue-leg order for audit/export.
fn aggregate_swap_executions(
    legs: &[VenueLegState],
    executions: Vec<crate::swappers::model::SwapExecution>,
) -> Result<Option<crate::swappers::model::SwapExecution>, String> {
    if executions.is_empty() {
        return Ok(None);
    }

    let first = executions.first().expect("non-empty executions").clone();
    if executions
        .iter()
        .any(|execution| execution.pay_asset != first.pay_asset || execution.receive_asset != first.receive_asset)
    {
        return Err(format!(
            "{MULTI_VENUE_PERMANENT_PREFIX}completed venue legs use different asset pairs"
        ));
    }

    let mut pay_amount = Nat::from(0u8);
    let mut receive_amount = Nat::from(0u8);
    let mut pay_weight = 0.0;
    let mut mid_notional = 0.0;
    let mut exec_notional = 0.0;
    let mut slippage_notional = 0.0;
    let mut quote_legs = Vec::new();
    let mut approval_count = None;
    let mut ts = 0;

    for (leg, execution) in legs
        .iter()
        .filter(|leg| leg.status == VenueLegStatus::Completed)
        .zip(executions)
    {
        let weight = leg.request.pay_amount.to_f64();
        pay_amount += execution.pay_amount.clone();
        receive_amount += execution.receive_amount.clone();
        pay_weight += weight;
        mid_notional += execution.mid_price * weight;
        exec_notional += execution.exec_price * weight;
        slippage_notional += execution.realized_slippage_bps * weight;
        quote_legs.extend(execution.legs);
        if let Some(count) = execution.approval_count {
            approval_count = Some(approval_count.unwrap_or(0u32).saturating_add(count));
        }
        ts = ts.max(execution.ts);
    }

    let weighted = |notional: f64| if pay_weight > 0.0 { notional / pay_weight } else { 0.0 };
    Ok(Some(crate::swappers::model::SwapExecution {
        swap_id: first.swap_id,
        request_id: first.request_id,
        status: "completed".to_string(),
        pay_asset: first.pay_asset,
        pay_amount,
        receive_asset: first.receive_asset,
        receive_amount,
        mid_price: weighted(mid_notional),
        exec_price: weighted(exec_notional),
        realized_slippage_bps: weighted(slippage_notional),
        legs: quote_legs,
        approval_count,
        ts,
    }))
}

#[async_trait]
impl Finalizer for MultiVenueFinalizer {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        if receipt.request.swap_args.is_none() {
            return Err(format!("{MULTI_VENUE_PERMANENT_PREFIX}receipt has no swap request"));
        }

        let liquidation_id = liq_id_from_receipt(&receipt)?;
        let row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt wrapper for liquidation {liquidation_id}"))?;

        // Routing precedence is deliberately based only on durable state. A
        // row is never upgraded or sent through a different venue after any
        // execution path has been committed.
        if wrapper.meta_v2.is_some() {
            return self.finalize_multi_venue(wal, receipt).await;
        }
        if wrapper.venue_execution.is_some()
            || !wrapper.meta.is_empty()
            || wrapper
                .finalizer_decision
                .as_ref()
                .is_some_and(|decision| matches!(decision.chosen.as_str(), "dex" | "cex"))
        {
            return Err(format!(
                "{MULTI_VENUE_PERMANENT_PREFIX}legacy execution state is not supported for liquidation {liquidation_id}"
            ));
        }

        self.finalize_multi_venue(wal, receipt).await
    }

    fn classify_error(&self, error: &str) -> FinalizerErrorKind {
        if error.starts_with(MULTI_VENUE_PERMANENT_PREFIX) {
            FinalizerErrorKind::Permanent
        } else if error.starts_with(MULTI_VENUE_CUSTODY_PREFIX) {
            FinalizerErrorKind::VenueCustody
        } else {
            FinalizerErrorKind::Retryable
        }
    }
}

/// Marks a retryable failure raised while a leg has already begun executing.
///
/// A leg that is past `Planned` may have moved funds to a pool subaccount or an
/// exchange. Failing such a row permanently once the retry budget runs out
/// removes it from the runnable queue forever, so the custody would never be
/// reconciled by anything. Tagging it routes budget exhaustion to an operator
/// park instead, which stays visible and can be requeued.
///
/// Errors that already carry a classification keep it because a permanent
/// decision is more specific than "a leg is running".
fn tag_custody_error(state: &MultiVenueExecutionState, error: String) -> String {
    if error.starts_with(MULTI_VENUE_PERMANENT_PREFIX) || error.starts_with(MULTI_VENUE_CUSTODY_PREFIX) {
        return error;
    }

    let holds_custody = state
        .legs
        .iter()
        .any(|leg| matches!(leg.status, VenueLegStatus::Running | VenueLegStatus::OperatorRequired));
    if holds_custody {
        format!("{MULTI_VENUE_CUSTODY_PREFIX}{error}")
    } else {
        error
    }
}

pub(super) fn apply_progress(leg: &mut VenueLegState, progress: VenueLegProgress) -> Result<(), String> {
    if progress.execution.venue != leg.venue_id {
        return Err(format!(
            "adapter returned venue `{}` for committed leg `{}` at venue `{}`",
            progress.execution.venue, leg.leg_id, leg.venue_id
        ));
    }
    leg.execution = progress.execution;
    leg.status = progress.status;
    leg.result = progress.result;
    leg.last_error = progress.last_error;
    Ok(())
}

pub(super) fn derive_outcome(legs: &[VenueLegState]) -> MultiVenueExecutionOutcome {
    let operator_leg_ids = legs
        .iter()
        .filter(|leg| leg.status == VenueLegStatus::OperatorRequired)
        .map(|leg| leg.leg_id.clone())
        .collect::<Vec<_>>();
    if !operator_leg_ids.is_empty() {
        return MultiVenueExecutionOutcome::OperatorRequired {
            leg_ids: operator_leg_ids,
        };
    }

    if legs
        .iter()
        .any(|leg| matches!(leg.status, VenueLegStatus::Planned | VenueLegStatus::Running))
    {
        return MultiVenueExecutionOutcome::Running;
    }

    let failed_leg_ids = legs
        .iter()
        .filter(|leg| leg.status == VenueLegStatus::FailedPermanent)
        .map(|leg| leg.leg_id.clone())
        .collect::<Vec<_>>();
    if !failed_leg_ids.is_empty() {
        return MultiVenueExecutionOutcome::PartialRecovered { failed_leg_ids };
    }

    if legs.iter().any(|leg| leg.status == VenueLegStatus::Recovered) {
        MultiVenueExecutionOutcome::Recovered
    } else {
        MultiVenueExecutionOutcome::Completed
    }
}

pub(super) fn set_meta_v2(wrapper: &mut LiqMetaWrapper, state: &MultiVenueExecutionState) -> Result<(), String> {
    let meta = FinalizerMetaV2 {
        version: FINALIZER_META_V2_VERSION,
        payload: FinalizerMetaPayload::MultiVenueSwap(state.clone()),
    };
    meta.validate()
        .map_err(|error| format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"))?;
    wrapper.meta_v2 = Some(meta);
    Ok(())
}

fn decision_snapshot(state: &MultiVenueExecutionState) -> FinalizerDecisionSnapshot {
    FinalizerDecisionSnapshot {
        mode: "multi_venue".to_string(),
        chosen: "multi_venue".to_string(),
        reason: format!("{} allocation committed", state.plan.strategy_id),
        min_required_bps: f64::from(state.plan.min_net_edge_bps),
        dex_preview_gross_bps: None,
        dex_preview_net_bps: None,
        cex_preview_gross_bps: None,
        cex_preview_net_bps: None,
        ts: state.plan.quoted_at,
        multi_venue_allocation: Some(MultiVenueAllocationSnapshot {
            strategy_id: state.plan.strategy_id.clone(),
            total_pay: state.plan.total_pay.clone(),
            allocations: state
                .legs
                .iter()
                .map(|leg| VenueAllocationSnapshot {
                    leg_id: leg.leg_id.clone(),
                    venue_id: leg.venue_id.clone(),
                    pay_amount: leg.request.pay_amount.clone(),
                    estimated_receive: leg.quote.estimated_receive.clone(),
                    conservative_receive: leg.quote.conservative_receive.clone(),
                    estimated_price_impact_bps: Some(leg.quote.estimated_price_impact_bps),
                    route_id: leg.quote.route_id.clone(),
                })
                .collect(),
            estimated_receive: state.plan.estimated_receive.clone(),
            conservative_receive: state.plan.conservative_receive.clone(),
            combined_net_edge_bps: state.plan.combined_net_edge_bps,
            reason: state.plan.allocation_reason.clone(),
        }),
    }
}
