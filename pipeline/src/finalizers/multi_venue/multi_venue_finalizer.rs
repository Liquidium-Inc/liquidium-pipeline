use std::sync::Arc;

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult},
    persistance::{
        FINALIZER_META_V2_VERSION, FinalizerDecisionSnapshot, FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper,
        MultiVenueAllocationSnapshot, MultiVenueExecutionOutcome, MultiVenueExecutionState, VenueAllocationSnapshot,
        VenueLegState, VenueLegStatus, WalStore,
    },
    stages::executor::ExecutionReceipt,
    utils::now_ts,
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
};
use async_trait::async_trait;

use super::{
    IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerConfig, MultiVenueAdapter,
    multi_venue_quote_book::VenueRegistry,
};

const MULTI_VENUE_PERMANENT_PREFIX: &str = "permanent multi-venue finalizer: ";

/// WAL orchestrator for a committed generic multi-venue execution plan.
///
/// It owns parent persistence, but delegates every venue side effect to the
/// amount-scoped adapter for exactly one persisted leg.
pub struct MultiVenueFinalizer {
    planner: IcpswapFirstPlanner,
    venues: VenueRegistry,
}

impl MultiVenueFinalizer {
    pub fn new(
        adapters: Vec<Arc<dyn MultiVenueAdapter>>,
        planner_config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, String> {
        let venues = VenueRegistry::new(adapters.clone())?;
        let planner = IcpswapFirstPlanner::new(adapters, planner_config).map_err(|error| error.to_string())?;
        Ok(Self { planner, venues })
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
                meta.validate()?;
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
        let state = self
            .planner
            .plan(&input, now_ts())
            .await
            .map_err(|error| error.to_string())?;

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
    async fn advance_legs(
        &self,
        wal: &dyn WalStore,
        row: &mut crate::persistance::LiqResultRecord,
        wrapper: &mut LiqMetaWrapper,
        state: &mut MultiVenueExecutionState,
    ) -> Result<(), String> {
        for index in 0..state.legs.len() {
            if !matches!(
                state.legs[index].status,
                VenueLegStatus::Planned | VenueLegStatus::Running
            ) {
                continue;
            }

            let current = state.legs[index].clone();
            let adapter = self
                .venues
                .adapter(&current.venue_id)
                .ok_or_else(|| format!("no adapter registered for committed venue `{}`", current.venue_id))?;
            let progress = adapter.advance(&current).await?;

            apply_progress(&mut state.legs[index], progress)?;
            state.outcome = derive_outcome(&state.legs);
            self.persist_state(wal, row, wrapper, state).await?;
        }

        state.outcome = derive_outcome(&state.legs);
        Ok(())
    }

    fn result_for_state(&self, state: &MultiVenueExecutionState) -> Result<FinalizerResult, String> {
        match &state.outcome {
            MultiVenueExecutionOutcome::Running => Ok(FinalizerResult {
                swap_result: None,
                finalized: false,
                swapper: Some("multi_venue".to_string()),
                reason: None,
            }),
            MultiVenueExecutionOutcome::OperatorRequired { leg_ids } => Ok(FinalizerResult {
                swap_result: None,
                finalized: false,
                swapper: Some("multi_venue".to_string()),
                reason: Some(format!("operator required for venue legs: {}", leg_ids.join(","))),
            }),
            MultiVenueExecutionOutcome::PartialRecovered { failed_leg_ids } => Err(format!(
                "{MULTI_VENUE_PERMANENT_PREFIX}venue legs failed permanently: {}",
                failed_leg_ids.join(",")
            )),
            MultiVenueExecutionOutcome::Completed | MultiVenueExecutionOutcome::Recovered => {
                let completed_results = state
                    .legs
                    .iter()
                    .filter(|leg| leg.status == VenueLegStatus::Completed)
                    .map(|leg| {
                        leg.result
                            .clone()
                            .ok_or_else(|| format!("completed venue leg `{}` has no execution result", leg.leg_id))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Step 7 replaces this single-result compatibility projection
                // with aggregation across every completed venue leg.
                let swap_result = if completed_results.len() == 1 {
                    completed_results.into_iter().next()
                } else {
                    None
                };
                Ok(FinalizerResult {
                    swap_result,
                    finalized: true,
                    swapper: Some("multi_venue".to_string()),
                    reason: None,
                })
            }
        }
    }
}

impl MultiVenueFinalizer {
    async fn finalize_multi_venue(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
    ) -> Result<FinalizerResult, String> {
        let (mut row, mut wrapper, mut state) = self.load_or_commit_plan(wal, &receipt).await?;
        self.advance_legs(wal, &mut row, &mut wrapper, &mut state).await?;

        // Persist a derived parent outcome even when no leg was runnable during
        // this cycle, for example after restart into OperatorRequired.
        self.persist_state(wal, &mut row, &mut wrapper, &state).await?;
        self.result_for_state(&state)
    }
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
        } else {
            FinalizerErrorKind::Retryable
        }
    }
}

fn apply_progress(leg: &mut VenueLegState, progress: super::VenueLegProgress) -> Result<(), String> {
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

fn derive_outcome(legs: &[VenueLegState]) -> MultiVenueExecutionOutcome {
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

fn set_meta_v2(wrapper: &mut LiqMetaWrapper, state: &MultiVenueExecutionState) -> Result<(), String> {
    let meta = FinalizerMetaV2 {
        version: FINALIZER_META_V2_VERSION,
        payload: FinalizerMetaPayload::MultiVenueSwap(state.clone()),
    };
    meta.validate()?;
    wrapper.meta_v2 = Some(meta);
    Ok(())
}

fn decision_snapshot(state: &MultiVenueExecutionState) -> FinalizerDecisionSnapshot {
    FinalizerDecisionSnapshot {
        mode: "hybrid".to_string(),
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

#[cfg(test)]
#[path = "multi_venue_finalizer_tests.rs"]
mod tests;
