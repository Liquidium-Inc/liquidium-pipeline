use std::sync::Arc;

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerError, FinalizerResult},
    finalizers::multi_venue::{
        IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerConfig, IcpswapFirstPlannerError,
        MultiVenueAdapter, VenueLegProgress,
    },
    persistance::{
        FINALIZER_META_V2_VERSION, FinalizerDecisionSnapshot, FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper,
        MultiVenueAllocationSnapshot, MultiVenueExecutionOutcome, MultiVenueExecutionState, RecoverySweepState,
        RecoverySweepStatus, VenueAllocationSnapshot, VenueLegState, VenueLegStatus, WalStore,
    },
    price_oracle::price_oracle::PriceOracle,
    stages::executor::ExecutionReceipt,
    utils::{ICP_LEDGER_PRINCIPAL, now_ts},
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
    watchdog::{Watchdog, WatchdogEvent, noop_watchdog},
};
use async_trait::async_trait;
use candid::Nat;
use ic_ledger_types::{AccountIdentifier, Subaccount};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::{TransferActions, TransferFailure},
};
use tracing::{debug, info};

use super::parent_leg_checkpoint::ParentLegCheckpoint;
use crate::finalizers::multi_venue::planning::venue_registry::VenueRegistry;

pub(super) const MULTI_VENUE_PERMANENT_PREFIX: &str = "permanent multi-venue finalizer: ";
const MULTI_VENUE_CUSTODY_PREFIX: &str = "multi-venue venue custody: ";
const MULTI_VENUE_UNRESUMABLE_PREFIX: &str = "unresumable multi-venue finalizer: ";

struct RecoverySweepRuntime {
    trader_transfers: Arc<dyn TransferActions + Send + Sync>,
    recovery_account: Account,
}

/// One liquidation's committed finalization envelope, plus whether this
/// invocation is the one that planned it.
///
/// `freshly_planned` is the submission gate for recovery sweeps: only the call
/// that durably wrote the transfer may submit it. Anything that reads the same
/// state back — a later cycle, a restarted process, a cancelled stage — cannot
/// distinguish "never submitted" from "submitted with an unknown outcome", so
/// it parks for an operator instead.
struct LoadedFinalization {
    row: crate::persistance::LiqResultRecord,
    wrapper: LiqMetaWrapper,
    payload: FinalizerMetaPayload,
    freshly_planned: bool,
}

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
    recovery: Option<RecoverySweepRuntime>,
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
            recovery: None,
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

    pub fn with_recovery_sweep(
        mut self,
        trader_transfers: Arc<dyn TransferActions + Send + Sync>,
        recovery_account: Account,
    ) -> Self {
        self.recovery = Some(RecoverySweepRuntime {
            trader_transfers,
            recovery_account,
        });
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
    ) -> Result<LoadedFinalization, String> {
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let mut row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for multi-venue liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)
            .map_err(unresumable_meta)?
            .ok_or_else(|| format!("missing receipt wrapper for multi-venue liquidation {liquidation_id}"))?;

        if wrapper.meta_v2.is_some() {
            let payload = {
                let meta = wrapper.meta_v2.as_ref().expect("meta_v2 checked above");
                // Committed state this binary cannot read is a code or config
                // mismatch, not a dead liquidation. Failing it here would drop a
                // row whose venue legs may still hold funds out of the queue,
                // so it is parked for an operator instead.
                meta.validate().map_err(unresumable_meta)?;
                meta.payload.clone()
            };
            return Ok(LoadedFinalization {
                row,
                wrapper,
                payload,
                freshly_planned: false,
            });
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
        //
        // Every rejection here is structural — a missing swap request, a
        // mismatched pay asset, an unsuccessful execution — and no retry can
        // change it, so the row fails once instead of spending its whole budget.
        let input = IcpswapFirstPlanInput::from_receipt(receipt)
            .map_err(|error| format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"))?;

        // This is the pure routing decision: quote the registered venues and
        // produce the immutable allocation plan plus initialized venue legs.
        // No swap, transfer, or order side effect is submitted by `plan`.
        let state = match self.planner.plan(&input, now_ts()).await {
            Ok(state) => state,
            Err(IcpswapFirstPlannerError::BelowVenueMinimum(reason)) => {
                let recovery = self.recovery.as_ref().ok_or_else(|| {
                    format!(
                        "{MULTI_VENUE_PERMANENT_PREFIX}no venue can execute this amount and recovery sweep is unavailable: {reason}"
                    )
                })?;
                let destination = recovery_destination(&input.total_pay.token, &recovery.recovery_account)?;
                let fee = input.total_pay.token.fee();
                let amount = if input.total_pay.value > fee {
                    input.total_pay.value.clone() - fee
                } else {
                    Nat::from(0u8)
                };
                let recovery_state = RecoverySweepState {
                    liquidation_id: liquidation_id.clone(),
                    reason: format!("no venue can execute this amount: {reason}"),
                    amount: ChainTokenAmount::from_raw(input.total_pay.token.clone(), amount.clone()),
                    destination,
                    // Collateral that cannot pay its own transfer fee never
                    // moves, so the sweep is finished the moment it is planned.
                    status: if amount == 0u8 {
                        RecoverySweepStatus::Completed
                    } else {
                        RecoverySweepStatus::ReadyToSubmit
                    },
                    txid: None,
                    last_error: None,
                };
                wrapper.finalizer_decision = Some(recovery_decision_snapshot(&recovery_state));
                // The exact transfer is durable before the caller submits it.
                self.persist_recovery_state(wal, &mut row, &mut wrapper, &recovery_state)
                    .await?;
                return Ok(LoadedFinalization {
                    row,
                    wrapper,
                    payload: FinalizerMetaPayload::RecoverySweep(recovery_state),
                    freshly_planned: true,
                });
            }
            Err(IcpswapFirstPlannerError::InvalidInput(error)) => {
                return Err(format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"));
            }
            Err(IcpswapFirstPlannerError::NoViableRoute(reason)) => {
                return Err(format!("no viable ICPSwap-first route: {reason}"));
            }
        };

        // Commit the entire decision and every initialized leg atomically.
        // Venue execution may begin only after this WAL write succeeds, so a
        // restart always resumes the same allocations instead of replanning.
        wrapper.finalizer_decision = Some(decision_snapshot(&state));
        self.persist_state(wal, &mut row, &mut wrapper, &state).await?;

        Ok(LoadedFinalization {
            row,
            wrapper,
            payload: FinalizerMetaPayload::MultiVenueSwap(state),
            freshly_planned: true,
        })
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
        self.persist_wrapper(wal, row, wrapper).await
    }

    async fn persist_recovery_state(
        &self,
        wal: &dyn WalStore,
        row: &mut crate::persistance::LiqResultRecord,
        wrapper: &mut LiqMetaWrapper,
        state: &RecoverySweepState,
    ) -> Result<(), String> {
        set_recovery_meta_v2(wrapper, state)?;
        self.persist_wrapper(wal, row, wrapper).await
    }

    /// The single point where a finalization envelope reaches the WAL. Callers
    /// stage their payload transition into `wrapper` first.
    async fn persist_wrapper(
        &self,
        wal: &dyn WalStore,
        row: &mut crate::persistance::LiqResultRecord,
        wrapper: &LiqMetaWrapper,
    ) -> Result<(), String> {
        row.updated_at = now_ts();
        encode_meta(row, wrapper)?;
        wal.upsert_result(row.clone())
            .await
            .map_err(|error| format!("failed to persist finalizer state for {}: {error}", row.id))
    }

    async fn finalize_recovery_sweep(
        &self,
        wal: &dyn WalStore,
        mut row: crate::persistance::LiqResultRecord,
        mut wrapper: LiqMetaWrapper,
        mut state: RecoverySweepState,
        freshly_planned: bool,
    ) -> Result<FinalizerResult, String> {
        if state.status != RecoverySweepStatus::ReadyToSubmit {
            return Ok(recovery_result(&state));
        }

        // Only the invocation that durably wrote this transfer may submit it.
        // Any other reader — a later cycle, a restarted process, a cancelled
        // stage — cannot tell "never submitted" from "submitted with an unknown
        // outcome", so it hands the sweep to an operator rather than guessing.
        if !freshly_planned {
            return self
                .park_recovery_sweep(
                    wal,
                    &mut row,
                    &mut wrapper,
                    &mut state,
                    "recovery transfer was loaded from the WAL instead of being submitted by the invocation that planned it; verify the trader and recovery ledger balances before requeueing".to_string(),
                )
                .await;
        }

        let runtime = self.recovery.as_ref().ok_or_else(|| {
            format!("{MULTI_VENUE_PERMANENT_PREFIX}committed recovery sweep has no configured runtime")
        })?;
        match runtime
            .trader_transfers
            .transfer(&state.amount.token, &state.destination, state.amount.value.clone())
            .await
        {
            Ok(txid) => {
                state.txid = Some(txid);
                state.status = RecoverySweepStatus::Completed;
                self.persist_recovery_state(wal, &mut row, &mut wrapper, &state).await?;
                Ok(recovery_result(&state))
            }
            // A refusal and a lost answer both stop the sweep, but they cost an
            // operator very different amounts of work, so say which happened.
            // Never automatically repeat a transfer that may already have been
            // applied.
            Err(failure) => {
                let reason = match &failure {
                    TransferFailure::Rejected(error) => format!(
                        "the ledger refused the recovery transfer and moved nothing, so the trader balance is intact: {error}"
                    ),
                    TransferFailure::Ambiguous(error) => format!(
                        "the recovery transfer got no decided answer and may already have been applied: {error}; compare the trader and recovery ledger balances before requeueing"
                    ),
                };
                self.park_recovery_sweep(wal, &mut row, &mut wrapper, &mut state, reason)
                    .await
            }
        }
    }

    /// Records why a sweep cannot proceed automatically and escalates it. The
    /// transfer is never retried from this state without an operator.
    async fn park_recovery_sweep(
        &self,
        wal: &dyn WalStore,
        row: &mut crate::persistance::LiqResultRecord,
        wrapper: &mut LiqMetaWrapper,
        state: &mut RecoverySweepState,
        reason: String,
    ) -> Result<FinalizerResult, String> {
        state.status = RecoverySweepStatus::OperatorRequired;
        state.last_error = Some(reason);
        self.persist_recovery_state(wal, row, wrapper, state).await?;
        self.notify_recovery_operator_required(state).await;
        Ok(recovery_result(state))
    }

    async fn notify_recovery_operator_required(&self, state: &RecoverySweepState) {
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: format!("recovery-{}", state.liquidation_id),
                venue: "recovery".to_string(),
                pending_step: "recovery_transfer_reconciliation".to_string(),
                owner: "trader".to_string(),
                details: state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "recovery transfer requires reconciliation".to_string()),
            })
            .await;
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
        let LoadedFinalization {
            row,
            wrapper,
            payload,
            freshly_planned,
        } = self.load_or_commit_plan(wal, &receipt).await?;
        match payload {
            FinalizerMetaPayload::MultiVenueSwap(state) => {
                let mut context = MultiVenueFinalizationContext::new(wal, row, wrapper, state);
                if let Err(error) = self.advance_legs(&mut context).await {
                    return Err(tag_custody_error(&context.state, error));
                }
                self.result_for_state(&context.state)
            }
            FinalizerMetaPayload::RecoverySweep(state) => {
                self.finalize_recovery_sweep(wal, row, wrapper, state, freshly_planned)
                    .await
            }
        }
    }
}

fn recovery_destination(token: &ChainToken, recovery: &Account) -> Result<ChainAccount, String> {
    let ChainToken::Icp { ledger, .. } = token else {
        return Err(format!(
            "{MULTI_VENUE_PERMANENT_PREFIX}automatic recovery sweep has no distinct destination for EVM collateral"
        ));
    };
    if ledger.to_text() == ICP_LEDGER_PRINCIPAL {
        let subaccount = Subaccount(recovery.subaccount.unwrap_or([0; 32]));
        Ok(ChainAccount::IcpLedger(
            AccountIdentifier::new(&recovery.owner, &subaccount).to_hex(),
        ))
    } else {
        Ok(ChainAccount::Icp(*recovery))
    }
}

fn recovery_result(state: &RecoverySweepState) -> FinalizerResult {
    FinalizerResult {
        swap_result: None,
        finalized: state.status == RecoverySweepStatus::Completed,
        operator_required: state.status == RecoverySweepStatus::OperatorRequired,
        swapper: Some("recovery".to_string()),
        reason: Some(match (state.status, state.txid.as_ref()) {
            (RecoverySweepStatus::Completed, Some(txid)) => {
                format!("unrouteable collateral swept to recovery (tx {txid})")
            }
            // Collateral below its own transfer fee stays in the trader account.
            (RecoverySweepStatus::Completed, None) => format!(
                "unrouteable collateral {} is below its transfer fee and was left in place",
                state.amount.formatted()
            ),
            (RecoverySweepStatus::OperatorRequired, _) => state
                .last_error
                .clone()
                .unwrap_or_else(|| "recovery sweep requires operator reconciliation".to_string()),
            (RecoverySweepStatus::ReadyToSubmit, _) => "unrouteable collateral recovery sweep is prepared".to_string(),
        }),
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

/// Converts this finalizer's internal error text into the decision the pipeline
/// acts on.
///
/// The sentinels are private to this module and never leave it: `finalize`
/// classifies once, at the boundary, so no caller re-derives a decision from a
/// message. New code should prefer building the variant directly.
/// Marks committed state this binary cannot interpret — an unknown payload
/// kind, an unreadable envelope, or a version from a newer build.
///
/// This is a code or config mismatch, not a dead liquidation. Left unmarked it
/// classifies as retryable, which spends the row's budget on a decode that can
/// never succeed and then fails it permanently, dropping a row whose venue legs
/// may still hold funds out of the queue. Parking keeps it visible instead.
fn unresumable_meta(error: impl std::fmt::Display) -> String {
    format!("{MULTI_VENUE_UNRESUMABLE_PREFIX}committed finalizer state cannot be read by this build: {error}")
}

fn classify(error: String) -> FinalizerError {
    if error.starts_with(MULTI_VENUE_PERMANENT_PREFIX) {
        FinalizerError::Permanent(error)
    } else if error.starts_with(MULTI_VENUE_UNRESUMABLE_PREFIX) {
        FinalizerError::Unresumable(error)
    } else if error.starts_with(MULTI_VENUE_CUSTODY_PREFIX) {
        FinalizerError::VenueCustody(error)
    } else {
        FinalizerError::Retryable(error)
    }
}

#[async_trait]
impl Finalizer for MultiVenueFinalizer {
    async fn finalize(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
    ) -> Result<FinalizerResult, FinalizerError> {
        self.finalize_inner(wal, receipt).await.map_err(classify)
    }
}

impl MultiVenueFinalizer {
    async fn finalize_inner(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        if receipt.request.swap_args.is_none() {
            return Err(format!("{MULTI_VENUE_PERMANENT_PREFIX}receipt has no swap request"));
        }

        let liquidation_id = liq_id_from_receipt(&receipt)?;
        let row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let wrapper = decode_receipt_wrapper(&row)
            .map_err(unresumable_meta)?
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
    // A runnable leg outranks a parked one. `OperatorRequired` drops the row out
    // of the pending queue, so reporting it while a sibling still has an open
    // order would leave that leg with nothing polling it until a human requeues
    // the row. The operator alert fires on the leg's own transition, not here,
    // so deferring the parked outcome until nothing is runnable costs no
    // alerting latency and strands no in-flight funds.
    if legs
        .iter()
        .any(|leg| matches!(leg.status, VenueLegStatus::Planned | VenueLegStatus::Running))
    {
        return MultiVenueExecutionOutcome::Running;
    }

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

fn set_recovery_meta_v2(wrapper: &mut LiqMetaWrapper, state: &RecoverySweepState) -> Result<(), String> {
    let meta = FinalizerMetaV2 {
        version: FINALIZER_META_V2_VERSION,
        payload: FinalizerMetaPayload::RecoverySweep(state.clone()),
    };
    meta.validate()
        .map_err(|error| format!("{MULTI_VENUE_PERMANENT_PREFIX}{error}"))?;
    wrapper.meta_v2 = Some(meta);
    Ok(())
}

fn recovery_decision_snapshot(state: &RecoverySweepState) -> FinalizerDecisionSnapshot {
    FinalizerDecisionSnapshot {
        mode: "recovery".to_string(),
        chosen: "recovery".to_string(),
        reason: state.reason.clone(),
        min_required_bps: 0.0,
        dex_preview_gross_bps: None,
        dex_preview_net_bps: None,
        cex_preview_gross_bps: None,
        cex_preview_net_bps: None,
        ts: now_ts(),
        multi_venue_allocation: None,
    }
}

fn decision_snapshot(state: &MultiVenueExecutionState) -> FinalizerDecisionSnapshot {
    FinalizerDecisionSnapshot {
        mode: "multi_venue".to_string(),
        chosen: "multi_venue".to_string(),
        reason: format!("{} allocation committed", state.plan.strategy_id),
        // Prefer the floor that was actually enforced; rows planned before that
        // field existed only carry the unsigned one.
        min_required_bps: state
            .plan
            .enforced_min_net_edge_bps
            .map_or_else(|| f64::from(state.plan.min_net_edge_bps), f64::from),
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
