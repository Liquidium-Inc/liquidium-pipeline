use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use num_traits::ToPrimitive;

use crate::{
    finalizers::dex_finalizer::{DexRouteFinalizer, DexRoutePreview},
    finalizers::finalizer::{Finalizer, FinalizerErrorKind, FinalizerResult},
    persistance::{FinalizerDecisionSnapshot, VenueExecutionState, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            execution::{IcpswapExecutionStateStore, WalIcpswapExecutionStateStore, approve_and_submit},
            recovery::recover,
            recovery_transfer::transfer_to_recovery,
            settlement::reconcile_swap_settlement,
            types::{IcpswapExecutionPhase, IcpswapExecutionState},
            venue::IcpswapVenueService,
        },
        model::{SwapExecution, SwapQuoteLeg, SwapRequest},
    },
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
};

pub(crate) const ICPSWAP_FINALIZER_PERMANENT_PREFIX: &str = "permanent ICPSwap finalizer: ";

type Clock = dyn Fn() -> u64 + Send + Sync;

/// WAL-backed finalizer for the complete ICPSwap execution lifecycle.
///
/// The phase persisted in the liquidation row is the sole dispatch key. In
/// particular, phases at or after submission can only reconcile or recover;
/// they can never return to swap submission.
pub struct IcpswapFinalizer {
    venue: Arc<dyn IcpswapVenueService>,
    trader: Account,
    recovery_destination: Account,
    automatic_refund_wait_nanos: u64,
    recovery_timeout_nanos: u64,
    clock: Arc<Clock>,
}

impl IcpswapFinalizer {
    pub fn new(
        venue: Arc<dyn IcpswapVenueService>,
        trader: Account,
        recovery_destination: Account,
        automatic_refund_wait_nanos: u64,
        recovery_timeout_nanos: u64,
    ) -> Self {
        Self::from_venue_with_clock(
            venue,
            trader,
            recovery_destination,
            automatic_refund_wait_nanos,
            recovery_timeout_nanos,
            Arc::new(system_time_nanos),
        )
    }

    pub fn from_venue_with_clock(
        venue: Arc<dyn IcpswapVenueService>,
        trader: Account,
        recovery_destination: Account,
        automatic_refund_wait_nanos: u64,
        recovery_timeout_nanos: u64,
        clock: Arc<Clock>,
    ) -> Self {
        Self {
            venue,
            trader,
            recovery_destination,
            automatic_refund_wait_nanos,
            recovery_timeout_nanos,
            clock,
        }
    }

    async fn load_state(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        liquidation_id: &str,
    ) -> Result<IcpswapExecutionState, String> {
        store
            .load(liquidation_id)
            .await?
            .ok_or_else(|| format!("missing persisted ICPSwap state for liquidation {liquidation_id}"))
    }

    async fn persist_pending_error(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        liquidation_id: &str,
        message: String,
    ) -> Result<IcpswapExecutionState, String> {
        let mut state = self.load_state(store, liquidation_id).await?;
        state.last_error = Some(message);
        store.persist(liquidation_id, &state).await?;
        Ok(state)
    }

    fn result_for_state(
        &self,
        receipt: &ExecutionReceipt,
        state: &IcpswapExecutionState,
        now_nanos: u64,
    ) -> Result<FinalizerResult, String> {
        match state.phase {
            IcpswapExecutionPhase::Completed => Ok(FinalizerResult {
                swap_result: Some(build_completed_execution(receipt, state, now_nanos)?),
                finalized: true,
                swapper: Some("icpswap".to_string()),
                reason: None,
            }),
            IcpswapExecutionPhase::Recovered => Ok(FinalizerResult {
                swap_result: None,
                finalized: true,
                swapper: Some("recovery".to_string()),
                reason: Some(recovery_reason(state)),
            }),
            IcpswapExecutionPhase::FailedTerminal => Err(permanent_error(state)),
            _ => Ok(FinalizerResult::noop()),
        }
    }
}

#[async_trait]
impl Finalizer for IcpswapFinalizer {
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        if !matches!(receipt.status, ExecutionStatus::Success) || receipt.request.swap_args.is_none() {
            return Ok(FinalizerResult::noop());
        }

        let liquidation_id = liq_id_from_receipt(&receipt)?;
        let store = WalIcpswapExecutionStateStore::new(wal);
        let initial = self.load_state(&store, &liquidation_id).await?;
        let now_nanos = (self.clock)();

        let step = match initial.phase {
            IcpswapExecutionPhase::Planned | IcpswapExecutionPhase::Approved => approve_and_submit(
                self.venue.as_ref(),
                &store,
                &liquidation_id,
                self.trader,
                None,
                now_nanos,
            )
            .await
            .map_err(|error| error.to_string()),

            IcpswapExecutionPhase::SubmissionUnknown
            | IcpswapExecutionPhase::AwaitingOutput
            | IcpswapExecutionPhase::RefundPending => reconcile_swap_settlement(
                self.venue.as_ref(),
                &store,
                &liquidation_id,
                self.trader,
                now_nanos,
                self.automatic_refund_wait_nanos,
            )
            .await
            .map_err(|error| error.to_string()),
            IcpswapExecutionPhase::FundsInPool | IcpswapExecutionPhase::RecoveryWithdrawSubmitted => recover(
                self.venue.as_ref(),
                &store,
                &liquidation_id,
                self.trader,
                now_nanos,
                self.recovery_timeout_nanos,
            )
            .await
            .map_err(|error| error.to_string()),
            IcpswapExecutionPhase::Refunded
            | IcpswapExecutionPhase::RecoveryTransferPending
            | IcpswapExecutionPhase::RecoveryTransferSubmitted => transfer_to_recovery(
                self.venue.as_ref(),
                &store,
                &liquidation_id,
                self.trader,
                initial.recovery_destination.unwrap_or(self.recovery_destination),
                now_nanos,
                self.recovery_timeout_nanos,
            )
            .await
            .map_err(|error| error.to_string()),
            IcpswapExecutionPhase::Completed
            | IcpswapExecutionPhase::Recovered
            | IcpswapExecutionPhase::FailedTerminal => Ok(initial),
        };

        let state = match step {
            Ok(state) => state,
            Err(error) => {
                let state = self
                    .persist_pending_error(&store, &liquidation_id, error.clone())
                    .await?;
                if state.phase == IcpswapExecutionPhase::FailedTerminal {
                    return Err(permanent_error(&state));
                }
                if phase_is_post_submission_pending(state.phase) {
                    return Ok(FinalizerResult::noop());
                }
                return Err(error);
            }
        };

        self.result_for_state(&receipt, &state, now_nanos)
    }

    fn classify_error(&self, error: &str) -> FinalizerErrorKind {
        if error.starts_with(ICPSWAP_FINALIZER_PERMANENT_PREFIX) {
            FinalizerErrorKind::Permanent
        } else {
            FinalizerErrorKind::Retryable
        }
    }
}

#[async_trait]
impl DexRouteFinalizer for IcpswapFinalizer {
    fn venue_id(&self) -> &'static str {
        VENUE_ID
    }

    async fn preview_route(&self, request: &SwapRequest) -> Result<DexRoutePreview, String> {
        let quoted = self
            .venue
            .quote_with_plan(request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(DexRoutePreview::new(quoted.quote, quoted.plan))
    }

    async fn has_committed_route(&self, wal: &dyn WalStore, receipt: &ExecutionReceipt) -> Result<bool, String> {
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let Some(row) = wal_load(wal, &liquidation_id).await? else {
            return Ok(false);
        };
        let Some(wrapper) = decode_receipt_wrapper(&row)? else {
            return Ok(false);
        };
        Ok(wrapper
            .venue_execution
            .as_ref()
            .is_some_and(|record| record.is_venue(VENUE_ID)))
    }

    async fn commit_route(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
        decision: FinalizerDecisionSnapshot,
        preview: DexRoutePreview,
    ) -> Result<(), String> {
        let plan = preview
            .plan::<crate::swappers::icpswap::types::IcpswapExecutionPlan>()
            .cloned()
            .ok_or_else(|| "ICPSwap finalizer received a preview from another venue".to_string())?;
        if decision.chosen != "dex" {
            return Err(format!(
                "ICPSwap route commit requires chosen=dex, got {}",
                decision.chosen
            ));
        }
        let liquidation_id = liq_id_from_receipt(receipt)?;
        let mut row = wal_load(wal, &liquidation_id)
            .await?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt wrapper in WAL meta_json for {}", row.id))?;

        if let Some(existing) = &wrapper.finalizer_decision
            && matches!(existing.chosen.as_str(), "dex" | "cex")
            && existing.chosen != "dex"
        {
            return Err(format!(
                "refusing to replace persisted {} route with dex for liquidation {liquidation_id}",
                existing.chosen
            ));
        }

        match &wrapper.venue_execution {
            Some(record) if record.is_venue(VENUE_ID) => {
                let existing = record
                    .decode::<IcpswapExecutionState>(VENUE_ID)?
                    .expect("venue was checked above");
                if existing.plan != plan {
                    return Err(format!(
                        "refusing to replace persisted ICPSwap pool plan for liquidation {liquidation_id}"
                    ));
                }
            }
            Some(record) => {
                return Err(format!(
                    "refusing to replace persisted {} execution state for liquidation {liquidation_id}",
                    record.venue
                ));
            }
            None => {
                let mut state = IcpswapExecutionState::planned(plan);
                state.recovery_destination = Some(self.recovery_destination);
                wrapper.venue_execution = Some(VenueExecutionState::new(VENUE_ID, &state)?);
            }
        }

        wrapper.finalizer_decision = Some(decision);
        encode_meta(&mut row, &wrapper)?;
        wal.upsert_result(row)
            .await
            .map_err(|error| format!("WAL ICPSwap route commit failed for {liquidation_id}: {error}"))
    }
}

fn phase_is_post_submission_pending(phase: IcpswapExecutionPhase) -> bool {
    matches!(
        phase,
        IcpswapExecutionPhase::SubmissionUnknown
            | IcpswapExecutionPhase::AwaitingOutput
            | IcpswapExecutionPhase::RefundPending
            | IcpswapExecutionPhase::FundsInPool
            | IcpswapExecutionPhase::RecoveryWithdrawSubmitted
            | IcpswapExecutionPhase::Refunded
            | IcpswapExecutionPhase::RecoveryTransferPending
            | IcpswapExecutionPhase::RecoveryTransferSubmitted
    )
}

fn build_completed_execution(
    receipt: &ExecutionReceipt,
    state: &IcpswapExecutionState,
    now_nanos: u64,
) -> Result<SwapExecution, String> {
    let gross = state
        .gross_swap_output
        .as_ref()
        .ok_or_else(|| permanent_message("completed state has no gross swap output"))?;
    if gross.token != state.plan.gross_quoted_out.token || gross.value <= state.plan.output_ledger_fee.value {
        return Err(permanent_message("completed output cannot cover its ledger fee"));
    }
    let pool_transaction_id = state
        .pool_transaction_id
        .as_ref()
        .ok_or_else(|| permanent_message("completed state has no pool transaction ID"))?;
    let ledger_block = state
        .settlement_ledger_block_index
        .as_ref()
        .ok_or_else(|| permanent_message("completed state has no settlement ledger block"))?;
    let swap_id = nat_to_u64(pool_transaction_id, "pool transaction ID")?;
    let request_id = nat_to_u64(ledger_block, "settlement ledger block")?;
    let net_value = gross.value.clone() - state.plan.output_ledger_fee.value.clone();
    let net_output = ChainTokenAmount::from_raw(gross.token.clone(), net_value.clone());
    let pay = state.plan.amount_in.to_f64();
    let receive = net_output.to_f64();
    let expected = state.plan.net_expected_output.to_f64();
    let exec_price = if pay > 0.0 { receive / pay } else { 0.0 };
    let mid_price = if pay > 0.0 { expected / pay } else { 0.0 };
    let slippage = if expected > 0.0 {
        ((expected - receive) / expected).max(0.0)
    } else {
        0.0
    };
    let swap_request = receipt
        .request
        .swap_args
        .as_ref()
        .ok_or_else(|| permanent_message("completed receipt has no swap request"))?;

    Ok(SwapExecution {
        swap_id,
        request_id,
        status: "filled".to_string(),
        pay_asset: state.plan.amount_in.token.asset_id(),
        pay_amount: state.plan.amount_in.value.clone(),
        receive_asset: net_output.token.asset_id(),
        receive_amount: net_value.clone(),
        mid_price,
        exec_price,
        slippage,
        legs: vec![SwapQuoteLeg {
            venue: "icpswap".to_string(),
            route_id: format!(
                "{}:transaction={pool_transaction_id}:ledger_block={ledger_block}",
                state.plan.pool
            ),
            pay_chain: swap_request.pay_asset.chain.clone(),
            pay_symbol: state.plan.amount_in.token.symbol(),
            pay_amount: state.plan.amount_in.value.clone(),
            receive_chain: swap_request.receive_asset.chain.clone(),
            receive_symbol: net_output.token.symbol(),
            receive_amount: net_value,
            price: exec_price,
            lp_fee: Nat::from(0u8),
            gas_fee: state.plan.output_ledger_fee.value.clone(),
        }],
        approval_count: Some(u32::from(state.approval_block_index.is_some())),
        ts: now_nanos / 1_000_000_000,
    })
}

fn nat_to_u64(value: &Nat, field: &str) -> Result<u64, String> {
    value
        .0
        .to_u64()
        .ok_or_else(|| permanent_message(&format!("{field} does not fit in u64")))
}

fn recovery_reason(state: &IcpswapExecutionState) -> String {
    if let Some(transaction_id) = &state.recovery_transaction_id {
        format!("ICPSwap failed; pool withdrawal {transaction_id} was moved to the recovery account")
    } else if let Some(transaction_id) = &state.refund_transaction_id {
        format!("ICPSwap failed; automatic refund {transaction_id} was moved to the recovery account")
    } else {
        "ICPSwap failed; zero transferable refund was finalized for recovery".to_string()
    }
}

fn permanent_error(state: &IcpswapExecutionState) -> String {
    permanent_message(
        state
            .last_error
            .as_deref()
            .unwrap_or("terminal ICPSwap state has no error detail"),
    )
}

fn permanent_message(message: &str) -> String {
    format!("{ICPSWAP_FINALIZER_PERMANENT_PREFIX}{message}")
}

fn system_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}
