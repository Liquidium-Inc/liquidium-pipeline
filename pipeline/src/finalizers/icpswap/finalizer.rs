use std::sync::Arc;

use icrc_ledger_types::icrc1::account::Account;

use crate::{
    swappers::{
        icpswap::{
            VENUE_ID, ensure_supported_pair,
            plan::input_fee_budget,
            session::{IcpswapExecutionSession, IcpswapExecutionSessionFactory},
            transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
            types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep},
            venue::IcpswapFinalizerLogic,
        },
        model::SwapRequest,
    },
    utils::now_nanos,
    watchdog::{Watchdog, WatchdogEvent, noop_watchdog},
};

/// Cooldown applied after a non-terminal step fails.
///
/// Such a failure re-enqueues the row without consuming the finalize stage's
/// retry budget, so nothing else paces it -- the exponential backoff there only
/// gates `FailedRetryable`. Roughly one order of magnitude above the daemon
/// cycle, so a persistently failing step backs off instead of re-running every
/// two seconds, while a transient blip still recovers promptly.
pub(super) const RETRY_COOLDOWN_NANOS: u64 = 20 * 1_000_000_000;

type Clock = dyn Fn() -> u64 + Send + Sync;

/// Steps after which no ICPSwap side effect may be submitted again.
pub(super) fn is_terminal_step(step: IcpswapStep) -> bool {
    matches!(
        step,
        IcpswapStep::Completed | IcpswapStep::Refunded | IcpswapStep::Failed
    )
}

/// The ICPSwap venue: plans and drives one leg of a multi-venue execution.
pub struct IcpswapFinalizer {
    pub(super) workflow: Arc<dyn IcpswapFinalizerLogic>,
    pub(super) trader: Account,
    pub(super) sessions: Arc<dyn IcpswapExecutionSessionFactory>,
    pub(super) clock: Arc<Clock>,
    pub(super) watchdog: Arc<dyn Watchdog>,
}

impl IcpswapFinalizer {
    pub fn new(
        workflow: Arc<dyn IcpswapFinalizerLogic>,
        trader: Account,
        sessions: Arc<dyn IcpswapExecutionSessionFactory>,
    ) -> Self {
        Self::from_workflow_with_clock(workflow, trader, sessions, Arc::new(now_nanos))
    }

    pub fn from_workflow_with_clock(
        workflow: Arc<dyn IcpswapFinalizerLogic>,
        trader: Account,
        sessions: Arc<dyn IcpswapExecutionSessionFactory>,
        clock: Arc<Clock>,
    ) -> Self {
        Self {
            workflow,
            trader,
            sessions,
            clock,
            watchdog: noop_watchdog(),
        }
    }

    pub(super) fn prepare_execution_state(
        &self,
        execution_id: &str,
        liquidation_id: &str,
        request: &SwapRequest,
        plan: IcpswapExecutionPlan,
    ) -> Result<IcpswapExecutionState, String> {
        ensure_supported_pair(request)?;
        if plan.amount_in.token.asset_id() != request.pay_asset
            || plan.gross_quoted_out.token.asset_id() != request.receive_asset
        {
            return Err("ICPSwap execution plan token pair does not match its swap request".to_string());
        }
        let identity = self.sessions.descriptor(liquidation_id)?;
        let child = Account {
            owner: identity.principal,
            subaccount: None,
        };
        let allocation = request.pay_amount.value.clone();
        let fee = plan.input_ledger_fee.value.clone();
        let required = plan.amount_in.value.clone() + input_fee_budget(&fee);
        if allocation != required {
            return Err(format!(
                "ICPSwap allocation {allocation} does not equal pool input plus three ledger fees {required}"
            ));
        }
        let funding = IcpswapFundingState::new(self.trader, child, plan.input_ledger_fee.clone());
        let address = request
            .receive_address
            .as_deref()
            .ok_or_else(|| "ICPSwap request is missing receive_address".to_string())?;
        let destination = Account {
            owner: candid::Principal::from_text(address)
                .map_err(|error| format!("invalid ICPSwap receive principal `{address}`: {error}"))?,
            subaccount: None,
        };
        let settlement = IcpswapSettlementState {
            kind: None,
            destination,
            fee: plan.output_ledger_fee.clone(),
            transfer: IcpswapLedgerTransferState::default(),
            interrupted_transfer: None,
            interrupted_observed_debit: None,
            recovery_credit: None,
            residual_dust: None,
        };
        IcpswapExecutionState::prepare(execution_id, plan, identity, funding, settlement)
    }

    pub(super) fn execution_session(&self, state: &IcpswapExecutionState) -> Result<IcpswapExecutionSession, String> {
        self.sessions.open(&state.identity)
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// Alerts for as long as the execution stays parked, not just on the way in.
    /// Alerting once means a single dropped webhook can hide ambiguous custody
    /// indefinitely. The watchdog's cooldown key throttles repeated polls into
    /// a periodic re-escalation.
    pub(super) async fn notify_operator_required(&self, execution_id: &str, state: &IcpswapExecutionState) {
        if state.step != IcpswapStep::OperatorRequired {
            return;
        }
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: execution_id.to_string(),
                venue: VENUE_ID.to_string(),
                pending_step: state
                    .operator_pending_step
                    .map(|step| format!("{step:?}"))
                    .unwrap_or_else(|| "unknown".to_string()),
                owner: state.owner.to_string(),
                details: state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "ICPSwap requires operator reconciliation".to_string()),
            })
            .await;
    }
}
