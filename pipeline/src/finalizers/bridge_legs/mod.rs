//! The bridge legs every venue shares: an ICP asset out to the bridge wallet on
//! another chain, and a foreign asset back to its ck form on ICP. A venue owns
//! the steps around them (an exchange trade, a Rift order), persists each leg's
//! state inside its own, and maps each cycle's outcome onto its own step.
mod bridge_in;
mod bridge_leg_state;
mod bridge_out;
mod bridge_sizing;

use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeError, BridgeMintReceipt, BridgeRouteSpec, BridgeStatus,
};
use liquidium_pipeline_connectors::backend::cex_backend::CexBackendError;
use liquidium_pipeline_core::types::side_cost::SideCost;
use log::{debug, info, warn};

use crate::{
    bridge::{
        call_sender::{BridgeCall, BridgeCallSender, prepare_bridge_call},
        runtime::BridgeRuntime,
    },
    outbound::driver::{OutboundError, OutboundRecord, step_outbound},
    utils::{now_ts, waited_secs},
};

pub use bridge_in::{BridgeInInputs, advance_bridge_in, awaits_other_deposit, in_route};
pub use bridge_leg_state::{BridgeInState, BridgeOutState};
pub use bridge_out::{BridgeDestinationProvider, BridgeOutInputs, FundingSlot, advance_bridge_out, out_route};
#[cfg(test)]
pub use bridge_sizing::reverse_bridge_source_fee_reserve;
pub use bridge_sizing::{
    bridge_minimum_shortfall, bridged_deposit_arrival, fee_adjusted_transfer, net_of_withdraw_fee,
    size_bridged_deposit, solana_repayment_bridge_amount,
};

/// The asset the ckERC20 and ckETH routes pay their provider fee in.
pub(crate) const BRIDGE_FEE_TOKEN_SYMBOL: &str = "ckETH";
pub(crate) const BRIDGE_FEE_TOKEN_CHAIN: &str = "ICP";
pub(crate) const CKETH_DECIMALS: u8 = 18;
/// Source balances are read as floats; this absorbs their rounding.
pub(crate) const SOURCE_BALANCE_TOLERANCE: f64 = crate::swappers::model::LIQUIDITY_EPS;
/// A native ETH source balance is reported net of a gas reserve; this keeps the
/// send under what the wallet can really spend.
pub(crate) const NATIVE_ETH_BRIDGEABLE_SAFETY_MARGIN: f64 = 1e-15;
/// Funds in the bridge wallet must start their bridge within this time. After
/// it, each waiting cycle errors, so the row parks for an operator.
pub(crate) const BRIDGE_START_TIMEOUT_SECS: i64 = 60 * 60;
/// A landed deposit send the provider has not accepted within this window is
/// ambiguous: only an operator can say where the funds are.
pub(crate) const DEPOSIT_NOTICE_WINDOW_SECS: i64 = 30 * 60;
/// A bridge that has not credited within this time is stamped on the leg.
pub(crate) const BRIDGE_CREDIT_STALL_SECS: i64 = 40 * 60;
/// How often a reverted deposit send is made again before an operator decides.
pub(crate) const MAX_REVERTED_BRIDGE_RESUBMITS: u32 = 2;
/// A confirmed Solana transfer is final after about 13 seconds.
pub(crate) const SOL_FINALITY_SECS: i64 = 20;

/// What one cycle of a bridge leg did. The caller maps it onto its own step.
#[derive(Debug, Clone, PartialEq)]
pub enum BridgeLegOutcome {
    /// Nothing moved. The leg waits for funds, the provider or the chain.
    /// `stalled` carries the wait note once the stall bound passed, for the
    /// caller to show.
    Waiting { stalled: Option<String> },
    /// A send or a provider call was saved. The caller persists the leg before
    /// the next step sends it. `side_costs` are what the sizing found this leg pays.
    Saved { side_costs: Vec<SideCost> },
    /// The funding transfer landed in the bridge wallet; the provider call comes
    /// next. `side_costs` are set when the same cycle also sized the transfer.
    Funded { side_costs: Vec<SideCost> },
    /// The provider holds the funds under a handle; the next cycles poll it.
    Submitted,
    /// The bridge delivered. The mint receipt when the provider reports one.
    Completed { receipt: Option<BridgeMintReceipt> },
}

/// Why a bridge leg cycle did not finish. The caller matches on the variant;
/// the text is for the operator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BridgeLegError {
    /// The amount is under a minimum on the route, so no later attempt can succeed with it.
    #[error("{0}")]
    BelowMinimum(String),
    /// A send or a call may have moved funds and no answer decides it. Only an
    /// operator can say what happened, so no automatic retry is safe.
    #[error("{0}")]
    Ambiguous(String),
    /// Any other failure, under the caller's normal retry policy.
    #[error("{0}")]
    Other(String),
}

impl From<String> for BridgeLegError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

impl From<&str> for BridgeLegError {
    fn from(message: &str) -> Self {
        Self::Other(message.to_string())
    }
}

impl From<BridgeError> for BridgeLegError {
    fn from(error: BridgeError) -> Self {
        match error {
            BridgeError::BelowMinimum(message) => Self::BelowMinimum(message),
            BridgeError::Ambiguous(message) => Self::Ambiguous(message),
            BridgeError::Deferred(message)
            | BridgeError::Unreadable(message)
            | BridgeError::Refused(message)
            | BridgeError::Other(message) => Self::Other(message),
        }
    }
}

impl From<OutboundError> for BridgeLegError {
    fn from(error: OutboundError) -> Self {
        match error {
            OutboundError::BelowMinimum(message) => Self::BelowMinimum(message),
            // A step that does not bound its resubmits leaves a revert to an operator.
            OutboundError::Park(message) | OutboundError::Reverted(message) => Self::Ambiguous(message),
            OutboundError::Retry(message) | OutboundError::Deferred(message) => Self::Other(message),
        }
    }
}

impl From<BridgeLegError> for CexBackendError {
    fn from(error: BridgeLegError) -> Self {
        match error {
            BridgeLegError::BelowMinimum(message) => Self::BelowMinimum(message),
            BridgeLegError::Ambiguous(message) => Self::Ambiguous(message),
            BridgeLegError::Other(message) => Self::Other(message),
        }
    }
}

/// How long a leg may wait for funds, the provider or the chain before an
/// operator must decide. `since` is set on first use.
pub struct WaitBound<'a> {
    pub since: &'a mut Option<i64>,
    pub timeout_secs: i64,
}

/// A wait that only funds, the provider or the chain can end. Past the bound it
/// errors, so the row parks with an alert.
pub fn wait_within_bound(
    leg_id: &str,
    wait: &mut WaitBound<'_>,
    what: &str,
    reason: &str,
) -> Result<BridgeLegOutcome, BridgeLegError> {
    let waited = waited_secs(wait.since);
    if waited > wait.timeout_secs {
        return Err(format!("{what} for liq_id {leg_id} still waits after {waited}s: {reason}").into());
    }
    Ok(BridgeLegOutcome::Waiting { stalled: None })
}

/// `asset@chain->target`, as every log line names a route.
fn route_label(route: &BridgeRouteSpec) -> String {
    format!("{}@{}->{}", route.source_asset, route.source_chain, route.target_asset)
}

/// The bridge status. A failed read says nothing about the bridge, so it reads
/// as pending with the reason kept for the wait note.
async fn read_bridge_status(
    bridge: &BridgeRuntime,
    bridge_id: &str,
) -> Result<(BridgeStatus, Option<String>), BridgeLegError> {
    match bridge.backend.get_bridge_status(bridge_id).await {
        Ok(status) => Ok((status, None)),
        Err(BridgeError::Unreadable(error)) => Ok((BridgeStatus::Pending, Some(error))),
        Err(error) => Err(error.into()),
    }
}

/// The error for a bridge the provider reports as failed or canceled.
fn bridge_failure(what: &str, leg_id: &str, bridge_id: &str, status: BridgeStatus) -> BridgeLegError {
    let (verb, reason) = match status {
        BridgeStatus::Failed { reason } => ("failed", reason.unwrap_or_else(|| "unknown failure".to_string())),
        BridgeStatus::Canceled { reason } => ("canceled", reason.unwrap_or_else(|| "no reason provided".to_string())),
        other => ("ended", format!("unexpected status {other:?}")),
    };
    format!("{what} {verb} for liq_id {leg_id} bridge_id {bridge_id}: {reason}").into()
}

/// What one step of a saved provider call did.
enum SavedCallStep {
    /// The provider answered with a bridge handle.
    Submitted(String),
    /// The call is saved or sent and has no answer yet.
    Pending,
    /// A busy bridge wallet. The driver dropped the call; the next cycle saves a new one.
    Deferred(String),
}

/// Drives a saved provider call: one step sends it, and a lost answer settles
/// from the provider history before any new call.
async fn drive_saved_call(
    bridge: &BridgeRuntime,
    call: &mut Option<OutboundRecord>,
) -> Result<SavedCallStep, BridgeLegError> {
    let sender = BridgeCallSender::new(bridge.backend.clone(), bridge.claims.clone());
    match step_outbound(call, &sender, None, now_ts()).await {
        Ok(Some(bridge_id)) => Ok(SavedCallStep::Submitted(bridge_id)),
        Ok(None) => Ok(SavedCallStep::Pending),
        Err(OutboundError::Deferred(error)) => Ok(SavedCallStep::Deferred(error)),
        Err(error) => Err(error.into()),
    }
}

/// Saves a provider call under a fresh id, with the history cursor taken
/// before it. The call runs on the next step, so a lost answer has a record.
async fn save_provider_call(
    bridge: &BridgeRuntime,
    mut call: BridgeCall,
    what: &str,
    leg_id: &str,
) -> Result<OutboundRecord, BridgeLegError> {
    call.cursor = bridge.backend.prepare_submission(&call.request()?).await?;
    info!(
        "[bridge] liq_id={leg_id} {what} funded; call saved amount={} destination={} cursor={:?}",
        call.amount, call.destination, call.cursor
    );
    let id = crate::utils::new_venue_execution_id("bridge-call");
    Ok(OutboundRecord::prepared(prepare_bridge_call(id, &call)?))
}

/// A bridge still pending: a debug line until the stall bound, then a warning.
/// Past the bound, a read that still fails is an error, so a lasting failure
/// parks with an alert; a bridge that is merely slow returns the note for the leg.
fn note_bridge_wait(
    leg_id: &str,
    submitted_at: Option<i64>,
    bridge_id: &str,
    unreadable: Option<&str>,
) -> Result<Option<String>, BridgeLegError> {
    let waited = submitted_at.map_or(0, |sent| now_ts().saturating_sub(sent));
    let detail = unreadable.map_or_else(
        || "not done yet".to_string(),
        |error| format!("could not read it: {error}"),
    );
    let line = format!("waiting on bridge {bridge_id} for liq_id {leg_id}: {detail}, waited={waited}s");
    if waited < BRIDGE_CREDIT_STALL_SECS {
        debug!("[bridge] {line}");
        return Ok(None);
    }
    warn!("[bridge] {line} (past {BRIDGE_CREDIT_STALL_SECS}s)");
    if unreadable.is_some() {
        return Err(line.into());
    }
    Ok(Some(line))
}
