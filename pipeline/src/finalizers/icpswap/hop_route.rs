//! A swap that crosses more than one ICPSwap pool.
//!
//! Each step is one normal single-pool execution. The route plans the next
//! step from the amount the last step really paid. Every step has a floor fixed
//! at planning, so no step can settle below the committed plan. The floor of a
//! later step assumes that each step before it paid only its minimum.
use candid::{Nat, Principal};
use futures::future::{OptionFuture, join_all};
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::finalizer::{IcpswapFinalizer, RETRY_COOLDOWN_NANOS};
use crate::{
    finalizers::multi_venue::{OperatorQuestion, VenueAmountError, VenuePlanningContext, VenueRoutePreview},
    outbound::driver::OperatorDecision,
    persistance::{VenueExecutionState, VenueLegStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            identity::IcpswapExecutionIdentity,
            listed_paths,
            plan::{amount_out_minimum, net_forwarded_output},
            unsupported_pair,
            types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapRoutePreview, IcpswapStep},
        },
        model::{SwapExecution, SwapQuote, SwapRequest, adverse_price_impact_bps},
    },
    watchdog::WatchdogEvent,
};

pub(super) const ROUTE_STATE_VERSION: u32 = 1;

/// Plans of the next step that may fail before the route asks an operator.
/// With the retry cooldown this is about ten minutes.
pub(super) const MAX_ROUTE_PLAN_ATTEMPTS: u32 = 30;

const BPS_DENOMINATOR: u32 = 10_000;

/// The widest slippage any ICPSwap swap may accept, whatever the configuration
/// says. A wrong setting cannot make a swap give up more than this.
pub(super) const HARD_MAX_SLIPPAGE_BPS: u32 = 500;

/// One step of a route: the request it trades and its single-pool execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct IcpswapRouteStep {
    pub request: SwapRequest,
    pub state: IcpswapExecutionState,
}

/// Durable state of a multi-step ICPSwap leg.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct IcpswapRouteState {
    pub route_version: u32,
    /// The signer every step uses. It stays at the top level so readers that
    /// only need the leg account do not depend on the route layout.
    pub identity: IcpswapExecutionIdentity,
    /// The request the whole route answers: its receiver, slippage cap and hint.
    pub request: SwapRequest,
    /// Tokens of the path, input first and output last.
    pub path: Vec<ChainToken>,
    /// The least gross output each step may accept, one entry for each step.
    /// They are fixed at planning; a later quote can raise a minimum, never lower it.
    pub floors: Vec<ChainTokenAmount>,
    pub finished: Vec<IcpswapRouteStep>,
    pub current: IcpswapRouteStep,
    #[serde(default)]
    pub plan_attempts: u32,
    #[serde(default)]
    pub next_plan_at_nanos: Option<u64>,
    /// Why the route waits for an operator before it plans the next step.
    #[serde(default)]
    pub parked: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
}

/// True when step `index` of a path with `tokens` tokens pays the output token.
fn pays_output(tokens: usize, index: usize) -> bool {
    index + 2 == tokens
}

impl IcpswapRouteState {
    fn is_first_step(&self) -> bool {
        self.finished.is_empty()
    }

    /// True when the current step pays the route's output token.
    pub(super) fn is_last_step(&self) -> bool {
        pays_output(self.path.len(), self.finished.len())
    }

    /// True when a step after the first one ended without its swap. The route then holds an intermediate asset.
    fn stopped_after_first_step(&self) -> bool {
        !self.is_first_step() && matches!(self.current.state.step, IcpswapStep::Refunded | IcpswapStep::Failed)
    }

    /// The asset the route holds between steps, with its amount when known.
    fn held_between_steps(&self) -> String {
        let step = &self.current;
        let token = &step.state.plan.gross_quoted_out.token;
        match step.state.settlement.transfer.credited_amount.as_ref() {
            Some(amount) => ChainTokenAmount::from_raw(token.clone(), amount.clone()).formatted(),
            None => token.symbol(),
        }
    }
}

/// What an ICPSwap leg saves: one execution, or a route of several.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum IcpswapLegState {
    Single(IcpswapExecutionState),
    Route(Box<IcpswapRouteState>),
}

impl IcpswapLegState {
    /// A route payload carries `route_version`; a payload without it is the
    /// single execution every older row holds.
    pub(super) fn decode(execution: &VenueExecutionState) -> Result<Self, String> {
        if !execution.is_venue(VENUE_ID) {
            return Err("ICPSwap leg execution state decoded to the wrong venue".to_string());
        }
        if execution.state.get("route_version").is_some() {
            let route: IcpswapRouteState = decode_payload(execution)?;
            if route.route_version != ROUTE_STATE_VERSION {
                return Err(format!(
                    "unsupported ICPSwap route version {}; expected {ROUTE_STATE_VERSION}",
                    route.route_version
                ));
            }
            return Ok(Self::Route(Box::new(route)));
        }
        decode_payload(execution).map(Self::Single)
    }

    pub(super) fn encode(&self) -> Result<VenueExecutionState, String> {
        match self {
            Self::Single(state) => VenueExecutionState::new(VENUE_ID, state),
            Self::Route(route) => VenueExecutionState::new(VENUE_ID, route),
        }
    }

    /// The execution that runs now.
    pub(super) fn current(&self) -> &IcpswapExecutionState {
        match self {
            Self::Single(state) => state,
            Self::Route(route) => &route.current.state,
        }
    }

    /// The same leg with the running execution replaced.
    pub(super) fn with_current(&self, state: IcpswapExecutionState) -> Self {
        match self {
            Self::Single(_) => Self::Single(state),
            Self::Route(route) => {
                let mut route = route.clone();
                route.current.state = state;
                Self::Route(route)
            }
        }
    }

    /// Projects the leg onto the generic leg lifecycle.
    pub(super) fn status(&self) -> VenueLegStatus {
        let step = self.current().step;
        match self {
            Self::Single(_) => single_status(step),
            Self::Route(route) if route.parked.is_some() => VenueLegStatus::OperatorRequired,
            Self::Route(route) => match step {
                IcpswapStep::Completed if route.is_last_step() => VenueLegStatus::Completed,
                // The next step is not planned yet.
                IcpswapStep::Completed => VenueLegStatus::Running,
                IcpswapStep::Refunded if route.is_first_step() => VenueLegStatus::Recovered,
                // A later step gave back an intermediate asset, not the collateral.
                IcpswapStep::Refunded => VenueLegStatus::FailedPermanent,
                other => single_status(other),
            },
        }
    }

    /// True when a route stopped after its first step, with an intermediate asset in the trader account.
    pub(super) fn stopped_after_first_step(&self) -> bool {
        matches!(self, Self::Route(route) if route.stopped_after_first_step())
    }

    /// The error a reader of the leg should see.
    pub(super) fn last_error(&self) -> Option<String> {
        match self {
            Self::Single(state) => state.last_error.clone(),
            Self::Route(route) => {
                let step_error = route.current.state.last_error.clone();
                if route.stopped_after_first_step() {
                    return Some(format!(
                        "ICPSwap route stopped at step {}: {} is back in the trader's accounts ({})",
                        route.finished.len() + 1,
                        route.current.state.plan.amount_in.formatted(),
                        step_error.unwrap_or_else(|| "no step error recorded".to_string())
                    ));
                }
                route.parked.clone().or(step_error).or_else(|| route.last_error.clone())
            }
        }
    }
}

fn decode_payload<T: serde::de::DeserializeOwned>(execution: &VenueExecutionState) -> Result<T, String> {
    execution
        .decode::<T>(VENUE_ID)?
        .ok_or_else(|| "ICPSwap leg execution state decoded to the wrong venue".to_string())
}

/// Projects one execution's state machine onto the generic leg lifecycle.
fn single_status(step: IcpswapStep) -> VenueLegStatus {
    match step {
        IcpswapStep::Completed => VenueLegStatus::Completed,
        IcpswapStep::Refunded => VenueLegStatus::Recovered,
        // Each execution has its own signer and pool account. Ambiguous custody
        // can therefore remain parked for an operator without blocking later
        // ICPSwap liquidations.
        IcpswapStep::OperatorRequired => VenueLegStatus::OperatorRequired,
        IcpswapStep::Failed => VenueLegStatus::FailedPermanent,
        _ => VenueLegStatus::Running,
    }
}

/// The signer of an ICPSwap leg, for a single execution and for a route.
pub fn leg_identity(execution: &VenueExecutionState) -> Result<Option<IcpswapExecutionIdentity>, String> {
    #[derive(Deserialize)]
    struct IdentityView {
        identity: IcpswapExecutionIdentity,
    }
    Ok(execution.decode::<IdentityView>(VENUE_ID)?.map(|view| view.identity))
}

/// Which price limit a quote answers to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IcpswapRouteLimits {
    /// Highest price impact of a single pool, in bps.
    pub direct_bps: f64,
    /// Highest total cost of a route over several pools, in bps.
    pub route_bps: f64,
}

/// One quoted step of a path.
struct QuotedStep {
    request: SwapRequest,
    /// The quote on what the step before is expected to pay.
    preview: IcpswapRoutePreview,
    /// The least gross output of this step, when each step before it paid only its minimum.
    floor: ChainTokenAmount,
}

impl QuotedStep {
    /// Returns what the receiver gets when this step pays only its floor.
    fn least_credit(&self) -> Result<ChainTokenAmount, String> {
        let net = net_forwarded_output(&self.floor.value, &self.preview.route.output_ledger_fee.value)
            .map_err(|error| error.to_string())?;
        Ok(ChainTokenAmount::from_raw(self.floor.token.clone(), net))
    }
}

/// One quoted path, with its steps in order.
struct PathQuote {
    path: Vec<ChainToken>,
    steps: Vec<QuotedStep>,
}

impl PathQuote {
    fn last(&self) -> &QuotedStep {
        self.steps.last().expect("a quoted path has at least one step")
    }

    fn net_output(&self) -> &Nat {
        &self.last().preview.quote.receive_amount
    }

    /// Slippage and pool fees of all steps together, against each pool's spot.
    fn cost_bps(&self) -> f64 {
        let kept: f64 = self
            .steps
            .iter()
            .map(|step| 1.0 - step.preview.quote.estimated_price_impact_bps / f64::from(BPS_DENOMINATOR))
            .product();
        (1.0 - kept) * f64::from(BPS_DENOMINATOR)
    }

    fn within(&self, limits: &IcpswapRouteLimits) -> bool {
        let limit = if self.steps.len() > 1 {
            limits.route_bps
        } else {
            limits.direct_bps
        };
        self.cost_bps() < limit
    }

    /// One quote for the whole path, in the shape the planner reads.
    fn quote(&self, request: &SwapRequest) -> SwapQuote {
        let receive = self.net_output().clone();
        let output_token = self.path.last().expect("a quoted path has an output token").clone();
        // Human units on both sides: the two tokens can have different decimals.
        let pay = request.pay_amount.to_f64();
        let exec_price = if pay > 0.0 {
            ChainTokenAmount::from_raw(output_token, receive.clone()).to_f64() / pay
        } else {
            0.0
        };
        SwapQuote {
            pay_asset: request.pay_asset.clone(),
            pay_amount: request.pay_amount.value.clone(),
            receive_asset: request.receive_asset.clone(),
            receive_amount: receive,
            mid_price: self.steps.iter().map(|step| step.preview.quote.mid_price).product(),
            exec_price,
            estimated_price_impact_bps: self.cost_bps(),
            legs: self
                .steps
                .iter()
                .flat_map(|step| step.preview.quote.legs.clone())
                .collect(),
        }
    }
}

/// The largest slippage, up to `cap_bps`, that keeps the minimum of `gross`
/// at or above `floor`. `None` when the quote itself is below the floor.
pub(super) fn slippage_within_floor(gross: &Nat, floor: &Nat, cap_bps: u32) -> Option<u32> {
    if gross < floor || *gross == 0u8 {
        return None;
    }
    let room = (gross.clone() - floor.clone()) * Nat::from(BPS_DENOMINATOR) / gross.clone();
    Some(room.0.to_u32().unwrap_or(BPS_DENOMINATOR).min(cap_bps))
}

/// Sets the plan's minimum so the swap cannot pay less than its floor.
///
/// The floor is an amount fixed at planning. A quote below it, a missing floor
/// or a floor in another token all refuse the plan: the swap does not run.
pub(super) fn hold_to_floor(plan: &mut IcpswapExecutionPlan, floor: Option<&ChainTokenAmount>) -> Result<(), String> {
    let floor = floor.ok_or_else(|| "the route has no floor for this step".to_string())?;
    if floor.token != plan.gross_quoted_out.token {
        return Err(format!(
            "the route floor is in {}, but this step pays {}",
            floor.token.symbol(),
            plan.gross_quoted_out.token.symbol()
        ));
    }
    let gross = plan.gross_quoted_out.value.clone();
    let slippage = slippage_within_floor(&gross, &floor.value, plan.max_slippage_bps).ok_or_else(|| {
        format!(
            "the quote {} is below the route floor {}",
            plan.gross_quoted_out.formatted(),
            floor.formatted()
        )
    })?;
    let minimum = amount_out_minimum(&gross, slippage).map_err(|error| error.to_string())?;
    // Checked again on the final numbers, so an error above cannot pass a low minimum.
    if minimum < floor.value {
        return Err(format!("the swap minimum {minimum} is below the route floor {}", floor.value));
    }
    plan.max_slippage_bps = slippage;
    plan.amount_out_minimum = ChainTokenAmount::from_raw(plan.gross_quoted_out.token.clone(), minimum);
    Ok(())
}

/// Returns the minimum of `expected`, or that of `least` when it is lower.
/// `least` is the same step quoted on the least that the step before can pay.
fn lower_minimum(expected: &IcpswapExecutionPlan, least: Option<IcpswapExecutionPlan>) -> ChainTokenAmount {
    match least {
        Some(least) if least.amount_out_minimum.value < expected.amount_out_minimum.value => least.amount_out_minimum,
        _ => expected.amount_out_minimum.clone(),
    }
}

/// Refuses a plan whose slippage cap is wider than the hard maximum.
fn refuse_wide_slippage(plan: &IcpswapExecutionPlan) -> Result<(), String> {
    if plan.max_slippage_bps > HARD_MAX_SLIPPAGE_BPS {
        return Err(format!(
            "the swap allows {} bps of slippage, above the hard maximum of {HARD_MAX_SLIPPAGE_BPS} bps",
            plan.max_slippage_bps
        ));
    }
    Ok(())
}

/// The slippage cap of a request, never above the hard maximum.
fn capped_slippage(requested: Option<u32>) -> Option<u32> {
    Some(requested.unwrap_or(HARD_MAX_SLIPPAGE_BPS).min(HARD_MAX_SLIPPAGE_BPS))
}

impl IcpswapFinalizer {
    /// Quotes every listed path for the request and returns the one to use.
    ///
    /// A path inside its own limit wins over one outside it; among equals the
    /// larger output wins, then the shorter path.
    pub(super) async fn preview_paths(
        &self,
        context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, VenueAmountError> {
        let paths = listed_paths(&request.pay_amount.token, &request.receive_asset);
        if paths.is_empty() {
            return Err(unsupported_pair(request).to_string().into());
        }
        let mut quotes = Vec::new();
        let mut failures = Vec::new();
        for outcome in join_all(paths.iter().map(|path| self.quote_path(request, path))).await {
            match outcome {
                Ok(quote) => quotes.push(quote),
                Err(error) => failures.push(error),
            }
        }
        // Stable sort: paths arrive shortest first, so equal quotes keep that order.
        quotes.sort_by(|left, right| {
            right
                .within(&self.route_limits)
                .cmp(&left.within(&self.route_limits))
                .then_with(|| right.net_output().cmp(left.net_output()))
        });
        let Some(best) = quotes.into_iter().next() else {
            return Err(format!("no usable ICPSwap path: {}", failures.join("; ")).into());
        };
        self.preview_for(context, request, best)
    }

    /// Quotes one path step by step. Each step trades what the step before
    /// it is expected to pay, and gets its floor from what that step pays at least.
    async fn quote_path(&self, request: &SwapRequest, ledgers: &[Principal]) -> Result<PathQuote, String> {
        let mut path = vec![request.pay_amount.token.clone()];
        let mut steps: Vec<QuotedStep> = Vec::new();

        for (index, pair) in ledgers.windows(2).enumerate() {
            let is_last = pays_output(ledgers.len(), index);
            let receive_asset = if is_last {
                request.receive_asset.clone()
            } else {
                self.route_tokens
                    .get(&pair[1])
                    .map(ChainToken::asset_id)
                    .ok_or_else(|| format!("path through {} has no configured token", pair[1]))?
            };
            let in_step = |error: String| format!("step {} -> {}: {error}", pair[0], pair[1]);
            // The first step trades the request amount, so it has one quote only.
            let (expected_pay, least_pay) = match steps.last() {
                None => (request.pay_amount.clone(), None),
                Some(before) => (
                    before.preview.route.net_expected_output(),
                    Some(before.least_credit().map_err(in_step)?),
                ),
            };
            let step_request = self.step_request(request, expected_pay, receive_asset.clone(), is_last);
            let least_request = least_pay.map(|pay| self.step_request(request, pay, receive_asset, is_last));
            let least_quote = OptionFuture::from(least_request.as_ref().map(|least| self.plan_step(least)));
            let (preview, least_preview) = futures::join!(self.plan_step(&step_request), least_quote);
            let preview = preview.map_err(in_step)?;
            let least_plan = least_preview.transpose().map_err(in_step)?.map(|least| least.route);
            let floor = lower_minimum(&preview.route, least_plan);
            path.push(floor.token.clone());
            steps.push(QuotedStep {
                request: step_request,
                preview,
                floor,
            });
        }
        Ok(PathQuote { path, steps })
    }

    /// Quotes one pool for one step request.
    async fn plan_step(&self, step_request: &SwapRequest) -> Result<IcpswapRoutePreview, String> {
        let preview = self
            .workflow
            .preview_route(step_request)
            .await
            .map_err(|error| error.to_string())?;
        refuse_wide_slippage(&preview.route)?;
        Ok(preview)
    }

    /// The request of one step. It keeps the route's receiver, slippage cap and
    /// venue hint; a step before the last pays the trader, which funds the next.
    fn step_request(
        &self,
        route_request: &SwapRequest,
        pay_amount: ChainTokenAmount,
        receive_asset: AssetId,
        is_last: bool,
    ) -> SwapRequest {
        SwapRequest {
            pay_asset: pay_amount.token.asset_id(),
            pay_amount,
            receive_asset,
            receive_address: if is_last {
                route_request.receive_address.clone()
            } else {
                Some(self.trader.owner.to_text())
            },
            max_slippage_bps: capped_slippage(route_request.max_slippage_bps),
            venue_hint: route_request.venue_hint.clone(),
        }
    }

    /// Turns the chosen path into the preview the planner commits.
    fn preview_for(
        &self,
        context: &VenuePlanningContext,
        request: &SwapRequest,
        best: PathQuote,
    ) -> Result<VenueRoutePreview, VenueAmountError> {
        // The planner checks the edge on this amount, so it is the least that the whole route can pay.
        let conservative_receive = best.last().least_credit()?;
        let floors = best.steps.iter().map(|step| step.floor.clone()).collect();
        let quote = if best.steps.len() > 1 {
            best.quote(request)
        } else {
            best.last().preview.quote.clone()
        };

        // The selected preview is persisted before execution, so its generated
        // ID becomes the durable idempotency key used for every later advance.
        let execution_id = crate::utils::new_venue_execution_id(VENUE_ID);
        let is_route = best.steps.len() > 1;
        let first = best.steps.into_iter().next().expect("a quoted path has a step");
        let first_request = first.request;
        let state =
            self.prepare_execution_state(&execution_id, &context.liquidation_id, &first_request, first.preview.route)?;
        let leg = if is_route {
            if request.receive_address.is_none() {
                return Err("ICPSwap request is missing receive_address".into());
            }
            IcpswapLegState::Route(Box::new(IcpswapRouteState {
                route_version: ROUTE_STATE_VERSION,
                identity: state.identity.clone(),
                request: request.clone(),
                path: best.path,
                floors,
                finished: Vec::new(),
                current: IcpswapRouteStep {
                    request: first_request,
                    state,
                },
                plan_attempts: 0,
                next_plan_at_nanos: None,
                parked: None,
                last_error: None,
            }))
        } else {
            IcpswapLegState::Single(state)
        };
        Ok(VenueRoutePreview::new(
            VENUE_ID,
            request.clone(),
            quote,
            conservative_receive,
            leg.encode()?,
        ))
    }

    /// Plans the step after a completed one and makes it the current step.
    ///
    /// A plan that fails changes nothing but the wait: the route tries again
    /// after the cooldown and asks an operator once the attempts are spent.
    pub(super) async fn plan_next_step(&self, route: &mut IcpswapRouteState, now_nanos: u64) -> bool {
        if route.next_plan_at_nanos.is_some_and(|ready_at| now_nanos < ready_at) {
            return false;
        }
        match self.next_step(route).await {
            Ok(next) => {
                let done = std::mem::replace(&mut route.current, next);
                route.finished.push(done);
                route.plan_attempts = 0;
                route.next_plan_at_nanos = None;
                route.last_error = None;
                true
            }
            Err(error) => {
                route.plan_attempts = route.plan_attempts.saturating_add(1);
                let held = route.held_between_steps();
                let message = format!(
                    "ICPSwap route holds {held} after step {}: the next swap is not planned ({error})",
                    route.finished.len() + 1
                );
                // One line at the first failure, so the retries that follow are not silent.
                if route.plan_attempts == 1 {
                    warn!(
                        liq_id = %route.identity.liquidation_id,
                        "[icpswap] {message}; the bot tries again for up to {MAX_ROUTE_PLAN_ATTEMPTS} cycles, then asks an operator"
                    );
                }
                if route.plan_attempts >= MAX_ROUTE_PLAN_ATTEMPTS {
                    route.parked = Some(message.clone());
                    route.next_plan_at_nanos = None;
                } else {
                    route.next_plan_at_nanos = Some(now_nanos.saturating_add(RETRY_COOLDOWN_NANOS));
                }
                route.last_error = Some(message);
                false
            }
        }
    }

    /// Quotes the next pool on the amount the finished step credited.
    async fn next_step(&self, route: &IcpswapRouteState) -> Result<IcpswapRouteStep, String> {
        let done = &route.current;
        let received = done
            .state
            .settlement
            .transfer
            .credited_amount
            .clone()
            .ok_or_else(|| "the finished step has no confirmed credit".to_string())?;
        let index = route.finished.len() + 1;
        let (pay_token, receive_token) = match (route.path.get(index), route.path.get(index + 1)) {
            (Some(pay), Some(receive)) => (pay.clone(), receive),
            _ => return Err("the route has no step left to plan".to_string()),
        };
        let is_last = pays_output(route.path.len(), index);
        let request = self.step_request(
            &route.request,
            ChainTokenAmount::from_raw(pay_token, received),
            receive_token.asset_id(),
            is_last,
        );
        let mut plan = self
            .workflow
            .preview_route(&request)
            .await
            .map_err(|error| error.to_string())?
            .route;
        refuse_wide_slippage(&plan)?;
        hold_to_floor(&mut plan, route.floors.get(index))?;
        let execution_id = crate::utils::new_venue_execution_id(VENUE_ID);
        let state = self.prepare_execution_state(&execution_id, &route.identity.liquidation_id, &request, plan)?;
        Ok(IcpswapRouteStep { request, state })
    }

    /// One result for the whole route, built from each step's own result.
    pub(super) async fn route_result(
        &self,
        request: &SwapRequest,
        route: &IcpswapRouteState,
        now_nanos: u64,
    ) -> Result<SwapExecution, String> {
        let mut steps = Vec::new();
        for step in route.finished.iter().chain(std::iter::once(&route.current)) {
            steps.push(self.workflow.finish(&step.request, &step.state, now_nanos).await?);
        }
        let last = steps.last().expect("a route has a current step").clone();
        let receive_token = route.current.state.plan.gross_quoted_out.token.clone();
        let pay = request.pay_amount.to_f64();
        let receive = ChainTokenAmount::from_raw(receive_token, last.receive_amount.clone()).to_f64();
        let mid_price: f64 = steps.iter().map(|step| step.mid_price).product();
        let expected = mid_price * pay;
        Ok(SwapExecution {
            pay_asset: request.pay_asset.clone(),
            pay_amount: request.pay_amount.value.clone(),
            mid_price,
            exec_price: if pay > 0.0 { receive / pay } else { 0.0 },
            realized_slippage_bps: if expected > 0.0 {
                adverse_price_impact_bps(expected, receive)
            } else {
                0.0
            },
            legs: steps.into_iter().flat_map(|step| step.legs).collect(),
            ..last
        })
    }

    /// Alerts that a route stopped after its first step. The bot does not swap the held asset, so an operator must.
    pub(super) async fn notify_route_stopped(&self, leg: &IcpswapLegState) {
        let reason = leg.last_error().unwrap_or_else(|| "no error recorded".to_string());
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: leg.current().execution_id.clone(),
                venue: VENUE_ID.to_string(),
                pending_step: "ManualSwap".to_string(),
                owner: self.trader.to_string(),
                details: format!("{reason}. The bot does not swap it: swap it by hand"),
            })
            .await;
    }

    /// Alerts while a route waits for an operator before its next step.
    pub(super) async fn notify_route_parked(&self, route: &IcpswapRouteState) {
        let Some(reason) = route.parked.as_ref() else {
            return;
        };
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: route.current.state.execution_id.clone(),
                venue: VENUE_ID.to_string(),
                pending_step: "RoutePlan".to_string(),
                owner: self.trader.to_string(),
                details: reason.clone(),
            })
            .await;
    }
}

/// What a route parked between two steps asks an operator.
pub(super) fn route_questions(route: &IcpswapRouteState) -> Option<Vec<OperatorQuestion>> {
    route.parked.as_ref().map(|reason| {
        vec![OperatorQuestion {
            detail: format!("{reason}: --release plans the next swap again"),
        }]
    })
}

/// Lets a parked route plan its next step again. No answer applies here: the
/// held asset is in the trader account, and only a new quote can move it.
pub(super) fn release_route(route: &mut IcpswapRouteState, answer: Option<&OperatorDecision>) -> Result<(), String> {
    if answer.is_some() {
        return Err("a route between two swaps takes --release only".to_string());
    }
    route.parked = None;
    route.plan_attempts = 0;
    route.next_plan_at_nanos = None;
    Ok(())
}
