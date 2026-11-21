use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{
    IcpswapRouteLimits,
    finalizer::{IcpswapFinalizer, RETRY_COOLDOWN_NANOS},
    hop_route::{
        HARD_MAX_SLIPPAGE_BPS, IcpswapLegState, IcpswapRouteState, MAX_ROUTE_PLAN_ATTEMPTS, hold_to_floor,
        leg_identity, slippage_within_floor,
    },
    tests::{
        RecordingCheckpoint, RecordingWatchdog, TEST_MNEMONIC, TestIcpswapClient, TestSessionFactory,
        planning_context, receiver, trader,
    },
};
use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenueAmountError, VenueLegProgress, VenueRoutePreview},
    outbound::driver::OperatorDecision,
    persistance::{VenueLegQuote, VenueLegState, VenueLegStatus},
    swappers::{
        icpswap::{
            VENUE_ID,
            client::MockIcpswapManualClient,
            execution::IcpswapExecutionStateStore,
            identity::IcpswapExecutionIdentity,
            session::IcpswapExecutionSession,
            types::{IcpswapExecutionPlan, IcpswapQuoteError, IcpswapRoutePreview, IcpswapState, IcpswapStep},
            venue::IcpswapFinalizerLogic,
        },
        model::{SwapQuote, SwapQuoteLeg, SwapRequest},
    },
    utils::{CKBTC_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL},
};

const LEDGER_FEE: u64 = 10;
const PAY: u64 = 1_000_000;
const START_NANOS: u64 = 1_000_000_000;

fn ledger(text: &str) -> Principal {
    Principal::from_text(text).expect("test ledger")
}

fn token(ledger_text: &str, symbol: &str) -> ChainToken {
    ChainToken::Icp {
        ledger: ledger(ledger_text),
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(LEDGER_FEE),
    }
}

fn ckbtc() -> ChainToken {
    token(CKBTC_LEDGER_PRINCIPAL, "ckBTC")
}

fn icp() -> ChainToken {
    token(ICP_LEDGER_PRINCIPAL, "ICP")
}

fn ckusdc() -> ChainToken {
    token(CKUSDC_LEDGER_PRINCIPAL, "ckUSDC")
}

fn ckusdt() -> ChainToken {
    token(CKUSDT_LEDGER_PRINCIPAL, "ckUSDT")
}

/// One scripted pool: it pays `numerator / denominator` per input unit.
#[derive(Clone, Copy)]
struct Pool {
    numerator: u64,
    denominator: u64,
    impact_bps: f64,
}

fn pool(numerator: u64, denominator: u64, impact_bps: f64) -> Pool {
    Pool {
        numerator,
        denominator,
        impact_bps,
    }
}

#[derive(Clone)]
enum StepOutcome {
    /// The step pays its minimum, not its quote.
    FillAtMinimum,
    Refund,
    Fail(String),
}

/// A workflow that quotes from scripted pools and settles a step at once.
#[derive(Default)]
struct ScriptedWorkflow {
    pools: Mutex<HashMap<(Principal, Principal), Pool>>,
    /// What a step does, by its input ledger. A step with no entry completes.
    outcomes: Mutex<HashMap<Principal, StepOutcome>>,
}

impl ScriptedWorkflow {
    fn with_pools(pools: &[(&ChainToken, &ChainToken, Pool)]) -> Arc<Self> {
        let workflow = Self::default();
        for (pay, receive, pool) in pools {
            workflow.set_pool(pay, receive, *pool);
        }
        Arc::new(workflow)
    }

    fn set_pool(&self, pay: &ChainToken, receive: &ChainToken, pool: Pool) {
        self.pools
            .lock()
            .expect("pools lock")
            .insert((token_ledger(pay), token_ledger(receive)), pool);
    }

    fn set_outcome(&self, input: &ChainToken, outcome: StepOutcome) {
        self.outcomes
            .lock()
            .expect("outcomes lock")
            .insert(token_ledger(input), outcome);
    }
}

fn token_ledger(token: &ChainToken) -> Principal {
    match token {
        ChainToken::Icp { ledger, .. } => *ledger,
        other => panic!("test token {other} is not an ICP ledger token"),
    }
}

fn known_token(ledger: Principal) -> ChainToken {
    [ckbtc(), icp(), ckusdc(), ckusdt()]
        .into_iter()
        .find(|token| token_ledger(token) == ledger)
        .expect("test token")
}

#[async_trait]
impl IcpswapFinalizerLogic for ScriptedWorkflow {
    async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        let pay_token = request.pay_amount.token.clone();
        let pay_ledger = token_ledger(&pay_token);
        let receive_ledger = ledger(&request.receive_asset.address);
        let receive_token = known_token(receive_ledger);
        let pool = self
            .pools
            .lock()
            .expect("pools lock")
            .get(&(pay_ledger, receive_ledger))
            .copied()
            .ok_or_else(|| IcpswapQuoteError::NoUsablePools {
                failures: vec![format!("no scripted pool {pay_ledger} -> {receive_ledger}")],
            })?;

        let fee = Nat::from(LEDGER_FEE);
        let amount_in = request.pay_amount.value.clone() - fee.clone() * Nat::from(3u8);
        let gross = amount_in.clone() * Nat::from(pool.numerator) / Nat::from(pool.denominator);
        let plan = IcpswapExecutionPlan::new(
            Principal::from_slice(&[9]),
            pay_ledger,
            receive_ledger,
            Nat::from(3_000u64),
            ChainTokenAmount::from_raw(pay_token.clone(), amount_in.clone()),
            ChainTokenAmount::from_raw(pay_token.clone(), fee.clone()),
            ChainTokenAmount::from_raw(receive_token.clone(), gross),
            ChainTokenAmount::from_raw(receive_token.clone(), fee),
            request.max_slippage_bps.unwrap_or(500),
        )?;
        let net = plan.net_expected_output().value;
        let price = pool.numerator as f64 / pool.denominator as f64;
        Ok(IcpswapRoutePreview {
            quote: SwapQuote {
                pay_asset: request.pay_asset.clone(),
                pay_amount: request.pay_amount.value.clone(),
                receive_asset: request.receive_asset.clone(),
                receive_amount: net.clone(),
                mid_price: price,
                exec_price: price,
                estimated_price_impact_bps: pool.impact_bps,
                legs: vec![SwapQuoteLeg {
                    venue: VENUE_ID.to_string(),
                    route_id: plan.pool.to_text(),
                    pay_chain: "ICP".to_string(),
                    pay_symbol: pay_token.symbol(),
                    pay_amount: amount_in,
                    receive_chain: "ICP".to_string(),
                    receive_symbol: receive_token.symbol(),
                    receive_amount: net,
                    price,
                    lp_fee: Nat::from(0u8),
                    gas_fee: Nat::from(0u8),
                }],
            },
            route: plan,
        })
    }

    async fn advance_loaded(
        &self,
        _session: &IcpswapExecutionSession,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        _now_nanos: u64,
        mut state: IcpswapState,
    ) -> Result<IcpswapState, String> {
        let outcome = self
            .outcomes
            .lock()
            .expect("outcomes lock")
            .get(&state.plan.token_in)
            .cloned();
        match outcome {
            Some(StepOutcome::Fail(error)) => return Err(error),
            Some(StepOutcome::Refund) => state.step = IcpswapStep::Refunded,
            Some(StepOutcome::FillAtMinimum) => {
                let minimum = state.plan.amount_out_minimum.value.clone();
                let fees = state.plan.output_ledger_fee.value.clone() * Nat::from(2u8);
                state.trade.gross_output_amount = Some(minimum.clone());
                state.settlement.transfer.credited_amount = Some(minimum - fees);
                state.step = IcpswapStep::Completed;
            }
            None => {
                state.trade.gross_output_amount = Some(state.plan.gross_quoted_out.value.clone());
                state.settlement.transfer.credited_amount = Some(state.plan.net_expected_output().value);
                state.step = IcpswapStep::Completed;
            }
        }
        store.persist(execution_id, &state).await?;
        Ok(state)
    }
}

/// Sessions are opened but not used: the scripted workflow settles each step itself.
fn sessions() -> Arc<TestSessionFactory> {
    Arc::new(TestSessionFactory {
        client: Arc::new(TestIcpswapClient {
            manual: MockIcpswapManualClient::new(),
            preview: None,
        }),
    })
}

struct Harness {
    finalizer: IcpswapFinalizer,
    workflow: Arc<ScriptedWorkflow>,
    clock: Arc<AtomicU64>,
}

fn limits() -> IcpswapRouteLimits {
    IcpswapRouteLimits {
        direct_bps: 100.0,
        route_bps: 300.0,
    }
}

fn harness(workflow: Arc<ScriptedWorkflow>) -> Harness {
    let clock = Arc::new(AtomicU64::new(START_NANOS));
    let reader = clock.clone();
    let finalizer = IcpswapFinalizer::from_workflow_with_clock(
        workflow.clone(),
        trader(),
        sessions(),
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .with_routes(vec![ckbtc(), icp(), ckusdc(), ckusdt()], limits());
    Harness {
        finalizer,
        workflow,
        clock,
    }
}

fn request(pay: &ChainToken, receive: &ChainToken) -> SwapRequest {
    SwapRequest {
        pay_asset: pay.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(pay.clone(), Nat::from(PAY)),
        receive_asset: receive.asset_id(),
        receive_address: Some(receiver().owner.to_text()),
        max_slippage_bps: Some(500),
        venue_hint: Some(VENUE_ID.to_string()),
    }
}

/// A request that pays `pay` base units.
fn request_paying(pay: &ChainToken, receive: &ChainToken, amount: u64) -> SwapRequest {
    let mut request = request(pay, receive);
    request.pay_amount = ChainTokenAmount::from_raw(pay.clone(), Nat::from(amount));
    request
}

async fn preview(harness: &Harness, request: &SwapRequest) -> VenueRoutePreview {
    harness
        .finalizer
        .preview(&planning_context(), request)
        .await
        .expect("preview")
}

/// Runs one advance of a new leg for the preview.
async fn advance_once(harness: &Harness, preview: &VenueRoutePreview) -> VenueLegProgress {
    harness
        .finalizer
        .advance(&leg_from(preview), &RecordingCheckpoint::default())
        .await
        .expect("advance")
}

/// Returns the floor of the last step of the planned route.
fn last_floor(preview: &VenueRoutePreview) -> ChainTokenAmount {
    route_of(&preview.initial_execution_state)
        .floors
        .last()
        .cloned()
        .expect("last floor")
}

fn leg_from(preview: &VenueRoutePreview) -> VenueLegState {
    VenueLegState {
        leg_id: "icpswap-0".to_string(),
        venue_id: VENUE_ID.to_string(),
        request: preview.request.clone(),
        quote: VenueLegQuote {
            pay_amount: preview.request.pay_amount.clone(),
            estimated_receive: ChainTokenAmount::from_raw(
                preview.conservative_receive.token.clone(),
                preview.quote.receive_amount.clone(),
            ),
            conservative_receive: preview.conservative_receive.clone(),
            estimated_price_impact_bps: preview.quote.estimated_price_impact_bps,
            route_id: "route".to_string(),
            side_costs: Vec::new(),
        },
        execution: preview.initial_execution_state.clone(),
        status: VenueLegStatus::Planned,
        result: None,
        last_error: None,
        failure: None,
    }
}

/// The leg as the parent would save it after one advance.
fn saved(leg: &VenueLegState, progress: &VenueLegProgress) -> VenueLegState {
    VenueLegState {
        execution: progress.execution.clone(),
        status: progress.status,
        ..leg.clone()
    }
}

fn route_of(execution: &crate::persistance::VenueExecutionState) -> IcpswapRouteState {
    match IcpswapLegState::decode(execution).expect("decode leg") {
        IcpswapLegState::Route(route) => *route,
        IcpswapLegState::Single(_) => panic!("expected a route, found one execution"),
    }
}

fn symbols(route: &IcpswapRouteState) -> Vec<String> {
    route.path.iter().map(ChainToken::symbol).collect()
}

/// ckBTC -> ckUSDC with a thin direct pool and two good pools through ICP.
fn thin_direct_pools() -> Arc<ScriptedWorkflow> {
    ScriptedWorkflow::with_pools(&[
        (&ckbtc(), &ckusdc(), pool(8, 10, 1_600.0)),
        (&ckbtc(), &icp(), pool(1, 1, 30.0)),
        (&icp(), &ckusdc(), pool(1, 1, 40.0)),
    ])
}

/// ckBTC -> ICP -> ckUSDC -> ckUSDT: the only good path when ICP -> ckUSDT is weak.
fn three_step_pools() -> Arc<ScriptedWorkflow> {
    ScriptedWorkflow::with_pools(&[
        (&ckbtc(), &icp(), pool(1, 1, 30.0)),
        (&icp(), &ckusdt(), pool(87, 100, 1_300.0)),
        (&icp(), &ckusdc(), pool(1, 1, 40.0)),
        (&ckusdc(), &ckusdt(), pool(1, 1, 30.0)),
    ])
}

#[tokio::test]
async fn a_thin_direct_pool_gives_way_to_the_route_through_icp() {
    let harness = harness(thin_direct_pools());
    let request = request(&ckbtc(), &ckusdc());

    let preview = preview(&harness, &request).await;

    let route = route_of(&preview.initial_execution_state);
    assert_eq!(symbols(&route), ["ckBTC", "ICP", "ckUSDC"]);
    assert_eq!(preview.quote.legs.len(), 2);
    // 1 - (1 - 0.0030) * (1 - 0.0040) = 69.88 bps for the two pools together.
    assert!((preview.quote.estimated_price_impact_bps - 69.88).abs() < 0.01);
    // The first step pays the trader, which funds the second step.
    assert_eq!(
        route.current.request.receive_address,
        Some(trader().owner.to_text())
    );
    assert_eq!(route.request.receive_address, Some(receiver().owner.to_text()));
    assert!(route.finished.is_empty());
}

#[tokio::test]
async fn the_quoted_price_is_in_human_units_when_decimals_differ() {
    let harness = harness(thin_direct_pools());
    let six_decimal_ckbtc = ChainToken::Icp {
        ledger: ledger(CKBTC_LEDGER_PRINCIPAL),
        symbol: "ckBTC".to_string(),
        decimals: 6,
        fee: Nat::from(LEDGER_FEE),
    };
    // PAY is 1.0 of a 6-decimal token; the output token has 8 decimals.
    let request = request(&six_decimal_ckbtc, &ckusdc());

    let preview = preview(&harness, &request).await;

    let received = ChainTokenAmount::from_raw(ckusdc(), preview.quote.receive_amount.clone()).to_f64();
    assert!(received < 0.02, "about 0.01 of the output token: {received}");
    assert!((preview.quote.exec_price - received).abs() < 1e-12);
}

#[tokio::test]
async fn a_direct_pool_inside_its_limit_wins_when_it_pays_more() {
    let workflow = ScriptedWorkflow::with_pools(&[
        (&ckbtc(), &ckusdc(), pool(1, 1, 50.0)),
        (&ckbtc(), &icp(), pool(1, 1, 30.0)),
        (&icp(), &ckusdc(), pool(1, 1, 40.0)),
    ]);
    let harness = harness(workflow);

    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;

    let leg = IcpswapLegState::decode(&preview.initial_execution_state).expect("decode leg");
    assert!(matches!(leg, IcpswapLegState::Single(_)));
    assert_eq!(preview.quote.legs.len(), 1);
}

#[tokio::test]
async fn a_route_inside_its_limit_wins_over_a_direct_pool_outside_its_own() {
    // The direct pool pays more, but its 150 bps is over the 100 bps limit of one pool.
    let workflow = ScriptedWorkflow::with_pools(&[
        (&ckbtc(), &ckusdc(), pool(101, 100, 150.0)),
        (&ckbtc(), &icp(), pool(1, 1, 60.0)),
        (&icp(), &ckusdc(), pool(1, 1, 60.0)),
    ]);
    let harness = harness(workflow);

    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;

    assert_eq!(symbols(&route_of(&preview.initial_execution_state)), ["ckBTC", "ICP", "ckUSDC"]);
}

#[tokio::test]
async fn a_target_with_a_weak_last_pool_takes_three_steps() {
    // ICP -> ckUSDT is the weak pool, so the route ends ckUSDC -> ckUSDT.
    let harness = harness(three_step_pools());

    let preview = preview(&harness, &request(&ckbtc(), &ckusdt())).await;

    let route = route_of(&preview.initial_execution_state);
    assert_eq!(symbols(&route), ["ckBTC", "ICP", "ckUSDC", "ckUSDT"]);
    assert_eq!(preview.quote.legs.len(), 3);
}

#[tokio::test]
async fn without_route_tokens_only_the_direct_pool_is_quoted() {
    let finalizer = IcpswapFinalizer::from_workflow_with_clock(
        thin_direct_pools(),
        trader(),
        sessions(),
        Arc::new(|| START_NANOS),
    );

    let preview = finalizer
        .preview(&planning_context(), &request(&ckbtc(), &ckusdc()))
        .await
        .expect("preview");

    let leg = IcpswapLegState::decode(&preview.initial_execution_state).expect("decode leg");
    assert!(matches!(leg, IcpswapLegState::Single(_)));
}

#[tokio::test]
async fn each_step_trades_what_the_step_before_it_paid() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let leg = leg_from(&preview);
    let checkpoint = RecordingCheckpoint::default();

    let progress = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");

    assert_eq!(progress.status, VenueLegStatus::Completed);
    let route = route_of(&progress.execution);
    assert_eq!(route.finished.len(), 1);
    let first_paid = route.finished[0]
        .state
        .settlement
        .transfer
        .credited_amount
        .clone()
        .expect("first step credit");
    assert_eq!(route.current.request.pay_amount.value, first_paid);
    assert_eq!(route.current.request.receive_address, Some(receiver().owner.to_text()));

    let result = progress.result.expect("route result");
    assert_eq!(result.pay_amount, Nat::from(PAY));
    assert_eq!(result.legs.len(), 2);
    assert_eq!(
        Some(result.receive_amount),
        route.current.state.settlement.transfer.credited_amount
    );

    // The second step is saved before it runs, and no saved state is final.
    let saved_steps = checkpoint.progresses.lock().expect("checkpoint lock");
    assert!(saved_steps.iter().all(|saved| saved.status == VenueLegStatus::Running));
    assert!(saved_steps.iter().any(|saved| {
        let route = route_of(&saved.execution);
        route.finished.len() == 1 && route.current.state.step == IcpswapStep::Funding
    }));
}

#[tokio::test]
async fn the_last_step_never_accepts_less_than_the_floor() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let floor = last_floor(&preview);
    // The second pool now quotes 8% less than at planning. The first step paid its quote,
    // so its unused 5% leaves about 2% for this swap.
    harness.workflow.set_pool(&icp(), &ckusdc(), pool(92, 100, 40.0));

    let progress = advance_once(&harness, &preview).await;

    let route = route_of(&progress.execution);
    let last_plan = &route.current.state.plan;
    assert!(last_plan.amount_out_minimum.value >= floor.value);
    assert!(last_plan.max_slippage_bps < 500);
    assert!(last_plan.max_slippage_bps >= 180);
}

#[tokio::test]
async fn a_first_step_at_its_minimum_leaves_the_next_step_its_full_slippage() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let floor = last_floor(&preview);
    harness.workflow.set_outcome(&ckbtc(), StepOutcome::FillAtMinimum);

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Completed);
    let last_plan = route_of(&progress.execution).current.state.plan;
    assert!(last_plan.amount_out_minimum.value >= floor.value);
    assert_eq!(last_plan.max_slippage_bps, 500);
}

#[tokio::test]
async fn the_conservative_amount_is_the_least_that_the_whole_route_can_pay() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let route = route_of(&preview.initial_execution_state);

    // Step 1: 999,970 in, minimum 949,971, credit 949,951. Step 2 on that credit:
    // 949,921 quoted, minimum 902,424. Two output fees leave 902,404.
    assert_eq!(route.floors[0].value, Nat::from(949_971u64));
    assert_eq!(route.floors[1].value, Nat::from(902_424u64));
    assert_eq!(preview.conservative_receive.value, Nat::from(902_404u64));
    // The expected amount still follows the quotes.
    assert_eq!(preview.quote.receive_amount, Nat::from(999_900u64));
}

#[tokio::test]
async fn every_step_has_a_floor_fixed_at_planning() {
    let harness = harness(three_step_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdt())).await;
    let planned = route_of(&preview.initial_execution_state);
    assert_eq!(planned.floors.len(), 3);
    assert_eq!(planned.floors[0], planned.current.state.plan.amount_out_minimum);

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Completed);
    let route = route_of(&progress.execution);
    let steps: Vec<_> = route.finished.iter().chain(std::iter::once(&route.current)).collect();
    for (step, floor) in steps.iter().zip(&route.floors) {
        assert!(step.state.plan.amount_out_minimum.value >= floor.value);
        assert!(step.state.plan.max_slippage_bps <= HARD_MAX_SLIPPAGE_BPS);
    }
}

#[tokio::test]
async fn a_middle_step_does_not_run_below_its_floor() {
    let harness = harness(three_step_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdt())).await;
    // The middle pool now pays 20% less than at planning.
    harness.workflow.set_pool(&icp(), &ckusdc(), pool(8, 10, 40.0));

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Running);
    let error = progress.last_error.expect("route error");
    assert!(error.contains("below the route floor"), "{error}");
    // The first step paid, and nothing was swapped after it.
    let route = route_of(&progress.execution);
    assert!(route.finished.is_empty());
    assert_eq!(route.current.state.plan.token_in, token_ledger(&ckbtc()));
}

#[tokio::test]
async fn no_swap_accepts_more_than_the_hard_slippage_maximum() {
    let harness = harness(thin_direct_pools());
    for requested in [Some(2_000), None] {
        let mut wide = request(&ckbtc(), &ckusdc());
        wide.max_slippage_bps = requested;
        let route = route_of(&preview(&harness, &wide).await.initial_execution_state);
        assert_eq!(route.current.state.plan.max_slippage_bps, HARD_MAX_SLIPPAGE_BPS);

        let mut wide = request(&ckbtc(), &icp());
        wide.max_slippage_bps = requested;
        let single = IcpswapLegState::decode(&preview(&harness, &wide).await.initial_execution_state)
            .expect("decode leg");
        assert_eq!(single.current().plan.max_slippage_bps, HARD_MAX_SLIPPAGE_BPS);
    }
}

#[tokio::test]
async fn a_floor_that_does_not_fit_the_step_refuses_it() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let plan = route_of(&preview.initial_execution_state).current.state.plan;

    // No floor recorded for the step.
    assert!(hold_to_floor(&mut plan.clone(), None).is_err());
    // A floor in another token.
    let wrong_token = ChainTokenAmount::from_raw(ckusdt(), Nat::from(1u8));
    assert!(hold_to_floor(&mut plan.clone(), Some(&wrong_token)).is_err());
    // A floor above the quote.
    let above = ChainTokenAmount::from_raw(
        plan.gross_quoted_out.token.clone(),
        plan.gross_quoted_out.value.clone() + Nat::from(1u8),
    );
    assert!(hold_to_floor(&mut plan.clone(), Some(&above)).is_err());
    // A floor at the quote leaves no slippage at all.
    let mut exact = plan.clone();
    hold_to_floor(&mut exact, Some(&plan.gross_quoted_out)).expect("floor at the quote");
    assert_eq!(exact.max_slippage_bps, 0);
    assert_eq!(exact.amount_out_minimum, plan.gross_quoted_out);
}

#[tokio::test]
async fn a_quote_below_the_floor_waits_then_asks_an_operator_and_resumes_on_release() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let leg = leg_from(&preview);
    let checkpoint = RecordingCheckpoint::default();
    // The second pool now pays half: far below the floor.
    harness.workflow.set_pool(&icp(), &ckusdc(), pool(1, 2, 40.0));

    let waiting = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");

    assert_eq!(waiting.status, VenueLegStatus::Running);
    assert!(waiting.retryable_error.is_none());
    let error = waiting.last_error.clone().expect("route error");
    assert!(error.contains("below the route floor"), "{error}");
    assert!(error.contains("ICP"), "{error}");
    let route = route_of(&waiting.execution);
    assert_eq!(route.plan_attempts, 1);
    assert_eq!(route.next_plan_at_nanos, Some(START_NANOS + RETRY_COOLDOWN_NANOS));
    assert!(route.finished.is_empty());

    // Before the cooldown ends nothing is planned again.
    let leg = saved(&leg, &waiting);
    let still = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");
    assert_eq!(route_of(&still.execution).plan_attempts, 1);

    // Each later cycle plans once more, until the attempts are spent.
    let mut leg = saved(&leg, &still);
    let mut last = still;
    for attempt in 2..=MAX_ROUTE_PLAN_ATTEMPTS {
        harness
            .clock
            .store(START_NANOS + u64::from(attempt) * (RETRY_COOLDOWN_NANOS + 1), Ordering::SeqCst);
        last = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");
        leg = saved(&leg, &last);
    }
    assert_eq!(last.status, VenueLegStatus::OperatorRequired);
    let questions = harness.finalizer.operator_questions(&leg).expect("questions");
    assert_eq!(questions.len(), 1);
    assert!(questions[0].detail.contains("--release"), "{}", questions[0].detail);

    // A parked route does not plan, and it takes a release only.
    let parked = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");
    assert_eq!(parked.status, VenueLegStatus::OperatorRequired);
    assert!(
        harness
            .finalizer
            .operator_release(&leg, Some(&OperatorDecision::Absent))
            .is_err()
    );

    harness.workflow.set_pool(&icp(), &ckusdc(), pool(1, 1, 40.0));
    leg.execution = harness.finalizer.operator_release(&leg, None).expect("release");
    let done = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");
    assert_eq!(done.status, VenueLegStatus::Completed);
}

#[tokio::test]
async fn a_first_step_that_refunds_recovers_the_collateral() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    harness.workflow.set_outcome(&ckbtc(), StepOutcome::Refund);

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Recovered);
}

#[tokio::test]
async fn a_later_step_that_refunds_fails_the_leg_and_names_the_held_asset() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    harness.workflow.set_outcome(&icp(), StepOutcome::Refund);

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::FailedPermanent);
    let error = progress.last_error.expect("route error");
    assert!(error.contains("stopped at step 2"), "{error}");
    assert!(error.contains("ICP"), "{error}");
}

#[tokio::test]
async fn a_later_step_that_refunds_alerts_an_operator_one_time() {
    let mut harness = harness(thin_direct_pools());
    let watchdog = Arc::new(RecordingWatchdog::default());
    harness.finalizer = harness.finalizer.with_watchdog(watchdog.clone());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    harness.workflow.set_outcome(&icp(), StepOutcome::Refund);
    let leg = leg_from(&preview);
    let checkpoint = RecordingCheckpoint::default();

    let stopped = harness.finalizer.advance(&leg, &checkpoint).await.expect("advance");

    let events = watchdog.events();
    assert_eq!(events.len(), 1);
    let (_, venue, pending_step, owner, details) = &events[0];
    assert_eq!(venue, VENUE_ID);
    assert_eq!(pending_step, "ManualSwap");
    assert_eq!(owner, &trader().to_string());
    assert!(details.contains("stopped at step 2"), "{details}");
    assert!(details.contains("ICP"), "{details}");
    assert!(details.contains("swap it by hand"), "{details}");

    // A later advance of the stopped leg sends no second alert.
    harness
        .finalizer
        .advance(&saved(&leg, &stopped), &checkpoint)
        .await
        .expect("advance");
    assert_eq!(watchdog.events().len(), 1);
}

#[tokio::test]
async fn a_first_step_that_refunds_sends_no_alert() {
    let mut harness = harness(thin_direct_pools());
    let watchdog = Arc::new(RecordingWatchdog::default());
    harness.finalizer = harness.finalizer.with_watchdog(watchdog.clone());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    harness.workflow.set_outcome(&ckbtc(), StepOutcome::Refund);

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Recovered);
    assert!(watchdog.events().is_empty());
}

#[tokio::test]
async fn a_step_error_is_retried_like_a_single_execution() {
    let harness = harness(thin_direct_pools());
    let preview = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    harness
        .workflow
        .set_outcome(&ckbtc(), StepOutcome::Fail("pool call failed".to_string()));

    let progress = advance_once(&harness, &preview).await;

    assert_eq!(progress.status, VenueLegStatus::Running);
    assert_eq!(progress.retryable_error.as_deref(), Some("pool call failed"));
    let route = route_of(&progress.execution);
    assert_eq!(
        route.current.state.next_attempt_at_nanos,
        Some(START_NANOS + RETRY_COOLDOWN_NANOS)
    );
}

#[tokio::test]
async fn a_route_and_a_single_execution_both_name_the_leg_signer() {
    let harness = harness(thin_direct_pools());
    let route = preview(&harness, &request(&ckbtc(), &ckusdc())).await;
    let single = preview(&harness, &request(&ckbtc(), &icp())).await;
    let (expected, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "42").expect("identity");

    for execution in [&route.initial_execution_state, &single.initial_execution_state] {
        assert_eq!(leg_identity(execution).expect("identity"), Some(expected.clone()));
    }
    assert!(matches!(
        IcpswapLegState::decode(&single.initial_execution_state).expect("decode leg"),
        IcpswapLegState::Single(_)
    ));
}

#[test]
fn the_slippage_left_for_the_last_swap_keeps_its_minimum_at_the_floor() {
    let nat = |value: u64| Nat::from(value);

    assert_eq!(slippage_within_floor(&nat(1_000), &nat(990), 500), Some(100));
    assert_eq!(slippage_within_floor(&nat(1_000), &nat(900), 500), Some(500));
    assert_eq!(slippage_within_floor(&nat(1_000), &nat(1_000), 500), Some(0));
    assert_eq!(slippage_within_floor(&nat(989), &nat(990), 500), None);
    // Rounding goes down, so the minimum stays at or above the floor.
    assert_eq!(slippage_within_floor(&nat(1_000), &nat(999), 500), Some(10));
    assert_eq!(slippage_within_floor(&nat(3), &nat(2), 500), Some(500));
}

/// 45 units leave 15 after the three input fees, and every first step quotes 15 or less: under the 20 of output fees.
#[tokio::test]
async fn an_amount_below_the_fees_on_every_path_is_too_small() {
    let harness = harness(thin_direct_pools());

    let error = harness
        .finalizer
        .preview(&planning_context(), &request_paying(&ckbtc(), &ckusdc(), 45))
        .await
        .unwrap_err();

    assert!(
        matches!(error, VenueAmountError::BelowMinimum(ref text) if text.contains("ledger fees")),
        "{error}"
    );
}

/// A path with no pool is not a fee problem, so the amount is not called too small.
#[tokio::test]
async fn an_amount_below_the_fees_on_some_paths_only_is_unavailable() {
    // ckBTC -> ckUSDC has no scripted pool, so the paths through ckUSDC fail for another reason.
    let harness = harness(three_step_pools());

    let error = harness
        .finalizer
        .preview(&planning_context(), &request_paying(&ckbtc(), &ckusdt(), 45))
        .await
        .unwrap_err();

    assert!(matches!(error, VenueAmountError::Unavailable(_)), "{error}");
}

/// 104 units pay 54 through ICP, but the least the route pays is 19, under the 20 of output fees.
/// The route ranks first, so it must drop out on its own for the direct pool to be chosen.
#[tokio::test]
async fn a_route_whose_least_output_cannot_pay_the_fees_gives_way_to_another_path() {
    let harness = harness(thin_direct_pools());

    let preview = harness
        .finalizer
        .preview(&planning_context(), &request_paying(&ckbtc(), &ckusdc(), 104))
        .await
        .expect("the direct pool can take it");

    let leg = IcpswapLegState::decode(&preview.initial_execution_state).expect("decode leg");
    assert!(matches!(leg, IcpswapLegState::Single(_)));
}
