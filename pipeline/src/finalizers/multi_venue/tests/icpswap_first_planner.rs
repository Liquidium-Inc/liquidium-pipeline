use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use candid::{Nat, Principal};
use liquidium_pipeline_core::{
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    },
};
use num_traits::ToPrimitive;
use tokio::sync::Barrier;

use super::super::planning::reference_price_usd;
use super::super::planning::venue_registry::{VenuePreviewOutcome, VenueRegistry};
use super::super::*;
use crate::{
    executors::executor::ExecutorRequest,
    persistance::{MultiVenueAllocationReason, VenueExecutionState, VenueLegState},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapQuote, SwapQuoteLeg, SwapRequest},
    utils::ICP_LEDGER_PRINCIPAL,
};

const TOTAL_PAY: u64 = 100_000_000;
const DEBT_REPAID: u64 = 190_000_000;
const KRAKEN_VENUE_ID: &str = "kraken";

struct BarrierAdapter {
    venue_id: &'static str,
    barrier: Arc<Barrier>,
}

/// Narrow test double used only to prove that every quote phase receives the
/// same liquidation-scoped planning context.
struct ContextRecordingAdapter {
    venue_id: &'static str,
    contexts: Arc<Mutex<Vec<String>>>,
}

type PreviewResponder = dyn Fn(&SwapRequest) -> Result<VenueRoutePreview, String> + Send + Sync;

struct PlannerAdapter {
    venue_id: &'static str,
    calls: Arc<Mutex<Vec<u64>>>,
    responder: Box<PreviewResponder>,
    validation_error: Option<String>,
    preview_forbidden: bool,
}

#[async_trait]
impl MultiVenueAdapter for PlannerAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    fn validate_configuration(&self) -> Result<(), String> {
        self.validation_error.clone().map_or(Ok(()), Err)
    }

    async fn preview(
        &self,
        _context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        assert!(!self.preview_forbidden, "{} must not be previewed", self.venue_id);
        self.calls
            .lock()
            .expect("calls lock")
            .push(request.pay_amount.value.0.to_u64().expect("test pay amount fits u64"));
        (self.responder)(request)
    }

    async fn advance(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }

    async fn recover(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }
}

#[async_trait]
impl MultiVenueAdapter for BarrierAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    async fn preview(
        &self,
        _context: &VenuePlanningContext,
        _request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        self.barrier.wait().await;
        Err(format!("{} unavailable", self.venue_id))
    }

    async fn advance(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }

    async fn recover(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }
}

#[async_trait]
impl MultiVenueAdapter for ContextRecordingAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    async fn preview(
        &self,
        context: &VenuePlanningContext,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String> {
        self.contexts
            .lock()
            .expect("contexts lock")
            .push(context.liquidation_id.clone());
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = match self.venue_id {
            ICPSWAP_VENUE_ID if pay > TOTAL_PAY / 2 => 200.0,
            ICPSWAP_VENUE_ID => 50.0,
            _ => 0.0,
        };
        Ok(proportional_preview(request, self.venue_id, impact, 2))
    }

    async fn advance(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }

    async fn recover(
        &self,
        _leg: &VenueLegState,
        _checkpoint: &dyn VenueLegCheckpoint,
    ) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }
}

fn native_icp() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("native ICP ledger"),
        symbol: "ICP".to_string(),
        decimals: 8,
        fee: Nat::from(10_000u64),
    }
}

fn non_native_icp_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_slice(&[9]),
        symbol: "ckBTC".to_string(),
        decimals: 8,
        fee: Nat::from(10u64),
    }
}

fn debt_token() -> ChainToken {
    ChainToken::Icp {
        ledger: Principal::from_slice(&[2]),
        symbol: "ckUSDC".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    }
}

fn input_with_pay_token(pay_token: ChainToken) -> IcpswapFirstPlanInput {
    let debt = debt_token();
    IcpswapFirstPlanInput {
        liquidation_id: "42".to_string(),
        total_pay: ChainTokenAmount::from_raw(pay_token, Nat::from(TOTAL_PAY)),
        receive_asset: debt.asset_id(),
        debt_repaid: ChainTokenAmount::from_raw(debt, Nat::from(DEBT_REPAID)),
        receive_address: Some("receiver".to_string()),
        max_execution_slippage_bps: Some(500),
        pay_reference_price_usd: Some(10.0),
    }
}

fn config(cex_min_exec_usd: f64) -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        max_price_impact_bps: 100.0,
        max_search_iterations: 16,
        cex_min_exec_usd,
        min_net_edge_bps: 150,
    }
}

fn preview(
    request: &SwapRequest,
    venue_id: &str,
    estimated_price_impact_bps: f64,
    receive_amount: u64,
    conservative_receive_amount: u64,
) -> VenueRoutePreview {
    let receive_token = debt_token();
    VenueRoutePreview {
        venue_id: venue_id.to_string(),
        request: request.clone(),
        quote: SwapQuote {
            pay_asset: request.pay_asset.clone(),
            pay_amount: request.pay_amount.value.clone(),
            receive_asset: request.receive_asset.clone(),
            receive_amount: Nat::from(receive_amount),
            mid_price: 1.0,
            exec_price: 1.0,
            estimated_price_impact_bps,
            legs: vec![SwapQuoteLeg {
                venue: venue_id.to_string(),
                route_id: format!("{venue_id}-route"),
                pay_chain: request.pay_asset.chain.clone(),
                pay_symbol: request.pay_asset.symbol.clone(),
                pay_amount: request.pay_amount.value.clone(),
                receive_chain: request.receive_asset.chain.clone(),
                receive_symbol: request.receive_asset.symbol.clone(),
                receive_amount: Nat::from(receive_amount),
                price: 1.0,
                lp_fee: Nat::from(0u8),
                gas_fee: Nat::from(0u8),
            }],
        },
        conservative_receive: ChainTokenAmount::from_raw(receive_token, Nat::from(conservative_receive_amount)),
        initial_execution_state: VenueExecutionState {
            venue: venue_id.to_string(),
            state: serde_json::json!({ "step": "planned" }),
        },
    }
}

fn proportional_preview(
    request: &SwapRequest,
    venue_id: &str,
    slippage_bps: f64,
    multiplier: u64,
) -> VenueRoutePreview {
    let pay = request.pay_amount.value.0.to_u64().expect("test pay amount fits u64");
    let receive = pay * multiplier;
    preview(request, venue_id, slippage_bps, receive, receive)
}

fn mock_adapter<F>(venue_id: &'static str, calls: Arc<Mutex<Vec<u64>>>, responder: F) -> PlannerAdapter
where
    F: Fn(&SwapRequest) -> Result<VenueRoutePreview, String> + Send + Sync + 'static,
{
    PlannerAdapter {
        venue_id,
        calls,
        responder: Box::new(responder),
        validation_error: None,
        preview_forbidden: false,
    }
}

fn planner(
    icpswap: PlannerAdapter,
    mexc: PlannerAdapter,
    planner_config: IcpswapFirstPlannerConfig,
) -> IcpswapFirstPlanner {
    IcpswapFirstPlanner::new(vec![Arc::new(icpswap), Arc::new(mexc)], planner_config).expect("valid planner")
}

// Input construction

#[test]
fn receipt_input_uses_actual_collateral_received_not_estimated_swap_amount() {
    let collateral = native_icp();
    let debt = debt_token();
    let estimated_request_amount = Nat::from(999u64);
    let actual_received = Nat::from(TOTAL_PAY);
    let receipt = ExecutionReceipt {
        request: ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(DEBT_REPAID),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: Some(SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(collateral.clone(), estimated_request_amount),
                receive_asset: debt.asset_id(),
                receive_address: Some("receiver".to_string()),
                max_slippage_bps: Some(500),
                venue_hint: None,
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 0,
            ref_price: Nat::from(10_000_000_000_000_000_000_000_000_000u128),
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            id: 1,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: actual_received.clone(),
                debt_repaid: Nat::from(DEBT_REPAID),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
        }),
        status: ExecutionStatus::Success,
        change_received: true,
    };

    let input = IcpswapFirstPlanInput::from_receipt(&receipt).expect("planner input");
    assert_eq!(input.liquidation_id, "1");
    assert_eq!(input.total_pay.value, actual_received);
}

// Allocation decisions

#[tokio::test]
async fn registry_previews_concurrently_and_preserves_registration_order() {
    let barrier = Arc::new(Barrier::new(2));
    let registry = VenueRegistry::new(vec![
        Arc::new(BarrierAdapter {
            venue_id: MEXC_VENUE_ID,
            barrier: barrier.clone(),
        }),
        Arc::new(BarrierAdapter {
            venue_id: KRAKEN_VENUE_ID,
            barrier,
        }),
    ])
    .expect("valid registry");
    let input = input_with_pay_token(native_icp());

    let quotes = tokio::time::timeout(
        Duration::from_secs(1),
        registry.preview_all(&input.planning_context(), |venue_id| {
            input.request_for(venue_id, Nat::from(TOTAL_PAY))
        }),
    )
    .await
    .expect("both previews must be polled concurrently");
    let previews = quotes.iter().collect::<Vec<_>>();

    assert_eq!(previews[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(previews[1].venue_id, KRAKEN_VENUE_ID);
    assert!(matches!(previews[0].outcome, VenuePreviewOutcome::Unavailable(_)));
    assert!(matches!(previews[1].outcome, VenuePreviewOutcome::Unavailable(_)));
}

#[tokio::test]
async fn liquidation_context_reaches_full_search_exact_and_overflow_previews() {
    let icpswap_contexts = Arc::new(Mutex::new(Vec::new()));
    let mexc_contexts = Arc::new(Mutex::new(Vec::new()));
    let icpswap = ContextRecordingAdapter {
        venue_id: ICPSWAP_VENUE_ID,
        contexts: icpswap_contexts.clone(),
    };
    let mexc = ContextRecordingAdapter {
        venue_id: MEXC_VENUE_ID,
        contexts: mexc_contexts.clone(),
    };

    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap), Arc::new(mexc)], config(0.0))
        .expect("valid planner");
    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("split plan");

    assert_eq!(state.legs.len(), 2);
    let icpswap_contexts = icpswap_contexts.lock().expect("contexts lock");
    let mexc_contexts = mexc_contexts.lock().expect("contexts lock");
    assert!(
        icpswap_contexts.len() > 2,
        "full quote, search, and exact quote must run"
    );
    assert!(!mexc_contexts.is_empty(), "overflow preview must run");
    assert!(icpswap_contexts.iter().all(|id| id == "42"));
    assert!(mexc_contexts.iter().all(|id| id == "42"));
}

#[test]
fn registry_rejects_an_adapter_with_missing_runtime_configuration() {
    let adapter = PlannerAdapter {
        venue_id: MEXC_VENUE_ID,
        calls: Arc::new(Mutex::new(Vec::new())),
        responder: Box::new(|_| Err("not used".to_string())),
        validation_error: Some("token registry is required".to_string()),
        preview_forbidden: true,
    };

    let error = VenueRegistry::new(vec![Arc::new(adapter)])
        .err()
        .expect("invalid adapter configuration");
    assert!(error.contains("token registry is required"));
}

#[tokio::test]
async fn full_icpswap_below_limit_wins_even_when_mexc_output_is_better() {
    let icpswap_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, icpswap_calls, |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 3))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("ICPSwap plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(state.plan.quoted_at, 123);
}

#[tokio::test]
async fn icpswap_only_accepts_a_safe_native_icp_quote() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_assert = calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, calls, |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(1.1)).expect("ICPSwap-only planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("safe ICPSwap quote");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(calls_for_assert.lock().expect("calls lock").as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn icpswap_only_rejects_an_unsafe_quote_without_searching() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_assert = calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, calls, |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 100.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(1.1)).expect("ICPSwap-only planner");

    let error = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("unsafe ICPSwap quote requires an overflow venue");

    assert!(matches!(error, IcpswapFirstPlannerError::NoViableRoute(_)));
    assert!(error.to_string().contains("no overflow venue is enabled"));
    assert_eq!(calls_for_assert.lock().expect("calls lock").as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn icpswap_only_rejects_non_native_collateral_without_quoting() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_assert = calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, calls, |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 0.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(1.1)).expect("ICPSwap-only planner");

    let error = planner
        .plan(&input_with_pay_token(non_native_icp_token()), 123)
        .await
        .expect_err("non-native collateral has no enabled venue");

    assert!(matches!(error, IcpswapFirstPlannerError::NoViableRoute(_)));
    assert!(calls_for_assert.lock().expect("calls lock").is_empty());
}

#[tokio::test]
async fn mexc_only_plans_native_icp_without_icpswap() {
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(mexc)], config(1.1)).expect("MEXC-only planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("MEXC-only plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn safe_full_icpswap_does_not_quote_any_overflow_venue() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2))
    });
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls_for_assert = mexc_calls.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 3))
    });
    let kraken_calls = Arc::new(Mutex::new(Vec::new()));
    let kraken_calls_for_assert = kraken_calls.clone();
    let kraken = mock_adapter(KRAKEN_VENUE_ID, kraken_calls, |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 0.0, 4))
    });
    let planner_config = config(1.1);
    let planner = IcpswapFirstPlanner::new(
        vec![Arc::new(icpswap), Arc::new(mexc), Arc::new(kraken)],
        planner_config,
    )
    .expect("valid planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("ICPSwap plan");

    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert!(mexc_calls_for_assert.lock().expect("calls lock").is_empty());
    assert!(kraken_calls_for_assert.lock().expect("calls lock").is_empty());
}

#[tokio::test]
async fn binary_search_builds_and_requotes_an_exact_split() {
    let icpswap_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap_calls_for_assert = icpswap_calls.clone();
    let mexc_calls_for_assert = mexc_calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, icpswap_calls, |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / 600_000.0;
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("split plan");

    assert_eq!(state.legs.len(), 2);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::PriceImpactSplit
    );
    let icpswap_pay = state.legs[0]
        .request
        .pay_amount
        .value
        .0
        .to_u64()
        .expect("u64 allocation");
    assert!(icpswap_pay < 60_000_000);
    assert!(60_000_000 - icpswap_pay <= 10_000);
    assert_eq!(
        state.legs[0].request.pay_amount.value.clone() + state.legs[1].request.pay_amount.value.clone(),
        Nat::from(TOTAL_PAY)
    );
    let calls = icpswap_calls_for_assert.lock().expect("calls lock");
    assert_eq!(calls.first(), Some(&TOTAL_PAY));
    assert_eq!(calls.last(), Some(&icpswap_pay));
    assert!(calls.iter().filter(|amount| **amount == icpswap_pay).count() >= 2);
    let mexc_calls = mexc_calls_for_assert.lock().expect("calls lock");
    let mexc_remainder = TOTAL_PAY - icpswap_pay;
    assert_eq!(mexc_calls.as_slice(), &[mexc_remainder]);
}

#[tokio::test]
async fn exact_price_impact_limit_is_excluded() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 100.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("MEXC-only plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn sub_minimum_mexc_remainder_uses_full_mexc_when_full_icpswap_is_unsafe() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / 900_000.0;
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("safe full-venue fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::RemainderBelowMinimum {
            skipped_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
            selected_venue_id: MEXC_VENUE_ID.to_string(),
        }
    );
}

#[tokio::test]
async fn missing_reference_price_splits_instead_of_forcing_full_icpswap() {
    // With no usable reference price, the planner cannot classify the CEX
    // remainder as below-minimum, so it keeps the confirmed-safe split.
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / 900_000.0;
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let mut input = input_with_pay_token(native_icp());
    input.pay_reference_price_usd = None;
    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input, 123)
        .await
        .expect("split plan");

    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::PriceImpactSplit
    );
    assert_eq!(state.legs.len(), 2);
    assert!(state.legs.iter().any(|leg| leg.venue_id == MEXC_VENUE_ID));
    // The ICPSwap leg stays inside the impact limit.
    let icpswap_leg = state
        .legs
        .iter()
        .find(|leg| leg.venue_id == ICPSWAP_VENUE_ID)
        .expect("icpswap leg");
    assert!(icpswap_leg.quote.estimated_price_impact_bps < 100.0);
}

#[tokio::test]
async fn unsafe_exact_icpswap_requote_falls_back_to_full_overflow() {
    // Safe on first sight, unsafe when the exact allocation is re-quoted: the
    // price moved between the binary search and the confirming quote.
    let quoted_amounts = Arc::new(Mutex::new(Vec::<u64>::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut seen = quoted_amounts.lock().expect("quoted amounts lock");
        let repeat = seen.contains(&pay);
        seen.push(pay);
        // Anything above half is unsafe, so the search settles on a half-sized
        // allocation with a remainder large enough for MEXC. Re-quoting that
        // same allocation then comes back unsafe.
        let impact = if pay > TOTAL_PAY / 2 || repeat { 200.0 } else { 50.0 };
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("full overflow fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    // The whole amount moves to the overflow venue, not just the remainder.
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::VenueUnavailable {
            selected_venue_id: MEXC_VENUE_ID.to_string(),
            unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
        }
    );
}

#[test]
fn unusable_reference_prices_are_absent_rather_than_zero() {
    // RAY-scaled: 10 * 1e27 is $10.
    assert_eq!(
        reference_price_usd(&Nat::from(10_000_000_000_000_000_000_000_000_000u128)),
        Some(10.0)
    );
    assert_eq!(reference_price_usd(&Nat::from(0u8)), None);

    // Large enough that `to_f64` cannot represent it as a finite price.
    let overflowing = Nat::parse(format!("1{}", "0".repeat(400)).as_bytes()).expect("huge nat");
    assert_eq!(reference_price_usd(&overflowing), None);
}

#[test]
fn present_but_non_positive_reference_price_is_rejected() {
    let mut input = input_with_pay_token(native_icp());
    input.pay_reference_price_usd = Some(0.0);
    assert!(matches!(
        input.validate(),
        Err(IcpswapFirstPlannerError::InvalidInput(_))
    ));

    input.pay_reference_price_usd = Some(f64::NAN);
    assert!(matches!(
        input.validate(),
        Err(IcpswapFirstPlannerError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn below_minimum_requote_uses_full_icpswap_only_when_the_fresh_quote_is_safe() {
    let call_count = Arc::new(Mutex::new(0u32));
    let icpswap_call_count = call_count.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut count = icpswap_call_count.lock().expect("count lock");
        *count += 1;
        // Only the re-quote for the full amount taken *after* the binary search
        // (the second call at pay == TOTAL_PAY) reflects the fresher price.
        let refreshed_full_quote = pay == TOTAL_PAY && *count > 1;
        let impact = if refreshed_full_quote {
            50.0
        } else {
            pay as f64 / 900_000.0
        };
        let receive = if refreshed_full_quote { pay * 3 } else { pay * 2 };
        Ok(preview(request, ICPSWAP_VENUE_ID, impact, receive, receive))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("fresh safe full ICPSwap plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(TOTAL_PAY * 3));
}

#[tokio::test]
async fn malformed_final_dust_icpswap_quote_falls_back_to_fresh_full_overflow_quote() {
    let amount_calls = Arc::new(Mutex::new(HashMap::<u64, u32>::new()));
    let icpswap_amount_calls = amount_calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut calls = icpswap_amount_calls.lock().expect("calls lock");
        let count = calls.entry(pay).or_default();
        *count += 1;
        let mut route = proportional_preview(request, ICPSWAP_VENUE_ID, pay as f64 / 900_000.0, 2);
        if pay == TOTAL_PAY && *count > 1 {
            route.initial_execution_state.venue = MEXC_VENUE_ID.to_string();
        }
        Ok(route)
    });
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls_for_assert = mexc_calls.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("full MEXC fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(
        mexc_calls_for_assert.lock().expect("calls lock").last(),
        Some(&TOTAL_PAY)
    );
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::VenueUnavailable {
            selected_venue_id: MEXC_VENUE_ID.to_string(),
            unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
        }
    );
}

#[tokio::test]
async fn malformed_final_split_icpswap_quote_falls_back_to_fresh_full_overflow_quote() {
    let amount_calls = Arc::new(Mutex::new(HashMap::<u64, u32>::new()));
    let icpswap_amount_calls = amount_calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut calls = icpswap_amount_calls.lock().expect("calls lock");
        let count = calls.entry(pay).or_default();
        *count += 1;
        let mut route = proportional_preview(request, ICPSWAP_VENUE_ID, pay as f64 / 600_000.0, 2);
        if pay != TOTAL_PAY && *count > 1 {
            route.initial_execution_state.venue = MEXC_VENUE_ID.to_string();
        }
        Ok(route)
    });
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls_for_assert = mexc_calls.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("full MEXC fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    let mexc_calls = mexc_calls_for_assert.lock().expect("calls lock");
    assert_ne!(mexc_calls.first(), Some(&TOTAL_PAY));
    assert_eq!(mexc_calls.last(), Some(&TOTAL_PAY));
    assert_eq!(mexc_calls.len(), 2);
}

#[tokio::test]
async fn below_minimum_overflow_fallback_records_every_skipped_venue() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        Ok(proportional_preview(
            request,
            ICPSWAP_VENUE_ID,
            pay as f64 / 900_000.0,
            2,
        ))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 0.0, 2))
    });
    let planner_config = config(1.1);
    let planner = IcpswapFirstPlanner::new(
        vec![Arc::new(icpswap), Arc::new(mexc), Arc::new(kraken)],
        planner_config,
    )
    .expect("valid planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("full overflow fallback");

    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::RemainderBelowMinimum {
            skipped_venue_ids: vec![ICPSWAP_VENUE_ID.to_string(), KRAKEN_VENUE_ID.to_string()],
            selected_venue_id: MEXC_VENUE_ID.to_string(),
        }
    );
}

#[tokio::test]
async fn safe_value_zero_quotes_mexc_once_after_binary_search() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 100.0, 2))
    });
    let call_count = Arc::new(Mutex::new(0u32));
    let mexc_call_count = call_count.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut count = mexc_call_count.lock().expect("count lock");
        *count += 1;
        // MEXC is not quoted until the immediately-unsafe ICPSwap search has
        // established that the full amount must go to overflow.
        let receive = if *count > 1 { pay * 3 } else { pay * 2 };
        Ok(preview(request, MEXC_VENUE_ID, 0.0, receive, receive))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("MEXC-only plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(TOTAL_PAY * 2));
    assert_eq!(*call_count.lock().expect("count lock"), 1);
}

#[tokio::test]
async fn safe_full_icpswap_below_minimum_still_requires_the_profit_floor() {
    let call_count = Arc::new(Mutex::new(0u32));
    let icpswap_call_count = call_count.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut count = icpswap_call_count.lock().expect("count lock");
        *count += 1;
        let impact = if pay == TOTAL_PAY && *count > 1 {
            50.0
        } else {
            pay as f64 / 900_000.0
        };
        Ok(preview(request, ICPSWAP_VENUE_ID, impact, 180_000_000, 180_000_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let error = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("unprofitable full ICPSwap route must fail");

    assert!(error.to_string().contains("below required 150 bps"));
}

// Venue availability

#[tokio::test]
async fn icpswap_unavailable_falls_back_to_executable_mexc() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("pool unavailable".to_string())
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("MEXC fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::VenueUnavailable {
            selected_venue_id: MEXC_VENUE_ID.to_string(),
            unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
        }
    );
}

#[tokio::test]
async fn icpswap_unavailable_selects_the_best_configured_overflow_venue() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("pool unavailable".to_string())
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 0.0, 3))
    });
    let planner_config = config(1.1);
    let planner = IcpswapFirstPlanner::new(
        vec![Arc::new(icpswap), Arc::new(mexc), Arc::new(kraken)],
        planner_config,
    )
    .expect("valid planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("Kraken fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, KRAKEN_VENUE_ID);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::VenueUnavailable {
            selected_venue_id: KRAKEN_VENUE_ID.to_string(),
            unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
        }
    );
}

#[tokio::test]
async fn equal_overflow_quotes_keep_environment_order() {
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 0.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(kraken), Arc::new(mexc)], config(1.1))
        .expect("valid overflow-only planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("first equal quote is selected");

    assert_eq!(state.legs[0].venue_id, KRAKEN_VENUE_ID);
}

#[tokio::test]
async fn mexc_unavailable_allows_only_a_normally_safe_full_icpswap_quote() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("exchange unavailable".to_string())
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("safe ICPSwap route");
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);

    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 100.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("exchange unavailable".to_string())
    });
    let error = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("unsafe ICPSwap must not be forced when MEXC is unavailable");
    assert!(error.to_string().contains("MEXC unavailable"));
}

#[tokio::test]
async fn below_minimum_remainder_rejects_when_full_icpswap_is_unsafe_and_overflow_is_unavailable() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        Ok(proportional_preview(
            request,
            ICPSWAP_VENUE_ID,
            pay as f64 / 900_000.0,
            2,
        ))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("exchange unavailable".to_string())
    });

    let error = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("no unsafe full-ICPSwap fallback is allowed");

    assert!(
        error
            .to_string()
            .contains("full ICPSwap quote exceeds the price-impact limit")
    );
    assert!(error.to_string().contains("MEXC unavailable"));
}

// Asset eligibility and adapter validation

#[tokio::test]
async fn non_native_icp_is_mexc_only_and_never_previews_icpswap() {
    let icpswap = PlannerAdapter {
        venue_id: ICPSWAP_VENUE_ID,
        calls: Arc::new(Mutex::new(Vec::new())),
        responder: Box::new(|_| Err("not used".to_string())),
        validation_error: None,
        preview_forbidden: true,
    };
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls_for_assert = mexc_calls.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(0.0))
        .plan(&input_with_pay_token(non_native_icp_token()), 123)
        .await
        .expect("MEXC-only plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(
        mexc_calls_for_assert.lock().expect("calls lock").as_slice(),
        &[TOTAL_PAY]
    );
}

#[tokio::test]
async fn malformed_icpswap_preview_is_dropped_and_mexc_remains_available() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let mut preview = proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2);
        preview.initial_execution_state.venue = MEXC_VENUE_ID.to_string();
        Ok(preview)
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("valid MEXC quote remains usable");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn malformed_mexc_preview_is_dropped_and_safe_icpswap_remains_available() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let mut preview = proportional_preview(request, MEXC_VENUE_ID, 0.0, 2);
        preview.initial_execution_state.venue = ICPSWAP_VENUE_ID.to_string();
        Ok(preview)
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("valid ICPSwap quote remains usable");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
}

#[tokio::test]
async fn planning_fails_when_every_quote_is_invalid() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let mut preview = proportional_preview(request, ICPSWAP_VENUE_ID, 99.0, 2);
        preview.initial_execution_state.venue = MEXC_VENUE_ID.to_string();
        Ok(preview)
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let mut preview = proportional_preview(request, MEXC_VENUE_ID, 0.0, 2);
        preview.initial_execution_state.venue = ICPSWAP_VENUE_ID.to_string();
        Ok(preview)
    });

    let error = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("no valid quote remains");

    assert!(matches!(error, IcpswapFirstPlannerError::NoViableRoute(_)));
    assert!(error.to_string().contains("quote rejected"));
}
