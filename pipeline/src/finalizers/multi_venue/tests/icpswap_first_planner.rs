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
use mockall::predicate::eq;
use num_traits::ToPrimitive;
use proptest::prelude::*;
use tokio::sync::Barrier;

use super::super::planning::reference_price_usd;
use super::super::planning::venue_registry::{VenuePreviewOutcome, VenueRegistry};
use super::super::*;
use crate::{
    executors::executor::ExecutorRequest,
    liquidation::collateral_service::USD_QUOTE_CURRENCY,
    persistance::{MultiVenueAllocationReason, VenueExecutionState, VenueLegState},
    price_oracle::price_oracle::MockPriceOracle,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapQuote, SwapQuoteLeg, SwapRequest},
    utils::{CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL},
};

const TOTAL_PAY: u64 = 100_000_000;
const DEBT_REPAID: u64 = 190_000_000;
const KRAKEN_VENUE_ID: &str = "kraken";
const RAY: u128 = 1_000_000_000_000_000_000_000_000_000;
/// The planning timestamp every test passes to `plan`. Inputs record prices at
/// this instant, so they read as fresh unless a test ages them.
const QUOTED_AT: i64 = 123;

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
        ledger: Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).expect("ckUSDC ledger"),
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
        pay_reference_price_ray: None,
        receive_reference_price_ray: None,
        reference_price_captured_at: Some(QUOTED_AT),
        buy_bad_debt: false,
    }
}

fn native_icp_to_ckusdt_input() -> IcpswapFirstPlanInput {
    let debt = ChainToken::Icp {
        ledger: Principal::from_text(CKUSDT_LEDGER_PRINCIPAL).expect("ckUSDT ledger"),
        symbol: "ckUSDT".to_string(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    };
    IcpswapFirstPlanInput {
        liquidation_id: "42".to_string(),
        total_pay: ChainTokenAmount::from_raw(native_icp(), Nat::from(TOTAL_PAY)),
        receive_asset: debt.asset_id(),
        debt_repaid: ChainTokenAmount::from_raw(debt, Nat::from(DEBT_REPAID)),
        receive_address: Some("receiver".to_string()),
        max_execution_slippage_bps: Some(500),
        pay_reference_price_usd: Some(10.0),
        pay_reference_price_ray: None,
        receive_reference_price_ray: None,
        reference_price_captured_at: Some(QUOTED_AT),
        buy_bad_debt: false,
    }
}

fn ckusdc_to_native_icp_input() -> IcpswapFirstPlanInput {
    let pay = debt_token();
    let receive = native_icp();
    IcpswapFirstPlanInput {
        liquidation_id: "42".to_string(),
        total_pay: ChainTokenAmount::from_raw(pay, Nat::from(TOTAL_PAY)),
        receive_asset: receive.asset_id(),
        debt_repaid: ChainTokenAmount::from_raw(receive, Nat::from(DEBT_REPAID)),
        receive_address: Some("receiver".to_string()),
        max_execution_slippage_bps: Some(500),
        pay_reference_price_usd: Some(1.0),
        pay_reference_price_ray: None,
        receive_reference_price_ray: None,
        reference_price_captured_at: Some(QUOTED_AT),
        buy_bad_debt: false,
    }
}

fn native_icp_to_ckusdc_with_oracle() -> IcpswapFirstPlanInput {
    let mut input = input_with_pay_token(native_icp());
    input.debt_repaid.value = Nat::from(1_000_000u64);
    input.pay_reference_price_ray = Some(Nat::from(2 * RAY));
    input.receive_reference_price_ray = Some(Nat::from(RAY));
    input
}

fn config(cex_min_exec_usd: f64) -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        max_price_impact_bps: 100.0,
        max_search_iterations: 16,
        dust_fallback_max_price_impact_bps: 150.0,
        max_cex_price_impact_bps: 200.0,
        cex_min_exec_usd,
        min_net_edge_bps: 150,
        bad_debt_min_net_edge_bps: 150,
        // Must stay above the 150 bps dust fallback impact cap, which already
        // includes the pool fee.
        max_oracle_discount_bps: 200,
        oracle_snapshot_max_age_secs: 300,
        icpswap_test_allocation_usd: None,
    }
}

fn production_oracle_config(cex_min_exec_usd: f64) -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        max_oracle_discount_bps: 250,
        ..config(cex_min_exec_usd)
    }
}

fn preview(
    request: &SwapRequest,
    venue_id: &str,
    estimated_price_impact_bps: f64,
    receive_amount: u64,
    conservative_receive_amount: u64,
) -> VenueRoutePreview {
    let receive_token = ChainToken::Icp {
        ledger: Principal::from_text(&request.receive_asset.address).expect("test receive ledger"),
        symbol: request.receive_asset.symbol.clone(),
        decimals: 6,
        fee: Nat::from(10_000u64),
    };
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
            debt_ref_price: Nat::from(RAY),
            ref_price_at: 0,
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
    assert_eq!(input.pay_reference_price_ray, Some(Nat::from(10 * RAY)));
    assert_eq!(input.receive_reference_price_ray, Some(Nat::from(RAY)));
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

    let planner =
        IcpswapFirstPlanner::new(vec![Arc::new(icpswap), Arc::new(mexc)], config(0.0)).expect("valid planner");
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
async fn test_usd_override_sends_one_dollar_to_icpswap_and_exact_remainder_to_mexc() {
    let icpswap_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, icpswap_calls.clone(), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 10.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls.clone(), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 10.0, 2))
    });
    let mut planner_config = config(8.0);
    planner_config.icpswap_test_allocation_usd = Some(1.0);

    let state = planner(icpswap, mexc, planner_config)
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("forced test split");

    // The test reference price is $10/ICP and ICP has 8 decimals, so $1 is
    // exactly 0.1 ICP = 10_000_000 native units.
    assert_eq!(*icpswap_calls.lock().expect("ICPSwap calls"), vec![10_000_000]);
    assert_eq!(*mexc_calls.lock().expect("MEXC calls"), vec![90_000_000]);
    assert_eq!(state.legs.len(), 2);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(state.legs[1].venue_id, MEXC_VENUE_ID);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::PriceImpactSplit
    );
}

#[tokio::test]
async fn test_usd_override_accepts_an_exact_eight_dollar_mexc_remainder() {
    let icpswap_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, icpswap_calls.clone(), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 10.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls.clone(), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let mut planner_config = config(8.0);
    planner_config.icpswap_test_allocation_usd = Some(2.0);

    let state = planner(icpswap, mexc, planner_config)
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("an exact $8 remainder meets the MEXC minimum");

    assert_eq!(*icpswap_calls.lock().expect("ICPSwap calls"), vec![20_000_000]);
    assert_eq!(*mexc_calls.lock().expect("MEXC calls"), vec![80_000_000]);
    assert_eq!(state.legs.len(), 2);
}

#[tokio::test]
async fn test_usd_override_routes_a_one_native_unit_below_eight_dollars_to_dust_fallback() {
    let icpswap_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, icpswap_calls.clone(), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 150.0, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls.clone(), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let mut planner_config = config(8.0);
    // At $10/ICP this becomes 20,000,001 native ICP units, leaving
    // 79,999,999 units = $7.9999999 for MEXC.
    planner_config.icpswap_test_allocation_usd = Some(2.000_000_1);

    let state = planner(icpswap, mexc, planner_config)
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("a sub-$8 remainder should use the inclusive dust fallback");

    assert_eq!(*icpswap_calls.lock().expect("ICPSwap calls"), vec![TOTAL_PAY]);
    assert!(mexc_calls.lock().expect("MEXC calls").is_empty());
    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert!(matches!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::RemainderBelowMinimum { .. }
    ));
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
        let impact = pay as f64 / 625_000.0;
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(8.0))
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
async fn below_minimum_remainder_allows_full_icpswap_at_the_150_bps_buffer() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / TOTAL_PAY as f64 * 150.0;
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc_calls_for_assert = mexc_calls.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls, |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(8.0))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("buffered full ICPSwap fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(state.legs[0].quote.estimated_price_impact_bps, 150.0);
    assert!(mexc_calls_for_assert.lock().expect("MEXC calls").is_empty());
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

/// A quote at a perfect price whose only shortfall is the three input ledger
/// fees ICPSwap reserves before the pool sees the money. Impact is reported as
/// zero, so whatever the guard sees here is pure fee drag.
fn ledger_fee_only_preview(request: &SwapRequest, receive_token: ChainToken) -> VenueRoutePreview {
    let pay = request.pay_amount.value.0.to_u64().expect("test pay fits u64");
    let input_fee = request.pay_amount.token.fee().0.to_u64().expect("fee fits u64");
    let executable = pay - 3 * input_fee;
    // Both prices are $1, so value is preserved and only the decimal scale moves.
    let pay_scale = 10u64.pow(u32::from(request.pay_amount.token.decimals()));
    let receive_scale = 10u64.pow(u32::from(receive_token.decimals()));
    let receive = executable / pay_scale * receive_scale + (executable % pay_scale) * receive_scale / pay_scale;
    let mut route = preview(request, ICPSWAP_VENUE_ID, 0.0, receive, receive);
    route.conservative_receive.token = receive_token;
    route
}

/// The guard measures a quote against the *full* allocation, but ICPSwap swaps
/// the allocation minus three input ledger fees. That gap is a share of the leg,
/// so it grows as the leg shrinks -- and how fast depends on the pay token's fee
/// relative to its unit value.
#[tokio::test]
async fn input_ledger_fee_drag_is_a_share_of_the_leg_and_depends_on_the_pay_token() {
    // One dollar of ICP: three 0.0001 ICP fees against 0.1 ICP is 30 bps.
    let mut icp_leg = input_with_pay_token(native_icp());
    icp_leg.total_pay.value = Nat::from(10_000_000u64);
    icp_leg.debt_repaid.value = Nat::from(1u8);
    icp_leg.pay_reference_price_ray = Some(Nat::from(RAY));
    icp_leg.receive_reference_price_ray = Some(Nat::from(RAY));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(ledger_fee_only_preview(request, debt_token()))
    });
    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&icp_leg, QUOTED_AT)
        .await
        .expect("30 bps of fee drag fits inside a 250 bps budget");
    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(99_700u64));

    // One dollar of ckUSDC: three $0.01 fees against $1 is 300 bps, which spends
    // the whole budget on fees before impact or basis is considered.
    let mut ckusdc_leg = ckusdc_to_native_icp_input();
    ckusdc_leg.total_pay.value = Nat::from(1_000_000u64);
    ckusdc_leg.debt_repaid.value = Nat::from(1u8);
    ckusdc_leg.pay_reference_price_ray = Some(Nat::from(RAY));
    ckusdc_leg.receive_reference_price_ray = Some(Nat::from(RAY));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(ledger_fee_only_preview(request, native_icp()))
    });
    let error = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&ckusdc_leg, QUOTED_AT)
        .await
        .expect_err("a one dollar ckUSDC leg cannot clear a 250 bps budget on fees alone");
    assert!(
        error.to_string().contains("300.00 bps below oracle-implied output"),
        "unexpected error: {error}"
    );
}

#[test]
fn oracle_discount_limit_must_leave_room_above_the_impact_caps() {
    // A venue's reported impact already contains its pool fee, so a limit at or
    // below the dust fallback cap would reject max-impact quotes that the impact
    // policy allows -- with no room left for ledger fees or oracle basis.
    let too_tight = IcpswapFirstPlannerConfig {
        max_oracle_discount_bps: 150,
        ..config(0.0)
    };
    let adapter = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1, 1))
    });
    let error = match IcpswapFirstPlanner::new(vec![Arc::new(adapter)], too_tight) {
        Ok(_) => panic!("a limit at the dust fallback cap must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("must exceed the 150.00 bps dust fallback"));

    // The shipped pairing leaves 100 bps above the cap.
    assert!(config(0.0).max_oracle_discount_bps as f64 > config(0.0).dust_fallback_max_price_impact_bps);
    assert!(
        production_oracle_config(0.0).max_oracle_discount_bps as f64
            > production_oracle_config(0.0).dust_fallback_max_price_impact_bps
    );
}

#[test]
fn cex_price_impact_limit_must_be_non_negative_and_bounded() {
    for invalid_limit in [-1.0, 10_001.0, f64::NAN] {
        let invalid = IcpswapFirstPlannerConfig {
            max_cex_price_impact_bps: invalid_limit,
            ..config(0.0)
        };
        let adapter = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
            Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1, 1))
        });
        let error = match IcpswapFirstPlanner::new(vec![Arc::new(adapter)], invalid) {
            Ok(_) => panic!("invalid CEX impact limit {invalid_limit:?} must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("maximum CEX price impact"));
    }
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
    assert_eq!(mexc_calls.as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn below_minimum_overflow_fallback_records_every_skipped_venue() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        Ok(proportional_preview(
            request,
            ICPSWAP_VENUE_ID,
            pay as f64 / 625_000.0,
            2,
        ))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 0.0, 2))
    });
    let planner_config = config(8.0);
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
async fn icpswap_unavailable_prioritizes_safe_mexc_over_a_better_kraken_quote() {
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
        .expect("ordered MEXC allocation");

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
async fn mexc_priority_is_independent_of_environment_order() {
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
        .expect("MEXC policy priority is selected");

    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn unsafe_mexc_allocation_is_sized_and_only_its_remainder_goes_to_kraken() {
    const MEXC_SAFE_CAPACITY: u64 = 60_000_000;
    let mexc_calls = Arc::new(Mutex::new(Vec::new()));
    let mexc = mock_adapter(MEXC_VENUE_ID, mexc_calls.clone(), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = if pay <= MEXC_SAFE_CAPACITY { 200.0 } else { 201.0 };
        Ok(proportional_preview(request, MEXC_VENUE_ID, impact, 2))
    });
    let kraken_calls = Arc::new(Mutex::new(Vec::new()));
    let kraken = mock_adapter(KRAKEN_VENUE_ID, kraken_calls.clone(), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 50.0, 2))
    });
    let planner =
        IcpswapFirstPlanner::new(vec![Arc::new(mexc), Arc::new(kraken)], config(0.0)).expect("valid overflow planner");

    let state = planner
        .plan(&input_with_pay_token(non_native_icp_token()), QUOTED_AT)
        .await
        .expect("MEXC and Kraken waterfall");

    assert_eq!(state.legs.len(), 2);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[1].venue_id, KRAKEN_VENUE_ID);
    let mexc_pay = state.legs[0].request.pay_amount.value.0.to_u64().expect("MEXC amount");
    let kraken_pay = state.legs[1]
        .request
        .pay_amount
        .value
        .0
        .to_u64()
        .expect("Kraken amount");
    assert!(mexc_pay <= MEXC_SAFE_CAPACITY);
    assert!(mexc_pay > 59_000_000, "binary search should approach the safe capacity");
    assert_eq!(mexc_pay + kraken_pay, TOTAL_PAY);
    assert_eq!(kraken_calls.lock().expect("calls lock").as_slice(), &[kraken_pay]);
    assert!(mexc_calls.lock().expect("calls lock").len() > 2);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::PriceImpactSplit
    );
}

#[tokio::test]
async fn liquidation_waterfall_persists_icpswap_then_mexc_then_kraken() {
    const ICPSWAP_SAFE_CAPACITY: u64 = 40_000_000;
    const MEXC_SAFE_CAPACITY: u64 = 30_000_000;
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = if pay <= ICPSWAP_SAFE_CAPACITY { 50.0 } else { 101.0 };
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, impact, 2))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = if pay <= MEXC_SAFE_CAPACITY { 200.0 } else { 201.0 };
        Ok(proportional_preview(request, MEXC_VENUE_ID, impact, 2))
    });
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 50.0, 2))
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap), Arc::new(mexc), Arc::new(kraken)], config(0.0))
        .expect("valid waterfall planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), QUOTED_AT)
        .await
        .expect("three-venue waterfall");

    assert_eq!(state.legs.len(), 3);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(state.legs[1].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[2].venue_id, KRAKEN_VENUE_ID);
    let allocations = state
        .legs
        .iter()
        .map(|leg| leg.request.pay_amount.value.0.to_u64().expect("allocation"))
        .collect::<Vec<_>>();
    assert!(allocations[0] <= ICPSWAP_SAFE_CAPACITY);
    assert!(allocations[0] > 39_000_000);
    assert!(allocations[1] <= MEXC_SAFE_CAPACITY);
    assert!(allocations[1] > 29_000_000);
    assert_eq!(allocations.iter().sum::<u64>(), TOTAL_PAY);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::PriceImpactSplit
    );
}

#[tokio::test]
async fn unavailable_mexc_sends_the_complete_overflow_allocation_to_kraken() {
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("exchange unavailable".to_string())
    });
    let kraken_calls = Arc::new(Mutex::new(Vec::new()));
    let kraken = mock_adapter(KRAKEN_VENUE_ID, kraken_calls.clone(), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 50.0, 2))
    });
    let planner =
        IcpswapFirstPlanner::new(vec![Arc::new(mexc), Arc::new(kraken)], config(0.0)).expect("valid overflow planner");

    let state = planner
        .plan(&input_with_pay_token(non_native_icp_token()), QUOTED_AT)
        .await
        .expect("Kraken fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, KRAKEN_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(kraken_calls.lock().expect("calls lock").as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn below_minimum_kraken_remainder_discards_partial_mexc_and_uses_full_kraken() {
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = if pay <= 95_000_000 { 200.0 } else { 201.0 };
        Ok(proportional_preview(request, MEXC_VENUE_ID, impact, 2))
    });
    let kraken_calls = Arc::new(Mutex::new(Vec::new()));
    let kraken = mock_adapter(KRAKEN_VENUE_ID, kraken_calls.clone(), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 50.0, 2))
    });
    // At $10 per ICP, the roughly 0.05 ICP remainder is below this $1 floor.
    let planner =
        IcpswapFirstPlanner::new(vec![Arc::new(mexc), Arc::new(kraken)], config(1.0)).expect("valid overflow planner");

    let state = planner
        .plan(&input_with_pay_token(non_native_icp_token()), QUOTED_AT)
        .await
        .expect("full Kraken dust fallback");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, KRAKEN_VENUE_ID);
    assert_eq!(state.legs[0].request.pay_amount.value, Nat::from(TOTAL_PAY));
    assert_eq!(kraken_calls.lock().expect("calls lock").as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn unsafe_kraken_remainder_rejects_the_waterfall() {
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("test amount");
        let impact = if pay <= 60_000_000 { 200.0 } else { 201.0 };
        Ok(proportional_preview(request, MEXC_VENUE_ID, impact, 2))
    });
    let kraken = mock_adapter(KRAKEN_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, KRAKEN_VENUE_ID, 201.0, 2))
    });
    let planner =
        IcpswapFirstPlanner::new(vec![Arc::new(mexc), Arc::new(kraken)], config(0.0)).expect("valid overflow planner");

    let error = planner
        .plan(&input_with_pay_token(non_native_icp_token()), QUOTED_AT)
        .await
        .expect_err("unsafe Kraken remainder must reject the plan");

    assert!(
        error
            .to_string()
            .contains("kraken quote impact 201.00 bps exceeds 200.00 bps")
    );
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
    assert!(error.to_string().contains("mexc preview failed"));
}

#[tokio::test]
async fn below_minimum_remainder_rejects_when_full_icpswap_is_unsafe_and_overflow_is_unavailable() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        Ok(proportional_preview(
            request,
            ICPSWAP_VENUE_ID,
            pay as f64 / 625_000.0,
            2,
        ))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |_request| {
        Err("exchange unavailable".to_string())
    });

    let error = planner(icpswap, mexc, config(8.0))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("no unsafe full-ICPSwap fallback is allowed");

    assert!(
        error
            .to_string()
            .contains("full ICPSwap quote exceeds the dust-fallback price-impact limit")
    );
    assert!(error.to_string().contains("mexc preview failed"));
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
async fn native_icp_to_ckusdt_is_mexc_only_and_never_previews_icpswap() {
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
        .plan(&native_icp_to_ckusdt_input(), 123)
        .await
        .expect("unsupported ICPSwap pair should use MEXC");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(
        mexc_calls_for_assert.lock().expect("calls lock").as_slice(),
        &[TOTAL_PAY]
    );
}

#[tokio::test]
async fn ckusdc_to_native_icp_remains_eligible_for_icpswap() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_assert = calls.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, calls, |request| {
        let mut preview = proportional_preview(request, ICPSWAP_VENUE_ID, 50.0, 2);
        preview.conservative_receive.token = native_icp();
        Ok(preview)
    });
    let planner = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(0.0)).expect("planner");

    let state = planner
        .plan(&ckusdc_to_native_icp_input(), 123)
        .await
        .expect("reverse canonical pair should use ICPSwap");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert_eq!(calls_for_assert.lock().expect("calls lock").as_slice(), &[TOTAL_PAY]);
}

#[tokio::test]
async fn oracle_discount_rejects_icpswap_and_falls_back_to_valid_mexc() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_000_000, 1_000_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, MEXC_VENUE_ID, 0.0, 2_000_000, 1_990_000))
    });

    let state = planner(icpswap, mexc, config(0.0))
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect("MEXC should survive the central oracle guard");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert!(matches!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::VenueUnavailable { .. }
    ));
}

#[tokio::test]
async fn oracle_discount_guard_rejects_mexc_at_the_same_top_level_boundary() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_000_000, 1_000_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, MEXC_VENUE_ID, 0.0, 1_500_000, 1_490_000))
    });

    let error = planner(icpswap, mexc, config(0.0))
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect_err("both below-oracle venue quotes must be rejected");

    assert!(error.to_string().contains("mexc quote is"));
    assert!(error.to_string().contains("below oracle-implied output"));
}

/// An oracle answering both sides of the ICP -> ckUSDC pair at fixed prices.
///
/// The receive leg is keyed by `USDC`, not `ckUSDC`: a planner asking for the
/// ledger symbol gets no match here and falls back to the recorded price.
fn mock_oracle(icp_usd: u128, usdc_usd: u128) -> Arc<MockPriceOracle> {
    let mut oracle = MockPriceOracle::new();
    oracle
        .expect_get_price()
        .with(eq("ICP"), eq(USD_QUOTE_CURRENCY))
        .returning(move |_, _| Ok((Nat::from(icp_usd * RAY), 27)));
    oracle
        .expect_get_price()
        .with(eq("USDC"), eq(USD_QUOTE_CURRENCY))
        .returning(move |_, _| Ok((Nat::from(usdc_usd * RAY), 27)));
    Arc::new(oracle)
}

#[test]
fn oracle_guard_waiver_banner_names_the_setting_and_the_way_out() {
    let banner = oracle_guard_waiver_banner(1.0);
    assert!(banner.contains("ORACLE QUOTE GUARD DISABLED"), "{banner}");
    assert!(banner.contains("ICPSWAP_TEST_ALLOCATION_USD=$1.00"), "{banner}");
    assert!(banner.contains("$10.00 minimum"), "{banner}");
    // An operator must not read this as "no quote is checked".
    assert!(banner.contains("remainder is still checked"), "{banner}");
    assert!(banner.contains("clear ICPSWAP_TEST_ALLOCATION_USD"), "{banner}");
}

#[test]
fn oracle_symbols_drop_the_chain_key_prefix() {
    assert_eq!(oracle_price_symbol(&native_icp()), "ICP");
    assert_eq!(oracle_price_symbol(&debt_token()), "USDC");
    assert_eq!(oracle_price_symbol(&non_native_icp_token()), "BTC");
}

/// An oracle that fails, standing in for a canister call that cannot be made.
fn failing_oracle() -> Arc<MockPriceOracle> {
    let mut oracle = MockPriceOracle::new();
    oracle
        .expect_get_price()
        .returning(|_, _| Err("oracle unavailable".to_string()));
    Arc::new(oracle)
}

#[tokio::test]
async fn live_oracle_price_replaces_a_stale_recorded_price() {
    // Recorded at $2/ICP, so the snapshot implies 2 ckUSDC and would reject this
    // quote. The market has since halved: at the live $1/ICP the same quote is
    // 2.5% *above* the oracle, and the collateral still gets swapped.
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_025_000, 1_025_000))
    });

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(mock_oracle(1, 1))
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect("a quote honest at the live price must not be rejected by a stale one");

    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(1_025_000u64));
}

#[tokio::test]
async fn live_oracle_price_rejects_a_quote_the_recorded_price_would_have_accepted() {
    // The reverse case: recorded at $2/ICP this quote sits inside the limit, but
    // ICP has since doubled, so 2 ckUSDC for 1 ICP is now 50% below the market.
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 2_000_000, 2_000_000))
    });

    let error = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(mock_oracle(4, 1))
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect_err("the guard must bound the quote against the live price");

    assert!(error.to_string().contains("below oracle-implied output"));
}

#[tokio::test]
async fn recorded_price_is_used_while_fresh_when_the_oracle_is_unavailable() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_500_000, 1_500_000))
    });

    let error = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(failing_oracle())
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect_err("a fresh recorded price still bounds the quote");

    assert!(error.to_string().contains("below oracle-implied output"));
}

#[tokio::test]
async fn stale_recorded_price_stands_the_guard_down_instead_of_blocking_the_swap() {
    // Nothing can price this quote: the oracle is down and the recorded price is
    // older than the configured window. Blocking here would strand collateral
    // the liquidation already holds, so the remaining planner checks decide.
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_500_000, 1_500_000))
    });
    let mut input = native_icp_to_ckusdc_with_oracle();
    input.reference_price_captured_at = Some(QUOTED_AT - 301);

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(failing_oracle())
        .plan(&input, QUOTED_AT)
        .await
        .expect("an unpriceable plan must fall back to the non-oracle checks");

    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(1_500_000u64));
}

#[tokio::test]
async fn an_unknown_recorded_price_age_is_treated_as_stale() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_500_000, 1_500_000))
    });
    let mut input = native_icp_to_ckusdc_with_oracle();
    input.reference_price_captured_at = None;

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(failing_oracle())
        .plan(&input, QUOTED_AT)
        .await
        .expect("a price that cannot be dated cannot be shown to describe the market");

    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(1_500_000u64));
}

#[tokio::test]
async fn one_missing_oracle_side_falls_back_instead_of_mixing_two_instants() {
    // Only the pay side answers. Pairing a live price with a recorded one would
    // measure the pair across two different instants, so the read is discarded
    // and the fresh snapshot decides -- which rejects this quote.
    let mut oracle = MockPriceOracle::new();
    oracle
        .expect_get_price()
        .with(eq("ICP"), eq(USD_QUOTE_CURRENCY))
        .returning(|_, _| Ok((Nat::from(RAY), 27)));
    oracle
        .expect_get_price()
        .with(eq("USDC"), eq(USD_QUOTE_CURRENCY))
        .returning(|_, _| Err("no USDC feed".to_string()));

    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_025_000, 1_025_000))
    });

    let error = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(Arc::new(oracle))
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect_err("a half-answered oracle read must not be used");

    assert!(error.to_string().contains("below oracle-implied output"));
}

#[tokio::test]
async fn a_non_positive_live_price_falls_back_to_the_recorded_price() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_500_000, 1_500_000))
    });

    let error = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .with_price_oracle(mock_oracle(0, 1))
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect_err("a zero price cannot bound anything, so the snapshot decides");

    assert!(error.to_string().contains("below oracle-implied output"));
}

#[tokio::test]
async fn oracle_discount_guard_accepts_quote_within_inclusive_limit() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, calls, |request| {
        // Oracle output is 2 ckUSDC. Exactly 2% below it is accepted.
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_960_000, 1_950_000))
    });

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(0.0))
        .expect("planner")
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect("boundary quote should pass");

    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
}

#[tokio::test]
async fn production_oracle_discount_accepts_exactly_250_bps_but_rejects_one_native_unit_less() {
    let exact = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        // Oracle output is 2,000,000 units; 1,950,000 is exactly 2.5% below it.
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_950_000, 1_950_000))
    });
    let accepted = IcpswapFirstPlanner::new(vec![Arc::new(exact)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect("the inclusive 250 bps boundary should pass");
    assert_eq!(accepted.legs[0].quote.estimated_receive.value, Nat::from(1_950_000u64));

    let one_less = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_949_999, 1_949_999))
    });
    let error = IcpswapFirstPlanner::new(vec![Arc::new(one_less)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect_err("one native output unit below the boundary must fail");
    assert!(error.to_string().contains("exceeding the 250 bps limit"));
}

#[tokio::test]
async fn oracle_discount_guard_accepts_a_quote_better_than_oracle() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 2_100_000, 2_100_000))
    });

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect("a better-than-oracle quote should pass the one-sided guard");

    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(2_100_000u64));
}

/// A forced leg under $10 is too small for the guard to price, so it is waived.
/// The MEXC remainder holds the rest of the collateral and stays checked, which
/// `forced_test_split_rejects_a_bad_mexc_leg_even_when_icpswap_is_valid` covers.
#[tokio::test]
async fn forced_test_split_waives_the_oracle_guard_for_a_sub_ten_dollar_icpswap_leg() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        // The $1 leg is 0.1 ICP, whose oracle output is 200,000 ckUSDC units.
        // 194,999 is past the 250 bps limit and would be rejected at full size.
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 194_999, 194_999))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, MEXC_VENUE_ID, 0.0, 1_800_000, 1_800_000))
    });
    let mut planner_config = production_oracle_config(8.0);
    planner_config.icpswap_test_allocation_usd = Some(1.0);

    let state = planner(icpswap, mexc, planner_config)
        .plan(&native_icp_to_ckusdc_with_oracle(), QUOTED_AT)
        .await
        .expect("a sub-$10 forced leg is exempt from the oracle guard");

    assert_eq!(state.legs.len(), 2);
    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(194_999u64));
    assert_eq!(state.legs[1].venue_id, MEXC_VENUE_ID);
}

#[tokio::test]
async fn forced_test_split_still_guards_a_ten_dollar_icpswap_leg() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        // The $10 leg is 1 ICP, whose oracle output is 2,000,000 ckUSDC units.
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_900_000, 1_900_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, MEXC_VENUE_ID, 0.0, 8_000_000, 8_000_000))
    });
    let mut planner_config = production_oracle_config(8.0);
    planner_config.icpswap_test_allocation_usd = Some(10.0);
    // 5 ICP at $10 leaves a $40 MEXC remainder after the $10 forced leg.
    let mut input = native_icp_to_ckusdc_with_oracle();
    input.total_pay.value = Nat::from(500_000_000u64);

    let error = planner(icpswap, mexc, planner_config)
        .plan(&input, QUOTED_AT)
        .await
        .expect_err("at $10 the forced leg is priced by the guard again");

    assert!(error.to_string().contains("icpswap quote is"));
    assert!(error.to_string().contains("250 bps limit"));
}

#[tokio::test]
async fn forced_test_split_rejects_a_bad_mexc_leg_even_when_icpswap_is_valid() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 200_000, 200_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        // The remainder is 0.9 ICP, whose oracle output is 1,800,000 units.
        Ok(preview(request, MEXC_VENUE_ID, 0.0, 1_754_999, 1_754_999))
    });
    let mut planner_config = production_oracle_config(8.0);
    planner_config.icpswap_test_allocation_usd = Some(1.0);

    let error = planner(icpswap, mexc, planner_config)
        .plan(&native_icp_to_ckusdc_with_oracle(), 123)
        .await
        .expect_err("a rejected MEXC leg must prevent the forced split");

    assert!(error.to_string().contains("mexc quote is"));
    assert!(error.to_string().contains("250 bps limit"));
}

#[tokio::test]
async fn legacy_receipt_without_debt_oracle_price_skips_only_the_oracle_guard() {
    let collateral = native_icp();
    let debt = debt_token();
    let receipt = ExecutionReceipt {
        request: ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(1_000_000u64),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: Some(SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(collateral.clone(), Nat::from(1u8)),
                receive_asset: debt.asset_id(),
                receive_address: Some("receiver".to_string()),
                max_slippage_bps: Some(500),
                venue_hint: None,
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 0,
            ref_price: Nat::from(2 * RAY),
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            id: 1,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(TOTAL_PAY),
                debt_repaid: Nat::from(1_000_000u64),
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
    let input = IcpswapFirstPlanInput::from_receipt(&receipt).expect("legacy planner input");
    assert_eq!(input.pay_reference_price_ray, None);
    assert_eq!(input.receive_reference_price_ray, None);

    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        // This would be 25% below the 2 ICP -> 2 ckUSDC oracle output if the
        // missing receive-side oracle price had been available.
        Ok(preview(request, ICPSWAP_VENUE_ID, 10.0, 1_500_000, 1_500_000))
    });
    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], production_oracle_config(0.0))
        .expect("planner")
        .plan(&input, 123)
        .await
        .expect("legacy receipt should retain non-oracle planner protections");

    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(1_500_000u64));
    assert!(state.plan.combined_net_edge_bps >= 1_500.0);
}

#[tokio::test]
async fn oracle_discount_guard_handles_ckusdc_to_icp_decimals_and_direction() {
    let mut input = ckusdc_to_native_icp_input();
    input.debt_repaid.value = Nat::from(4_000_000_000u64);
    input.pay_reference_price_ray = Some(Nat::from(RAY));
    input.receive_reference_price_ray = Some(Nat::from(2 * RAY));
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let mut route = preview(request, ICPSWAP_VENUE_ID, 10.0, 4_950_000_000, 4_900_000_000);
        route.conservative_receive.token = native_icp();
        Ok(route)
    });

    let state = IcpswapFirstPlanner::new(vec![Arc::new(icpswap)], config(0.0))
        .expect("planner")
        .plan(&input, 123)
        .await
        .expect("reverse quote within oracle limit should pass");

    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn oracle_guard_is_exact_across_generated_decimals_and_both_directions(
        pay_decimals in 0u8..=12,
        receive_decimals in 0u8..=12,
        whole_pay in 1u64..=100,
        pay_price in 1u64..=20,
        receive_price in 1u64..=20,
        reverse in any::<bool>(),
    ) {
        let mut pay_token = if reverse { debt_token() } else { native_icp() };
        let mut receive_token = if reverse { native_icp() } else { debt_token() };
        let ChainToken::Icp { decimals, .. } = &mut pay_token else {
            unreachable!("test tokens are ICP-family tokens")
        };
        *decimals = pay_decimals;
        let ChainToken::Icp { decimals, .. } = &mut receive_token else {
            unreachable!("test tokens are ICP-family tokens")
        };
        *decimals = receive_decimals;

        let pay_scale = 10u64.pow(u32::from(pay_decimals));
        let receive_scale = 10u64.pow(u32::from(receive_decimals));
        let pay_value = whole_pay.checked_mul(pay_scale).expect("generated pay fits u64");
        let oracle_output = u128::from(whole_pay) * u128::from(pay_price) * u128::from(receive_scale)
            / u128::from(receive_price);
        prop_assume!(oracle_output <= u128::from(u64::MAX));
        let oracle_output = oracle_output as u64;

        // The validator compares actual*10,000 with oracle*9,750. Therefore
        // the smallest accepted integer amount is the ceiling of that ratio.
        let minimum_accepted = (u128::from(oracle_output) * 9_750 + 9_999) / 10_000;
        prop_assume!(minimum_accepted >= 2 && minimum_accepted <= u128::from(u64::MAX));
        let minimum_accepted = minimum_accepted as u64;

        let make_input = || IcpswapFirstPlanInput {
            liquidation_id: "42".to_string(),
            total_pay: ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(pay_value)),
            receive_asset: receive_token.asset_id(),
            debt_repaid: ChainTokenAmount::from_raw(receive_token.clone(), Nat::from(1u8)),
            receive_address: Some("receiver".to_string()),
            max_execution_slippage_bps: Some(500),
            pay_reference_price_usd: Some(pay_price as f64),
            pay_reference_price_ray: Some(Nat::from(u128::from(pay_price) * RAY)),
            receive_reference_price_ray: Some(Nat::from(u128::from(receive_price) * RAY)),
            reference_price_captured_at: Some(QUOTED_AT),
            buy_bad_debt: false,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        let accepted_receive_token = receive_token.clone();
        let accepted_adapter = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
            let mut route = preview(
                request,
                ICPSWAP_VENUE_ID,
                10.0,
                minimum_accepted,
                minimum_accepted,
            );
            route.conservative_receive.token = accepted_receive_token.clone();
            Ok(route)
        });
        let accepted = runtime.block_on(async {
            IcpswapFirstPlanner::new(vec![Arc::new(accepted_adapter)], production_oracle_config(0.0))
                .expect("planner")
                .plan(&make_input(), 123)
                .await
        });
        prop_assert!(accepted.is_ok(), "minimum integer boundary should pass: {accepted:?}");

        let rejected_amount = minimum_accepted - 1;
        let rejected_receive_token = receive_token.clone();
        let rejected_adapter = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
            let mut route = preview(
                request,
                ICPSWAP_VENUE_ID,
                10.0,
                rejected_amount,
                rejected_amount,
            );
            route.conservative_receive.token = rejected_receive_token.clone();
            Ok(route)
        });
        let rejected = runtime.block_on(async {
            IcpswapFirstPlanner::new(vec![Arc::new(rejected_adapter)], production_oracle_config(0.0))
                .expect("planner")
                .plan(&make_input(), 123)
                .await
        });
        prop_assert!(rejected.is_err(), "one unit below the integer boundary must fail");
        prop_assert!(rejected.unwrap_err().to_string().contains("250 bps limit"));
    }
}
