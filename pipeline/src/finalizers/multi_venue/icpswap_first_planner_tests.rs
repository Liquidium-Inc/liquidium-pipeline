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

use super::multi_venue_quote_book::{VenuePreviewOutcome, VenueRegistry};
use super::*;
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

#[async_trait]
impl MultiVenueAdapter for BarrierAdapter {
    fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    async fn preview(&self, _request: &SwapRequest) -> Result<VenueRoutePreview, String> {
        self.barrier.wait().await;
        Err(format!("{} unavailable", self.venue_id))
    }

    async fn advance(&self, _leg: &VenueLegState) -> Result<VenueLegProgress, String> {
        Err("not used by planner test".to_string())
    }

    async fn recover(&self, _leg: &VenueLegState) -> Result<VenueLegProgress, String> {
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
        total_pay: ChainTokenAmount::from_raw(pay_token, Nat::from(TOTAL_PAY)),
        receive_asset: debt.asset_id(),
        debt_repaid: ChainTokenAmount::from_raw(debt, Nat::from(DEBT_REPAID)),
        receive_address: Some("receiver".to_string()),
        max_execution_slippage_bps: Some(500),
        pay_reference_price_usd: 10.0,
    }
}

fn config(cex_min_exec_usd: f64) -> IcpswapFirstPlannerConfig {
    IcpswapFirstPlannerConfig {
        max_price_impact_bps: 100.0,
        max_search_iterations: 16,
        cex_min_exec_usd,
        min_net_edge_bps: 150,
        overflow_venue_ids: vec![MEXC_VENUE_ID.to_string()],
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

fn mock_adapter<F>(venue_id: &'static str, calls: Arc<Mutex<Vec<u64>>>, responder: F) -> MockMultiVenueAdapter
where
    F: Fn(&SwapRequest) -> Result<VenueRoutePreview, String> + Send + Sync + 'static,
{
    let mut adapter = MockMultiVenueAdapter::new();
    adapter.expect_venue_id().return_const(venue_id);
    adapter.expect_validate_configuration().returning(|| Ok(()));
    adapter.expect_preview().returning(move |request| {
        calls
            .lock()
            .expect("calls lock")
            .push(request.pay_amount.value.0.to_u64().expect("test pay amount fits u64"));
        responder(request)
    });
    adapter
}

fn planner(
    icpswap: MockMultiVenueAdapter,
    mexc: MockMultiVenueAdapter,
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
        registry.preview_all(|venue_id| input.request_for(venue_id, Nat::from(TOTAL_PAY))),
    )
    .await
    .expect("both previews must be polled concurrently");
    let previews = quotes.iter().collect::<Vec<_>>();

    assert_eq!(previews[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(previews[1].venue_id, KRAKEN_VENUE_ID);
    assert!(matches!(previews[0].outcome, VenuePreviewOutcome::Unavailable(_)));
    assert!(matches!(previews[1].outcome, VenuePreviewOutcome::Unavailable(_)));
}

#[test]
fn registry_rejects_an_adapter_with_missing_runtime_configuration() {
    let mut adapter = MockMultiVenueAdapter::new();
    adapter.expect_venue_id().return_const(MEXC_VENUE_ID);
    adapter
        .expect_validate_configuration()
        .returning(|| Err("token registry is required".to_string()));

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
async fn full_preview_quotes_newly_registered_venues_without_planner_changes() {
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
    let mut planner_config = config(1.1);
    planner_config.overflow_venue_ids.push(KRAKEN_VENUE_ID.to_string());
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
    assert_eq!(
        mexc_calls_for_assert.lock().expect("calls lock").as_slice(),
        &[TOTAL_PAY]
    );
    assert_eq!(
        kraken_calls_for_assert.lock().expect("calls lock").as_slice(),
        &[TOTAL_PAY]
    );
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
    assert_eq!(mexc_calls.first(), Some(&TOTAL_PAY));
    assert_eq!(mexc_calls.last(), Some(&mexc_remainder));
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
async fn sub_minimum_mexc_remainder_uses_full_icpswap_as_dust_exception() {
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
        .expect("dust exception plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, ICPSWAP_VENUE_ID);
    assert!(state.legs[0].quote.estimated_price_impact_bps >= 100.0);
    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::RemainderBelowMinimum {
            skipped_venue_ids: vec![MEXC_VENUE_ID.to_string()],
            selected_venue_id: ICPSWAP_VENUE_ID.to_string(),
        }
    );
}

#[tokio::test]
async fn dust_exception_requotes_icpswap_after_binary_search_instead_of_reusing_stale_preview() {
    let call_count = Arc::new(Mutex::new(0u32));
    let icpswap_call_count = call_count.clone();
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / 900_000.0;
        let mut count = icpswap_call_count.lock().expect("count lock");
        *count += 1;
        // Only the re-quote for the full amount taken *after* the binary search
        // (the second call at pay == TOTAL_PAY) reflects the fresher price.
        let receive = if pay == TOTAL_PAY && *count > 1 {
            pay * 3
        } else {
            pay * 2
        };
        Ok(preview(request, ICPSWAP_VENUE_ID, impact, receive, receive))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("dust exception plan");

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
    assert_eq!(mexc_calls.first(), Some(&TOTAL_PAY));
    assert_eq!(mexc_calls.last(), Some(&TOTAL_PAY));
    assert!(mexc_calls.len() >= 3);
}

#[tokio::test]
async fn dust_reason_records_every_skipped_overflow_venue() {
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
    let mut planner_config = config(1.1);
    planner_config.overflow_venue_ids.push(KRAKEN_VENUE_ID.to_string());
    let planner = IcpswapFirstPlanner::new(
        vec![Arc::new(icpswap), Arc::new(mexc), Arc::new(kraken)],
        planner_config,
    )
    .expect("valid planner");

    let state = planner
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("dust exception plan");

    assert_eq!(
        state.plan.allocation_reason,
        MultiVenueAllocationReason::RemainderBelowMinimum {
            skipped_venue_ids: vec![MEXC_VENUE_ID.to_string(), KRAKEN_VENUE_ID.to_string()],
            selected_venue_id: ICPSWAP_VENUE_ID.to_string(),
        }
    );
}

#[tokio::test]
async fn safe_value_zero_requotes_mexc_after_binary_search_instead_of_reusing_stale_preview() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, ICPSWAP_VENUE_ID, 100.0, 2))
    });
    let call_count = Arc::new(Mutex::new(0u32));
    let mexc_call_count = call_count.clone();
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), move |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let mut count = mexc_call_count.lock().expect("count lock");
        *count += 1;
        // The re-quote taken after the (immediately-unsafe) search reflects the
        // fresher price; the stale initial quote must not be reused.
        let receive = if *count > 1 { pay * 3 } else { pay * 2 };
        Ok(preview(request, MEXC_VENUE_ID, 0.0, receive, receive))
    });

    let state = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect("MEXC-only plan");

    assert_eq!(state.legs.len(), 1);
    assert_eq!(state.legs[0].venue_id, MEXC_VENUE_ID);
    assert_eq!(state.legs[0].quote.estimated_receive.value, Nat::from(TOTAL_PAY * 3));
}

#[tokio::test]
async fn dust_exception_still_requires_the_profit_floor() {
    let icpswap = mock_adapter(ICPSWAP_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        let pay = request.pay_amount.value.0.to_u64().expect("u64 pay");
        let impact = pay as f64 / 900_000.0;
        Ok(preview(request, ICPSWAP_VENUE_ID, impact, 180_000_000, 180_000_000))
    });
    let mexc = mock_adapter(MEXC_VENUE_ID, Arc::new(Mutex::new(Vec::new())), |request| {
        Ok(proportional_preview(request, MEXC_VENUE_ID, 0.0, 2))
    });

    let error = planner(icpswap, mexc, config(1.1))
        .plan(&input_with_pay_token(native_icp()), 123)
        .await
        .expect_err("unprofitable dust exception must fail");

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
    let mut planner_config = config(1.1);
    planner_config.overflow_venue_ids.push(KRAKEN_VENUE_ID.to_string());
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
        .expect_err("unsafe ICPSwap must not use dust exception when MEXC is unavailable");
    assert!(error.to_string().contains("MEXC unavailable"));
}

// Asset eligibility and adapter validation

#[tokio::test]
async fn non_native_icp_is_mexc_only_and_never_previews_icpswap() {
    let mut icpswap = MockMultiVenueAdapter::new();
    icpswap.expect_venue_id().return_const(ICPSWAP_VENUE_ID);
    icpswap.expect_validate_configuration().returning(|| Ok(()));
    icpswap.expect_preview().times(0);
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
