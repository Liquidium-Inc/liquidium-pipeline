use std::{fmt, fmt::Display};

use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use num_traits::ToPrimitive;

use crate::{
    error::{AppError, error_codes},
    persistance::{FinalizerDecisionSnapshot, now_secs},
    stages::executor::ExecutionReceipt,
    swappers::model::SwapRequest,
};

/// Route tiny notional swaps to DEX to avoid CEX overhead/noise for dust-sized trades.
pub(crate) const DEX_DUST_MAX_USD: f64 = 2.5;
/// Basis points per 1.00 ratio value (100%).
const BPS_PER_RATIO_UNIT: f64 = 10_000.0;
/// Basis points per 1.00 percentage-point value.
const BPS_PER_PERCENTAGE_POINT: f64 = 100.0;
/// Fixed-point divisor used to convert RAY price (1e27) into decimal price.
pub(crate) const RAY_PRICE_SCALE: f64 = 1e27_f64;

/// Route venues available in hybrid finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteVenue {
    Dex,
    Cex,
}

impl Display for RouteVenue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouteVenue::Dex => formatter.write_str("dex"),
            RouteVenue::Cex => formatter.write_str("cex"),
        }
    }
}

/// Comparable route candidate represented by projected net edge.
#[derive(Debug, Clone)]
pub(crate) struct RouteCandidate {
    pub(crate) venue: RouteVenue,
    pub(crate) gross_edge_bps: f64,
    pub(crate) net_edge_bps: f64,
    pub(crate) reason: String,
}

/// Convert internal route venue into external swapper id used by exports/UI.
pub(crate) fn swapper_id(venue: RouteVenue) -> &'static str {
    match venue {
        RouteVenue::Dex => "kong",
        RouteVenue::Cex => "mexc",
    }
}

/// Net edge in bps after configured CEX-specific haircuts.
///
/// Note: slippage is intentionally not a parameter because CEX gross edge is
/// computed from previewed output amount, which already includes slippage impact.
pub(crate) fn net_edge_bps(gross_edge_bps: f64, fee_bps: f64, delay_bps: f64) -> f64 {
    gross_edge_bps - fee_bps - delay_bps
}

/// Convert previewed receive amount into gross edge bps against debt repaid.
pub(crate) fn preview_gross_edge_bps(estimated_receive_amount: f64, debt_repaid_amount: f64) -> f64 {
    if debt_repaid_amount <= 0.0 {
        return 0.0;
    }
    ((estimated_receive_amount - debt_repaid_amount) / debt_repaid_amount) * BPS_PER_RATIO_UNIT
}

pub(crate) fn debt_repaid_f64(receipt: &ExecutionReceipt) -> Result<f64, AppError> {
    let liquidation_result = receipt.liquidation_result.as_ref().ok_or_else(|| {
        AppError::from_def(error_codes::INVALID_INPUT).with_context("missing liquidation_result in receipt")
    })?;
    let debt_repaid_amount = ChainTokenAmount::from_raw(
        receipt.request.debt_asset.clone(),
        liquidation_result.amounts.debt_repaid.clone(),
    )
    .to_f64();
    if debt_repaid_amount <= 0.0 {
        return Err(
            AppError::from_def(error_codes::INVALID_INPUT).with_context("invalid debt_repaid amount in receipt")
        );
    }
    Ok(debt_repaid_amount)
}

/// Normalize quote slippage into basis points.
pub(crate) fn dex_slippage_bps(slippage: f64) -> f64 {
    // Kong reports slippage in percentage points, e.g. 0.72 => 0.72%.
    // 1% = 100 bps, hence *100.
    (slippage.max(0.0)) * BPS_PER_PERCENTAGE_POINT
}

/// Estimate swap notional in USD using strategy-provided reference price (ray format 1e27).
pub(crate) fn estimate_swap_value_usd(receipt: &ExecutionReceipt, swap_req: &SwapRequest) -> f64 {
    let reference_price_usd = receipt.request.ref_price.0.to_f64().unwrap_or(0.0) / RAY_PRICE_SCALE;
    if reference_price_usd <= 0.0 {
        return 0.0;
    }
    let swap_input_amount = swap_req.pay_amount.to_f64();
    (swap_input_amount * reference_price_usd).max(0.0)
}

/// Dust swaps are forced to DEX to avoid CEX overhead on tiny notional.
pub(crate) fn is_dust_swap(receipt: &ExecutionReceipt, swap_req: &SwapRequest) -> bool {
    let estimated_swap_value_usd = estimate_swap_value_usd(receipt, swap_req);
    estimated_swap_value_usd > 0.0 && estimated_swap_value_usd < DEX_DUST_MAX_USD
}

/// In hybrid mode, force CEX when estimated notional is above configured threshold.
/// A non-positive threshold disables this fast-path override.
pub(crate) fn should_force_cex_over_threshold(
    receipt: &ExecutionReceipt,
    swap_req: &SwapRequest,
    force_cex_threshold_usd: f64,
) -> bool {
    let estimated_swap_value_usd = estimate_swap_value_usd(receipt, swap_req);
    if force_cex_threshold_usd <= 0.0 {
        return false;
    }
    estimated_swap_value_usd > force_cex_threshold_usd
}

/// Pick route with the highest net edge among viable candidates.
pub(crate) fn choose_best_route(dex: Option<RouteCandidate>, cex: Option<RouteCandidate>) -> Option<RouteCandidate> {
    match (dex, cex) {
        (Some(dex), Some(cex)) => {
            if cex.net_edge_bps > dex.net_edge_bps {
                Some(cex)
            } else {
                Some(dex)
            }
        }
        (Some(dex), None) => Some(dex),
        (None, Some(cex)) => Some(cex),
        (None, None) => None,
    }
}

pub(crate) fn make_snapshot(
    mode: &str,
    chosen: &str,
    reason: impl Into<String>,
    min_required_bps: f64,
    dex_preview: Option<&RouteCandidate>,
    cex_preview: Option<&RouteCandidate>,
) -> FinalizerDecisionSnapshot {
    FinalizerDecisionSnapshot {
        mode: mode.to_string(),
        chosen: chosen.to_string(),
        reason: reason.into(),
        min_required_bps,
        dex_preview_gross_bps: dex_preview.map(|candidate| candidate.gross_edge_bps),
        dex_preview_net_bps: dex_preview.map(|candidate| candidate.net_edge_bps),
        cex_preview_gross_bps: cex_preview.map(|candidate| candidate.gross_edge_bps),
        cex_preview_net_bps: cex_preview.map(|candidate| candidate.net_edge_bps),
        ts: now_secs(),
    }
}
