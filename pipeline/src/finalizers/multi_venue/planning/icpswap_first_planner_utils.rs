use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use num_traits::ToPrimitive;

use crate::{
    finalizers::multi_venue::VenueRoutePreview,
    persistance::{VenueLegQuote, VenueLegState, VenueLegStatus},
    swappers::model::SwapRequest,
};

use super::icpswap_first_planner::{BPS_DENOMINATOR, IcpswapFirstPlannerError};

// Ensures an adapter response describes the exact venue, request, assets, and
// conservative output that the planner can safely persist and execute.
pub(super) fn validate_preview(
    expected_venue: &str,
    request: &SwapRequest,
    preview: &VenueRoutePreview,
) -> Result<(), IcpswapFirstPlannerError> {
    if preview.venue_id != expected_venue || preview.initial_execution_state.venue != expected_venue {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "preview venue does not match adapter `{expected_venue}`"
        )));
    }
    if &preview.request != request {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "{expected_venue} preview request does not match requested allocation"
        )));
    }
    if preview.quote.pay_asset != request.pay_asset || preview.quote.receive_asset != request.receive_asset {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "{expected_venue} quote assets do not match request"
        )));
    }
    if preview.conservative_receive.token.asset_id() != request.receive_asset {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "{expected_venue} conservative output token does not match receive asset"
        )));
    }
    if preview.conservative_receive.value > preview.quote.receive_amount {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "{expected_venue} conservative output exceeds estimated output"
        )));
    }
    if !preview.quote.estimated_price_impact_bps.is_finite() || preview.quote.estimated_price_impact_bps < 0.0 {
        return Err(IcpswapFirstPlannerError::InvalidInput(format!(
            "{expected_venue} quote price impact must be finite and non-negative"
        )));
    }
    Ok(())
}

// Converts a validated preview into the initial persisted state for one venue
// leg. No side effects have happened while the leg is Planned.
pub(super) fn preview_to_leg(preview: VenueRoutePreview) -> VenueLegState {
    let route_id = if preview.quote.legs.is_empty() {
        preview.venue_id.clone()
    } else {
        preview
            .quote
            .legs
            .iter()
            .map(|leg| leg.route_id.as_str())
            .collect::<Vec<_>>()
            .join(">")
    };
    let estimated_receive = ChainTokenAmount::from_raw(
        preview.conservative_receive.token.clone(),
        preview.quote.receive_amount.clone(),
    );

    VenueLegState {
        leg_id: format!("{}-0", preview.venue_id),
        venue_id: preview.venue_id,
        request: preview.request.clone(),
        quote: VenueLegQuote {
            pay_amount: preview.request.pay_amount,
            estimated_receive,
            conservative_receive: preview.conservative_receive,
            estimated_price_impact_bps: preview.quote.estimated_price_impact_bps,
            route_id,
        },
        execution: preview.initial_execution_state,
        status: VenueLegStatus::Planned,
        result: None,
        last_error: None,
    }
}

// Adds estimated or conservative outputs across selected legs while enforcing
// that every leg returns the same token.
pub(super) fn sum_leg_outputs(
    legs: &[VenueLegState],
    conservative: bool,
) -> Result<ChainTokenAmount, IcpswapFirstPlannerError> {
    let first = legs.first().ok_or_else(|| {
        IcpswapFirstPlannerError::InvalidInput("multi-venue plan must contain at least one leg".to_string())
    })?;
    let first_amount = if conservative {
        &first.quote.conservative_receive
    } else {
        &first.quote.estimated_receive
    };
    let token = first_amount.token.clone();
    let mut value = Nat::from(0u8);
    for leg in legs {
        let amount = if conservative {
            &leg.quote.conservative_receive
        } else {
            &leg.quote.estimated_receive
        };
        if amount.token != token {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "venue leg output tokens do not match".to_string(),
            ));
        }
        value = value + amount.value.clone();
    }
    Ok(ChainTokenAmount::from_raw(token, value))
}

// Uses integer arithmetic for the pass/fail edge decision so floating-point
// rounding cannot approve an otherwise unprofitable plan.
pub(super) fn meets_minimum_edge(
    conservative: &ChainTokenAmount,
    debt_repaid: &ChainTokenAmount,
    minimum_bps: u32,
) -> Result<bool, IcpswapFirstPlannerError> {
    ensure_same_output_token(conservative, debt_repaid)?;
    let required = debt_repaid.value.clone() * Nat::from(BPS_DENOMINATOR + minimum_bps);
    let actual = conservative.value.clone() * Nat::from(BPS_DENOMINATOR);
    Ok(actual >= required)
}

// Computes the floating-point edge used only for reporting and persistence;
// the actual eligibility decision is made by `meets_minimum_edge` above.
pub(super) fn edge_bps(
    conservative: &ChainTokenAmount,
    debt_repaid: &ChainTokenAmount,
) -> Result<f64, IcpswapFirstPlannerError> {
    ensure_same_output_token(conservative, debt_repaid)?;
    let debt = debt_repaid.value.0.to_f64().unwrap_or(f64::INFINITY);
    let output = conservative.value.0.to_f64().unwrap_or(f64::INFINITY);
    if debt <= 0.0 || !debt.is_finite() || !output.is_finite() {
        return Err(IcpswapFirstPlannerError::InvalidInput(
            "cannot represent output edge safely".to_string(),
        ));
    }
    Ok(((output - debt) / debt) * f64::from(BPS_DENOMINATOR))
}

fn ensure_same_output_token(
    conservative: &ChainTokenAmount,
    debt_repaid: &ChainTokenAmount,
) -> Result<(), IcpswapFirstPlannerError> {
    if conservative.token != debt_repaid.token {
        return Err(IcpswapFirstPlannerError::InvalidInput(
            "conservative output token does not match repaid debt token".to_string(),
        ));
    }
    Ok(())
}
