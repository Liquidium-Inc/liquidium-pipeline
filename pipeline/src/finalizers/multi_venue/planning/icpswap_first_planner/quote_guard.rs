use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use num_traits::ToPrimitive;
use tracing::{debug, warn};

use crate::{
    finalizers::multi_venue::VenueRoutePreview,
    liquidation::{collateral_service::USD_QUOTE_CURRENCY, liquidation_math::oracle_implied_output},
    swappers::model::{SwapRequest, adverse_price_impact_bps},
};

use super::{
    BPS_DENOMINATOR, IcpswapFirstPlanInput, IcpswapFirstPlanner, IcpswapFirstPlannerError,
    ORACLE_GUARD_MIN_TEST_ALLOCATION_USD, input::positive_price,
};
use crate::finalizers::multi_venue::planning::icpswap_first_planner_utils::validate_preview;

/// Startup notice that a configured test allocation has switched the oracle
/// quote guard off for that leg. Loud on purpose: it is a live routing setting,
/// not a test-harness flag, so it can reach production unnoticed.
pub(in crate::finalizers::multi_venue) fn oracle_guard_waiver_banner(target_usd: f64) -> String {
    let rule = "=".repeat(66);
    format!(
        "\n{rule}\n\
         ⚠️  ORACLE QUOTE GUARD DISABLED FOR THE ICPSWAP TEST LEG  ⚠️\n\
         {rule}\n\
         ICPSWAP_TEST_ALLOCATION_USD=${target_usd:.2} is below the \
         ${ORACLE_GUARD_MIN_TEST_ALLOCATION_USD:.2} minimum, so that leg's\n\
         quote is NOT priced against the oracle on any liquidation. At this size\n\
         its fixed ledger fees are a larger share of the leg than the whole\n\
         discount budget, so the check cannot say anything about the price.\n\
         The overflow remainder is still checked. Raise the allocation to \
         ${ORACLE_GUARD_MIN_TEST_ALLOCATION_USD:.2} or\n\
         clear ICPSWAP_TEST_ALLOCATION_USD to price every leg again.\n\
         {rule}"
    )
}

/// Maps a ledger token onto the symbol the price oracle is keyed by.
///
/// The registry names chain-key wrappers after their ledger (`ckUSDC`) while the
/// oracle prices the underlying asset (`USDC`), so the prefix is stripped here.
pub(in crate::finalizers::multi_venue) fn oracle_price_symbol(token: &ChainToken) -> String {
    let symbol = token.symbol();
    match symbol.strip_prefix("ck") {
        Some(underlying) if !underlying.is_empty() => underlying.to_string(),
        _ => symbol,
    }
}

impl IcpswapFirstPlanner {
    /// Prefers a live oracle read over the prices recorded on the receipt.
    ///
    /// Recorded prices are only a fresh fallback; stale prices cannot safely
    /// bound a current venue quote.
    pub(super) async fn resolve_oracle_prices(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> IcpswapFirstPlanInput {
        let mut resolved = input.clone();
        if let Some((pay_price, receive_price)) = self.live_oracle_prices(input).await {
            debug!(
                "[multi-venue] liq_id={} using live oracle prices for the venue quote guard",
                input.liquidation_id
            );
            resolved.pay_reference_price_ray = Some(pay_price);
            resolved.receive_reference_price_ray = Some(receive_price);
            return resolved;
        }
        if resolved.pay_reference_price_ray.is_none() {
            return resolved;
        }

        let age_secs = input
            .reference_price_captured_at
            .map(|captured_at| quoted_at.saturating_sub(captured_at));
        match age_secs {
            Some(age) if age <= self.config.oracle_snapshot_max_age_secs => {
                debug!(
                    "[multi-venue] liq_id={} oracle unavailable; using recorded prices from {age}s ago",
                    input.liquidation_id
                );
            }
            _ => {
                warn!(
                    "[multi-venue] liq_id={} oracle unavailable and recorded prices are stale (age={:?}s, max={}s); skipping the venue quote oracle guard",
                    input.liquidation_id, age_secs, self.config.oracle_snapshot_max_age_secs
                );
                resolved.pay_reference_price_ray = None;
                resolved.receive_reference_price_ray = None;
            }
        }
        resolved
    }

    /// Reads both pair prices from one live oracle snapshot.
    async fn live_oracle_prices(&self, input: &IcpswapFirstPlanInput) -> Option<(Nat, Nat)> {
        let oracle = self.price_oracle.as_ref()?;
        let pay_symbol = oracle_price_symbol(&input.total_pay.token);
        let receive_symbol = oracle_price_symbol(&input.debt_repaid.token);
        let (pay, receive) = tokio::join!(
            oracle.get_price(&pay_symbol, USD_QUOTE_CURRENCY),
            oracle.get_price(&receive_symbol, USD_QUOTE_CURRENCY),
        );
        match (pay, receive) {
            (Ok((pay_price, _)), Ok((receive_price, _))) => {
                match (positive_price(&pay_price), positive_price(&receive_price)) {
                    (Some(pay_price), Some(receive_price)) => Some((pay_price, receive_price)),
                    _ => {
                        warn!(
                            "[multi-venue] liq_id={} oracle returned a non-positive price for {pay_symbol} or {receive_symbol}",
                            input.liquidation_id
                        );
                        None
                    }
                }
            }
            (pay, receive) => {
                warn!(
                    "[multi-venue] liq_id={} live oracle read failed ({pay_symbol}: {:?}, {receive_symbol}: {:?})",
                    input.liquidation_id,
                    pay.err(),
                    receive.err()
                );
                None
            }
        }
    }

    /// Verifies request identity and bounds a venue's output against the oracle.
    pub(super) fn validate_venue_preview(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
        preview: &VenueRoutePreview,
    ) -> Result<(), IcpswapFirstPlannerError> {
        validate_preview(venue_id, request, preview)?;

        let Some(oracle_output) = oracle_expected_output(input, request, preview)? else {
            return Ok(());
        };
        if oracle_output == Nat::from(0u8) {
            return Ok(());
        }

        let allowed_bps = BPS_DENOMINATOR - self.config.max_oracle_discount_bps;
        let actual_scaled = preview.quote.receive_amount.clone() * Nat::from(BPS_DENOMINATOR);
        let minimum_scaled = oracle_output.clone() * Nat::from(allowed_bps);
        if actual_scaled >= minimum_scaled {
            return Ok(());
        }

        let discount_bps = adverse_price_impact_bps(
            oracle_output.0.to_f64().unwrap_or(f64::INFINITY),
            preview.quote.receive_amount.0.to_f64().unwrap_or(0.0),
        );

        Err(IcpswapFirstPlannerError::NoViableRoute(format!(
            "{venue_id} quote is {:.2} bps below oracle-implied output, exceeding the {} bps limit",
            discount_bps, self.config.max_oracle_discount_bps
        )))
    }
}

/// Converts one venue allocation into receive-token native units at oracle prices.
fn oracle_expected_output(
    input: &IcpswapFirstPlanInput,
    request: &SwapRequest,
    preview: &VenueRoutePreview,
) -> Result<Option<Nat>, IcpswapFirstPlannerError> {
    let (Some(pay_price), Some(receive_price)) = (
        input.pay_reference_price_ray.as_ref(),
        input.receive_reference_price_ray.as_ref(),
    ) else {
        return Ok(None);
    };
    oracle_implied_output(
        &request.pay_amount.value,
        pay_price,
        receive_price,
        u64::from(request.pay_amount.token.decimals()),
        u64::from(preview.conservative_receive.token.decimals()),
    )
    .map(Some)
    .map_err(IcpswapFirstPlannerError::InvalidInput)
}
