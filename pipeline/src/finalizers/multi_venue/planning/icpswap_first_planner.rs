use std::{cmp::max, sync::Arc};

use candid::Nat;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};
use num_traits::ToPrimitive;
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenuePlanningContext, VenueRoutePreview},
    liquidation::{collateral_service::USD_QUOTE_CURRENCY, liquidation_math::oracle_implied_output},
    persistance::{
        MultiVenueAllocationReason, MultiVenueExecutionOutcome, MultiVenueExecutionPlan, MultiVenueExecutionState,
        VenueLegState,
    },
    price_oracle::price_oracle::PriceOracle,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{identity::parse_liquidation_id, supports_pair},
        model::{SwapRequest, adverse_price_impact_bps},
    },
};

use super::{
    icpswap_first_planner_utils::{edge_bps, meets_minimum_edge, preview_to_leg, sum_leg_outputs, validate_preview},
    venue_registry::{VenuePreviewOutcome, VenueQuoteBook, VenueRegistry},
};

pub const ICPSWAP_FIRST_STRATEGY_ID: &str = "icpswap_first";
pub const ICPSWAP_VENUE_ID: &str = "icpswap";
pub const MEXC_VENUE_ID: &str = "mexc";

pub(super) const BPS_DENOMINATOR: u32 = 10_000;
const RAY_PRICE_SCALE: f64 = 1e27;
/// Smallest forced ICPSwap test allocation the oracle quote guard is applied to,
/// in USD. See `oracle_guard_waived_for_test_leg`.
const ORACLE_GUARD_MIN_TEST_ALLOCATION_USD: f64 = 10.0;

#[derive(Debug, Clone, PartialEq)]
pub struct IcpswapFirstPlannerConfig {
    pub max_price_impact_bps: f64,
    pub max_search_iterations: u8,
    pub dust_fallback_max_price_impact_bps: f64,
    pub cex_min_exec_usd: f64,
    pub min_net_edge_bps: u32,
    /// Edge floor applied instead of `min_net_edge_bps` when the liquidation was
    /// bought as bad debt. Signed, because such a row repays more than the
    /// collateral is worth by construction: a non-negative floor can never be
    /// met and would leave the collateral stranded. `-10000` recycles it
    /// whatever the shortfall; the quote is still bounded by the oracle guard.
    pub bad_debt_min_net_edge_bps: i32,
    pub max_oracle_discount_bps: u32,
    /// How old a recorded price may be, in seconds, before the guard stops using
    /// it as a fallback for a live oracle read.
    pub oracle_snapshot_max_age_secs: i64,
    pub icpswap_test_allocation_usd: Option<f64>,
}

impl IcpswapFirstPlannerConfig {
    // Rejects configuration that could make allocation ambiguous or unsafe.
    fn validate(&self) -> Result<(), IcpswapFirstPlannerError> {
        if !self.max_price_impact_bps.is_finite() || self.max_price_impact_bps <= 0.0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "maximum ICPSwap price impact must be finite and positive".to_string(),
            ));
        }
        if self.max_search_iterations == 0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "max search iterations must be positive".to_string(),
            ));
        }
        if !self.dust_fallback_max_price_impact_bps.is_finite()
            || self.dust_fallback_max_price_impact_bps < self.max_price_impact_bps
            || self.dust_fallback_max_price_impact_bps > f64::from(BPS_DENOMINATOR)
        {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "dust fallback ICPSwap impact must be between {:.2} and {} bps",
                self.max_price_impact_bps, BPS_DENOMINATOR
            )));
        }
        if !self.cex_min_exec_usd.is_finite() || self.cex_min_exec_usd < 0.0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "CEX minimum execution USD must be finite and non-negative".to_string(),
            ));
        }
        if self.min_net_edge_bps > BPS_DENOMINATOR {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "minimum net edge {} bps exceeds {} bps",
                self.min_net_edge_bps, BPS_DENOMINATOR
            )));
        }
        let bps_limit = i32::try_from(BPS_DENOMINATOR).unwrap_or(i32::MAX);
        if self.bad_debt_min_net_edge_bps > bps_limit || self.bad_debt_min_net_edge_bps < -bps_limit {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "bad-debt minimum net edge {} bps must stay within ±{} bps",
                self.bad_debt_min_net_edge_bps, BPS_DENOMINATOR
            )));
        }
        // A discount of exactly BPS_DENOMINATOR permits any output at all, which
        // silently disables the guard rather than loosening it.
        if self.max_oracle_discount_bps >= BPS_DENOMINATOR {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "maximum oracle discount {} bps must stay below {} bps",
                self.max_oracle_discount_bps, BPS_DENOMINATOR
            )));
        }
        // A venue's reported price impact already includes its pool fee, because
        // impact is measured against a fee-free pool spot. The oracle guard sees
        // that same shortfall plus the input ledger fees and the pool-versus-
        // oracle basis, so its limit has to sit above the impact caps or it would
        // reject the quotes those caps deliberately allow.
        if f64::from(self.max_oracle_discount_bps) <= self.dust_fallback_max_price_impact_bps {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "maximum oracle discount {} bps must exceed the {:.2} bps dust fallback impact cap",
                self.max_oracle_discount_bps, self.dust_fallback_max_price_impact_bps
            )));
        }
        if self.oracle_snapshot_max_age_secs < 0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "oracle snapshot max age must not be negative".to_string(),
            ));
        }
        if let Some(value) = self.icpswap_test_allocation_usd
            && (!value.is_finite() || value <= 0.0)
        {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "test ICPSwap allocation USD must be finite and positive".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IcpswapFirstPlanInput {
    pub liquidation_id: String,
    pub total_pay: ChainTokenAmount,
    pub receive_asset: AssetId,
    pub debt_repaid: ChainTokenAmount,
    pub receive_address: Option<String>,
    pub max_execution_slippage_bps: Option<u32>,
    /// USD price of the pay asset, or `None` when the receipt carries no usable
    /// price. Absent must stay absent rather than collapse to `0.0`, because a
    /// zero price makes every notional look below the CEX minimum.
    pub pay_reference_price_usd: Option<f64>,
    /// Raw RAY oracle prices retained for exact, direction-neutral venue quote
    /// validation. Missing values are supported for legacy WAL receipts.
    pub pay_reference_price_ray: Option<Nat>,
    pub receive_reference_price_ray: Option<Nat>,
    /// Unix seconds when the prices above were recorded, or `None` when the
    /// receipt predates the field. Used to decide whether they are still fresh
    /// enough to bound a venue quote.
    pub reference_price_captured_at: Option<i64>,
    /// Whether this liquidation was bought as bad debt. Such a row repays more
    /// than the collateral is worth by design, so it is held to its own edge
    /// floor rather than the profitable-liquidation one.
    pub buy_bad_debt: bool,
}

impl IcpswapFirstPlanInput {
    /// Builds planner input from confirmed liquidation results, using the
    /// collateral actually received rather than the estimated request amount.
    pub fn from_receipt(receipt: &ExecutionReceipt) -> Result<Self, IcpswapFirstPlannerError> {
        if !matches!(receipt.status, ExecutionStatus::Success) {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "receipt execution is not successful".to_string(),
            ));
        }
        let swap = receipt
            .request
            .swap_args
            .as_ref()
            .ok_or_else(|| IcpswapFirstPlannerError::InvalidInput("receipt has no swap request".to_string()))?;
        let liquidation = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| IcpswapFirstPlannerError::InvalidInput("receipt has no liquidation result".to_string()))?;
        let total_pay = ChainTokenAmount::from_raw(
            receipt.request.collateral_asset.clone(),
            liquidation.amounts.collateral_received.clone(),
        );
        let debt_repaid = ChainTokenAmount::from_raw(
            receipt.request.debt_asset.clone(),
            liquidation.amounts.debt_repaid.clone(),
        );
        if swap.pay_asset != total_pay.token.asset_id() {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "swap pay asset does not match received collateral".to_string(),
            ));
        }
        let pay_reference_price_usd = reference_price_usd(&receipt.request.ref_price);
        let (pay_reference_price_ray, receive_reference_price_ray) = match (
            positive_price(&receipt.request.ref_price),
            positive_price(&receipt.request.debt_ref_price),
        ) {
            (Some(pay), Some(receive)) => (Some(pay), Some(receive)),
            _ => (None, None),
        };

        let input = Self {
            liquidation_id: liquidation.id.to_string(),
            total_pay,
            receive_asset: swap.receive_asset.clone(),
            debt_repaid,
            receive_address: swap.receive_address.clone(),
            max_execution_slippage_bps: swap.max_slippage_bps,
            pay_reference_price_usd,
            pay_reference_price_ray,
            receive_reference_price_ray,
            reference_price_captured_at: (receipt.request.ref_price_at > 0).then_some(receipt.request.ref_price_at),
            buy_bad_debt: receipt.request.liquidation.buy_bad_debt,
        };
        input.validate()?;
        Ok(input)
    }

    // Verifies the amount and asset invariants required by every venue quote.
    pub(in crate::finalizers::multi_venue) fn validate(&self) -> Result<(), IcpswapFirstPlannerError> {
        // Shares the identity module's rule: a plan must never validate an ID
        // that would derive a different signing principal at execution time.
        parse_liquidation_id(&self.liquidation_id).map_err(IcpswapFirstPlannerError::InvalidInput)?;
        if self.total_pay.value == Nat::from(0u8) {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "total pay amount must be positive".to_string(),
            ));
        }
        if self.debt_repaid.value == Nat::from(0u8) {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "debt repaid amount must be positive".to_string(),
            ));
        }
        if self.debt_repaid.token.asset_id() != self.receive_asset {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "debt repaid token does not match receive asset".to_string(),
            ));
        }
        if let Some(price) = self.pay_reference_price_usd
            && (!price.is_finite() || price <= 0.0)
        {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "pay reference price must be finite and positive when present".to_string(),
            ));
        }
        if self.pay_reference_price_ray.is_some() != self.receive_reference_price_ray.is_some() {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "oracle pay and receive prices must either both be present or both be absent".to_string(),
            ));
        }
        Ok(())
    }

    pub(in crate::finalizers::multi_venue) fn planning_context(&self) -> VenuePlanningContext {
        VenuePlanningContext {
            liquidation_id: self.liquidation_id.clone(),
        }
    }

    // Produces an amount-scoped request tagged for one specific venue.
    pub(in crate::finalizers::multi_venue) fn request_for(&self, venue_id: &str, pay_value: Nat) -> SwapRequest {
        SwapRequest {
            pay_asset: self.total_pay.token.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(self.total_pay.token.clone(), pay_value),
            receive_asset: self.receive_asset.clone(),
            receive_address: self.receive_address.clone(),
            max_slippage_bps: self.max_execution_slippage_bps,
            venue_hint: Some(venue_id.to_string()),
        }
    }

    fn is_icpswap_supported_pair(&self) -> bool {
        supports_pair(&self.total_pay.token, &self.receive_asset)
    }

    fn meets_cex_minimum(&self, amount: &ChainTokenAmount, minimum_usd: f64) -> bool {
        if minimum_usd <= 0.0 {
            return true;
        }
        // An unusable price makes the notional unknown, not zero. Reporting
        // unknown as below-minimum would filter every overflow venue out of
        // `best_executable_overflow` and push `plan_split_or_fallback` into
        // forcing the whole amount through ICPSwap at an impact it already
        // rejected. Defer to normal sizing and let the venue apply its own
        // minimum instead.
        let Some(price) = self.pay_reference_price_usd else {
            return true;
        };
        let notional = amount.to_f64() * price;
        notional.is_finite() && notional >= minimum_usd
    }
}

/// Converts a RAY-scaled receipt price into USD, or `None` when it cannot size a
/// notional. Zero, negative, and values `to_f64` cannot represent (it saturates
/// to infinity) are all absent prices, not cheap ones.
pub(in crate::finalizers::multi_venue) fn reference_price_usd(ref_price_ray: &Nat) -> Option<f64> {
    let price = ref_price_ray.0.to_f64()? / RAY_PRICE_SCALE;
    (price.is_finite() && price > 0.0).then_some(price)
}

fn positive_price(price_ray: &Nat) -> Option<Nat> {
    (price_ray > &Nat::from(0u8)).then(|| price_ray.clone())
}

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapFirstPlannerError {
    #[error("invalid ICPSwap-first planner input: {0}")]
    InvalidInput(String),
    #[error("no viable ICPSwap-first route: {0}")]
    NoViableRoute(String),
    /// Every venue that could take this collateral answered, and the only
    /// reason none can execute is that the amount is below their execution
    /// minimum. Unlike `NoViableRoute` this is a property of the amount rather
    /// than of venue availability, so retrying cannot resolve it and the
    /// caller may move the collateral out of the swap path.
    ///
    /// Never construct this while any venue failed to quote: an outage may end,
    /// and a retry would then produce a real route.
    #[error("no venue can execute this amount: {0}")]
    BelowVenueMinimum(String),
}

pub struct IcpswapFirstPlanner {
    venues: Arc<VenueRegistry>,
    overflow_venue_ids: Vec<String>,
    config: IcpswapFirstPlannerConfig,
    price_oracle: Option<Arc<dyn PriceOracle>>,
}

impl IcpswapFirstPlanner {
    /// Builds a planner from an ordered adapter list and verifies that every
    /// venue required by the strategy is registered exactly once.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(
        adapters: Vec<Arc<dyn MultiVenueAdapter>>,
        config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, IcpswapFirstPlannerError> {
        let venues = Arc::new(VenueRegistry::new(adapters).map_err(IcpswapFirstPlannerError::InvalidInput)?);
        Self::from_registry(venues, config)
    }

    pub(in crate::finalizers::multi_venue) fn from_registry(
        venues: Arc<VenueRegistry>,
        config: IcpswapFirstPlannerConfig,
    ) -> Result<Self, IcpswapFirstPlannerError> {
        config.validate()?;
        if let Some(target_usd) = config.icpswap_test_allocation_usd
            && target_usd < ORACLE_GUARD_MIN_TEST_ALLOCATION_USD
        {
            warn!("{}", oracle_guard_waiver_banner(target_usd));
        }
        let overflow_venue_ids = venues
            .venue_ids()
            .into_iter()
            .filter(|venue_id| venue_id != ICPSWAP_VENUE_ID)
            .collect();
        Ok(Self {
            venues,
            overflow_venue_ids,
            config,
            price_oracle: None,
        })
    }

    /// Supplies the oracle the venue guard reads at planning time. Without one
    /// the guard falls back to the receipt prices while they are younger than
    /// `oracle_snapshot_max_age_secs`.
    pub(in crate::finalizers::multi_venue) fn with_price_oracle(mut self, price_oracle: Arc<dyn PriceOracle>) -> Self {
        self.price_oracle = Some(price_oracle);
        self
    }

    /// Quotes eligible venues and applies the ICPSwap-first allocation policy
    /// without writing the WAL or executing any swap.
    pub async fn plan(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        input.validate()?;

        // Resolve the guard's oracle prices once, before any venue is quoted, so
        // every leg of one plan is bounded against the same reference.
        let resolved = self.resolve_oracle_prices(input, quoted_at).await;
        let input = &resolved;
        let context = input.planning_context();

        if !input.is_icpswap_supported_pair() || !self.venues.contains(ICPSWAP_VENUE_ID) {
            return self.plan_overflow_only(input, quoted_at).await;
        }

        if let Some(target_usd) = self.config.icpswap_test_allocation_usd {
            return self.plan_test_fixed_icpswap_split(input, quoted_at, target_usd).await;
        }

        // The common safe-ICPSwap path needs no CEX data. Quote ICPSwap alone
        // first so adding overflow venues does not add unconditional latency or
        // API load to every liquidation.
        let quotes = self
            .venues
            .preview_venues(&context, &[ICPSWAP_VENUE_ID.to_string()], |venue_id| {
                input.request_for(venue_id, input.total_pay.value.clone())
            })
            .await
            .map_err(IcpswapFirstPlannerError::InvalidInput)?;
        let quotes = self.drop_invalid_quotes(input, quotes);
        let icpswap = quotes
            .get(ICPSWAP_VENUE_ID)
            .ok_or_else(|| IcpswapFirstPlannerError::InvalidInput("ICPSwap full preview is missing".to_string()))?;

        match &icpswap.outcome {
            VenuePreviewOutcome::Quoted(preview) if self.is_safe_icpswap(preview) => self.build_state(
                input,
                vec![preview.clone()],
                MultiVenueAllocationReason::SingleVenue {
                    venue_id: ICPSWAP_VENUE_ID.to_string(),
                },
                quoted_at,
            ),
            VenuePreviewOutcome::Quoted(_) if self.overflow_venue_ids.is_empty() => {
                Err(IcpswapFirstPlannerError::NoViableRoute(
                    "ICPSwap full quote exceeds the price-impact limit and no overflow venue is enabled".to_string(),
                ))
            }
            VenuePreviewOutcome::Quoted(_) => self.plan_split_or_fallback(input, quoted_at).await,
            VenuePreviewOutcome::Unavailable(icpswap_error) | VenuePreviewOutcome::Invalid(icpswap_error) => {
                let fallthrough_reason = format!("ICPSwap quote rejected ({icpswap_error})");
                let overflow_quotes = self
                    .preview_overflow(input, input.total_pay.value.clone())
                    .await
                    .map_err(|error| self.classify_overflow_preview_error(error, Some(&fallthrough_reason)))?;
                let overflow = self.best_executable_overflow(input, &overflow_quotes).ok_or_else(|| {
                    IcpswapFirstPlannerError::NoViableRoute(format!(
                        "ICPSwap quote rejected ({icpswap_error}); {}",
                        self.overflow_failure_summary(&overflow_quotes)
                    ))
                })?;
                let mut unavailable_venue_ids = vec![ICPSWAP_VENUE_ID.to_string()];
                unavailable_venue_ids.extend(
                    overflow_quotes
                        .iter()
                        .filter(|preview| !matches!(preview.outcome, VenuePreviewOutcome::Quoted(_)))
                        .map(|preview| preview.venue_id.clone()),
                );
                self.build_state(
                    input,
                    vec![overflow.clone()],
                    MultiVenueAllocationReason::VenueUnavailable {
                        selected_venue_id: overflow.venue_id.clone(),
                        unavailable_venue_ids,
                    },
                    quoted_at,
                )
            }
        }
    }

    /// Test-only deterministic split used to exercise concurrent venue status
    /// and execution. Normal price-impact allocation remains unchanged when
    /// the override is absent.
    async fn plan_test_fixed_icpswap_split(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
        target_usd: f64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        if self.overflow_venue_ids.is_empty() {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "test ICPSwap split requires an enabled overflow venue".to_string(),
            ));
        }
        let reference_price = input.pay_reference_price_usd.ok_or_else(|| {
            IcpswapFirstPlannerError::NoViableRoute(
                "test ICPSwap USD split requires a positive collateral reference price".to_string(),
            )
        })?;
        let icpswap_value =
            ChainTokenAmount::from_formatted(input.total_pay.token.clone(), target_usd / reference_price).value;
        if icpswap_value == Nat::from(0u8) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "test ICPSwap allocation ${target_usd:.2} rounds to zero native units"
            )));
        }
        if icpswap_value >= input.total_pay.value {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "received collateral is too small to split ${target_usd:.2} to ICPSwap and leave a MEXC remainder"
            )));
        }

        let remainder_value = input.total_pay.value.clone() - icpswap_value.clone();
        let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());
        if !input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd) {
            return self.plan_below_minimum_remainder(input, quoted_at).await;
        }

        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, icpswap_value);
        // Only the forced leg is exempted; the MEXC remainder holds the bulk of
        // the collateral and is large enough for the guard to mean something.
        let unguarded_input = self.oracle_guard_waived_for_test_leg(target_usd).then(|| {
            warn!(
                "[multi-venue] liq_id={} oracle quote guard waived for the ${target_usd:.2} ICPSwap test leg",
                input.liquidation_id
            );
            let mut unguarded = input.clone();
            unguarded.pay_reference_price_ray = None;
            unguarded.receive_reference_price_ray = None;
            unguarded
        });
        let icpswap_input = unguarded_input.as_ref().unwrap_or(input);
        let (icpswap_result, overflow_result) = tokio::join!(
            self.preview_exact(icpswap_input, ICPSWAP_VENUE_ID, &icpswap_request),
            self.preview_overflow(input, remainder_value),
        );
        let icpswap = icpswap_result?;
        if !self.is_safe_icpswap(&icpswap) {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "test ICPSwap allocation impact {:.2} bps is not below {:.2} bps",
                icpswap.quote.estimated_price_impact_bps, self.config.max_price_impact_bps
            )));
        }
        let overflow_quotes = overflow_result?;
        let overflow = self
            .best_executable_overflow(input, &overflow_quotes)
            .ok_or_else(|| IcpswapFirstPlannerError::NoViableRoute(self.overflow_failure_summary(&overflow_quotes)))?;

        self.build_state(
            input,
            vec![icpswap, overflow.clone()],
            MultiVenueAllocationReason::PriceImpactSplit,
            quoted_at,
        )
    }

    // Avoids a dust overflow leg by re-quoting the full amount on ICPSwap with
    // the narrowly relaxed fallback cap. If that quote is still too expensive,
    // the full amount is sent to the best executable overflow venue.
    async fn plan_below_minimum_remainder(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, input.total_pay.value.clone());
        let icpswap = match self.preview_exact(input, ICPSWAP_VENUE_ID, &icpswap_request).await {
            Ok(icpswap) => icpswap,
            Err(error) => {
                return self
                    .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                    .await;
            }
        };
        if self.is_safe_dust_fallback_icpswap(&icpswap) {
            return self.build_state(
                input,
                vec![icpswap],
                MultiVenueAllocationReason::RemainderBelowMinimum {
                    skipped_venue_ids: self.overflow_venue_ids.clone(),
                    selected_venue_id: ICPSWAP_VENUE_ID.to_string(),
                },
                quoted_at,
            );
        }

        let overflow = self
            .full_amount_overflow(
                input,
                Some(
                    "full ICPSwap quote exceeds the dust-fallback price-impact limit and the below-minimum remainder cannot be split"
                        .to_string(),
                ),
            )
            .await?;
        let mut skipped_venue_ids = vec![ICPSWAP_VENUE_ID.to_string()];
        skipped_venue_ids.extend(
            self.overflow_venue_ids
                .iter()
                .filter(|venue_id| venue_id.as_str() != overflow.venue_id.as_str())
                .cloned(),
        );
        self.build_state(
            input,
            vec![overflow.clone()],
            MultiVenueAllocationReason::RemainderBelowMinimum {
                skipped_venue_ids,
                selected_venue_id: overflow.venue_id.clone(),
            },
            quoted_at,
        )
    }

    // Quotes the complete allocation on every overflow venue and returns the
    // best executable one.
    //
    // `fallthrough_reason` prefixes the rejection summary when the caller
    // reached this path because another venue fell through. Its presence is
    // load-bearing, not cosmetic: every caller that supplies one arrived here
    // through a venue state a later retry could resolve, so those failures are
    // never reported as a permanent below-minimum dead end.
    async fn full_amount_overflow(
        &self,
        input: &IcpswapFirstPlanInput,
        fallthrough_reason: Option<String>,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let quotes = self
            .preview_overflow(input, input.total_pay.value.clone())
            .await
            .map_err(|error| self.classify_overflow_preview_error(error, fallthrough_reason.as_deref()))?;
        self.best_executable_overflow(input, &quotes).cloned().ok_or_else(|| {
            let summary = self.overflow_failure_summary(&quotes);
            match fallthrough_reason {
                Some(reason) => IcpswapFirstPlannerError::NoViableRoute(format!("{reason}; {summary}")),
                None if self.every_overflow_venue_quoted(&quotes) => {
                    IcpswapFirstPlannerError::BelowVenueMinimum(summary)
                }
                None => IcpswapFirstPlannerError::NoViableRoute(summary),
            }
        })
    }

    // Whether every configured overflow venue actually answered. Combined with
    // `best_executable_overflow` returning nothing, this is what distinguishes
    // "all venues are healthy and this amount is simply too small" from "a
    // venue is down", which is not a permanent property of the amount.
    fn every_overflow_venue_quoted(&self, quotes: &VenueQuoteBook) -> bool {
        !self.overflow_venue_ids.is_empty()
            && self.overflow_venue_ids.iter().all(|venue_id| {
                matches!(
                    quotes.get(venue_id).map(|venue| &venue.outcome),
                    Some(VenuePreviewOutcome::Quoted(_))
                )
            })
    }

    fn below_minimum_overflow_summary(&self) -> String {
        self.overflow_venue_ids
            .iter()
            .map(|venue_id| format!("{} amount is below its minimum", venue_id.to_uppercase()))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn classify_overflow_preview_error(
        &self,
        error: IcpswapFirstPlannerError,
        fallthrough_reason: Option<&str>,
    ) -> IcpswapFirstPlannerError {
        match (error, fallthrough_reason) {
            (IcpswapFirstPlannerError::BelowVenueMinimum(summary), Some(reason)) => {
                IcpswapFirstPlannerError::NoViableRoute(format!("{reason}; {summary}"))
            }
            (error, _) => error,
        }
    }

    // Non-native ICP collateral cannot use ICPSwap, so choose the executable
    // overflow venue with the highest conservative output.
    async fn plan_overflow_only(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        if self.overflow_venue_ids.is_empty() {
            return Err(IcpswapFirstPlannerError::NoViableRoute(
                "no enabled venue can accept this collateral asset".to_string(),
            ));
        }
        let preview = self.full_amount_overflow(input, None).await?;
        let venue_id = preview.venue_id.clone();
        self.build_state(
            input,
            vec![preview],
            MultiVenueAllocationReason::SingleVenue { venue_id },
            quoted_at,
        )
    }

    // Re-plans the complete amount on overflow venues when ICPSwap's final
    // exact quote cannot be used. The earlier remainder quotes are discarded
    // because they do not cover the amount that must now be allocated.
    async fn plan_full_overflow_after_icpswap_failure(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
        icpswap_error: &IcpswapFirstPlannerError,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let overflow = self
            .full_amount_overflow(input, Some(format!("ICPSwap exact quote rejected ({icpswap_error})")))
            .await?;
        let selected_venue_id = overflow.venue_id.clone();
        self.build_state(
            input,
            vec![overflow],
            MultiVenueAllocationReason::VenueUnavailable {
                selected_venue_id,
                unavailable_venue_ids: vec![ICPSWAP_VENUE_ID.to_string()],
            },
            quoted_at,
        )
    }

    // Finds the largest safe ICPSwap leg, then either sends the remainder to
    // the best overflow venue or selects one safe full-amount venue when the
    // remainder cannot meet the overflow minimum.
    async fn plan_split_or_fallback(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let safe_icpswap = self.search_safe_icpswap(input).await?;
        let safe_value = safe_icpswap
            .as_ref()
            .map(|preview| preview.request.pay_amount.value.clone())
            .unwrap_or_else(|| Nat::from(0u8));
        let remainder_value = input.total_pay.value.clone() - safe_value.clone();
        let remainder = ChainTokenAmount::from_raw(input.total_pay.token.clone(), remainder_value.clone());

        if !input.meets_cex_minimum(&remainder, self.config.cex_min_exec_usd) {
            return self.plan_below_minimum_remainder(input, quoted_at).await;
        }

        if safe_value == Nat::from(0u8) {
            let overflow = self.full_amount_overflow(input, None).await?;
            let venue_id = overflow.venue_id.clone();
            return self.build_state(
                input,
                vec![overflow],
                MultiVenueAllocationReason::SingleVenue { venue_id },
                quoted_at,
            );
        }

        let icpswap_request = input.request_for(ICPSWAP_VENUE_ID, safe_value);
        let (icpswap_result, overflow_result) = tokio::join!(
            self.preview_exact(input, ICPSWAP_VENUE_ID, &icpswap_request),
            self.preview_overflow(input, remainder_value),
        );
        let icpswap = match icpswap_result {
            Ok(icpswap) => icpswap,
            Err(error) => {
                return self
                    .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                    .await;
            }
        };
        if !self.is_safe_icpswap(&icpswap) {
            // An exact quote that comes back unsafe is no more usable than one
            // that failed outright, so it takes the same route: re-quote the full
            // amount on overflow venues instead of abandoning a liquidation that
            // a single venue could still execute. This has to be decided before
            // the remainder is resolved, because the split is already dead here:
            // letting a remainder that no venue will quote fail first would
            // report that failure instead of taking the fallback.
            let error = IcpswapFirstPlannerError::NoViableRoute(format!(
                "exact ICPSwap allocation impact {:.2} bps is not below {:.2} bps",
                icpswap.quote.estimated_price_impact_bps, self.config.max_price_impact_bps
            ));
            return self
                .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                .await;
        }

        let overflow_quotes = overflow_result?;
        let overflow = self
            .best_executable_overflow(input, &overflow_quotes)
            .ok_or_else(|| IcpswapFirstPlannerError::NoViableRoute(self.overflow_failure_summary(&overflow_quotes)))?;

        self.build_state(
            input,
            vec![icpswap, overflow.clone()],
            MultiVenueAllocationReason::PriceImpactSplit,
            quoted_at,
        )
    }

    // Binary-searches for the largest amount whose confirmed ICPSwap impact
    // remains strictly below the configured limit. Quote failures are unsafe.
    async fn search_safe_icpswap(
        &self,
        input: &IcpswapFirstPlanInput,
    ) -> Result<Option<VenueRoutePreview>, IcpswapFirstPlannerError> {
        // `lower` is always zero or an amount backed by a confirmed safe quote.
        // `upper` is the smallest amount currently known to be unsafe or
        // unquotable. We never return `upper` as an allocation.
        let mut lower = Nat::from(0u8);
        let mut upper = input.total_pay.value.clone();

        // Searching more precisely than one ledger fee is not executable, and
        // total_pay / 10_000 caps relative allocation error at one basis point.
        let proportional_tolerance = input.total_pay.value.clone() / Nat::from(BPS_DENOMINATOR);
        let tolerance = max(Nat::from(1u8), max(input.total_pay.token.fee(), proportional_tolerance));

        // Keep the complete quote, not only its amount, so the returned lower
        // bound is always the last amount that was positively confirmed safe.
        let mut last_safe = None;

        for _ in 0..self.config.max_search_iterations {
            let range = upper.clone() - lower.clone();
            if range <= tolerance {
                break;
            }

            // Halve the remaining interval. The equality guard prevents a
            // non-progressing loop when integer division rounds to a bound.
            let midpoint = (lower.clone() + upper.clone()) / Nat::from(2u8);
            if midpoint == lower || midpoint == upper {
                break;
            }
            let request = input.request_for(ICPSWAP_VENUE_ID, midpoint.clone());
            match self.preview_exact(input, ICPSWAP_VENUE_ID, &request).await {
                Ok(preview) if self.is_safe_icpswap(&preview) => {
                    // Safe midpoint: retain its quote and search higher.
                    lower = midpoint;
                    last_safe = Some(preview);
                }
                // Unsafe impact, an unavailable quote, or a malformed quote all
                // mean this midpoint is not confirmed safe; continue below it.
                Ok(_) | Err(_) => upper = midpoint,
            }
        }

        Ok(last_safe)
    }

    /// Whether the forced test leg is too small for the oracle guard to say
    /// anything about its price.
    ///
    /// ICPSwap reserves three input ledger fees before the pool sees the money,
    /// and the guard measures against the whole allocation. That gap is a share
    /// of the leg, so on a small one it can exceed the entire discount budget by
    /// itself -- three ckUSDC transfers are 3% of a $1 leg -- and the guard would
    /// reject a quote priced perfectly.
    fn oracle_guard_waived_for_test_leg(&self, target_usd: f64) -> bool {
        target_usd < ORACLE_GUARD_MIN_TEST_ALLOCATION_USD
    }

    // The threshold is strict: exactly 100 bps is not below a 100 bps limit.
    fn is_safe_icpswap(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps < self.config.max_price_impact_bps
    }

    // This relaxed cap applies only when the overflow remainder cannot meet
    // its venue minimum. "Up to" is inclusive at exactly the configured cap.
    fn is_safe_dust_fallback_icpswap(&self, preview: &VenueRoutePreview) -> bool {
        preview.quote.estimated_price_impact_bps <= self.config.dust_fallback_max_price_impact_bps
    }

    // Re-quotes one venue for an exact allocation and verifies that its state,
    // request, and assets match what the planner asked for.
    async fn preview_exact(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, IcpswapFirstPlannerError> {
        let adapter = self.venues.adapter(venue_id).ok_or_else(|| {
            IcpswapFirstPlannerError::InvalidInput(format!("venue adapter `{venue_id}` is not registered"))
        })?;
        let context = input.planning_context();
        let preview = adapter.preview(&context, request).await.map_err(|error| {
            IcpswapFirstPlannerError::NoViableRoute(format!("{} preview failed: {error}", adapter.venue_id()))
        })?;
        self.validate_venue_preview(input, adapter.venue_id(), request, &preview)?;
        Ok(preview)
    }

    // Quotes configured overflow venues for the exact amount left after the
    // safe ICPSwap allocation has been determined.
    async fn preview_overflow(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_value: Nat,
    ) -> Result<VenueQuoteBook, IcpswapFirstPlannerError> {
        // Overflow dust must never reach a venue adapter. Fixed transfer fees
        // can dominate a tiny allocation and make a fair market quote look like
        // an oracle-price failure, while the venue cannot execute the amount
        // economically in any case.
        let pay_amount = ChainTokenAmount::from_raw(input.total_pay.token.clone(), pay_value.clone());
        if !input.meets_cex_minimum(&pay_amount, self.config.cex_min_exec_usd) {
            return Err(IcpswapFirstPlannerError::BelowVenueMinimum(
                self.below_minimum_overflow_summary(),
            ));
        }

        let context = input.planning_context();
        let quotes = self
            .venues
            .preview_venues(&context, &self.overflow_venue_ids, |venue_id| {
                input.request_for(venue_id, pay_value.clone())
            })
            .await
            .map_err(IcpswapFirstPlannerError::InvalidInput)?;
        Ok(self.drop_invalid_quotes(input, quotes))
    }

    // Converts malformed responses into per-venue Invalid outcomes so one bad
    // quote cannot prevent valid venues from being considered.
    fn drop_invalid_quotes(&self, input: &IcpswapFirstPlanInput, mut quotes: VenueQuoteBook) -> VenueQuoteBook {
        for venue in quotes.iter_mut() {
            let validation_error = match &venue.outcome {
                VenuePreviewOutcome::Quoted(preview) => self
                    .validate_venue_preview(input, &venue.venue_id, &venue.request, preview)
                    .err(),
                VenuePreviewOutcome::Unavailable(_) | VenuePreviewOutcome::Invalid(_) => None,
            };
            if let Some(error) = validation_error {
                venue.outcome = VenuePreviewOutcome::Invalid(error.to_string());
            }
        }
        quotes
    }

    /// Prefers a live oracle read over the prices recorded on the receipt.
    ///
    /// The recorded prices are captured before the liquidation executes, so by
    /// planning time the market may have moved and a one-sided guard would
    /// reject quotes that are honest at the current price. They are used only as
    /// a fallback, and only while young enough; past that the guard stands down
    /// instead of blocking collateral the liquidation already holds.
    async fn resolve_oracle_prices(&self, input: &IcpswapFirstPlanInput, quoted_at: i64) -> IcpswapFirstPlanInput {
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

        // An unknown capture time is treated as too old: it cannot be shown to
        // describe the current market.
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

    /// Reads both sides of the pair from the oracle, or `None` when no oracle is
    /// configured, or when either price is missing or unusable. A partial answer
    /// is never mixed with a recorded price: the two must describe one instant.
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

    /// Checks every venue quote in one place, including ICPSwap and MEXC.
    ///
    /// The quote must match what was asked for and must not sit far below the
    /// oracle-implied output. Venue code only builds the quote, so no venue can
    /// skip either half.
    ///
    /// The bound applies to the estimated output. `meets_minimum_edge` is the
    /// floor on the conservative output, which legitimately sits an execution
    /// slippage and a ledger fee below the estimate.
    fn validate_venue_preview(
        &self,
        input: &IcpswapFirstPlanInput,
        venue_id: &str,
        request: &SwapRequest,
        preview: &VenueRoutePreview,
    ) -> Result<(), IcpswapFirstPlannerError> {
        // Reject a malformed quote or one that belongs to a different request.
        validate_preview(venue_id, request, preview)?;

        // With no usable oracle prices there is nothing to compare against.
        let Some(oracle_output) = oracle_expected_output(input, request, preview)? else {
            return Ok(());
        };
        // A zero baseline cannot express a meaningful minimum output.
        if oracle_output == Nat::from(0u8) {
            return Ok(());
        }

        // With a 250 bps limit the quote must return at least 9,750 / 10,000 of
        // the oracle-implied output. Both sides are multiplied out so the
        // decision stays in exact integer token units.
        let allowed_bps = BPS_DENOMINATOR - self.config.max_oracle_discount_bps;
        let actual_scaled = preview.quote.receive_amount.clone() * Nat::from(BPS_DENOMINATOR);
        let minimum_scaled = oracle_output.clone() * Nat::from(allowed_bps);
        if actual_scaled >= minimum_scaled {
            return Ok(());
        }

        // Floating point only describes the rejection; the decision above was
        // already made exactly. An unrepresentable ratio reports an infinite
        // discount rather than a misleading number.
        let discount_bps = adverse_price_impact_bps(
            oracle_output.0.to_f64().unwrap_or(f64::INFINITY),
            preview.quote.receive_amount.0.to_f64().unwrap_or(0.0),
        );

        Err(IcpswapFirstPlannerError::NoViableRoute(format!(
            "{venue_id} quote is {:.2} bps below oracle-implied output, exceeding the {} bps limit",
            discount_bps, self.config.max_oracle_discount_bps
        )))
    }

    // Excludes unavailable, invalid, and below-minimum overflow quotes, then
    // selects the venue with the highest conservative receive amount.
    fn best_executable_overflow<'a>(
        &self,
        input: &IcpswapFirstPlanInput,
        quotes: &'a VenueQuoteBook,
    ) -> Option<&'a VenueRoutePreview> {
        self.overflow_venue_ids
            .iter()
            .filter_map(|venue_id| quotes.get(venue_id))
            .filter_map(|venue| match &venue.outcome {
                VenuePreviewOutcome::Quoted(preview)
                    if input.meets_cex_minimum(&preview.request.pay_amount, self.config.cex_min_exec_usd) =>
                {
                    Some(preview)
                }
                _ => None,
            })
            // Replace the current selection only for a strictly better quote,
            // so an exact tie keeps the first venue from environment order.
            .fold(None, |best, candidate| match best {
                Some(current) if current.conservative_receive.value >= candidate.conservative_receive.value => {
                    Some(current)
                }
                _ => Some(candidate),
            })
    }

    // Explains why none of the configured overflow venues can execute.
    fn overflow_failure_summary(&self, quotes: &VenueQuoteBook) -> String {
        self.overflow_venue_ids
            .iter()
            .map(|venue_id| match quotes.get(venue_id).map(|venue| &venue.outcome) {
                Some(VenuePreviewOutcome::Unavailable(error)) => {
                    format!("{} unavailable ({error})", venue_id.to_uppercase())
                }
                Some(VenuePreviewOutcome::Invalid(error)) => {
                    format!("{} quote rejected ({error})", venue_id.to_uppercase())
                }
                Some(VenuePreviewOutcome::Quoted(_)) => {
                    format!("{} amount is below its minimum", venue_id.to_uppercase())
                }
                None => format!("{} preview is missing", venue_id.to_uppercase()),
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    // Converts selected previews into persisted legs, enforces the combined
    // conservative edge, and validates the complete execution state.
    fn build_state(
        &self,
        input: &IcpswapFirstPlanInput,
        previews: Vec<VenueRoutePreview>,
        allocation_reason: MultiVenueAllocationReason,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        let legs: Vec<VenueLegState> = previews.into_iter().map(preview_to_leg).collect();
        let estimated_receive = sum_leg_outputs(&legs, false)?;
        let conservative_receive = sum_leg_outputs(&legs, true)?;
        let combined_net_edge_bps = edge_bps(&conservative_receive, &input.debt_repaid)?;

        // Bad debt is bought at a known loss, so measuring the swap against the
        // debt it repaid can only ever reject it. Its own floor decides how much
        // of that shortfall may be recycled without a human; the quote is still
        // held to the oracle by `validate_venue_preview`.
        let required_edge_bps = if input.buy_bad_debt {
            self.config.bad_debt_min_net_edge_bps
        } else {
            i32::try_from(self.config.min_net_edge_bps).unwrap_or(i32::MAX)
        };

        if !meets_minimum_edge(&conservative_receive, &input.debt_repaid, required_edge_bps)? {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "combined conservative output has {:.2} bps edge, below required {} bps",
                combined_net_edge_bps, required_edge_bps
            )));
        }

        let state = MultiVenueExecutionState {
            plan: MultiVenueExecutionPlan {
                strategy_id: ICPSWAP_FIRST_STRATEGY_ID.to_string(),
                total_pay: input.total_pay.clone(),
                receive_asset: input.receive_asset.clone(),
                debt_repaid: input.debt_repaid.clone(),
                allocation_reason,
                min_net_edge_bps: self.config.min_net_edge_bps,
                enforced_min_net_edge_bps: Some(required_edge_bps),
                estimated_receive,
                conservative_receive,
                combined_net_edge_bps,
                quoted_at,
            },
            legs,
            outcome: MultiVenueExecutionOutcome::Running,
        };
        state.validate().map_err(IcpswapFirstPlannerError::InvalidInput)?;
        Ok(state)
    }
}

/// Converts one venue's pay allocation into receive-token native units at the
/// resolved oracle prices.
///
/// The scales come from the tokens carried by the two amounts being compared:
/// `AssetId` equality ignores decimals, so reading them off the plan input could
/// silently rescale the bound.
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
