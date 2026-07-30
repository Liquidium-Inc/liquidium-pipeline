use std::{cmp::max, sync::Arc};

use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};
use num_traits::ToPrimitive;
use thiserror::Error;

use crate::{
    finalizers::multi_venue::{MultiVenueAdapter, VenuePlanningContext, VenueRoutePreview},
    persistance::{
        MultiVenueAllocationReason, MultiVenueExecutionOutcome, MultiVenueExecutionPlan, MultiVenueExecutionState,
        VenueLegState,
    },
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::SwapRequest,
    utils::ICP_LEDGER_PRINCIPAL,
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

#[derive(Debug, Clone, PartialEq)]
pub struct IcpswapFirstPlannerConfig {
    pub max_price_impact_bps: f64,
    pub max_search_iterations: u8,
    pub dust_fallback_max_price_impact_bps: f64,
    pub cex_min_exec_usd: f64,
    pub min_net_edge_bps: u32,
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

        let input = Self {
            liquidation_id: liquidation.id.to_string(),
            total_pay,
            receive_asset: swap.receive_asset.clone(),
            debt_repaid,
            receive_address: swap.receive_address.clone(),
            max_execution_slippage_bps: swap.max_slippage_bps,
            pay_reference_price_usd,
        };
        input.validate()?;
        Ok(input)
    }

    // Verifies the amount and asset invariants required by every venue quote.
    pub(in crate::finalizers::multi_venue) fn validate(&self) -> Result<(), IcpswapFirstPlannerError> {
        let parsed_liquidation_id = self.liquidation_id.parse::<u128>().map_err(|error| {
            IcpswapFirstPlannerError::InvalidInput(format!(
                "liquidation ID `{}` is not a u128: {error}",
                self.liquidation_id
            ))
        })?;
        if parsed_liquidation_id.to_string() != self.liquidation_id {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "liquidation ID must use canonical unsigned decimal notation".to_string(),
            ));
        }
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

    fn is_native_icp(&self) -> bool {
        let Ok(native_ledger) = Principal::from_text(ICP_LEDGER_PRINCIPAL) else {
            return false;
        };
        matches!(&self.total_pay.token, ChainToken::Icp { ledger, .. } if *ledger == native_ledger)
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapFirstPlannerError {
    #[error("invalid ICPSwap-first planner input: {0}")]
    InvalidInput(String),
    #[error("no viable ICPSwap-first route: {0}")]
    NoViableRoute(String),
}

pub struct IcpswapFirstPlanner {
    venues: Arc<VenueRegistry>,
    overflow_venue_ids: Vec<String>,
    config: IcpswapFirstPlannerConfig,
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
        let overflow_venue_ids = venues
            .venue_ids()
            .into_iter()
            .filter(|venue_id| venue_id != ICPSWAP_VENUE_ID)
            .collect();
        Ok(Self {
            venues,
            overflow_venue_ids,
            config,
        })
    }

    /// Quotes eligible venues and applies the ICPSwap-first allocation policy
    /// without writing the WAL or executing any swap.
    pub async fn plan(
        &self,
        input: &IcpswapFirstPlanInput,
        quoted_at: i64,
    ) -> Result<MultiVenueExecutionState, IcpswapFirstPlannerError> {
        input.validate()?;
        let context = input.planning_context();

        if !input.is_native_icp() || !self.venues.contains(ICPSWAP_VENUE_ID) {
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
        let quotes = self.drop_invalid_quotes(quotes);
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
                let overflow_quotes = self.preview_overflow(input, input.total_pay.value.clone()).await?;
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
        let (icpswap_result, overflow_result) = tokio::join!(
            self.preview_exact(input, ICPSWAP_VENUE_ID, &icpswap_request),
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

        let overflow_quotes = self.preview_overflow(input, input.total_pay.value.clone()).await?;
        let overflow = self.best_executable_overflow(input, &overflow_quotes).ok_or_else(|| {
            IcpswapFirstPlannerError::NoViableRoute(format!(
                "full ICPSwap quote exceeds the dust-fallback price-impact limit and the below-minimum remainder cannot be split; {}",
                self.overflow_failure_summary(&overflow_quotes)
            ))
        })?;
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
        let quotes = self.preview_overflow(input, input.total_pay.value.clone()).await?;
        let preview = self
            .best_executable_overflow(input, &quotes)
            .ok_or_else(|| IcpswapFirstPlannerError::NoViableRoute(self.overflow_failure_summary(&quotes)))?;
        self.build_state(
            input,
            vec![preview.clone()],
            MultiVenueAllocationReason::SingleVenue {
                venue_id: preview.venue_id.clone(),
            },
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
        let quotes = self.preview_overflow(input, input.total_pay.value.clone()).await?;
        let overflow = self.best_executable_overflow(input, &quotes).ok_or_else(|| {
            IcpswapFirstPlannerError::NoViableRoute(format!(
                "ICPSwap exact quote rejected ({icpswap_error}); {}",
                self.overflow_failure_summary(&quotes)
            ))
        })?;
        self.build_state(
            input,
            vec![overflow.clone()],
            MultiVenueAllocationReason::VenueUnavailable {
                selected_venue_id: overflow.venue_id.clone(),
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
            let quotes = self.preview_overflow(input, input.total_pay.value.clone()).await?;
            let overflow = self
                .best_executable_overflow(input, &quotes)
                .ok_or_else(|| IcpswapFirstPlannerError::NoViableRoute(self.overflow_failure_summary(&quotes)))?;
            return self.build_state(
                input,
                vec![overflow.clone()],
                MultiVenueAllocationReason::SingleVenue {
                    venue_id: overflow.venue_id.clone(),
                },
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
        let overflow_quotes = overflow_result?;
        let overflow = self
            .best_executable_overflow(input, &overflow_quotes)
            .ok_or_else(|| IcpswapFirstPlannerError::NoViableRoute(self.overflow_failure_summary(&overflow_quotes)))?;
        if !self.is_safe_icpswap(&icpswap) {
            // An exact quote that comes back unsafe is no more usable than one
            // that failed outright, so it takes the same route: re-quote the full
            // amount on overflow venues instead of abandoning a liquidation that
            // a single venue could still execute.
            let error = IcpswapFirstPlannerError::NoViableRoute(format!(
                "exact ICPSwap allocation impact {:.2} bps is not below {:.2} bps",
                icpswap.quote.estimated_price_impact_bps, self.config.max_price_impact_bps
            ));
            return self
                .plan_full_overflow_after_icpswap_failure(input, quoted_at, &error)
                .await;
        }

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
        validate_preview(adapter.venue_id(), request, &preview)?;
        Ok(preview)
    }

    // Quotes configured overflow venues for the exact amount left after the
    // safe ICPSwap allocation has been determined.
    async fn preview_overflow(
        &self,
        input: &IcpswapFirstPlanInput,
        pay_value: Nat,
    ) -> Result<VenueQuoteBook, IcpswapFirstPlannerError> {
        let context = input.planning_context();
        let quotes = self
            .venues
            .preview_venues(&context, &self.overflow_venue_ids, |venue_id| {
                input.request_for(venue_id, pay_value.clone())
            })
            .await
            .map_err(IcpswapFirstPlannerError::InvalidInput)?;
        Ok(self.drop_invalid_quotes(quotes))
    }

    // Converts malformed responses into per-venue Invalid outcomes so one bad
    // quote cannot prevent valid venues from being considered.
    fn drop_invalid_quotes(&self, mut quotes: VenueQuoteBook) -> VenueQuoteBook {
        for venue in quotes.iter_mut() {
            let validation_error = match &venue.outcome {
                VenuePreviewOutcome::Quoted(preview) => {
                    validate_preview(&venue.venue_id, &venue.request, preview).err()
                }
                VenuePreviewOutcome::Unavailable(_) | VenuePreviewOutcome::Invalid(_) => None,
            };
            if let Some(error) = validation_error {
                venue.outcome = VenuePreviewOutcome::Invalid(error.to_string());
            }
        }
        quotes
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

        if !meets_minimum_edge(&conservative_receive, &input.debt_repaid, self.config.min_net_edge_bps)? {
            return Err(IcpswapFirstPlannerError::NoViableRoute(format!(
                "combined conservative output has {:.2} bps edge, below required {} bps",
                combined_net_edge_bps, self.config.min_net_edge_bps
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
