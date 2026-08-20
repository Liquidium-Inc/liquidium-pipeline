use std::collections::BTreeMap;

use candid::Nat;
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};
use num_traits::ToPrimitive;

use crate::{
    finalizers::multi_venue::VenuePlanningContext,
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::{
        icpswap::{identity::parse_liquidation_id, supports_pair},
        model::SwapRequest,
    },
};

use super::IcpswapFirstPlannerError;

const RAY_PRICE_SCALE: f64 = 1e27;

/// Immutable liquidation data used to collect and compare venue previews.
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
    /// Smallest allocation each overflow venue can actually execute, in pay-token
    /// units, for the venues that report a floor.
    ///
    /// Separate from `cex_min_exec_usd` because a bridge minimum is a native
    /// per-asset amount, not a notional: the same $8 is 0.005 ETH at one price
    /// and half that at another, so a dollar floor cannot stand in for it.
    /// Resolved once per plan, since part of the number is a live gas quote and
    /// every leg has to be measured against the same floor.
    pub venue_minimums: BTreeMap<String, ChainTokenAmount>,
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
            // Filled in by the planner once the venues have been asked; a
            // receipt carries no venue state of its own.
            venue_minimums: BTreeMap::new(),
        };
        input.validate()?;
        Ok(input)
    }

    /// Verifies the amount and asset invariants required by every venue quote.
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

    /// Produces an amount-scoped request tagged for one specific venue.
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

    pub(super) fn is_icpswap_supported_pair(&self) -> bool {
        supports_pair(&self.total_pay.token, &self.receive_asset)
    }

    pub(super) fn meets_cex_minimum(&self, amount: &ChainTokenAmount, minimum_usd: f64) -> bool {
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

    /// The receive asset's oracle price in USD, when the receipt carried one.
    ///
    /// Present exactly when the pay-side ray price is, because the two are
    /// recorded together and validated as a pair.
    pub(super) fn receive_reference_price_usd(&self) -> Option<f64> {
        self.receive_reference_price_ray.as_ref().and_then(reference_price_usd)
    }

    /// Whether a quoted output is large enough to leave the venue afterwards.
    ///
    /// Every CEX withdraws its whole leg in one transfer, and venues refuse
    /// anything under a per-asset minimum of their own -- a refusal that arrives
    /// after the trade, with the proceeds already sitting on the exchange. The
    /// planner cannot ask them for that number, so it applies the operator's
    /// floor to the conservative output instead.
    ///
    /// An unusable price makes the notional unknown rather than too small, and
    /// is deferred to the venue for the same reason as [`Self::meets_cex_minimum`]:
    /// filtering on missing data would reject legs that execute perfectly well.
    pub(super) fn meets_receive_minimum(&self, receive: &ChainTokenAmount, minimum_usd: f64) -> bool {
        if minimum_usd <= 0.0 {
            return true;
        }
        let Some(price) = self.receive_reference_price_usd() else {
            return true;
        };
        let notional = receive.to_f64() * price;
        notional.is_finite() && notional >= minimum_usd
    }

    /// Whether this allocation clears the floor the venue itself reported.
    ///
    /// A venue that reported nothing is unbounded here: its own deposit path
    /// still enforces the real minimum, so a failed lookup must not remove a
    /// venue that can execute perfectly well.
    pub(super) fn meets_venue_minimum(&self, venue_id: &str, amount: &ChainTokenAmount) -> bool {
        self.venue_minimums
            .get(venue_id)
            .is_none_or(|minimum| amount.value >= minimum.value)
    }

    /// Whether every one of these venues has a floor above the whole allocation.
    ///
    /// True means no slice can clear any venue, because every slice is smaller
    /// than the total that already failed. That makes the amount unexecutable
    /// rather than merely hard to route, which is a different answer for the
    /// caller: retrying cannot grow it.
    pub(super) fn below_every_venue_minimum(&self, venue_ids: &[String], amount: &ChainTokenAmount) -> bool {
        !venue_ids.is_empty()
            && venue_ids
                .iter()
                .all(|venue_id| !self.meets_venue_minimum(venue_id, amount))
    }

    /// Describes the reported floors that this allocation fails, for an
    /// operator-facing error.
    pub(super) fn venue_minimum_summary(&self, venue_ids: &[String]) -> String {
        venue_ids
            .iter()
            .filter_map(|venue_id| {
                self.venue_minimums
                    .get(venue_id)
                    .map(|minimum| format!("{venue_id} needs at least {}", minimum.formatted()))
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Converts a RAY-scaled receipt price into USD, or `None` when it cannot size a
/// notional. Zero, negative, and values `to_f64` cannot represent (it saturates
/// to infinity) are all absent prices, not cheap ones.
pub(in crate::finalizers::multi_venue) fn reference_price_usd(ref_price_ray: &Nat) -> Option<f64> {
    let price = ref_price_ray.0.to_f64()? / RAY_PRICE_SCALE;
    (price.is_finite() && price > 0.0).then_some(price)
}

pub(super) fn positive_price(price_ray: &Nat) -> Option<Nat> {
    (price_ray > &Nat::from(0u8)).then(|| price_ray.clone())
}
