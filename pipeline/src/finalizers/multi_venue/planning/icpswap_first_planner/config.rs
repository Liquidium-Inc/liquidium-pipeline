use super::{BPS_DENOMINATOR, IcpswapFirstPlannerError};

/// Risk and sizing controls for the ICPSwap-first allocation policy.
#[derive(Debug, Clone, PartialEq)]
pub struct IcpswapFirstPlannerConfig {
    pub max_price_impact_bps: f64,
    pub max_search_iterations: u8,
    pub dust_fallback_max_price_impact_bps: f64,
    /// Maximum amount-scoped price impact accepted from any CEX preview.
    /// The ordered overflow policy uses this limit to size the MEXC leg and
    /// to reject an unsafe final Kraken remainder.
    pub max_cex_price_impact_bps: f64,
    pub cex_min_exec_usd: f64,
    /// Minimum USD a CEX leg's conservative output must be worth, on top of the
    /// dust allowance, before the leg may be planned. Guards the exit the venue
    /// enforces at withdrawal time and this planner cannot ask it about.
    pub min_leg_receive_usd: f64,
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
    /// Fixed test allocation for MEXC. When present, the forced ICPSwap test
    /// split becomes ICPSwap, then MEXC, then the exact Kraken remainder.
    pub mexc_test_allocation_usd: Option<f64>,
}

impl IcpswapFirstPlannerConfig {
    /// Rejects configuration that could make allocation ambiguous or unsafe.
    pub(super) fn validate(&self) -> Result<(), IcpswapFirstPlannerError> {
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
        if !self.max_cex_price_impact_bps.is_finite()
            || self.max_cex_price_impact_bps < 0.0
            || self.max_cex_price_impact_bps > f64::from(BPS_DENOMINATOR)
        {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "maximum CEX price impact must be finite, non-negative, and no greater than {} bps",
                BPS_DENOMINATOR
            )));
        }
        if !self.cex_min_exec_usd.is_finite() || self.cex_min_exec_usd < 0.0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "CEX minimum execution USD must be finite and non-negative".to_string(),
            ));
        }
        if !self.min_leg_receive_usd.is_finite() || self.min_leg_receive_usd < 0.0 {
            return Err(IcpswapFirstPlannerError::InvalidInput(
                "minimum CEX leg receive USD must be finite and non-negative".to_string(),
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
        // Both caps, because they are set independently: a CEX cap above the
        // oracle limit lets the waterfall size a leg to an impact the guard
        // then refuses, so the plan dies after the quotes are spent.
        let (cap_name, cap_bps) = if self.dust_fallback_max_price_impact_bps >= self.max_cex_price_impact_bps {
            ("dust fallback", self.dust_fallback_max_price_impact_bps)
        } else {
            ("CEX", self.max_cex_price_impact_bps)
        };
        if f64::from(self.max_oracle_discount_bps) <= cap_bps {
            return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                "maximum oracle discount {} bps must exceed the {:.2} bps {} impact cap",
                self.max_oracle_discount_bps, cap_bps, cap_name
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
        if let Some(value) = self.mexc_test_allocation_usd {
            if !value.is_finite() || value <= 0.0 {
                return Err(IcpswapFirstPlannerError::InvalidInput(
                    "test MEXC allocation USD must be finite and positive".to_string(),
                ));
            }
            if self.icpswap_test_allocation_usd.is_none() {
                return Err(IcpswapFirstPlannerError::InvalidInput(
                    "test MEXC allocation requires a test ICPSwap allocation".to_string(),
                ));
            }
            if value < self.cex_min_exec_usd {
                return Err(IcpswapFirstPlannerError::InvalidInput(format!(
                    "test MEXC allocation ${value:.2} is below the ${:.2} CEX execution minimum",
                    self.cex_min_exec_usd
                )));
            }
        }
        Ok(())
    }
}
