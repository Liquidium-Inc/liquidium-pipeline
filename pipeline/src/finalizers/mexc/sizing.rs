//! Pure amount sizing: fee reserves, bridge minimums and the net amounts a
//! bridged deposit or withdrawal sends and receives.
use super::*;

/// What a bridged deposit sends and what the bridge receives from it.
pub(in crate::finalizers::mexc) struct BridgeDepositSizing {
    pub transfer_value: Nat,
    pub bridge_amount: ChainTokenAmount,
    pub source_fee_reserve: Option<ChainTokenAmount>,
    pub fee_budget: BridgeFeeBudget,
}

/// A bridged deposit from funding to the amount MEXC should see arrive.
pub(in crate::finalizers::mexc) struct BridgedDepositArrival {
    pub sizing: BridgeDepositSizing,
    /// What the bridge call sends.
    pub submit_amount: f64,
    /// What reaches MEXC after the route's destination fee.
    pub expected_amount: f64,
}

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// Computes the source-token reserve needed before submitting an ICP reverse bridge.
    ///
    /// Bridged deposits first transfer seized collateral into the configured bridge source account.
    /// That account must retain the source ledger approval fee for ckERC20 and ckSOL routes;
    /// native `ckETH -> ETH` also needs its quoted minter withdrawal fee budget. ckSOL's
    /// minter fee instead reduces the SOL received at the destination. This helper
    /// returns the reserved amount and the net amount that can be submitted to the bridge.
    pub(super) fn compute_reverse_bridge_source_fee_reserve(
        route: &BridgeRouteSpec,
        deposit_asset: &ChainToken,
        transfer_value: &Nat,
        source_fee_budget: f64,
    ) -> Result<Option<(ChainTokenAmount, ChainTokenAmount)>, String> {
        if !matches!(
            route.route_kind,
            BridgeRouteKind::CkEthErc20Reverse | BridgeRouteKind::CkEthToEth | BridgeRouteKind::IcpToSolana
        ) || !route.source_chain.eq_ignore_ascii_case("ICP")
            || !deposit_asset.symbol().eq_ignore_ascii_case(route.source_asset)
            || !deposit_asset.chain().eq_ignore_ascii_case(route.source_chain)
        {
            return Ok(None);
        }

        let bridge_amount = Self::amount_after_fee_reserve(
            deposit_asset,
            transfer_value,
            source_fee_budget,
            "reverse bridge source fee budget",
        )?;
        if bridge_amount.value == *transfer_value {
            return Ok(None);
        }
        let reserve = ChainTokenAmount::from_raw(
            deposit_asset.clone(),
            transfer_value.clone() - bridge_amount.value.clone(),
        );
        Ok(Some((reserve, bridge_amount)))
    }

    fn amount_after_fee_reserve(
        token: &ChainToken,
        amount: &Nat,
        reserve_budget: f64,
        label: &str,
    ) -> Result<ChainTokenAmount, String> {
        if !reserve_budget.is_finite() || reserve_budget < 0.0 {
            return Err(format!("invalid {label}: {reserve_budget}"));
        }
        let reserve = ChainTokenAmount::from_formatted(token.clone(), reserve_budget);
        Self::amount_less_reserve(token, amount, reserve.value, label)
    }

    /// The amount left after a reserve. A reserve that takes the whole amount
    /// is a permanent floor error, because a retry cannot make it larger.
    pub(super) fn amount_less_reserve(
        token: &ChainToken,
        amount: &Nat,
        reserve: Nat,
        label: &str,
    ) -> Result<ChainTokenAmount, String> {
        if reserve != Nat::from(0u8) && *amount <= reserve {
            return Err(format!(
                "{}: deposit amount {} too small to reserve {} {} for {}",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                ChainTokenAmount::from_raw(token.clone(), amount.clone()).formatted(),
                label,
                ChainTokenAmount::from_raw(token.clone(), reserve).formatted(),
                token.symbol(),
            ));
        }
        Ok(ChainTokenAmount::from_raw(token.clone(), amount.clone() - reserve))
    }

    /// The SOL a return sends after the wallet keeps `source_fee_budget` for
    /// the send fee. The caller passes the budget it already read.
    pub(in crate::finalizers::mexc) async fn solana_repayment_bridge_amount(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        gross: &ChainTokenAmount,
        source_fee_budget: f64,
    ) -> Result<ChainTokenAmount, String> {
        // SOL and ckSOL have the same base units; keep the repayment token metadata.
        let net = Self::amount_after_fee_reserve(
            &gross.token,
            &gross.value,
            source_fee_budget,
            "repayment bridge source fee budget",
        )?;
        Self::ensure_minimum_bridge_amount(bridge, route, &net).await?;
        Ok(net)
    }

    /// Sizes a bridged deposit from its allocation: the ledger fee comes off
    /// the transfer, then the route's source-side reserve. Planning and the
    /// deposit use this one sizing, so they agree on the minimum.
    pub(in crate::finalizers::mexc) async fn size_bridged_deposit(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        deposit_asset: &ChainToken,
        size_in: &ChainTokenAmount,
    ) -> Result<BridgeDepositSizing, String> {
        let (transfer_value, mut bridge_amount) =
            Self::compute_fee_adjusted_deposit_transfer(deposit_asset, size_in, true)?;
        let fee_budget = bridge
            .backend
            .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
            .await?;
        let mut source_fee_reserve = None;
        if let Some((reserve, net)) = Self::compute_reverse_bridge_source_fee_reserve(
            route,
            deposit_asset,
            &transfer_value,
            fee_budget.source_fee_budget,
        )? {
            bridge_amount = net;
            source_fee_reserve = Some(reserve);
        }
        Ok(BridgeDepositSizing {
            transfer_value,
            bridge_amount,
            source_fee_reserve,
            fee_budget,
        })
    }

    /// Sizes a bridged deposit through to its MEXC arrival. The preview and
    /// the deposit both use this, so a quote counts the fees execution pays.
    pub(in crate::finalizers::mexc) async fn bridged_deposit_arrival(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        deposit_asset: &ChainToken,
        size_in: &ChainTokenAmount,
    ) -> Result<BridgedDepositArrival, String> {
        let sizing = Self::size_bridged_deposit(bridge, route, deposit_asset, size_in).await?;
        Self::ensure_minimum_bridge_amount(bridge, route, &sizing.bridge_amount).await?;
        let submit_amount = Self::spend_f64(&sizing.bridge_amount)?;
        let expected_amount = Self::compute_expected_bridge_deposit_amount(
            route,
            submit_amount,
            sizing.fee_budget.destination_fee_budget,
        )?;
        Ok(BridgedDepositArrival {
            sizing,
            submit_amount,
            expected_amount,
        })
    }

    /// The route minimum this amount falls short of, as `(amount, minimum)`.
    pub(in crate::finalizers::mexc) async fn bridge_minimum_shortfall(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        amount: &ChainTokenAmount,
    ) -> Result<Option<(f64, f64)>, String> {
        let minimum = bridge
            .backend
            .get_minimum_bridge_amount(route.source_asset, route.source_chain, route.target_asset)
            .await?;
        if !minimum.is_finite() {
            return Err(format!("invalid bridge minimum {minimum} for {}@{}", route.source_asset, route.source_chain));
        }
        if minimum <= 0.0 {
            return Ok(None);
        }
        let current = amount.to_f64();
        Ok((current < minimum).then_some((current, minimum)))
    }

    pub(super) async fn ensure_minimum_bridge_amount(
        bridge: &BridgeRuntime,
        route: &BridgeRouteSpec,
        amount: &ChainTokenAmount,
    ) -> Result<(), String> {
        if let Some((current, minimum)) = Self::bridge_minimum_shortfall(bridge, route, amount).await? {
            return Err(format!(
                "{}: {}@{} -> {} amount below minimum withdrawal (amount={} minimum={})",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                route.source_asset,
                route.source_chain,
                route.target_asset,
                current,
                minimum
            ));
        }

        Ok(())
    }

    /// Converts a native amount for the float bridge API without rounding
    /// above the budget the wallet actually holds.
    pub(super) fn spend_f64(amount: &ChainTokenAmount) -> Result<f64, String> {
        nat_units_to_spend_amount(&amount.value, amount.token.decimals())
    }

    /// Nets an exchange withdrawal fee out of an amount. A zero fee leaves the
    /// amount unchanged; a fee that consumes the amount is an error.
    pub(in crate::finalizers::mexc) fn net_of_withdraw_fee(amount: f64, fee: f64) -> Result<f64, String> {
        if !fee.is_finite() || fee < 0.0 {
            return Err(format!("invalid withdrawal fee {fee}"));
        }
        if fee <= LIQUIDITY_EPS {
            return Ok(amount);
        }
        let net = amount - fee;
        if net <= LIQUIDITY_EPS {
            return Err(format!("fee {fee} consumes amount {amount}"));
        }
        Ok(net)
    }

    pub(super) fn compute_expected_bridge_deposit_amount(
        route: &BridgeRouteSpec,
        bridge_amount: f64,
        destination_fee_budget: f64,
    ) -> Result<f64, String> {
        if destination_fee_budget <= 0.0 {
            return Ok(bridge_amount);
        }

        if bridge_amount <= destination_fee_budget {
            return Err(format!(
                "{}: bridge amount {} too small to cover destination fee budget {} for {}@{} -> {}",
                FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
                bridge_amount,
                destination_fee_budget,
                route.source_asset,
                route.source_chain,
                route.target_asset
            ));
        }

        Ok(bridge_amount - destination_fee_budget)
    }
}
