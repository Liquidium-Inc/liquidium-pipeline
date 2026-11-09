//! Pure amount sizing: fee reserves, bridge minimums and the net amounts a
//! bridge leg sends and receives. Planning and execution use the same sizing,
//! so a quote counts the fees execution pays.
use candid::Nat;
use liquidium_pipeline_connectors::backend::amount_utils::{nat_units_to_amount_via_core, nat_units_to_spend_amount};
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeFeeBudget, BridgeRouteKind, BridgeRouteSpec};
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
};

use super::BridgeLegError;
use crate::{bridge::runtime::BridgeRuntime, finalizers::bridge_planner::parse_icp_account_like};

/// A bridged deposit sends no approval bumps, so only its transfer fee is held back.
const BRIDGED_DEPOSIT_FEE_COUNT: u8 = 1;

/// The symbol a return route's gas cost carries in its fee budget.
const GAS_SYMBOL: &str = "ETH";
const GAS_DECIMALS: u8 = 18;

/// Checks that the bridge wallet holds the gas the return route pays before
/// any funds commit to it. The balance read keeps a reserve back, so this
/// asks a little more than the send needs.
pub async fn ensure_return_gas(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    budget: &BridgeFeeBudget,
) -> Result<(), String> {
    if route.route_kind != BridgeRouteKind::CkEthErc20Forward {
        return Ok(());
    }
    let needed = budget
        .side_costs
        .iter()
        .filter(|cost| cost.symbol == GAS_SYMBOL)
        .fold(Nat::from(0u8), |sum, cost| sum + cost.amount.clone());
    if needed == 0u8 {
        return Ok(());
    }
    let needed = nat_units_to_amount_via_core(&needed, GAS_DECIMALS)?;
    let wallet = bridge.sources.for_chain(route.source_chain)?;
    let held = bridge
        .backend
        .get_source_balance(GAS_SYMBOL, route.source_chain, &wallet)
        .await?;
    if held < needed {
        return Err(format!(
            "bridge wallet {wallet} holds {held} ETH, below the {needed} ETH of gas the {} return needs",
            route.source_asset
        ));
    }
    Ok(())
}

/// What a bridged deposit sends and what the bridge receives from it.
pub struct BridgeDepositSizing {
    pub transfer_value: Nat,
    pub bridge_amount: ChainTokenAmount,
    pub source_fee_reserve: Option<ChainTokenAmount>,
    pub fee_budget: BridgeFeeBudget,
}

/// A bridged deposit from funding to the amount the far side should see arrive.
pub struct BridgedDepositArrival {
    pub sizing: BridgeDepositSizing,
    /// What the bridge call sends.
    pub submit_amount: f64,
    /// What reaches the far side after the route's destination fee.
    pub expected_amount: f64,
}

/// The transfer that funds a leg, less `fee_count` ledger fees, as `(transfer
/// value, the amount the leg then holds)`.
pub fn fee_adjusted_transfer(
    asset: &ChainToken,
    size_in: &ChainTokenAmount,
    fee_count: u8,
) -> Result<(Nat, ChainTokenAmount), BridgeLegError> {
    let fee = asset.fee() * fee_count;
    let transfer_amount = amount_less_reserve(asset, &size_in.value, fee, "ledger fee")?;
    Ok((transfer_amount.value.clone(), transfer_amount))
}

/// The account the funding transfer goes to on the source chain of a route.
pub fn bridge_source_transfer_destination(source_chain: &str, source_address: &str) -> Result<ChainAccount, String> {
    if source_chain.eq_ignore_ascii_case("ICP") {
        let account = parse_icp_account_like(source_address)?;
        return Ok(ChainAccount::Icp(account));
    }
    if source_chain.eq_ignore_ascii_case("ETH") {
        return Ok(ChainAccount::Evm(source_address.to_string()));
    }
    Err(format!(
        "unsupported bridge source chain '{}' for transfer destination",
        source_chain
    ))
}

/// The source-token reserve an ICP reverse bridge needs before it is submitted.
///
/// The bridge wallet must retain the source ledger approval fee for ckERC20 and
/// ckSOL routes; native `ckETH -> ETH` also needs its quoted minter withdrawal
/// fee budget. ckSOL's minter fee instead reduces the SOL received at the
/// destination. Returns the reserve and the net amount the bridge can take.
pub fn reverse_bridge_source_fee_reserve(
    route: &BridgeRouteSpec,
    asset: &ChainToken,
    transfer_value: &Nat,
    source_fee_budget: f64,
) -> Result<Option<(ChainTokenAmount, ChainTokenAmount)>, BridgeLegError> {
    if !matches!(
        route.route_kind,
        BridgeRouteKind::CkEthErc20Reverse | BridgeRouteKind::CkEthToEth | BridgeRouteKind::IcpToSolana
    ) || !route.source_chain.eq_ignore_ascii_case("ICP")
        || !asset.symbol().eq_ignore_ascii_case(route.source_asset)
        || !asset.chain().eq_ignore_ascii_case(route.source_chain)
    {
        return Ok(None);
    }

    let bridge_amount = amount_after_fee_reserve(
        asset,
        transfer_value,
        source_fee_budget,
        "reverse bridge source fee budget",
    )?;
    if bridge_amount.value == *transfer_value {
        return Ok(None);
    }
    let reserve = ChainTokenAmount::from_raw(asset.clone(), transfer_value.clone() - bridge_amount.value.clone());
    Ok(Some((reserve, bridge_amount)))
}

fn amount_after_fee_reserve(
    token: &ChainToken,
    amount: &Nat,
    reserve_budget: f64,
    label: &str,
) -> Result<ChainTokenAmount, BridgeLegError> {
    if !reserve_budget.is_finite() || reserve_budget < 0.0 {
        return Err(format!("invalid {label}: {reserve_budget}").into());
    }
    let reserve = ChainTokenAmount::from_formatted(token.clone(), reserve_budget);
    amount_less_reserve(token, amount, reserve.value, label)
}

/// The amount left after a reserve. A reserve that takes the whole amount
/// is a permanent floor error, because a retry cannot make it larger.
fn amount_less_reserve(
    token: &ChainToken,
    amount: &Nat,
    reserve: Nat,
    label: &str,
) -> Result<ChainTokenAmount, BridgeLegError> {
    if reserve != 0u8 && *amount <= reserve {
        return Err(BridgeLegError::BelowMinimum(format!(
            "deposit amount {} too small to reserve {} {} for {}",
            ChainTokenAmount::from_raw(token.clone(), amount.clone()).formatted(),
            label,
            ChainTokenAmount::from_raw(token.clone(), reserve).formatted(),
            token.symbol(),
        )));
    }
    Ok(ChainTokenAmount::from_raw(token.clone(), amount.clone() - reserve))
}

/// The SOL a return sends after the wallet keeps `source_fee_budget` for
/// the send fee. The caller passes the budget it already read.
pub async fn solana_repayment_bridge_amount(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    gross: &ChainTokenAmount,
    source_fee_budget: f64,
) -> Result<ChainTokenAmount, BridgeLegError> {
    // SOL and ckSOL have the same base units; keep the repayment token metadata.
    let net = amount_after_fee_reserve(
        &gross.token,
        &gross.value,
        source_fee_budget,
        "repayment bridge source fee budget",
    )?;
    ensure_minimum_bridge_amount(bridge, route, &net).await?;
    Ok(net)
}

/// Sizes a bridged deposit from its allocation: the ledger fee comes off
/// the transfer, then the route's source-side reserve.
pub async fn size_bridged_deposit(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    asset: &ChainToken,
    size_in: &ChainTokenAmount,
) -> Result<BridgeDepositSizing, BridgeLegError> {
    let (transfer_value, mut bridge_amount) = fee_adjusted_transfer(asset, size_in, BRIDGED_DEPOSIT_FEE_COUNT)?;
    let fee_budget = bridge
        .backend
        .get_fee_budget(route.source_asset, route.source_chain, route.target_asset)
        .await?;
    let mut source_fee_reserve = None;
    if let Some((reserve, net)) =
        reverse_bridge_source_fee_reserve(route, asset, &transfer_value, fee_budget.source_fee_budget)?
    {
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

/// Sizes a bridged deposit through to its arrival on the far side.
pub async fn bridged_deposit_arrival(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    asset: &ChainToken,
    size_in: &ChainTokenAmount,
) -> Result<BridgedDepositArrival, BridgeLegError> {
    let sizing = size_bridged_deposit(bridge, route, asset, size_in).await?;
    ensure_minimum_bridge_amount(bridge, route, &sizing.bridge_amount).await?;
    let submit_amount = spend_f64(&sizing.bridge_amount)?;
    let expected_amount =
        expected_bridge_deposit_amount(route, submit_amount, sizing.fee_budget.destination_fee_budget)?;
    Ok(BridgedDepositArrival {
        sizing,
        submit_amount,
        expected_amount,
    })
}

/// The route minimum this amount falls short of, as `(amount, minimum)`.
pub async fn bridge_minimum_shortfall(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    amount: &ChainTokenAmount,
) -> Result<Option<(f64, f64)>, String> {
    let minimum = bridge
        .backend
        .get_minimum_bridge_amount(route.source_asset, route.source_chain, route.target_asset)
        .await?;
    if !minimum.is_finite() {
        return Err(format!(
            "invalid bridge minimum {minimum} for {}@{}",
            route.source_asset, route.source_chain
        ));
    }
    if minimum <= 0.0 {
        return Ok(None);
    }
    let current = amount.to_f64();
    Ok((current < minimum).then_some((current, minimum)))
}

async fn ensure_minimum_bridge_amount(
    bridge: &BridgeRuntime,
    route: &BridgeRouteSpec,
    amount: &ChainTokenAmount,
) -> Result<(), BridgeLegError> {
    if let Some((current, minimum)) = bridge_minimum_shortfall(bridge, route, amount).await? {
        return Err(BridgeLegError::BelowMinimum(format!(
            "{}@{} -> {} amount below minimum withdrawal (amount={} minimum={})",
            route.source_asset, route.source_chain, route.target_asset, current, minimum
        )));
    }
    Ok(())
}

/// Converts a native amount for the float bridge API without rounding
/// above the budget the wallet actually holds.
pub fn spend_f64(amount: &ChainTokenAmount) -> Result<f64, String> {
    nat_units_to_spend_amount(&amount.value, amount.token.decimals())
}

/// Nets an exchange withdrawal fee out of an amount. A zero fee leaves the
/// amount unchanged; a fee that consumes the amount is an error.
pub fn net_of_withdraw_fee(amount: f64, fee: f64) -> Result<f64, String> {
    let eps = crate::swappers::model::LIQUIDITY_EPS;
    if !fee.is_finite() || fee < 0.0 {
        return Err(format!("invalid withdrawal fee {fee}"));
    }
    if fee <= eps {
        return Ok(amount);
    }
    let net = amount - fee;
    if net <= eps {
        return Err(format!("fee {fee} consumes amount {amount}"));
    }
    Ok(net)
}

fn expected_bridge_deposit_amount(
    route: &BridgeRouteSpec,
    bridge_amount: f64,
    destination_fee_budget: f64,
) -> Result<f64, BridgeLegError> {
    if destination_fee_budget <= 0.0 {
        return Ok(bridge_amount);
    }
    if bridge_amount <= destination_fee_budget {
        return Err(BridgeLegError::BelowMinimum(format!(
            "bridge amount {} too small to cover destination fee budget {} for {}@{} -> {}",
            bridge_amount, destination_fee_budget, route.source_asset, route.source_chain, route.target_asset
        )));
    }
    Ok(bridge_amount - destination_fee_budget)
}
