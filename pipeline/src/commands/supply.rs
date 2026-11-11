//! `liquidator supply ...`: manage the liquidator's Liquidium supply for `LIQUIDATION_FUNDING=supply`.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{
    chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait,
};
use prettytable::{Cell, Row, Table, format};

use crate::context::{PipelineContext, init_context};
use crate::liquidation::supply_funding::{self, DepositError};

/// Registers a Liquidium profile for the liquidator principal, or shows the existing one.
pub async fn register() -> Result<()> {
    let ctx = init_context().await.map_err(|e| anyhow!(e))?;
    let config = &ctx.config;

    if let Some(profile) = existing_profile(&ctx).await? {
        println!("Liquidator already has profile {profile}");
        return Ok(());
    }

    let profile = supply_funding::register_profile(
        ctx.agent.as_ref(),
        &config.lending_canister,
        config.liquidator_principal,
    )
    .await
    .map_err(|e| anyhow!(e))?;
    println!("Registered profile {profile} for {}", config.liquidator_principal);
    Ok(())
}

/// Prints wallet and supplied balances for every ICP-ledger debt asset.
pub async fn status() -> Result<()> {
    let ctx = Arc::new(init_context().await.map_err(|e| anyhow!(e))?);
    let profile = require_profile(&ctx).await?;

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_BOX_CHARS);
    table.add_row(Row::new(vec![
        Cell::new("Asset"),
        Cell::new("Wallet"),
        Cell::new("Supplied"),
    ]));

    for token in icp_debt_tokens(&ctx) {
        let wallet = ctx
            .main_service
            .get_balance(&token.asset_id())
            .await
            .map(|b| b.formatted())
            .unwrap_or_else(|e| format!("error: {e}"));
        let supplied = match pool_for(&ctx, &token).await {
            Ok(pool) => {
                supply_funding::supplied_native(ctx.agent.as_ref(), &ctx.config.lending_canister, profile, pool)
                    .await
                    .map(|value| ChainTokenAmount::from_raw(token.clone(), value).formatted())
                    .unwrap_or_else(|e| format!("error: {e}"))
            }
            Err(e) => format!("error: {e}"),
        };
        table.add_row(Row::new(vec![
            Cell::new(&token.symbol()),
            Cell::new(&wallet),
            Cell::new(&supplied),
        ]));
    }

    println!("Profile: {profile}");
    table.printstd();
    Ok(())
}

/// Supplies `amount` (decimal, or "all") of `asset` from the liquidator wallet.
pub async fn deposit(asset: &str, amount: &str) -> Result<()> {
    let ctx = Arc::new(init_context().await.map_err(|e| anyhow!(e))?);
    let profile = require_profile(&ctx).await?;
    let token = find_token(&ctx, asset)?;
    let pool = pool_for(&ctx, &token).await?;

    let amount = if amount.eq_ignore_ascii_case("all") {
        let balance = ctx
            .main_service
            .get_balance(&token.asset_id())
            .await
            .map_err(|e| anyhow!(e))?
            .value;
        if balance <= token.fee() {
            bail!("wallet balance does not cover the transfer fee");
        }
        balance - token.fee()
    } else {
        parse_amount(&token, amount)?
    };

    let tx = supply_funding::deposit(
        ctx.agent.as_ref(),
        ctx.main_transfers.actions().as_ref(),
        &token,
        pool,
        profile,
        amount.clone(),
    )
    .await
    .map_err(|error| match error {
        DepositError::NotSent(_) => anyhow!("{error}; nothing was sent"),
        DepositError::Ambiguous(_) | DepositError::NotCredited { .. } => anyhow!(
            "{error}; do not send again. Run `liquidator supply status`: the running bot credits a landed deposit on its next pass"
        ),
    })?;
    println!(
        "Supplied {} {} (tx {tx})",
        ChainTokenAmount::from_raw(token.clone(), amount).formatted(),
        token.symbol()
    );
    Ok(())
}

/// Withdraws `amount` (decimal) of `asset` from supply back to the liquidator wallet.
pub async fn withdraw(asset: &str, amount: &str) -> Result<()> {
    let ctx = Arc::new(init_context().await.map_err(|e| anyhow!(e))?);
    let config = &ctx.config;
    let profile = require_profile(&ctx).await?;
    let token = find_token(&ctx, asset)?;
    let pool = pool_for(&ctx, &token).await?;
    // The lending canister treats 0 as "withdraw everything".
    let amount = if amount.eq_ignore_ascii_case("all") {
        Nat::from(0u8)
    } else {
        parse_amount(&token, amount)?
    };

    supply_funding::withdraw_supply(
        ctx.agent.as_ref(),
        &config.lending_canister,
        config.liquidator_principal,
        profile,
        pool,
        amount,
    )
    .await
    .map_err(|e| anyhow!(e))?;
    println!(
        "Withdrawal of {} scheduled to {}",
        token.symbol(),
        config.liquidator_principal
    );
    Ok(())
}

async fn existing_profile(ctx: &PipelineContext) -> Result<Option<Principal>> {
    supply_funding::lookup_profile(
        ctx.agent.as_ref(),
        &ctx.config.lending_canister,
        ctx.config.liquidator_principal,
    )
    .await
    .map_err(|e| anyhow!(e))
}

async fn require_profile(ctx: &PipelineContext) -> Result<Principal> {
    existing_profile(ctx)
        .await?
        .ok_or_else(|| anyhow!("no Liquidium profile for the liquidator; run `liquidator supply register` first"))
}

fn icp_debt_tokens(ctx: &PipelineContext) -> Vec<ChainToken> {
    ctx.registry
        .debt_assets()
        .into_iter()
        .map(|(_, token)| token)
        .filter(|token| matches!(token, ChainToken::Icp { .. }))
        .collect()
}

fn find_token(ctx: &PipelineContext, asset: &str) -> Result<ChainToken> {
    icp_debt_tokens(ctx)
        .into_iter()
        .find(|token| token.symbol().eq_ignore_ascii_case(asset))
        .ok_or_else(|| anyhow!("unknown ICP debt asset `{asset}`"))
}

async fn pool_for(ctx: &PipelineContext, token: &ChainToken) -> Result<Principal> {
    let ChainToken::Icp { ledger, .. } = token else {
        bail!("{} is not an ICP ledger token", token.symbol());
    };
    supply_funding::pool_for_ledger(ctx.agent.as_ref(), &ctx.config.lending_canister, *ledger)
        .await
        .map_err(|e| anyhow!(e))
}

fn parse_amount(token: &ChainToken, amount: &str) -> Result<Nat> {
    let native = parse_decimal_native(amount.trim(), token.decimals())?;
    if native == 0u8 {
        bail!("amount must be greater than zero");
    }
    Ok(native)
}

/// Parses a decimal string into native units exactly, rejecting more fractional digits than
/// the token has instead of rounding.
fn parse_decimal_native(amount: &str, decimals: u8) -> Result<Nat> {
    let (whole, fraction) = amount.split_once('.').unwrap_or((amount, ""));
    let is_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if (whole.is_empty() && fraction.is_empty()) || !is_digits(whole) || !is_digits(fraction) {
        bail!("amount must be a decimal number or `all`");
    }
    if fraction.len() > decimals as usize {
        bail!("amount has more than {decimals} decimal places");
    }

    let digits = format!("{whole}{fraction:0<width$}", width = decimals as usize);
    let value: u128 = digits.parse().map_err(|_| anyhow!("amount is too large"))?;
    Ok(Nat::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_decimal_native_is_exact() {
        // given / when / then
        assert_eq!(parse_decimal_native("10000", 6).unwrap(), Nat::from(10_000_000_000u64));
        assert_eq!(parse_decimal_native("0.1", 6).unwrap(), Nat::from(100_000u64));
        assert_eq!(parse_decimal_native("1.234567", 6).unwrap(), Nat::from(1_234_567u64));
        assert_eq!(parse_decimal_native(".5", 8).unwrap(), Nat::from(50_000_000u64));
        assert_eq!(parse_decimal_native("0", 8).unwrap(), Nat::from(0u8));
    }

    #[test]
    fn parse_decimal_native_rejects_bad_input() {
        // given / when / then
        assert!(parse_decimal_native("1.2345678", 6).is_err());
        assert!(parse_decimal_native("1e6", 6).is_err());
        assert!(parse_decimal_native("-1", 6).is_err());
        assert!(parse_decimal_native(".", 6).is_err());
        assert!(parse_decimal_native("999999999999999999999999999999999999999", 6).is_err());
    }
}
