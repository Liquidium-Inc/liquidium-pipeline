//! The `withdraw` command: an interactive wizard, or flags for scripts. Both
//! build the same plan, and one path shows, confirms and sends it.
use anyhow::{Result, anyhow, bail};
use candid::{Decode, Encode, Nat, Principal};
use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};
use std::str::FromStr;

use crate::commands::withdrawal::{
    WithdrawDestination, WithdrawSource, destination_account, parse_typed_destination, withdraw_amount,
};
use crate::config::ConfigTrait;
use crate::context::{PipelineContext, init_context};
use crate::output::plain_logs_enabled;

/// One transfer the command will send.
struct Planned {
    asset: AssetId,
    amount: ChainTokenAmount,
    to: ChainAccount,
    /// The source balance before the send, for the operator to compare.
    balance: String,
}

pub async fn withdraw() -> Result<()> {
    if plain_logs_enabled() {
        bail!(
            "Interactive withdraw wizard is disabled in plain-logs mode. Use non-interactive flags: liquidator withdraw --source <main|trader|recovery|bridge> --destination <main|trader|recovery|bridge|ACCOUNT|0xEVM_ADDRESS|SOLANA_BASE58> --asset <SYMBOL|all> --amount <DECIMAL|all>."
        );
    }

    let theme = ColorfulTheme::default();
    println!("\n=== Withdraw Wizard ===\n");
    let ctx = init_context()
        .await
        .map_err(|error| anyhow!("failed to init context: {error}"))?;

    let sources = [
        ("Main (Liquidator)", WithdrawSource::Main),
        ("Trader (Principal)", WithdrawSource::Trader),
        ("Recovery (Trader)", WithdrawSource::Recovery),
        ("Bridge", WithdrawSource::Bridge),
    ];
    let source = sources[select(&theme, "Select source account", &sources.map(|(label, _)| label))?].1;

    let assets = sorted_assets(&ctx);
    let mut items = Vec::with_capacity(assets.len() + 1);
    for (id, _) in &assets {
        items.push(format!(
            "{}:{}  —  {}",
            id.chain,
            id.symbol,
            source_balance(&ctx, source, id).await
        ));
    }
    items.push("All".to_string());
    let picked = select(&theme, "Select asset to withdraw", &items)?;

    let plan = if picked == assets.len() {
        println!("Withdrawing all assets — skipping amount input.");
        // "All" sends ICP tokens only, so any ICP token sets the destination menu.
        let Some((_, icp_token)) = assets.iter().find(|(_, token)| matches!(token, ChainToken::Icp { .. })) else {
            bail!("no ICP assets to withdraw");
        };
        let Some((destination, typed)) = prompt_destination(&theme, &ctx, icp_token)? else {
            return Ok(());
        };
        plan_all_icp(&ctx, source, &assets, "all", destination, &typed).await
    } else {
        let (id, token) = &assets[picked];
        let amount: String = Input::with_theme(&theme)
            .with_prompt("Enter amount (or 'all')")
            .interact_text()?;
        let Some((destination, typed)) = prompt_destination(&theme, &ctx, token)? else {
            return Ok(());
        };
        vec![plan_one(&ctx, source, id, token, &amount, destination, &typed).await?]
    };
    send_plan(&ctx, source, plan, Some(&theme)).await
}

/// Non-interactive withdraw flow.
///
/// Arguments (case-insensitive where noted):
/// - `source`: "main" | "trader" | "recovery" | "bridge"
/// - `destination`: for ICP assets: "main" | "trader" | "recovery" | "bridge" | full Account text.
///   for EVM assets: "main" | "bridge" | `0x...`.
///   for Solana assets: "main" | "bridge" | base58 pubkey.
/// - `asset`: token symbol (e.g., "ckUSDT", "USDC") | "all" (`all` executes ICP assets only)
/// - `amount`: decimal string (respects token decimals) | "all"
pub async fn withdraw_noninteractive(source: &str, destination: &str, asset: &str, amount: &str) -> Result<()> {
    println!("\n=== Withdraw (non-interactive) ===\n");
    let ctx = init_context()
        .await
        .map_err(|error| anyhow!("failed to init context: {error}"))?;
    let source = WithdrawSource::parse(source).ok_or_else(|| anyhow!("invalid source account: {source}"))?;
    let kind = WithdrawDestination::parse(destination);
    let assets = sorted_assets(&ctx);

    let plan = if asset.eq_ignore_ascii_case("all") {
        plan_all_icp(&ctx, source, &assets, amount, kind, destination).await
    } else {
        let Some((id, token)) = assets.iter().find(|(id, _)| id.symbol.eq_ignore_ascii_case(asset)) else {
            bail!("unknown asset symbol: {asset}");
        };
        vec![plan_one(&ctx, source, id, token, amount, kind, destination).await?]
    };
    send_plan(&ctx, source, plan, None).await
}

fn select(theme: &ColorfulTheme, prompt: &str, items: &[impl std::fmt::Display]) -> Result<usize> {
    Ok(Select::with_theme(theme)
        .with_prompt(prompt)
        .default(0)
        .items(items)
        .interact()?)
}

/// Asks where the funds go, with the choices of the token's chain. `None`
/// means the operator stopped.
fn prompt_destination(
    theme: &ColorfulTheme,
    ctx: &PipelineContext,
    token: &ChainToken,
) -> Result<Option<(WithdrawDestination, String)>> {
    use WithdrawDestination::{Bridge, Main, Manual, Recovery, Trader};
    let choices: &[(&str, WithdrawDestination)] = match token {
        ChainToken::Icp { .. } => &[
            ("Main (Liquidator)", Main),
            ("Trader", Trader),
            ("Recovery", Recovery),
            ("Bridge", Bridge),
            ("Manual input", Manual),
        ],
        ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => &[
            ("Main (Liquidator EVM)", Main),
            ("Bridge EVM", Bridge),
            ("Manual input", Manual),
        ],
        ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => &[
            ("Main (Liquidator SOL)", Main),
            ("Bridge SOL", Bridge),
            ("Manual input", Manual),
        ],
    };
    let labels: Vec<&str> = choices.iter().map(|(label, _)| *label).collect();
    let destination = choices[select(theme, "Select destination", &labels)?].1;
    if destination != Manual {
        return Ok(Some((destination, String::new())));
    }

    let typed: String = Input::with_theme(theme)
        .with_prompt("Enter destination address")
        .validate_with(|text: &String| parse_typed_destination(token, text).map(|_| ()))
        .interact_text()?;
    // A typed trader account is easy to mistake for an outside one.
    let config = &ctx.config;
    let trader_owned = Account::from_str(typed.trim())
        .is_ok_and(|account| account == config.get_recovery_account() || account == config.trader_principal.into());
    if trader_owned
        && !Confirm::with_theme(theme)
            .with_prompt("Warning: destination is Recovery (Trader). Continue?")
            .default(false)
            .interact()?
    {
        println!("Aborted by user.");
        return Ok(None);
    }
    Ok(Some((Manual, typed)))
}

fn sorted_assets(ctx: &PipelineContext) -> Vec<(AssetId, ChainToken)> {
    let mut assets: Vec<(AssetId, ChainToken)> = ctx
        .registry
        .tokens
        .iter()
        .map(|(id, token)| (id.clone(), token.clone()))
        .collect();
    assets.sort_by(|(a, _), (b, _)| a.chain.cmp(&b.chain).then(a.symbol.cmp(&b.symbol)));
    assets
}

/// One transfer per ICP asset. An asset that cannot be planned is skipped with
/// its reason, so one empty balance does not stop the rest.
async fn plan_all_icp(
    ctx: &PipelineContext,
    source: WithdrawSource,
    assets: &[(AssetId, ChainToken)],
    amount: &str,
    destination: WithdrawDestination,
    typed: &str,
) -> Vec<Planned> {
    let mut plan = Vec::new();
    for (id, token) in assets
        .iter()
        .filter(|(_, token)| matches!(token, ChainToken::Icp { .. }))
    {
        match plan_one(ctx, source, id, token, amount, destination, typed).await {
            Ok(planned) => plan.push(planned),
            Err(err) => eprintln!("Skipping {}: {err:#}", id.symbol),
        }
    }
    plan
}

async fn plan_one(
    ctx: &PipelineContext,
    source: WithdrawSource,
    asset: &AssetId,
    token: &ChainToken,
    amount: &str,
    destination: WithdrawDestination,
    typed: &str,
) -> Result<Planned> {
    let to = destination_account(ctx, token, destination, typed).map_err(|err| anyhow!(err))?;
    let amount = withdraw_amount(ctx, source, asset, token, amount, &to)
        .await
        .map_err(|err| anyhow!(err))?;
    Ok(Planned {
        asset: asset.clone(),
        amount,
        to,
        balance: source_balance(ctx, source, asset).await,
    })
}

/// Shows the plan, asks for a yes when a theme is given, then sends each
/// transfer. Every transfer is tried, but any failure fails the command.
async fn send_plan(
    ctx: &PipelineContext,
    source: WithdrawSource,
    plan: Vec<Planned>,
    confirm: Option<&ColorfulTheme>,
) -> Result<()> {
    if plan.is_empty() {
        bail!("no transfers to execute.");
    }
    println!("\nPlanned transfers:\n");
    for (i, planned) in plan.iter().enumerate() {
        println!("Transfer #{}:", i + 1);
        print_transfer(ctx, source, planned);
        println!();
    }
    if let Some(theme) = confirm
        && !Confirm::with_theme(theme)
            .with_prompt(format!("Proceed with {} transfer(s)?", plan.len()))
            .default(false)
            .interact()?
    {
        println!("Aborted by user.");
        return Ok(());
    }

    println!("\nExecuting withdrawals...\n");
    // A caller that reads only the exit code must not be told the funds moved
    // when they did not.
    let mut failures: Vec<String> = Vec::new();
    for planned in &plan {
        let result = source
            .transfers(ctx)
            .transfer_by_asset_id(&planned.asset, planned.to.clone(), planned.amount.value.clone())
            .await;
        println!("Transfer result:");
        print_transfer(ctx, source, planned);
        match result {
            Ok(txid) => println!("  Result:      ok: {txid}"),
            Err(err) => {
                println!("  Result:      err: {err}");
                failures.push(format!("{}: {err}", planned.asset.symbol));
            }
        }
        println!();
    }
    show_icp_destination_balances(ctx, &plan).await;

    if !failures.is_empty() {
        bail!(
            "{} of {} transfers failed: {}",
            failures.len(),
            plan.len(),
            failures.join("; ")
        );
    }
    println!("\nAll transfers processed.\n");
    Ok(())
}

fn print_transfer(ctx: &PipelineContext, source: WithdrawSource, planned: &Planned) {
    println!("  Source:      {}", source.address(ctx, &planned.amount.token));
    println!("  Destination: {}", planned.to);
    println!("  Asset:       {} ({})", planned.asset.symbol, planned.asset.address);
    println!("  Amount:      {}", planned.amount.formatted());
    println!("  Balance:     {}", planned.balance);
}

/// Shows the new balances when every transfer paid the same ICP account.
async fn show_icp_destination_balances(ctx: &PipelineContext, plan: &[Planned]) {
    let Some(ChainAccount::Icp(account)) = plan.first().map(|planned| &planned.to) else {
        return;
    };
    if plan.iter().any(|planned| planned.to != ChainAccount::Icp(*account)) {
        return;
    }
    println!("Updated destination balances ({account}):");
    for planned in plan {
        let ChainToken::Icp { ledger, .. } = &planned.amount.token else {
            continue;
        };
        if let Some(balance) = icrc1_balance(ctx, *ledger, account).await {
            let balance = ChainTokenAmount::from_raw(planned.amount.token.clone(), balance);
            println!("  {} | {}", planned.asset.symbol, format_balance(&balance));
        }
    }
}

async fn icrc1_balance(ctx: &PipelineContext, ledger: Principal, account: &Account) -> Option<Nat> {
    let bytes = ctx
        .agent
        .query(&ledger, "icrc1_balance_of")
        .with_arg(Encode!(account).ok()?)
        .call()
        .await
        .ok()?;
    Decode!(&bytes, Nat).ok()
}

async fn source_balance(ctx: &PipelineContext, source: WithdrawSource, asset: &AssetId) -> String {
    source
        .balances(ctx)
        .get_balance(asset)
        .await
        .map(|balance| format_balance(&balance))
        .unwrap_or_else(|_| "n/a".to_string())
}

/// A balance cut to six decimals for menus and plans.
fn format_balance(bal: &ChainTokenAmount) -> String {
    let raw = bal.value.clone();
    let decimals = bal.token.decimals() as u32;

    if decimals == 0 {
        return format!("{} {}", raw, bal.token.symbol());
    }

    let display_decimals = decimals.min(6);
    let scale = 10u128.pow(decimals - display_decimals);
    let scaled = raw / scale;

    let int_part = scaled.clone() / 10u128.pow(display_decimals);
    let frac_part = scaled % 10u128.pow(display_decimals);

    let frac_clean = frac_part.to_string().replace('_', "");
    let frac_str = format!("{:0>width$}", frac_clean, width = display_decimals as usize);

    format!("{}.{} {}", int_part, frac_str, bal.token.symbol())
}
