use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};

use std::str::FromStr;
use std::sync::Arc;

use candid::Principal;
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;

use crate::commands::funds::sync_balances;
use crate::{
    account::account::{IcrcAccountActions, LiquidatorAccount},
    config::{Config, ConfigTrait},
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
};

pub async fn withdraw() {
    let theme = ColorfulTheme::default();
    println!("\n=== Withdraw Wizard ===\n");

    // Load config
    let config = Config::load().await.expect("Failed to load config");

    // Step 1: Select account (Main | Recovery)
    let account_choices = vec!["Main (Liquidator)", "Recovery (Trader)"];
    let account_idx = Select::with_theme(&theme)
        .with_prompt("Select source account")
        .default(0)
        .items(&account_choices)
        .interact()
        .unwrap();
    let use_main = account_idx == 0;

    // Source selection
    let (src_identity, account) = if use_main {
        (config.liquidator_identity.clone(), config.liquidator_principal.into())
    } else {
        (config.trader_identity.clone(), config.get_recovery_account())
    };

    // Initialize Agent
    let agent = Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(src_identity)
        .with_max_tcp_error_retries(3)
        .build()
        .expect("Failed to initialize IC agent");
    let agent = Arc::new(agent);
    let mut account_service = LiquidatorAccount::new(agent.clone());

    if !use_main {
        account_service.set_sub_account(*account.effective_subaccount());
    }

    let account_service = Arc::new(account_service);

    // Step 2: Select asset(s)
    let mut assets: Vec<(String, IcrcToken)> = Vec::new();
    for (_pid, t) in config.get_debt_assets() {
        assets.push((t.symbol.clone(), t.clone()));
    }
    for (_pid, t) in config.get_collateral_assets() {
        if !assets.iter().any(|(s, _)| *s == t.symbol) {
            assets.push((t.symbol.clone(), t.clone()));
        }
    }
    assets.sort_by(|a, b| a.0.cmp(&b.0));

    // Build unique ledger principals for balance fetch
    use std::collections::{HashMap, HashSet};
    let mut principals: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (_sym, tok) in &assets {
        let ptxt = tok.ledger.to_text();
        if seen.insert(ptxt.clone()) {
            principals.push(ptxt);
        }
    }

    // Fetch balances via funds::sync_balances
    let balances_vec = sync_balances(account_service.clone(), &principals, account).await;
    let mut balances: HashMap<Principal, String> = HashMap::new();
    for (formatted, principal) in balances_vec.into_iter().flatten() {
        balances.insert(principal, formatted);
    }

    let mut symbols: Vec<String> = assets
        .iter()
        .map(|(s, t)| {
            let b = balances.get(&t.ledger).cloned().unwrap_or_else(|| "n/a".to_string());
            format!("{}  —  {}", s, b)
        })
        .collect();
    symbols.push("All".to_string());

    let asset_idx = Select::with_theme(&theme)
        .with_prompt("Select asset to withdraw")
        .default(0)
        .items(&symbols)
        .interact()
        .unwrap();

    let withdraw_all_assets = asset_idx == symbols.len() - 1;

    // Step 3: Enter amount (skip if all assets)
    let amount_input: String = if withdraw_all_assets {
        println!("Withdrawing all assets — skipping amount input.");
        "all".to_string()
    } else {
        Input::with_theme(&theme)
            .with_prompt("Enter amount (or 'all')")
            .interact_text()
            .unwrap()
    };

    // Step 4: Select destination (default: Main or manual input)
    let dest_choices = vec!["Main (Liquidator)", "Manual input"];
    let dest_idx = Select::with_theme(&theme)
        .with_prompt("Select destination account")
        .default(0)
        .items(&dest_choices)
        .interact()
        .unwrap();

    let dst_account = if dest_idx == 0 {
        config.liquidator_principal.into()
    } else {
        let manual_input: String = Input::with_theme(&theme)
            .with_prompt("Enter destination principal")
            .validate_with(|input: &String| -> Result<(), &str> {
                if Account::from_str(input).is_ok() {
                    Ok(())
                } else {
                    Err("Invalid account format")
                }
            })
            .interact_text()
            .unwrap();

        let entered_account = Account::from_str(&manual_input).unwrap();

        // Warn if destination is Recovery (Trader)
        if entered_account == config.get_recovery_account() || entered_account == config.trader_principal.into() {
            let confirm_recovery = Confirm::with_theme(&theme)
                .with_prompt("Warning: destination is Recovery (Trader). Continue?")
                .default(false)
                .interact()
                .unwrap();
            if !confirm_recovery {
                println!("Aborted by user.");
                return;
            }
        }

        entered_account
    };

    // plan entries: (display_sym, token, amount_nat, balance_fmt)
    let mut plan: Vec<(String, IcrcToken, IcrcTokenAmount, String)> = Vec::new();

    if withdraw_all_assets {
        for (_, tok) in &assets {
            let bal_fmt = balances.get(&tok.ledger).cloned().unwrap_or_else(|| "n/a".to_string());
            let amount_nat = if amount_input.trim().eq_ignore_ascii_case("all") {
                let spend_units = extract_numeric_segment(&bal_fmt)
                    .and_then(|n| decimal_to_units(&n, tok.decimals))
                    .unwrap_or(0);
                let fee_u128: u128 = tok.fee.0.to_string().parse().unwrap_or(0);
                let spendable = spend_units.saturating_sub(fee_u128);
                IcrcTokenAmount {
                    token: tok.clone(),
                    value: candid::Nat::from(spendable),
                }
            } else {
                let val = f64::from_str(&amount_input).unwrap_or(0.0);
                IcrcTokenAmount::from_formatted(tok.clone(), val)
            };
            plan.push((tok.symbol.clone(), tok.clone(), amount_nat, bal_fmt));
        }
    } else {
        let (sym, tok) = assets[asset_idx].clone();
        let bal_fmt = balances.get(&tok.ledger).cloned().unwrap_or_else(|| "n/a".to_string());
        let amount_nat = if amount_input.trim().eq_ignore_ascii_case("all") {
            let spend_units = extract_numeric_segment(&bal_fmt)
                .and_then(|n| decimal_to_units(&n, tok.decimals))
                .unwrap_or(0);
            let fee_u128: u128 = tok.fee.0.to_string().parse().unwrap_or(0);
            let spendable = spend_units.saturating_sub(fee_u128);
            IcrcTokenAmount {
                token: tok.clone(),
                value: candid::Nat::from(spendable),
            }
        } else {
            let val = f64::from_str(&amount_input).unwrap_or(0.0);
            IcrcTokenAmount::from_formatted(tok.clone(), val)
        };
        plan.push((sym, tok, amount_nat, bal_fmt));
    }

    // Preview (labeled, plain prints)
    println!("\nPlanned transfers:\n");
    for (i, (sym, _tok, _amt, bal)) in plan.iter().enumerate() {
        println!("Transfer #{}:", i + 1);
        println!("  Source:      {}", account);
        println!("  Destination: {}", dst_account);
        println!("  Asset:       {}", sym);
        println!("  Amount:      {}", amount_input);
        println!("  Balance:     {}", bal);
        println!();
    }

    let confirm = Confirm::with_theme(&theme)
        .with_prompt(format!("Proceed with {} transfer(s)?", plan.len()))
        .default(false)
        .interact()
        .unwrap();

    if !confirm {
        println!("Aborted by user.");
        return;
    }

    // Step 6: Execute transfers and show summary (plain prints)
    println!("\nExecuting withdrawals...\n");

    for (sym, _tok, amt, bal_fmt) in plan {
        let func = if use_main {
            LiquidatorAccount::transfer
        } else {
            LiquidatorAccount::transfer_from_subaccount
        };
        match func(&account_service, amt, dst_account).await {
            Ok(txid) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", sym);
                println!("  Amount:      {}", amount_input);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      ok: {}", txid);
                println!();
            }
            Err(e) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", sym);
                println!("  Amount:      {}", amount_input);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      err: {}", e);
                println!();
            }
        }
    }

    // Step 7: Show updated balance after transfers
    println!("\nRefreshing balances after transfer...\n");

    let principals: Vec<String> = assets.iter().map(|(_, t)| t.ledger.to_text()).collect();
    let balances_after_vec = sync_balances(account_service.clone(), &principals, dst_account).await;

    println!("Updated destination balances ({}):", dst_account);
    for (formatted, principal) in balances_after_vec.into_iter().flatten() {
        println!("  {} | {}", principal, formatted);
    }

    println!("\nAll transfers processed.\n");
}

// Extract the first numeric segment from a string (e.g., "ckUSDT: 12.662686" or "12.662686 ckUSDT")
fn extract_numeric_segment(s: &str) -> Option<String> {
    // Split on spaces and colons, pick the first segment containing a digit
    for part in s.split([' ', ':']) {
        let p = part.trim();
        if p.chars().any(|c| c.is_ascii_digit()) {
            return Some(p.to_string());
        }
    }
    None
}

// Convert a decimal string to u128 units, given a number of decimals
fn decimal_to_units(dec_str: &str, decimals: u8) -> Option<u128> {
    let mut parts = dec_str.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return None;
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut frac_norm = frac.to_string();
    if frac_norm.len() > decimals as usize {
        return None;
    }
    while frac_norm.len() < decimals as usize {
        frac_norm.push('0');
    }
    let combined = if whole.is_empty() {
        format!("0{}", frac_norm)
    } else {
        format!("{}{}", whole, frac_norm)
    };
    combined.parse::<u128>().ok()
}


/// Non-interactive withdraw flow.
///
/// Arguments (case-insensitive where noted):
/// - `source`: "main" | "recovery"
/// - `destination`: "main" | full Account text (e.g., "aaaaa-aa" or "aaaaa-aa:beef...")
/// - `asset`: token symbol (e.g., "ckUSDT") | "all"
/// - `amount`: decimal string (respects token decimals) | "all"
pub async fn withdraw_noninteractive(
    source: &str,
    destination: &str,
    asset: &str,
    amount: &str,
) {
    println!("\n=== Withdraw (non-interactive) ===\n");

    // Load config
    let config = match Config::load().await {
        Ok(c) => c,
        Err(e) => { eprintln!("Failed to load config: {e}"); return; }
    };

    // Resolve source
    let use_main = matches!(source.to_lowercase().as_str(), "main" | "m" | "liquidator");
    let (src_identity, account) = if use_main {
        (config.liquidator_identity.clone(), config.liquidator_principal.into())
    } else {
        (config.trader_identity.clone(), config.get_recovery_account())
    };

    // Initialize Agent and account service
    let agent = match Agent::builder()
        .with_url(config.ic_url.clone())
        .with_identity(src_identity)
        .with_max_tcp_error_retries(3)
        .build() {
        Ok(a) => Arc::new(a),
        Err(e) => { eprintln!("Failed to initialize IC agent: {e}"); return; }
    };
    let mut svc = LiquidatorAccount::new(agent.clone());
    if !use_main { svc.set_sub_account(*account.effective_subaccount()); }
    let account_service = Arc::new(svc);

    // Build asset catalog (debt ∪ collateral)
    let mut assets: Vec<(String, IcrcToken)> = Vec::new();
    for (_pid, t) in config.get_debt_assets() { assets.push((t.symbol.clone(), t.clone())); }
    for (_pid, t) in config.get_collateral_assets() {
        if !assets.iter().any(|(s, _)| *s == t.symbol) { assets.push((t.symbol.clone(), t.clone())); }
    }
    assets.sort_by(|a, b| a.0.cmp(&b.0));

    // Unique ledgers for balance fetch
    use std::collections::{HashMap, HashSet};
    let mut principals: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (_sym, tok) in &assets {
        let ptxt = tok.ledger.to_text();
        if seen.insert(ptxt.clone()) { principals.push(ptxt); }
    }

    // Fetch balances for SOURCE account
    let balances_vec = sync_balances(account_service.clone(), &principals, account).await;
    let mut balances: HashMap<Principal, String> = HashMap::new();
    for (formatted, principal) in balances_vec.into_iter().flatten() { balances.insert(principal, formatted); }

    // Resolve destination
    let dst_account: Account = if destination.eq_ignore_ascii_case("main") {
        config.liquidator_principal.into()
    } else {
        match Account::from_str(destination) {
            Ok(a) => {
                if a == config.get_recovery_account() || a == config.trader_principal.into() {
                    eprintln!("Warning: destination is Recovery (Trader). Proceeding.");
                }
                a
            }
            Err(_) => { eprintln!("Invalid destination account: {}", destination); return; }
        }
    };

    // Build plan: (symbol, token, amount, balance_fmt)
    let mut plan: Vec<(String, IcrcToken, IcrcTokenAmount, String)> = Vec::new();
    let all_assets = asset.eq_ignore_ascii_case("all");

    if all_assets {
        for (_, tok) in &assets {
            let bal_fmt = balances.get(&tok.ledger).cloned().unwrap_or_else(|| "n/a".to_string());
            let amount_nat = if amount.eq_ignore_ascii_case("all") {
                let spend_units = extract_numeric_segment(&bal_fmt)
                    .and_then(|n| decimal_to_units(&n, tok.decimals))
                    .unwrap_or(0);
                let fee_u128: u128 = tok.fee.0.to_string().parse().unwrap_or(0);
                let spendable = spend_units.saturating_sub(fee_u128);
                IcrcTokenAmount { token: tok.clone(), value: candid::Nat::from(spendable) }
            } else {
                let val = f64::from_str(amount).unwrap_or(0.0);
                IcrcTokenAmount::from_formatted(tok.clone(), val)
            };
            plan.push((tok.symbol.clone(), tok.clone(), amount_nat, bal_fmt));
        }
    } else {
        // pick by symbol
        let picked = assets.iter().find(|(s, _)| s.eq_ignore_ascii_case(asset)).cloned();
        let Some((sym, tok)) = picked else { eprintln!("Unknown asset symbol: {}", asset); return; };
        let bal_fmt = balances.get(&tok.ledger).cloned().unwrap_or_else(|| "n/a".to_string());
        let amount_nat = if amount.eq_ignore_ascii_case("all") {
            let spend_units = extract_numeric_segment(&bal_fmt)
                .and_then(|n| decimal_to_units(&n, tok.decimals))
                .unwrap_or(0);
            let fee_u128: u128 = tok.fee.0.to_string().parse().unwrap_or(0);
            let spendable = spend_units.saturating_sub(fee_u128);
            IcrcTokenAmount { token: tok.clone(), value: candid::Nat::from(spendable) }
        } else {
            let val = f64::from_str(amount).unwrap_or(0.0);
            IcrcTokenAmount::from_formatted(tok.clone(), val)
        };
        plan.push((sym, tok, amount_nat, bal_fmt));
    }

    // Echo plan
    println!("Planned transfers (non-interactive):\n");
    for (i, (sym, _tok, _amt, bal)) in plan.iter().enumerate() {
        println!("Transfer #{}:", i + 1);
        println!("  Source:      {}", account);
        println!("  Destination: {}", dst_account);
        println!("  Asset:       {}", sym);
        println!("  Amount:      {}", amount);
        println!("  Balance:     {}", bal);
        println!();
    }

    // Execute
    for (sym, _tok, amt, bal_fmt) in plan {
        match account_service.transfer(amt, dst_account.clone()).await {
            Ok(txid) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", sym);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      ok: {}", txid);
                println!();
            }
            Err(e) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", sym);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      err: {}", e);
                println!();
            }
        }
    }

    // Show updated dest balances
    let principals_after: Vec<String> = assets.iter().map(|(_, t)| t.ledger.to_text()).collect();
    let balances_after_vec = sync_balances(account_service.clone(), &principals_after, dst_account).await;
    println!("Updated destination balances:");
    for (formatted, principal) in balances_after_vec.into_iter().flatten() {
        println!("  {} | {}", principal, formatted);
    }
}