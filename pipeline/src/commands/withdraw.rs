use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};
use liquidium_pipeline::{config::ConfigTrait, context::init_context};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use candid::{Decode, Encode, Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;

use num_traits::ToPrimitive;

use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};

use liquidium_pipeline_core::account::model::ChainAccount;

use alloy::primitives::Address as EvmAddress;

enum Destination {
    Icp(Account),
    Evm(String),
}

pub async fn withdraw() {
    let theme = ColorfulTheme::default();
    println!("\n=== Withdraw Wizard ===\n");

    fn format_chain_balance(bal: &ChainTokenAmount) -> String {
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

    // Initialize pipeline context (config + registry + backends)
    let ctx = init_context().await.expect("Failed to init context");
    let config = ctx.config.clone();

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

    // Step 2: Select asset(s) from the cross-chain registry (ICP + EVM).
    let mut assets: Vec<(AssetId, ChainToken)> = Vec::new();
    for (id, token) in ctx.registry.tokens.iter() {
        assets.push((id.clone(), token.clone()));
    }
    // Sort by chain, then symbol for nicer UX
    assets.sort_by(|(id_a, _), (id_b, _)| id_a.chain.cmp(&id_b.chain).then(id_a.symbol.cmp(&id_b.symbol)));

    // Fetch balances for this source account across all assets using BalanceService
    let balance_service = if use_main {
        &ctx.main_service
    } else {
        &ctx.recovery_service
    };

    let mut balances: HashMap<AssetId, ChainTokenAmount> = HashMap::new();
    for (id, _token) in &assets {
        if let Ok(bal) = balance_service.get_balance(id).await {
            balances.insert(id.clone(), bal);
        }
    }

    let mut symbols: Vec<String> = assets
        .iter()
        .map(|(id, _token)| {
            if let Some(bal) = balances.get(id) {
                format!("{}:{}  —  {}", id.chain, id.symbol, format_chain_balance(bal))
            } else {
                format!("{}:{}  —  n/a", id.chain, id.symbol)
            }
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

    // Step 4: Select destination, depending on token chain.
    let dst: Destination = if withdraw_all_assets {
        // "All" currently only applies to ICP tokens (EVM tokens are skipped in the plan).
        let dest_choices = vec!["Main (Liquidator)", "Manual input"];
        let dest_idx = Select::with_theme(&theme)
            .with_prompt("Select destination ICP account")
            .default(0)
            .items(&dest_choices)
            .interact()
            .unwrap();

        let dst_account = if dest_idx == 0 {
            config.liquidator_principal.into()
        } else {
            let manual_input: String = Input::with_theme(&theme)
                .with_prompt("Enter destination ICP account (principal or account)")
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

        Destination::Icp(dst_account)
    } else {
        // Single-asset flow: destination depends on selected token chain.
        let (_id_selected, token_selected) = assets[asset_idx].clone();

        match token_selected {
            ChainToken::Icp { .. } => {
                let dest_choices = vec!["Main (Liquidator)", "Manual input"];
                let dest_idx = Select::with_theme(&theme)
                    .with_prompt("Select destination ICP account")
                    .default(0)
                    .items(&dest_choices)
                    .interact()
                    .unwrap();

                let dst_account = if dest_idx == 0 {
                    config.liquidator_principal.into()
                } else {
                    let manual_input: String = Input::with_theme(&theme)
                        .with_prompt("Enter destination ICP account (principal or account)")
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
                    if entered_account == config.get_recovery_account()
                        || entered_account == config.trader_principal.into()
                    {
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

                Destination::Icp(dst_account)
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                let manual_input: String = Input::with_theme(&theme)
                    .with_prompt("Enter destination EVM address (0x...)")
                    .validate_with(|input: &String| -> Result<(), &str> {
                        if input.starts_with("0x") && input.len() == 42 {
                            Ok(())
                        } else {
                            Err("Invalid EVM address format")
                        }
                    })
                    .interact_text()
                    .unwrap();

                Destination::Evm(manual_input)
            }
            _ => {
                println!("Selected asset chain is not supported for withdraw.");
                return;
            }
        }
    };

    // plan entries: (asset_id, token, amount_nat, balance_fmt)
    let mut plan: Vec<(AssetId, ChainToken, ChainTokenAmount, String)> = Vec::new();

    if withdraw_all_assets {
        // "All" currently only applies to ICP tokens for execution, but balances are cross-chain.
        for (id, token) in &assets {
            // Keep behavior as ICP-only for 'all' transfers for now.
            if !matches!(token, ChainToken::Icp { .. }) {
                continue;
            }

            let bal = match balances.get(id) {
                Some(b) => b,
                None => continue,
            };

            let bal_fmt = format_chain_balance(bal);
            let amount_native = bal.value.clone();

            let amount_nat = ChainTokenAmount {
                token: token.clone(),
                value: Nat::from(amount_native),
            };

            plan.push((id.clone(), token.clone(), amount_nat, bal_fmt));
        }
    } else {
        let (id, token) = assets[asset_idx].clone();

        let bal_opt = balances.get(&id);
        let bal_fmt = bal_opt
            .as_ref()
            .map(|b| format_chain_balance(b))
            .unwrap_or_else(|| "n/a".to_string());

        let amount_nat = if amount_input.trim().eq_ignore_ascii_case("all") {
            // For "all", behavior depends on token type.
            let amount_native: Nat = match &token {
                // ICP and non-native tokens: send full balance in native units.
                ChainToken::Icp { .. } => bal_opt.map(|b| b.value.clone()).unwrap_or(0u8.into()),
                ChainToken::EvmErc20 { .. } => bal_opt.map(|b| b.value.clone()).unwrap_or(0u8.into()),

                // EVM native: reserve a gas buffer so the tx can actually be sent.
                ChainToken::EvmNative { .. } => {
                    let bal_native = bal_opt.map(|b| b.value.clone()).unwrap_or(0u8.into());

                    // Simple fixed gas buffer in wei (e.g. ~0.0002 ETH).
                    let gas_buffer_wei: u128 = 200_000_000_000_000u128;
                    let send_native = bal_native.0.to_u128().unwrap().saturating_sub(gas_buffer_wei);

                    if send_native == 0 {
                        println!("Not enough native balance to cover gas for EVM transfer; aborting.");
                        return;
                    }

                    send_native.into()
                }

                // Fallback: just use full balance if present.
                _ => bal_opt.map(|b| b.value.clone()).unwrap_or(0u128.into()),
            };

            ChainTokenAmount {
                token: token.clone(),
                value: Nat::from(amount_native),
            }
        } else {
            let val = f64::from_str(&amount_input).unwrap_or(0.0);
            ChainTokenAmount::from_formatted(token.clone(), val)
        };

        plan.push((id, token, amount_nat, bal_fmt));
    }

    // Preview (labeled, plain prints)
    println!("\nPlanned transfers:\n");
    for (i, (asset_id, tok, _amt, bal)) in plan.iter().enumerate() {
        let src_str = match tok {
            ChainToken::Icp { .. } => account.to_string(),
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => ctx.evm_address.clone(),
            _ => account.to_string(),
        };

        println!("Transfer #{}:", i + 1);
        println!("  Source:      {}", src_str);
        println!(
            "  Destination: {}",
            match &dst {
                Destination::Icp(acc) => acc.to_string(),
                Destination::Evm(addr) => addr.clone(),
            }
        );
        println!("  Asset:       {}", asset_id.symbol);
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

    let transfer_service = if use_main {
        &ctx.main_transfers
    } else {
        &ctx.recovery_transfers
    };

    for (asset_id, token, amt, bal_fmt) in plan {
        let src_str = match &token {
            ChainToken::Icp { .. } => account.to_string(),
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => ctx.evm_address.clone(),
            _ => account.to_string(),
        };

        let to = match (&token, &dst) {
            (ChainToken::Icp { .. }, Destination::Icp(acc)) => ChainAccount::Icp(*acc),
            (ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. }, Destination::Evm(addr_str)) => {
                let addr = addr_str
                    .parse::<EvmAddress>()
                    .map_err(|e| format!("invalid EVM address: {e}"))
                    .unwrap();
                ChainAccount::Evm(addr.to_string())
            }
            _ => {
                println!("Destination / token chain mismatch; skipping.");
                continue;
            }
        };

        match transfer_service
            .transfer_by_asset_id(&asset_id, to.clone(), amt.value)
            .await
        {
            Ok(txid) => {
                println!("Transfer result:");
                println!("  Source:      {}", src_str);
                println!(
                    "  Destination: {}",
                    match &dst {
                        Destination::Icp(acc) => acc.to_string(),
                        Destination::Evm(addr) => addr.clone(),
                    }
                );
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount_input);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      ok: {}", txid);
                println!();
            }
            Err(e) => {
                println!("Transfer result:");
                println!("  Source:      {}", src_str);
                println!(
                    "  Destination: {}",
                    match &dst {
                        Destination::Icp(acc) => acc.to_string(),
                        Destination::Evm(addr) => addr.clone(),
                    }
                );
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount_input);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      err: {}", e);
                println!();
            }
        }
    }

    // Step 7: Show updated balances after transfers (ICP only)
    if let Destination::Icp(dst_account) = &dst {
        println!("\nRefreshing balances after transfer...\n");

        let balances_after = sync_balances(agent.clone(), &assets, *dst_account).await;

        println!("Updated destination balances ({}):", dst_account);
        for (principal, formatted) in balances_after {
            println!("  {} | {}", principal, formatted);
        }
    }

    println!("\nAll transfers processed.\n");
}

// Fetch balances for a set of tokens for a given account, using the agent and formatting with token decimals.
async fn sync_balances(
    agent: Arc<Agent>,
    assets: &[(AssetId, ChainToken)],
    account: Account,
) -> std::collections::HashMap<Principal, String> {
    use std::collections::{HashMap, HashSet};

    // Map ledger -> decimals, only for ICP tokens
    let mut decimals_map: HashMap<Principal, u8> = HashMap::new();
    for (_id, token) in assets {
        if let ChainToken::Icp { ledger, decimals, .. } = token {
            decimals_map.entry(*ledger).or_insert(*decimals);
        }
    }

    let mut seen = HashSet::new();
    let mut balances: HashMap<Principal, String> = HashMap::new();

    for (_id, token) in assets {
        let (ledger, decimals) = match token {
            ChainToken::Icp { ledger, decimals, .. } => (*ledger, *decimals),
            _ => continue,
        };
        if !seen.insert(ledger) {
            continue;
        }

        let decimals = *decimals_map.get(&ledger).unwrap_or(&decimals);

        let arg = account.clone();

        let res = agent
            .query(&ledger, "icrc1_balance_of")
            .with_arg(Encode!(&arg).unwrap())
            .call()
            .await;

        if let Ok(bytes) = res
            && let Ok(balance) = Decode!(&bytes, Nat)
        {
            // Convert Nat (smallest units) to u128 for formatting
            let units: u128 = balance.0.to_string().parse().unwrap_or(0);

            if decimals == 0 {
                balances.insert(ledger, format!("{}", units));
            } else {
                let d = decimals as u32;
                let pow = 10u128.pow(d);
                let int_part = units / pow;
                let frac_part = units % pow;
                let frac_str = format!("{:0>width$}", frac_part, width = d as usize);
                balances.insert(ledger, format!("{}.{}", int_part, frac_str));
            }
        }
    }

    balances
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
pub async fn withdraw_noninteractive(source: &str, destination: &str, asset: &str, amount: &str) {
    println!("\n=== Withdraw (non-interactive) ===\n");

    // Load context (config + registry)
    let ctx = match init_context().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to init context: {e}");
            return;
        }
    };
    let config = ctx.config.clone();

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
        .build()
    {
        Ok(a) => Arc::new(a),
        Err(e) => {
            eprintln!("Failed to initialize IC agent: {e}");
            return;
        }
    };
    // Remove LiquidatorAccount service, use TransferService instead

    // Build asset catalog from registry: all ICP tokens
    let mut assets: Vec<(AssetId, ChainToken)> = Vec::new();
    for (id, token) in ctx.registry.tokens.iter() {
        if let ChainToken::Icp { .. } = token {
            assets.push((id.clone(), token.clone()));
        }
    }
    assets.sort_by(|(id_a, _), (id_b, _)| id_a.symbol.cmp(&id_b.symbol));

    // Fetch balances for SOURCE account using the agent and token list
    let balances: std::collections::HashMap<Principal, String> = sync_balances(agent.clone(), &assets, account).await;

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
            Err(_) => {
                eprintln!("Invalid destination account: {}", destination);
                return;
            }
        }
    };

    // Build plan: (asset_id, token, amount, balance_fmt)
    let mut plan: Vec<(AssetId, ChainToken, ChainTokenAmount, String)> = Vec::new();
    let all_assets = asset.eq_ignore_ascii_case("all");

    if all_assets {
        for (id, token) in &assets {
            let (ledger, decimals) = match token {
                ChainToken::Icp { ledger, decimals, .. } => (*ledger, *decimals),
                _ => continue,
            };
            let bal_fmt = balances.get(&ledger).cloned().unwrap_or_else(|| "n/a".to_string());
            let amount_nat = if amount.eq_ignore_ascii_case("all") {
                let spend_units = extract_numeric_segment(&bal_fmt)
                    .and_then(|n| decimal_to_units(&n, decimals))
                    .unwrap_or(0);
                ChainTokenAmount {
                    token: token.clone(),
                    value: candid::Nat::from(spend_units),
                }
            } else {
                let val = f64::from_str(amount).unwrap_or(0.0);
                ChainTokenAmount::from_formatted(token.clone(), val)
            };
            plan.push((id.clone(), token.clone(), amount_nat, bal_fmt));
        }
    } else {
        // pick by symbol
        let picked = assets
            .iter()
            .find(|(id, _)| id.symbol.eq_ignore_ascii_case(asset))
            .cloned();
        let Some((id, token)) = picked else {
            eprintln!("Unknown asset symbol: {}", asset);
            return;
        };

        let (ledger, decimals) = match &token {
            ChainToken::Icp { ledger, decimals, .. } => (*ledger, *decimals),
            _ => {
                eprintln!("Selected asset is not an ICP token; aborting.");
                return;
            }
        };

        let bal_fmt = balances.get(&ledger).cloned().unwrap_or_else(|| "n/a".to_string());
        let amount_nat = if amount.eq_ignore_ascii_case("all") {
            let spend_units = extract_numeric_segment(&bal_fmt)
                .and_then(|n| decimal_to_units(&n, decimals))
                .unwrap_or(0);
            ChainTokenAmount {
                token: token.clone(),
                value: candid::Nat::from(spend_units),
            }
        } else {
            let val = f64::from_str(amount).unwrap_or(0.0);
            ChainTokenAmount::from_formatted(token.clone(), val)
        };
        plan.push((id, token, amount_nat, bal_fmt));
    }

    // Echo plan
    println!("Planned transfers (non-interactive):\n");
    for (i, (asset_id, _tok, _amt, bal)) in plan.iter().enumerate() {
        println!("Transfer #{}:", i + 1);
        println!("  Source:      {}", account);
        println!("  Destination: {}", dst_account);
        println!("  Asset:       {}", asset_id.symbol);
        println!("  Amount:      {}", amount);
        println!("  Balance:     {}", bal);
        println!();
    }

    // Execute
    let transfer_service = if use_main {
        &ctx.main_transfers
    } else {
        &ctx.recovery_transfers
    };

    for (asset_id, _tok, amt, bal_fmt) in plan {
        let to = ChainAccount::Icp(dst_account.clone());

        match transfer_service
            .transfer_by_asset_id(&asset_id, to.clone(), amt.value)
            .await
        {
            Ok(txid) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      ok: {}", txid);
                println!();
            }
            Err(e) => {
                println!("Transfer result:");
                println!("  Source:      {}", account);
                println!("  Destination: {}", dst_account);
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      err: {}", e);
                println!();
            }
        }
    }

    // Show updated dest balances
    let balances_after = sync_balances(agent.clone(), &assets, dst_account).await;
    println!("Updated destination balances:");
    for (principal, formatted) in balances_after {
        println!("  {} | {}", principal, formatted);
    }
}
