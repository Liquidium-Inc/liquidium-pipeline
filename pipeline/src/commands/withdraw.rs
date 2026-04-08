use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::network::AnyNetwork;
use candid::{Decode, Encode, Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;

use num_traits::ToPrimitive;

use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};

use liquidium_pipeline_core::account::model::ChainAccount;

use alloy::primitives::Address as EvmAddress;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;

use crate::config::ConfigTrait;
use crate::context::{PipelineContext, init_context};
use crate::output::plain_logs_enabled;

enum Destination {
    Icp(Account),
    Evm(String),
}

pub async fn withdraw() {
    if plain_logs_enabled() {
        eprintln!(
            "Interactive withdraw wizard is disabled in plain-logs mode. Use non-interactive flags: liquidator withdraw --source <main|trader|recovery|bridge> --destination <main|trader|recovery|bridge|ACCOUNT|0xEVM_ADDRESS> --asset <SYMBOL|all> --amount <DECIMAL|all>."
        );
        return;
    }

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

    // Step 1: Select account (Main | Trader | Recovery)
    let account_choices = vec!["Main (Liquidator)", "Trader (Principal)", "Recovery (Trader)", "Bridge"];
    let account_idx = Select::with_theme(&theme)
        .with_prompt("Select source account")
        .default(0)
        .items(&account_choices)
        .interact()
        .unwrap();
    let source_kind = match account_idx {
        0 => "main",
        1 => "trader",
        2 => "recovery",
        3 => "bridge",
        _ => "main",
    };

    // Source selection
    let (src_identity, account) = match source_kind {
        "main" => (config.liquidator_identity.clone(), config.liquidator_principal.into()),
        "trader" => (config.trader_identity.clone(), config.trader_principal.into()),
        "recovery" => (config.trader_identity.clone(), config.get_recovery_account()),
        "bridge" => (
            config.bridge_ic_identity.clone(),
            config.bridge_ic_account_for_symbol(""),
        ),
        _ => (config.liquidator_identity.clone(), config.liquidator_principal.into()),
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
    let mut balances: HashMap<AssetId, ChainTokenAmount> = HashMap::new();
    for (id, _token) in &assets {
        let service = balance_service_for_source(&ctx, source_kind, &id.symbol);
        if let Ok(bal) = service.get_balance(id).await {
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
        let dest_choices = vec!["Main (Liquidator)", "Trader", "Recovery", "Bridge", "Manual input"];
        let dest_idx = Select::with_theme(&theme)
            .with_prompt("Select destination ICP account")
            .default(0)
            .items(&dest_choices)
            .interact()
            .unwrap();

        let dst_account = match dest_idx {
            0 => config.liquidator_principal.into(),
            1 => config.trader_principal.into(),
            2 => config.get_recovery_account(),
            3 => {
                eprintln!(
                    "Bridge destination for --asset all is ambiguous (route-specific subaccounts). Select a single asset."
                );
                return;
            }
            _ => {
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
                if entered_account == config.get_recovery_account() || entered_account == config.trader_principal.into()
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
            }
        };

        Destination::Icp(dst_account)
    } else {
        // Single-asset flow: destination depends on selected token chain.
        let (id_selected, token_selected) = assets[asset_idx].clone();

        match token_selected {
            ChainToken::Icp { .. } => {
                let dest_choices = vec!["Main (Liquidator)", "Trader", "Recovery", "Bridge", "Manual input"];
                let dest_idx = Select::with_theme(&theme)
                    .with_prompt("Select destination ICP account")
                    .default(0)
                    .items(&dest_choices)
                    .interact()
                    .unwrap();

                let dst_account = match dest_idx {
                    0 => config.liquidator_principal.into(),
                    1 => config.trader_principal.into(),
                    2 => config.get_recovery_account(),
                    3 => config.bridge_ic_account_for_symbol(&id_selected.symbol),
                    _ => {
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
                    }
                };

                Destination::Icp(dst_account)
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                let dest_choices = vec!["Main (Liquidator EVM)", "Bridge EVM", "Manual input"];
                let dest_idx = Select::with_theme(&theme)
                    .with_prompt("Select destination EVM address")
                    .default(0)
                    .items(&dest_choices)
                    .interact()
                    .unwrap();

                match dest_idx {
                    0 => Destination::Evm(ctx.evm_address.clone()),
                    1 => Destination::Evm(config.bridge_evm_address.clone()),
                    _ => {
                        let manual_input: String = Input::with_theme(&theme)
                            .with_prompt("Enter destination EVM address (0x...)")
                            .validate_with(|input: &String| -> Result<(), &str> {
                                if input.parse::<EvmAddress>().is_ok() {
                                    Ok(())
                                } else {
                                    Err("Invalid EVM address format")
                                }
                            })
                            .interact_text()
                            .unwrap();

                        Destination::Evm(manual_input)
                    }
                }
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
            let amount_native = match token {
                ChainToken::Icp { ledger, .. } => {
                    let fee = fetch_icrc1_fee(agent.clone(), *ledger).await;
                    let send_amount = bal
                        .value
                        .clone()
                        .0
                        .to_u128()
                        .unwrap_or(0)
                        .saturating_sub(fee.0.to_u128().unwrap_or(0));
                    if send_amount == 0 {
                        println!("Skipping {}: balance too low to cover fee.", id.symbol);
                        continue;
                    }
                    Nat::from(send_amount)
                }
                _ => bal.value.clone(),
            };

            let amount_nat = ChainTokenAmount {
                token: token.clone(),
                value: amount_native,
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
                ChainToken::Icp { ledger, .. } => {
                    let bal_native = bal_opt.map(|b| b.value.clone()).unwrap_or(0u8.into());
                    let fee = fetch_icrc1_fee(agent.clone(), *ledger).await;
                    let send_amount = bal_native
                        .0
                        .to_u128()
                        .unwrap_or(0)
                        .saturating_sub(fee.0.to_u128().unwrap_or(0));
                    if send_amount == 0 {
                        println!("Not enough balance to cover fee; aborting.");
                        return;
                    }
                    Nat::from(send_amount)
                }
                ChainToken::EvmErc20 { .. } => {
                    let (gas_reserve_wei, native_balance_wei) = match estimate_evm_gas_reserve_and_native_balance_wei(
                        &config,
                        source_kind,
                        EVM_ERC20_TRANSFER_GAS_LIMIT,
                    )
                    .await
                    {
                        Ok(v) => v,
                        Err(err) => {
                            println!("Failed to estimate EVM gas reserve: {err}; aborting.");
                            return;
                        }
                    };
                    if native_balance_wei <= gas_reserve_wei {
                        println!(
                            "Not enough native balance to cover gas for EVM token transfer; aborting. native_balance_wei={} gas_reserve_wei={}",
                            native_balance_wei, gas_reserve_wei
                        );
                        return;
                    }
                    bal_opt.map(|b| b.value.clone()).unwrap_or(0u8.into())
                }

                // EVM native: reserve a gas buffer so the tx can actually be sent.
                ChainToken::EvmNative { .. } => {
                    let (gas_reserve_wei, native_balance_wei) = match estimate_evm_gas_reserve_and_native_balance_wei(
                        &config,
                        source_kind,
                        EVM_NATIVE_TRANSFER_GAS_LIMIT,
                    )
                    .await
                    {
                        Ok(v) => v,
                        Err(err) => {
                            println!("Failed to estimate EVM gas reserve: {err}; aborting.");
                            return;
                        }
                    };
                    let send_native = native_balance_wei.saturating_sub(gas_reserve_wei);

                    if send_native == 0 {
                        println!(
                            "Not enough native balance to cover gas for EVM transfer; aborting. native_balance_wei={} gas_reserve_wei={}",
                            native_balance_wei, gas_reserve_wei
                        );
                        return;
                    }

                    send_native.into()
                }
            };

            ChainTokenAmount {
                token: token.clone(),
                value: amount_native,
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
            ChainToken::Icp { .. } => {
                if source_kind == "bridge" {
                    config.bridge_ic_account_for_symbol(&asset_id.symbol).to_string()
                } else {
                    account.to_string()
                }
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if source_kind == "bridge" {
                    config.bridge_evm_address.clone()
                } else {
                    ctx.evm_address.clone()
                }
            }
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
        println!("  Asset:       {} ({})", asset_id.symbol, asset_id.address);
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

    for (asset_id, token, amt, bal_fmt) in plan {
        let transfer_service = transfer_service_for_source(&ctx, source_kind, &asset_id.symbol);
        let src_str = match &token {
            ChainToken::Icp { .. } => {
                if source_kind == "bridge" {
                    config.bridge_ic_account_for_symbol(&asset_id.symbol).to_string()
                } else {
                    account.to_string()
                }
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if source_kind == "bridge" {
                    config.bridge_evm_address.clone()
                } else {
                    ctx.evm_address.clone()
                }
            }
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

        let res = agent
            .query(&ledger, "icrc1_balance_of")
            .with_arg(Encode!(&account).unwrap())
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

async fn fetch_icrc1_fee(agent: Arc<Agent>, ledger: Principal) -> Nat {
    let res = agent
        .query(&ledger, "icrc1_fee")
        .with_arg(Encode!(&()).unwrap())
        .call()
        .await;

    if let Ok(bytes) = res
        && let Ok(fee) = Decode!(&bytes, Nat)
    {
        fee
    } else {
        Nat::from(0u8)
    }
}

const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 200_000_000_000_000;
const EVM_NATIVE_TRANSFER_GAS_LIMIT: u128 = 21_000;
const EVM_ERC20_TRANSFER_GAS_LIMIT: u128 = 100_000;
const EVM_GAS_RESERVE_MULTIPLIER: u128 = 2;

fn evm_private_key_for_source<'a>(config: &'a crate::config::Config, source_kind: &str) -> &'a str {
    if source_kind == "bridge" {
        &config.bridge_evm_private_key
    } else {
        &config.evm_private_key
    }
}

async fn estimate_evm_gas_reserve_and_native_balance_wei(
    config: &crate::config::Config,
    source_kind: &str,
    gas_limit: u128,
) -> Result<(u128, u128), String> {
    let signer: PrivateKeySigner = evm_private_key_for_source(config, source_kind)
        .parse()
        .map_err(|e| format!("failed to parse EVM private key for source '{source_kind}': {e}"))?;
    let signer_address = signer.address();
    let rpc_url = config
        .evm_rpc_url
        .parse()
        .map_err(|e| format!("invalid EVM RPC URL '{}': {e}", config.evm_rpc_url))?;

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(rpc_url);

    let gas_price_wei = provider
        .get_gas_price()
        .await
        .map_err(|e| format!("failed to fetch EVM gas price: {e}"))?;
    let dynamic_reserve = gas_price_wei
        .saturating_mul(gas_limit)
        .saturating_mul(EVM_GAS_RESERVE_MULTIPLIER);
    let gas_reserve_wei = dynamic_reserve.max(EVM_NATIVE_GAS_BUFFER_WEI);

    let native_balance_wei = provider
        .get_balance(signer_address)
        .await
        .map_err(|e| format!("failed to fetch EVM native balance for {}: {e}", signer_address))?
        .to::<u128>();

    Ok((gas_reserve_wei, native_balance_wei))
}

fn format_chain_balance_plain(bal: &ChainTokenAmount) -> String {
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

fn balance_service_for_source(
    ctx: &PipelineContext,
    source_kind: &str,
    asset_symbol: &str,
) -> Arc<liquidium_pipeline_core::balance_service::BalanceService> {
    match source_kind {
        "main" => ctx.main_service.clone(),
        "trader" => ctx.trader_service.clone(),
        "recovery" => ctx.recovery_service.clone(),
        "bridge" => ctx.bridge_balance_service_for_symbol(asset_symbol),
        _ => ctx.main_service.clone(),
    }
}

fn transfer_service_for_source(
    ctx: &PipelineContext,
    source_kind: &str,
    asset_symbol: &str,
) -> Arc<liquidium_pipeline_core::transfer::transfer_service::TransferService> {
    match source_kind {
        "main" => ctx.main_transfers.clone(),
        "trader" => ctx.trader_transfers.clone(),
        "recovery" => ctx.recovery_transfers.clone(),
        "bridge" => ctx.bridge_transfer_service_for_symbol(asset_symbol),
        _ => ctx.main_transfers.clone(),
    }
}

fn resolve_icp_destination(
    config: &crate::config::Config,
    destination: &str,
    asset_symbol: &str,
) -> Result<Account, String> {
    if destination.eq_ignore_ascii_case("main") {
        Ok(config.liquidator_principal.into())
    } else if destination.eq_ignore_ascii_case("trader") {
        Ok(config.trader_principal.into())
    } else if destination.eq_ignore_ascii_case("recovery") {
        Ok(config.get_recovery_account())
    } else if destination.eq_ignore_ascii_case("bridge") {
        Ok(config.bridge_ic_account_for_symbol(asset_symbol))
    } else {
        Account::from_str(destination).map_err(|_| format!("Invalid destination ICP account: {destination}"))
    }
}

fn resolve_evm_destination(default_main_evm: &str, bridge_evm: &str, destination: &str) -> Result<String, String> {
    if destination.eq_ignore_ascii_case("main") {
        return Ok(default_main_evm.to_string());
    }
    if destination.eq_ignore_ascii_case("bridge") {
        return Ok(bridge_evm.to_string());
    }
    if destination.eq_ignore_ascii_case("trader") || destination.eq_ignore_ascii_case("recovery") {
        return Err(
            "destination 'trader/recovery' is ICP-only for EVM assets; use --destination main|bridge|0x...".to_string(),
        );
    }
    let parsed = destination
        .parse::<EvmAddress>()
        .map_err(|_| format!("Invalid destination EVM address: {destination}"))?;
    Ok(parsed.to_string())
}

/// Non-interactive withdraw flow.
///
/// Arguments (case-insensitive where noted):
/// - `source`: "main" | "trader" | "recovery" | "bridge"
/// - `destination`: for ICP assets: "main" | "trader" | "recovery" | "bridge" | full Account text.
///                  for EVM assets: "main" | "bridge" | `0x...`.
/// - `asset`: token symbol (e.g., "ckUSDT", "USDC") | "all" (`all` executes ICP assets only)
/// - `amount`: decimal string (respects token decimals) | "all"
#[allow(dead_code)]
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
    let source_kind = match source.to_lowercase().as_str() {
        "main" | "m" | "liquidator" => "main",
        "trader" | "t" => "trader",
        "recovery" | "r" => "recovery",
        "bridge" | "b" => "bridge",
        other => {
            eprintln!("Invalid source account: {}", other);
            return;
        }
    };
    let (src_identity, account) = match source_kind {
        "main" => (config.liquidator_identity.clone(), config.liquidator_principal.into()),
        "trader" => (config.trader_identity.clone(), config.trader_principal.into()),
        "recovery" => (config.trader_identity.clone(), config.get_recovery_account()),
        "bridge" => (
            config.bridge_ic_identity.clone(),
            config.bridge_ic_account_for_symbol(""),
        ),
        _ => (config.liquidator_identity.clone(), config.liquidator_principal.into()),
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
    // Build asset catalog from registry: ICP + EVM tokens.
    let mut assets: Vec<(AssetId, ChainToken)> = ctx
        .registry
        .tokens
        .iter()
        .map(|(id, token)| (id.clone(), token.clone()))
        .collect();
    assets.sort_by(|(id_a, _), (id_b, _)| id_a.chain.cmp(&id_b.chain).then(id_a.symbol.cmp(&id_b.symbol)));

    let icp_assets: Vec<(AssetId, ChainToken)> = assets
        .iter()
        .filter_map(|(id, token)| match token {
            ChainToken::Icp { .. } => Some((id.clone(), token.clone())),
            _ => None,
        })
        .collect();

    // Build plan: (asset_id, token, destination, amount, balance_fmt)
    let mut plan: Vec<(AssetId, ChainToken, ChainAccount, ChainTokenAmount, String)> = Vec::new();
    let all_assets = asset.eq_ignore_ascii_case("all");

    if all_assets {
        for (id, token) in &icp_assets {
            let ChainToken::Icp { ledger, .. } = token else {
                continue;
            };

            let dst_account = match resolve_icp_destination(&config, destination, &id.symbol) {
                Ok(account) => account,
                Err(err) => {
                    eprintln!("{err}");
                    return;
                }
            };

            let balance_service = balance_service_for_source(&ctx, source_kind, &id.symbol);
            let bal = match balance_service.get_balance(id).await {
                Ok(balance) => balance,
                Err(err) => {
                    eprintln!("Skipping {}: failed to read balance ({err})", id.symbol);
                    continue;
                }
            };
            let bal_fmt = format_chain_balance_plain(&bal);

            let amount_nat = if amount.eq_ignore_ascii_case("all") {
                let balance_units = bal.value.0.to_u128().unwrap_or(0);
                let fee = fetch_icrc1_fee(agent.clone(), *ledger).await;
                let fee_units = fee.0.to_u128().unwrap_or(0);
                let send_units = balance_units.saturating_sub(fee_units);
                if send_units == 0 {
                    eprintln!("Skipping {}: balance too low to cover fee.", id.symbol);
                    continue;
                }
                ChainTokenAmount {
                    token: token.clone(),
                    value: candid::Nat::from(send_units),
                }
            } else {
                let val = f64::from_str(amount).unwrap_or(0.0);
                ChainTokenAmount::from_formatted(token.clone(), val)
            };

            plan.push((
                id.clone(),
                token.clone(),
                ChainAccount::Icp(dst_account),
                amount_nat,
                bal_fmt,
            ));
        }
    } else {
        // Single-asset flow supports both ICP and EVM assets.
        let picked = assets
            .iter()
            .find(|(id, _)| id.symbol.eq_ignore_ascii_case(asset))
            .cloned();
        let Some((id, token)) = picked else {
            eprintln!("Unknown asset symbol: {}", asset);
            return;
        };

        let balance_service = balance_service_for_source(&ctx, source_kind, &id.symbol);
        let bal_opt = balance_service.get_balance(&id).await.ok();
        let bal_fmt = bal_opt
            .as_ref()
            .map(format_chain_balance_plain)
            .unwrap_or_else(|| "n/a".to_string());

        let destination_account = match &token {
            ChainToken::Icp { .. } => match resolve_icp_destination(&config, destination, &id.symbol) {
                Ok(dst_account) => ChainAccount::Icp(dst_account),
                Err(err) => {
                    eprintln!("{err}");
                    return;
                }
            },
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                let dst = match resolve_evm_destination(&ctx.evm_address, &config.bridge_evm_address, destination) {
                    Ok(dst) => dst,
                    Err(err) => {
                        eprintln!("{err}");
                        return;
                    }
                };
                ChainAccount::Evm(dst)
            }
        };

        let amount_nat = if amount.eq_ignore_ascii_case("all") {
            match &token {
                ChainToken::Icp { ledger, .. } => {
                    let bal_native = bal_opt.map(|b| b.value).unwrap_or_else(|| Nat::from(0u8));
                    let fee = fetch_icrc1_fee(agent.clone(), *ledger).await;
                    let send_amount = bal_native
                        .0
                        .to_u128()
                        .unwrap_or(0)
                        .saturating_sub(fee.0.to_u128().unwrap_or(0));
                    if send_amount == 0 {
                        eprintln!("Not enough balance to cover fee; aborting.");
                        return;
                    }
                    ChainTokenAmount {
                        token: token.clone(),
                        value: Nat::from(send_amount),
                    }
                }
                ChainToken::EvmErc20 { .. } => ChainTokenAmount {
                    token: token.clone(),
                    value: {
                        let (gas_reserve_wei, native_balance_wei) =
                            match estimate_evm_gas_reserve_and_native_balance_wei(
                                &config,
                                source_kind,
                                EVM_ERC20_TRANSFER_GAS_LIMIT,
                            )
                            .await
                            {
                                Ok(v) => v,
                                Err(err) => {
                                    eprintln!("Failed to estimate EVM gas reserve: {err}");
                                    return;
                                }
                            };
                        if native_balance_wei <= gas_reserve_wei {
                            eprintln!(
                                "Not enough native balance to cover gas for EVM token transfer; aborting. native_balance_wei={} gas_reserve_wei={}",
                                native_balance_wei, gas_reserve_wei
                            );
                            return;
                        }
                        bal_opt.map(|b| b.value).unwrap_or_else(|| Nat::from(0u8))
                    },
                },
                ChainToken::EvmNative { .. } => {
                    let (gas_reserve_wei, native_balance_wei) = match estimate_evm_gas_reserve_and_native_balance_wei(
                        &config,
                        source_kind,
                        EVM_NATIVE_TRANSFER_GAS_LIMIT,
                    )
                    .await
                    {
                        Ok(v) => v,
                        Err(err) => {
                            eprintln!("Failed to estimate EVM gas reserve: {err}");
                            return;
                        }
                    };
                    let send_native = native_balance_wei.saturating_sub(gas_reserve_wei);
                    if send_native == 0 {
                        eprintln!(
                            "Not enough native balance to cover gas for EVM transfer; aborting. native_balance_wei={} gas_reserve_wei={}",
                            native_balance_wei, gas_reserve_wei
                        );
                        return;
                    }
                    ChainTokenAmount {
                        token: token.clone(),
                        value: Nat::from(send_native),
                    }
                }
            }
        } else {
            let val = f64::from_str(amount).unwrap_or(0.0);
            ChainTokenAmount::from_formatted(token.clone(), val)
        };

        plan.push((id, token, destination_account, amount_nat, bal_fmt));
    }

    if plan.is_empty() {
        eprintln!("No transfers to execute.");
        return;
    }

    // Echo plan
    println!("Planned transfers (non-interactive):\n");
    for (i, (asset_id, token, to, _amt, bal)) in plan.iter().enumerate() {
        let src = match token {
            ChainToken::Icp { .. } => {
                if source_kind == "bridge" {
                    config.bridge_ic_account_for_symbol(&asset_id.symbol).to_string()
                } else {
                    account.to_string()
                }
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if source_kind == "bridge" {
                    config.bridge_evm_address.clone()
                } else {
                    ctx.evm_address.clone()
                }
            }
        };
        let dst = match to {
            ChainAccount::Icp(acc) => acc.to_string(),
            ChainAccount::Evm(addr) => addr.clone(),
            ChainAccount::IcpLedger(v) => v.clone(),
        };

        println!("Transfer #{}:", i + 1);
        println!("  Source:      {}", src);
        println!("  Destination: {}", dst);
        println!("  Asset:       {}", asset_id.symbol);
        println!("  Amount:      {}", amount);
        println!("  Balance:     {}", bal);
        println!();
    }

    for (asset_id, token, to, amt, bal_fmt) in &plan {
        let transfer_service = transfer_service_for_source(&ctx, source_kind, &asset_id.symbol);
        let src = match token {
            ChainToken::Icp { .. } => {
                if source_kind == "bridge" {
                    config.bridge_ic_account_for_symbol(&asset_id.symbol).to_string()
                } else {
                    account.to_string()
                }
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if source_kind == "bridge" {
                    config.bridge_evm_address.clone()
                } else {
                    ctx.evm_address.clone()
                }
            }
        };
        let dst = match to {
            ChainAccount::Icp(acc) => acc.to_string(),
            ChainAccount::Evm(addr) => addr.clone(),
            ChainAccount::IcpLedger(v) => v.clone(),
        };

        match transfer_service
            .transfer_by_asset_id(asset_id, to.clone(), amt.value.clone())
            .await
        {
            Ok(txid) => {
                println!("Transfer result:");
                println!("  Source:      {}", src);
                println!("  Destination: {}", dst);
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      ok: {}", txid);
                println!();
            }
            Err(e) => {
                println!("Transfer result:");
                println!("  Source:      {}", src);
                println!("  Destination: {}", dst);
                println!("  Asset:       {}", asset_id.symbol);
                println!("  Amount:      {}", amount);
                println!("  Balance:     {}", bal_fmt);
                println!("  Result:      err: {}", e);
                println!();
            }
        }
    }

    // Show updated destination balances only for pure ICP-destination plans.
    let icp_destination = plan.iter().try_fold(None::<Account>, |acc, (_, _, to, _, _)| match to {
        ChainAccount::Icp(a) => match acc {
            None => Ok(Some(a.clone())),
            Some(existing) if existing == *a => Ok(Some(existing)),
            Some(_) => Err(()),
        },
        _ => Err(()),
    });

    if let Ok(Some(dst_account)) = icp_destination {
        let balances_after = sync_balances(agent, &icp_assets, dst_account.clone()).await;
        println!("Updated destination balances ({dst_account}):");
        for (principal, formatted) in balances_after {
            println!("  {} | {}", principal, formatted);
        }
    }
}
