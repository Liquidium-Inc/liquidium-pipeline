//! Reads what a Kraken leg needs before it runs: the keys, the balances, the
//! allowed markets and, for each route, the deposit address and whether our
//! destination is a verified withdrawal address. It moves nothing.
use ic_ledger_types::{AccountIdentifier, Subaccount};
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use solana_sdk::{signature::Keypair, signer::Signer};

use crate::config::{Config, ConfigTrait};
use crate::swappers::kraken::{KrakenClient, KrakenRouteCheck};

/// The assets the Kraken routes deposit, trade or withdraw.
const BALANCE_ASSETS: [&str; 7] = ["ICP", "USD", "USDT", "USDC", "ETH", "SOL", "BTC"];

pub async fn check() -> Result<(), String> {
    let config = Config::load().await?;
    let (api_key, secret) = config
        .get_cex_credentials("kraken")
        .map_err(|_| "CEX_KRAKEN_API_KEY/CEX_KRAKEN_API_SECRET are not set".to_string())?;
    let client = KrakenClient::new(api_key, secret, config.cex_kraken_available_pairs.clone())?;

    println!("== balances");
    for asset in BALANCE_ASSETS {
        match client.get_balance(asset).await {
            Ok(balance) => println!("  {asset:<5} {balance}"),
            Err(error) => println!("  {asset:<5} read failed: {error}"),
        }
    }

    println!("== markets in CEX_KRAKEN_AVAILABLE_PAIRS");
    for (market, limits) in client.pair_limits().await? {
        match limits {
            Some(limits) => println!(
                "  {market:<9} {} order_min={} cost_min={} {}",
                limits.api_name,
                limits.order_min,
                limits.cost_min,
                if limits.online { "online" } else { "NOT ONLINE" }
            ),
            None => println!("  {market:<9} NOT LISTED by Kraken"),
        }
    }

    // The destinations a leg withdraws to: native ICP straight to the
    // liquidator's account, everything else to a bridge wallet.
    let icp_account = AccountIdentifier::new(&config.liquidator_principal, &Subaccount([0; 32])).to_hex();
    let bridge_solana = Keypair::new_from_array(config.bridge_solana_private_key_bytes)
        .pubkey()
        .to_string();
    let routes = [
        ("ICP", "ICP", icp_account),
        ("USDT", "ETH", config.bridge_evm_address.clone()),
        ("USDC", "ETH", config.bridge_evm_address.clone()),
        ("ETH", "ETH", config.bridge_evm_address.clone()),
        ("SOL", "SOL", bridge_solana),
    ];
    let mut missing = Vec::new();
    for (asset, network, destination) in routes {
        println!("== {asset} on {network}");
        let route = client.route_check(asset, network, &destination).await;
        if !print_route(&route, &destination) {
            missing.push(format!("{asset}@{network} -> {destination}"));
        }
    }

    if missing.is_empty() {
        println!("RESULT: every route has a deposit address and a verified withdrawal address");
    } else {
        println!("RESULT: these withdrawals need a verified address in the Kraken account:");
        for route in missing {
            println!("  {route}");
        }
    }
    Ok(())
}

/// Prints one route; returns whether the destination is a verified withdrawal address.
fn print_route(route: &KrakenRouteCheck, destination: &str) -> bool {
    match &route.deposit {
        Ok(deposit) => println!(
            "  deposit:    {} min={} address={} memo={}",
            deposit.method,
            deposit.minimum,
            deposit.address,
            deposit
                .tag
                .as_deref()
                .filter(|tag| !tag.trim().is_empty())
                .unwrap_or("none")
        ),
        Err(error) => {
            println!("  deposit:    unavailable: {error}");
            println!("  listed:     {:?}", route.listed_deposit_methods);
        }
    }
    println!("  saved:      {:?}", route.saved_withdrawal_addresses);
    match &route.withdrawal {
        Ok(withdrawal) => {
            println!(
                "  withdrawal: {} network={} min={}",
                withdrawal.method,
                withdrawal.network.as_deref().unwrap_or("-"),
                withdrawal.minimum
            );
            println!("  verified:   {:?}", withdrawal.verified);
            match &withdrawal.fee_at_minimum {
                Some(Ok(fee)) => println!("  target:     {destination} is verified; fee at the minimum {fee}"),
                Some(Err(error)) => println!("  target:     {destination} is verified; fee read failed: {error}"),
                None => println!("  target:     {destination} is NOT a verified withdrawal address"),
            }
            withdrawal.destination_verified
        }
        Err(error) => {
            println!("  withdrawal: unavailable: {error}");
            println!("  listed:     {:?}", route.listed_withdrawal_methods);
            false
        }
    }
}
