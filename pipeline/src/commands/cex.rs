use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    network::AnyNetwork, primitives::Address as EvmAddress, providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use async_trait::async_trait;
use candid::{Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeBackend, BridgeDestination, BridgeRequest, BridgeStatus, CkEthErc20BridgeBackend,
};
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, SwapExecutionOptions};
use liquidium_pipeline_core::{
    account::model::ChainAccount, tokens::chain_token::ChainToken, transfer::actions::TransferActions,
};
use tokio::time::sleep;

use crate::{
    config::Config, finalizers::mexc::mexc_finalizer::MexcFinalizer, swappers::mexc::mexc_adapter::MexcClient,
};

const SMOKE_FROM_ASSET: &str = "CKBTC";
const SMOKE_TO_ASSET: &str = "USDC";
const BRIDGE_SOURCE_CHAIN: &str = "ETH";
const BRIDGE_TARGET_ASSET: &str = "ckUSDC";
const BRIDGE_SOURCE_FUNDING_POLL_INTERVAL: Duration = Duration::from_secs(10);
const BRIDGE_SOURCE_FUNDING_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const BRIDGE_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(5);
const BRIDGE_STATUS_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Default)]
struct NoopTransferActions;

#[async_trait]
impl TransferActions for NoopTransferActions {
    async fn transfer(&self, _token: &ChainToken, _to: &ChainAccount, _amount_native: Nat) -> Result<String, String> {
        Err("noop transfer service: transfer is not supported".to_string())
    }

    async fn approve(
        &self,
        _token: &ChainToken,
        _spender: &ChainAccount,
        _amount_native: Nat,
    ) -> Result<String, String> {
        Err("noop transfer service: approve is not supported".to_string())
    }
}

fn build_mexc_smoke_finalizer<B: CexBackend>(backend: Arc<B>, config: &Config) -> MexcFinalizer<B> {
    MexcFinalizer::new_with_tunables(
        backend,
        Arc::new(NoopTransferActions),
        config.liquidator_principal,
        config.max_allowed_cex_slippage_bps as f64,
        config.cex_min_exec_usd,
        config.cex_slice_target_ratio,
        config.cex_buy_truncation_trigger_ratio,
        config.cex_buy_inverse_overspend_bps,
        config.cex_buy_inverse_max_retries,
        config.cex_buy_inverse_enabled,
    )
    .with_route_config(
        config.cex_mexc_available_pairs.clone(),
        config.cex_mexc_max_hops as usize,
    )
}

fn resolve_withdraw_destination(withdraw_address: Option<&str>, evm_private_key: &str) -> Result<String, String> {
    if let Some(address) = withdraw_address {
        let normalized = address.trim();
        if normalized.is_empty() {
            return Err("withdraw destination cannot be empty".to_string());
        }
        return Ok(normalized.to_string());
    }

    let signer: PrivateKeySigner = evm_private_key
        .parse()
        .map_err(|e| format!("invalid EVM private key in config: {e}"))?;
    Ok(signer.address().to_string())
}

fn resolve_smoke_withdraw_destination(
    config: &Config,
    withdraw_address: Option<&str>,
    bridge_after_withdraw: bool,
) -> Result<String, String> {
    if !bridge_after_withdraw {
        return resolve_withdraw_destination(withdraw_address, &config.evm_private_key);
    }

    let bridge_address = config.bridge_evm_address.parse::<EvmAddress>().map_err(|e| {
        format!(
            "invalid configured bridge EVM address '{}': {e}",
            config.bridge_evm_address
        )
    })?;
    if let Some(address) = withdraw_address {
        let parsed = address
            .trim()
            .parse::<EvmAddress>()
            .map_err(|e| format!("invalid --withdraw-address '{}': {e}", address.trim()))?;
        if parsed != bridge_address {
            return Err(format!(
                "bridge step requires withdraw destination to be bridge address {}; got {}",
                config.bridge_evm_address,
                address.trim()
            ));
        }
    }

    Ok(config.bridge_evm_address.clone())
}

fn resolve_bridge_destination_account(
    destination: Option<&str>,
    default_principal: Principal,
) -> Result<Account, String> {
    let Some(destination) = destination else {
        return Ok(Account {
            owner: default_principal,
            subaccount: None,
        });
    };

    let trimmed = destination.trim();
    if trimmed.is_empty() {
        return Err("bridge destination cannot be empty".to_string());
    }

    if let Ok(account) = Account::from_str(trimmed) {
        return Ok(account);
    }
    if let Ok(owner) = Principal::from_text(trimmed) {
        return Ok(Account {
            owner,
            subaccount: None,
        });
    }

    Err(format!(
        "invalid bridge destination '{}'; expected ICP account or principal",
        trimmed
    ))
}

fn with_route_resolution_hint(err: String) -> String {
    if err.contains("no configured hop route")
        || err.contains("no configured MEXC pairs for hop discovery")
        || err.contains("hop discovery disabled")
    {
        return format!("{err}; hint: set CEX_MEXC_AVAILABLE_PAIRS=CKBTC_BTC,BTC_USDC and CEX_MEXC_MAX_HOPS>=1");
    }
    err
}

pub async fn mexc_deposit_address(asset: &str, network: Option<&str>) -> Result<(), String> {
    let _ = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;
    let client = MexcClient::from_env()?;
    let network = network.unwrap_or("ICP");
    let addr = client.get_deposit_address(asset, network).await?;

    println!("MEXC deposit address:");
    println!("  asset   : {}", addr.asset);
    println!("  network : {}", addr.network);
    println!("  address : {}", addr.address);
    if let Some(tag) = addr.tag.as_ref()
        && !tag.is_empty()
    {
        println!("  tag     : {}", tag);
    }

    Ok(())
}

pub async fn mexc_smoke_swap_withdraw(
    amount_ckbtc: f64,
    execute: bool,
    withdraw_address: Option<&str>,
    withdraw_network: &str,
    bridge_after_withdraw: bool,
    bridge_destination: Option<&str>,
) -> Result<(), String> {
    if bridge_after_withdraw && !execute {
        return Err("bridge step requires --execute".to_string());
    }

    let config = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;
    let backend = Arc::new(MexcClient::from_env()?);
    let finalizer = build_mexc_smoke_finalizer(backend.clone(), &config);
    let destination = resolve_smoke_withdraw_destination(&config, withdraw_address, bridge_after_withdraw)?;

    let maybe_withdrawn_amount = run_mexc_smoke_with_backend(
        backend,
        &finalizer,
        amount_ckbtc,
        execute,
        &destination,
        withdraw_network,
    )
    .await?;

    if bridge_after_withdraw {
        let withdrawn_amount = maybe_withdrawn_amount.ok_or_else(|| "bridge step requires --execute".to_string())?;
        let destination_account = resolve_bridge_destination_account(bridge_destination, config.liquidator_principal)?;
        run_bridge_step_after_mexc_withdraw(&config, withdrawn_amount, destination_account).await?;
    }

    Ok(())
}

pub(crate) async fn run_mexc_smoke_with_backend<B: CexBackend>(
    backend: Arc<B>,
    finalizer: &MexcFinalizer<B>,
    amount_ckbtc: f64,
    execute: bool,
    withdraw_destination: &str,
    withdraw_network: &str,
) -> Result<Option<f64>, String> {
    if !amount_ckbtc.is_finite() || amount_ckbtc <= 0.0 {
        return Err("preflight failed: amount_ckbtc must be positive".to_string());
    }
    if withdraw_network.trim().is_empty() {
        return Err("preflight failed: withdraw_network cannot be empty".to_string());
    }
    if withdraw_destination.trim().is_empty() {
        return Err("preflight failed: withdraw destination cannot be empty".to_string());
    }

    let available_ckbtc = backend
        .get_balance(SMOKE_FROM_ASSET)
        .await
        .map_err(|e| format!("preflight failed: could not fetch {SMOKE_FROM_ASSET} balance: {e}"))?;
    if available_ckbtc + 1e-12 < amount_ckbtc {
        return Err(format!(
            "preflight failed: insufficient {SMOKE_FROM_ASSET} balance on MEXC (available={} required={})",
            available_ckbtc, amount_ckbtc
        ));
    }

    let route_legs = finalizer
        .resolve_trade_legs_for_symbols(SMOKE_FROM_ASSET, SMOKE_TO_ASSET)
        .await
        .map_err(|e| format!("preflight failed: {}", with_route_resolution_hint(e)))?;

    println!("MEXC smoke preflight");
    println!("  pair            : {} -> {}", SMOKE_FROM_ASSET, SMOKE_TO_ASSET);
    println!("  amount_ckbtc    : {}", amount_ckbtc);
    println!("  available_ckbtc : {}", available_ckbtc);
    println!("  mode            : {}", if execute { "live" } else { "dry-run" });
    println!("  withdraw_asset  : {}", SMOKE_TO_ASSET);
    println!("  withdraw_network: {}", withdraw_network);
    println!("  withdraw_dest   : {}", withdraw_destination);
    println!("  route_legs      : {}", route_legs.len());
    for (idx, leg) in route_legs.iter().enumerate() {
        println!("    {}. market={} side={}", idx + 1, leg.market, leg.side);
    }

    if !execute {
        println!("Dry-run complete: no side effects executed.");
        return Ok(None);
    }

    let mut current_in = amount_ckbtc;
    for (idx, leg) in route_legs.iter().enumerate() {
        let report = backend
            .execute_swap_detailed_with_options(&leg.market, &leg.side, current_in, SwapExecutionOptions::default())
            .await
            .map_err(|e| {
                format!(
                    "live execution failed on leg {}/{} (market={} side={} amount_in={}): {}",
                    idx + 1,
                    route_legs.len(),
                    leg.market,
                    leg.side,
                    current_in,
                    e
                )
            })?;

        if !report.output_received.is_finite() || report.output_received <= 0.0 {
            return Err(format!(
                "live execution failed on leg {}/{} (market={} side={}): non-positive output {}",
                idx + 1,
                route_legs.len(),
                leg.market,
                leg.side,
                report.output_received
            ));
        }

        println!(
            "  executed leg {}/{}: market={} side={} input_consumed={} output_received={}",
            idx + 1,
            route_legs.len(),
            leg.market,
            leg.side,
            report.input_consumed,
            report.output_received
        );
        current_in = report.output_received;
    }

    let receipt = backend
        .withdraw(SMOKE_TO_ASSET, withdraw_network, withdraw_destination, current_in)
        .await
        .map_err(|e| {
            format!(
                "live execution failed during withdraw (asset={} network={} destination={} amount={}): {}",
                SMOKE_TO_ASSET, withdraw_network, withdraw_destination, current_in, e
            )
        })?;

    println!("Withdrawal submitted");
    println!("  asset      : {}", receipt.asset);
    println!("  network    : {}", receipt.network);
    println!("  amount     : {}", receipt.amount);
    println!("  txid       : {}", receipt.txid.as_deref().unwrap_or("-"));
    println!("  internal_id: {}", receipt.internal_id.as_deref().unwrap_or("-"));

    Ok(Some(receipt.amount))
}

async fn run_bridge_step_after_mexc_withdraw(
    config: &Config,
    amount: f64,
    destination_account: Account,
) -> Result<(), String> {
    if !amount.is_finite() || amount <= 0.0 {
        return Err(format!("bridge step amount must be positive; got {amount}"));
    }

    let bridge_signer: PrivateKeySigner = config
        .bridge_evm_private_key
        .parse()
        .map_err(|err| format!("failed to parse bridge EVM private key: {err}"))?;
    let bridge_rpc_url = config
        .evm_rpc_url
        .parse()
        .map_err(|err| format!("invalid EVM RPC URL for bridge step: {err}"))?;
    let bridge_provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(bridge_signer)
        .connect_http(bridge_rpc_url);

    let bridge_agent = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.bridge_ic_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|err| format!("failed to build IC agent for bridge step: {err}"))?,
    );

    let bridge_backend =
        CkEthErc20BridgeBackend::new(bridge_agent, bridge_provider, config.bridge_cketh_minter_canister);

    println!(
        "Waiting for bridge source funds (timeout={}s, poll={}s)...",
        BRIDGE_SOURCE_FUNDING_TIMEOUT.as_secs(),
        BRIDGE_SOURCE_FUNDING_POLL_INTERVAL.as_secs()
    );
    let bridge_balance = wait_for_bridge_source_funding(&bridge_backend, &config.bridge_evm_address, amount).await?;
    let bridge_submit_amount = bridge_balance;
    println!("Bridge source funded");
    println!("  available  : {}", bridge_balance);
    println!("  min_needed : {}", amount);
    println!("  submit_all : {}", bridge_submit_amount);
    println!("  source     : {}", config.bridge_evm_address);

    let request = BridgeRequest {
        asset: SMOKE_TO_ASSET.to_string(),
        source_chain: BRIDGE_SOURCE_CHAIN.to_string(),
        source_address: config.bridge_evm_address.clone(),
        target_asset: BRIDGE_TARGET_ASSET.to_string(),
        destination: BridgeDestination::IcpAccount(destination_account.clone()),
        amount: bridge_submit_amount,
    };

    println!("Starting bridge");
    println!(
        "  route      : {}@{} -> {}",
        SMOKE_TO_ASSET, BRIDGE_SOURCE_CHAIN, BRIDGE_TARGET_ASSET
    );
    println!("  source     : {}", config.bridge_evm_address);
    println!("  amount     : {}", bridge_submit_amount);
    println!("  destination: {}", destination_account);

    let submission = bridge_backend
        .submit_bridge(request)
        .await
        .map_err(|err| format!("bridge step failed: {err}"))?;

    println!("Bridge submitted");
    println!(
        "  route      : {}@{} -> {}",
        SMOKE_TO_ASSET, BRIDGE_SOURCE_CHAIN, BRIDGE_TARGET_ASSET
    );
    println!("  source     : {}", config.bridge_evm_address);
    println!("  amount     : {}", bridge_submit_amount);
    println!("  bridge_id  : {}", submission.bridge_id);
    println!(
        "Waiting for bridge completion (timeout={}s, poll={}s)...",
        BRIDGE_STATUS_TIMEOUT.as_secs(),
        BRIDGE_STATUS_POLL_INTERVAL.as_secs()
    );

    wait_for_bridge_completion(&bridge_backend, &submission.bridge_id).await?;
    println!("Bridge completed");
    println!("  bridge_id  : {}", submission.bridge_id);

    Ok(())
}

async fn wait_for_bridge_source_funding<B>(
    backend: &B,
    source_address: &str,
    minimum_required_amount: f64,
) -> Result<f64, String>
where
    B: BridgeBackend + Send + Sync,
{
    let started = Instant::now();
    let mut poll_count = 0u64;

    loop {
        let balance = backend
            .get_source_balance(SMOKE_TO_ASSET, BRIDGE_SOURCE_CHAIN, source_address)
            .await
            .map_err(|err| format!("bridge source balance check failed: {err}"))?;

        if balance.is_finite() && balance + 1e-12 >= minimum_required_amount {
            return Ok(balance);
        }

        poll_count += 1;
        println!(
            "  bridge funding: available={} min_needed={} (poll={} elapsed={}s)",
            balance,
            minimum_required_amount,
            poll_count,
            started.elapsed().as_secs()
        );

        if started.elapsed() >= BRIDGE_SOURCE_FUNDING_TIMEOUT {
            return Err(format!(
                "timed out waiting for bridge source funds after {}s (available={} min_needed={} source={})",
                BRIDGE_SOURCE_FUNDING_TIMEOUT.as_secs(),
                balance,
                minimum_required_amount,
                source_address
            ));
        }

        sleep(BRIDGE_SOURCE_FUNDING_POLL_INTERVAL).await;
    }
}

async fn wait_for_bridge_completion<B>(backend: &B, bridge_id: &str) -> Result<(), String>
where
    B: BridgeBackend + Send + Sync,
{
    let started = Instant::now();
    let mut poll_count = 0u64;

    loop {
        let status = backend
            .get_bridge_status(bridge_id)
            .await
            .map_err(|err| format!("bridge status check failed for {bridge_id}: {err}"))?;

        match status {
            BridgeStatus::Completed => return Ok(()),
            BridgeStatus::Failed { reason } => {
                return Err(format!(
                    "bridge failed for {bridge_id}: {}",
                    reason.unwrap_or_else(|| "unknown failure".to_string())
                ));
            }
            BridgeStatus::Canceled { reason } => {
                return Err(format!(
                    "bridge canceled for {bridge_id}: {}",
                    reason.unwrap_or_else(|| "no reason provided".to_string())
                ));
            }
            BridgeStatus::Pending | BridgeStatus::Unknown => {
                poll_count += 1;
                println!(
                    "  bridge status: {:?} (poll={} elapsed={}s)",
                    status,
                    poll_count,
                    started.elapsed().as_secs()
                );
            }
        }

        if started.elapsed() >= BRIDGE_STATUS_TIMEOUT {
            return Err(format!(
                "timed out waiting for bridge completion after {}s for bridge_id={bridge_id}",
                BRIDGE_STATUS_TIMEOUT.as_secs()
            ));
        }
        sleep(BRIDGE_STATUS_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use candid::Principal;
    use liquidium_pipeline_connectors::backend::cex_backend::{
        MockCexBackend, OrderBook, OrderBookLevel, SwapFillReport, WithdrawalReceipt,
    };

    fn make_test_finalizer(
        backend: Arc<MockCexBackend>,
        available_pairs: Vec<String>,
        max_hops: usize,
    ) -> MexcFinalizer<MockCexBackend> {
        MexcFinalizer::new_with_tunables(
            backend,
            Arc::new(NoopTransferActions),
            Principal::anonymous(),
            200.0,
            1.1,
            0.7,
            0.25,
            10,
            1,
            true,
        )
        .with_route_config(available_pairs, max_hops)
    }

    fn liquid_sell_book() -> OrderBook {
        OrderBook {
            bids: vec![OrderBookLevel {
                price: 1.0,
                quantity: 1_000_000.0,
            }],
            asks: vec![],
        }
    }

    #[tokio::test]
    async fn mexc_smoke_dry_run_resolves_route_without_side_effects() {
        let mut backend = MockCexBackend::new();
        backend.expect_get_balance().times(1).returning(|asset| {
            assert_eq!(asset, "CKBTC");
            Ok(1.0)
        });
        backend.expect_get_orderbook().times(1).returning(|market, _| {
            assert_eq!(market, "CKBTC_USDC");
            Ok(liquid_sell_book())
        });
        backend.expect_execute_swap_detailed_with_options().times(0);
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let finalizer = make_test_finalizer(backend.clone(), vec![], 2);

        let result = run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.01,
            false,
            "0x1111111111111111111111111111111111111111",
            "ETH",
        )
        .await
        .expect("dry-run should succeed");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn mexc_smoke_live_executes_legs_in_order_and_withdraws_final_amount() {
        let mut backend = MockCexBackend::new();
        backend.expect_get_balance().times(1).returning(|_| Ok(10.0));

        let empty = OrderBook {
            bids: vec![],
            asks: vec![],
        };
        let liquid = liquid_sell_book();
        backend
            .expect_get_orderbook()
            .times(4)
            .returning(move |market, _| match market {
                "CKBTC_USDC" | "USDC_CKBTC" => Ok(empty.clone()),
                "CKBTC_BTC" | "BTC_USDC" => Ok(liquid.clone()),
                other => Err(format!("unexpected market {other}")),
            });

        let swap_calls = Arc::new(Mutex::new(0usize));
        let swap_calls_handle = swap_calls.clone();
        backend
            .expect_execute_swap_detailed_with_options()
            .times(2)
            .returning(move |market, side, amount_in, _| {
                let mut call_idx = swap_calls_handle.lock().expect("mutex");
                *call_idx += 1;
                match *call_idx {
                    1 => {
                        assert_eq!(market, "CKBTC_BTC");
                        assert_eq!(side, "sell");
                        assert!((amount_in - 0.5).abs() < 1e-12);
                        Ok(SwapFillReport {
                            input_consumed: amount_in,
                            output_received: 1.25,
                        })
                    }
                    2 => {
                        assert_eq!(market, "BTC_USDC");
                        assert_eq!(side, "sell");
                        assert!((amount_in - 1.25).abs() < 1e-12);
                        Ok(SwapFillReport {
                            input_consumed: amount_in,
                            output_received: 2_000.0,
                        })
                    }
                    _ => panic!("unexpected swap invocation"),
                }
            });

        backend
            .expect_withdraw()
            .times(1)
            .returning(|asset, network, address, amount| {
                assert_eq!(asset, "USDC");
                assert_eq!(network, "ETH");
                assert_eq!(address, "0x2222222222222222222222222222222222222222");
                assert!((amount - 2_000.0).abs() < 1e-9);
                Ok(WithdrawalReceipt {
                    asset: asset.to_string(),
                    network: network.to_string(),
                    amount,
                    txid: Some("tx123".to_string()),
                    internal_id: Some("wid123".to_string()),
                })
            });

        let backend = Arc::new(backend);
        let finalizer = make_test_finalizer(
            backend.clone(),
            vec!["CKBTC_BTC".to_string(), "BTC_USDC".to_string()],
            1,
        );

        let result = run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.5,
            true,
            "0x2222222222222222222222222222222222222222",
            "ETH",
        )
        .await
        .expect("live smoke should succeed");
        assert_eq!(result, Some(2_000.0));
    }

    #[test]
    fn resolve_withdraw_destination_uses_evm_key_when_address_missing() {
        let evm_private_key = "0x59c6995e998f97a5a0044976f53f58816f2f94f4a7f87b5c6a1f7fbf3f5e6be7";
        let signer: PrivateKeySigner = evm_private_key.parse().expect("private key should parse");
        let got = resolve_withdraw_destination(None, evm_private_key).expect("should resolve from key");
        assert_eq!(got, signer.address().to_string());
    }

    #[test]
    fn resolve_bridge_destination_account_defaults_to_liquidator_principal() {
        let principal = Principal::management_canister();
        let account = resolve_bridge_destination_account(None, principal).expect("default account");
        assert_eq!(
            account,
            Account {
                owner: principal,
                subaccount: None,
            }
        );
    }

    #[test]
    fn resolve_bridge_destination_account_accepts_principal_text() {
        let principal = Principal::management_canister();
        let account = resolve_bridge_destination_account(Some(&principal.to_text()), Principal::anonymous())
            .expect("principal destination");
        assert_eq!(
            account,
            Account {
                owner: principal,
                subaccount: None,
            }
        );
    }

    #[tokio::test]
    async fn mexc_smoke_errors_on_insufficient_balance() {
        let mut backend = MockCexBackend::new();
        backend.expect_get_balance().times(1).returning(|_| Ok(0.001));
        backend.expect_get_orderbook().times(0);
        backend.expect_execute_swap_detailed_with_options().times(0);
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let finalizer = make_test_finalizer(backend.clone(), vec![], 2);

        let err = run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.01,
            false,
            "0x3333333333333333333333333333333333333333",
            "ETH",
        )
        .await
        .expect_err("balance check should fail");
        assert!(err.contains("insufficient CKBTC balance on MEXC"));
    }

    #[tokio::test]
    async fn mexc_smoke_route_error_includes_config_hint() {
        let mut backend = MockCexBackend::new();
        backend.expect_get_balance().times(1).returning(|_| Ok(1.0));
        let empty = OrderBook {
            bids: vec![],
            asks: vec![],
        };
        backend
            .expect_get_orderbook()
            .times(2)
            .returning(move |_, _| Ok(empty.clone()));
        backend.expect_execute_swap_detailed_with_options().times(0);
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let finalizer = make_test_finalizer(backend.clone(), vec![], 2);

        let err = run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.01,
            false,
            "0x4444444444444444444444444444444444444444",
            "ETH",
        )
        .await
        .expect_err("route resolution should fail");
        assert!(err.contains("CEX_MEXC_AVAILABLE_PAIRS=CKBTC_BTC,BTC_USDC"));
        assert!(err.contains("CEX_MEXC_MAX_HOPS>=1"));
    }
}
