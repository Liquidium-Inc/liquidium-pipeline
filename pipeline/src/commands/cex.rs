use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, SwapExecutionOptions};
use liquidium_pipeline_core::{
    account::model::ChainAccount, tokens::chain_token::ChainToken, transfer::actions::TransferActions,
};

use crate::{
    config::Config, finalizers::mexc::mexc_finalizer::MexcFinalizer, swappers::mexc::mexc_adapter::MexcClient,
};

const SMOKE_FROM_ASSET: &str = "CKBTC";
const SMOKE_TO_ASSET: &str = "USDC";

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
) -> Result<(), String> {
    let config = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;
    let backend = Arc::new(MexcClient::from_env()?);
    let finalizer = build_mexc_smoke_finalizer(backend.clone(), &config);
    let destination = resolve_withdraw_destination(withdraw_address, &config.evm_private_key)?;

    run_mexc_smoke_with_backend(
        backend,
        &finalizer,
        amount_ckbtc,
        execute,
        &destination,
        withdraw_network,
    )
    .await
}

pub(crate) async fn run_mexc_smoke_with_backend<B: CexBackend>(
    backend: Arc<B>,
    finalizer: &MexcFinalizer<B>,
    amount_ckbtc: f64,
    execute: bool,
    withdraw_destination: &str,
    withdraw_network: &str,
) -> Result<(), String> {
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
        return Ok(());
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

    Ok(())
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

        run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.01,
            false,
            "0x1111111111111111111111111111111111111111",
            "ETH",
        )
        .await
        .expect("dry-run should succeed");
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

        run_mexc_smoke_with_backend(
            backend,
            &finalizer,
            0.5,
            true,
            "0x2222222222222222222222222222222222222222",
            "ETH",
        )
        .await
        .expect("live smoke should succeed");
    }

    #[test]
    fn resolve_withdraw_destination_uses_evm_key_when_address_missing() {
        let evm_private_key = "0x59c6995e998f97a5a0044976f53f58816f2f94f4a7f87b5c6a1f7fbf3f5e6be7";
        let signer: PrivateKeySigner = evm_private_key.parse().expect("private key should parse");
        let got = resolve_withdraw_destination(None, evm_private_key).expect("should resolve from key");
        assert_eq!(got, signer.address().to_string());
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
