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
use candid::{CandidType, Encode, Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use icrc_ledger_types::icrc1::transfer::{TransferArg, TransferError};
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeBackend, BridgeDestination, BridgeRequest, BridgeStatus, CkErc20BridgeBackend, resolve_route,
};
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, SwapExecutionOptions};
use liquidium_pipeline_connectors::backend::{evm_backend::EvmBackendImpl, icp_backend::IcpBackendImpl};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::TransferActions,
};
use serde::Deserialize;
use tokio::time::sleep;

use crate::{
    config::Config,
    context::{PipelineContext, init_context},
    finalizers::mexc::mexc_finalizer::MexcFinalizer,
    swappers::mexc::mexc_adapter::MexcClient,
};

const SMOKE_CKBTC_TO_USDC_FROM_ASSET: &str = "CKBTC";
const SMOKE_CKBTC_TO_USDC_TO_ASSET: &str = "USDC";
const SMOKE_USDC_TO_CKBTC_FROM_ASSET: &str = "USDC";
const SMOKE_USDC_TO_CKBTC_TO_ASSET: &str = "CKBTC";
const FORWARD_BRIDGE_SOURCE_CHAIN: &str = "ETH";
const FORWARD_BRIDGE_TARGET_ASSET: &str = "ckUSDC";
const REVERSE_BRIDGE_SOURCE_ASSET: &str = "ckUSDC";
const REVERSE_BRIDGE_SOURCE_CHAIN: &str = "ICP";
const REVERSE_BRIDGE_TARGET_ASSET: &str = "USDC";
const MEXC_ETH_NETWORK: &str = "ETH";
const MEXC_BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MEXC_BALANCE_TIMEOUT: Duration = Duration::from_secs(45 * 60);
const BRIDGE_SOURCE_FUNDING_POLL_INTERVAL: Duration = Duration::from_secs(10);
const BRIDGE_SOURCE_FUNDING_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const BRIDGE_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(5);
const BRIDGE_STATUS_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(CandidType, Deserialize, Clone, Debug)]
struct Eip1559TransactionPriceArg {
    ckerc20_ledger_id: Principal,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct Eip1559TransactionPrice {
    max_priority_fee_per_gas: Nat,
    max_fee_per_gas: Nat,
    max_transaction_fee: Nat,
    timestamp: Option<u64>,
    gas_limit: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkEthMinterInfo {
    #[serde(default)]
    cketh_ledger_id: Option<Principal>,
}

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

fn with_route_resolution_hint(err: String, pair_hint: &str) -> String {
    if err.contains("no configured hop route")
        || err.contains("no configured MEXC pairs for hop discovery")
        || err.contains("hop discovery disabled")
    {
        return format!("{err}; hint: set CEX_MEXC_AVAILABLE_PAIRS={pair_hint} and CEX_MEXC_MAX_HOPS>=1");
    }
    err
}

fn resolve_reverse_ckerc20_ledger_id() -> Result<Principal, String> {
    let route = resolve_route(
        REVERSE_BRIDGE_SOURCE_ASSET,
        REVERSE_BRIDGE_SOURCE_CHAIN,
        REVERSE_BRIDGE_TARGET_ASSET,
    )
    .ok_or_else(|| {
        format!(
            "missing bridge route {}@{} -> {}",
            REVERSE_BRIDGE_SOURCE_ASSET, REVERSE_BRIDGE_SOURCE_CHAIN, REVERSE_BRIDGE_TARGET_ASSET
        )
    })?;

    let ledger = route.ckerc20_ledger_id.ok_or_else(|| {
        format!(
            "route {}@{} -> {} has no ckERC20 ledger id",
            route.source_asset, route.source_chain, route.target_asset
        )
    })?;

    Principal::from_text(ledger).map_err(|e| {
        format!(
            "invalid ckERC20 ledger id '{}' for route {}@{} -> {}: {}",
            ledger, route.source_asset, route.source_chain, route.target_asset, e
        )
    })
}

async fn fetch_reverse_bridge_required_cketh_fee(bridge_agent: &Agent, minter: Principal) -> Result<Nat, String> {
    let ckerc20_ledger_id = resolve_reverse_ckerc20_ledger_id()?;
    let args = Encode!(&Some(Eip1559TransactionPriceArg { ckerc20_ledger_id }))
        .map_err(|e| format!("encode eip_1559_transaction_price args failed: {e}"))?;
    let quote = bridge_agent
        .call_query::<Eip1559TransactionPrice>(&minter, "eip_1559_transaction_price", args)
        .await
        .map_err(|e| format!("eip_1559_transaction_price failed for minter {}: {}", minter, e))?;
    Ok(quote.max_transaction_fee)
}

async fn fetch_reverse_bridge_cketh_ledger_id(bridge_agent: &Agent, minter: Principal) -> Result<Principal, String> {
    let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
    let info = match bridge_agent
        .call_query::<CkEthMinterInfo>(&minter, "get_minter_info", args.clone())
        .await
    {
        Ok(v) => v,
        Err(query_err) => bridge_agent
            .call_update::<CkEthMinterInfo>(&minter, "get_minter_info", args)
            .await
            .map_err(|update_err| {
                format!(
                    "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                    minter
                )
            })?,
    };
    info.cketh_ledger_id
        .ok_or_else(|| format!("minter {} returned no cketh_ledger_id in get_minter_info", minter))
}

async fn icrc1_balance_of(agent: &Agent, ledger: Principal, account: &Account) -> Result<Nat, String> {
    let args = Encode!(account).map_err(|e| format!("encode icrc1_balance_of args failed: {e}"))?;
    agent
        .call_query::<Nat>(&ledger, "icrc1_balance_of", args)
        .await
        .map_err(|e| format!("icrc1_balance_of failed on ledger {}: {}", ledger, e))
}

async fn icrc1_decimals(agent: &Agent, ledger: Principal) -> Result<u8, String> {
    let args = Encode!(&()).map_err(|e| format!("encode icrc1_decimals args failed: {e}"))?;
    agent
        .call_query::<u8>(&ledger, "icrc1_decimals", args)
        .await
        .map_err(|e| format!("icrc1_decimals failed on ledger {}: {}", ledger, e))
}

async fn icrc1_transfer(
    agent: &Agent,
    ledger: Principal,
    from: &Account,
    to: &Account,
    amount: Nat,
) -> Result<Nat, String> {
    let transfer_arg = TransferArg {
        from_subaccount: from.subaccount,
        to: *to,
        amount,
        fee: None,
        memo: None,
        created_at_time: None,
    };
    let args = Encode!(&transfer_arg).map_err(|e| format!("encode icrc1_transfer args failed: {e}"))?;
    let result = agent
        .call_update::<Result<Nat, TransferError>>(&ledger, "icrc1_transfer", args)
        .await
        .map_err(|e| format!("icrc1_transfer call failed on ledger {}: {}", ledger, e))?;
    result.map_err(|e| format!("icrc1_transfer error on ledger {}: {}", ledger, e))
}

fn resolve_icp_asset_by_symbol(ctx: &PipelineContext, symbol: &str) -> Result<(AssetId, ChainToken), String> {
    let candidates: Vec<(AssetId, ChainToken)> = ctx
        .main_transfers
        .registry()
        .all()
        .into_iter()
        .filter(|(_, token)| matches!(token, ChainToken::Icp { .. }) && token.symbol().eq_ignore_ascii_case(symbol))
        .collect();

    match candidates.as_slice() {
        [] => Err(format!("could not find ICP asset '{}' in token registry", symbol)),
        [single] => Ok(single.clone()),
        _ => Err(format!(
            "ambiguous ICP asset symbol '{}': {} matches in registry",
            symbol,
            candidates.len()
        )),
    }
}

async fn ensure_bridge_asset_funded_from_liquidator(
    ctx: &PipelineContext,
    asset_id: &AssetId,
    token: &ChainToken,
    required_bridge_balance: Nat,
    bridge_source_account: &Account,
    purpose: &str,
) -> Result<(), String> {
    if required_bridge_balance <= Nat::from(0u8) {
        return Err(format!("invalid required bridge balance for {}: must be > 0", purpose));
    }

    let bridge_balance = ctx
        .bridge_service
        .get_balance(asset_id)
        .await
        .map_err(|e| format!("failed reading bridge {} balance: {}", asset_id.symbol, e))?
        .value;

    let required_fmt = ChainTokenAmount::from_raw(token.clone(), required_bridge_balance.clone()).formatted();
    let available_fmt = ChainTokenAmount::from_raw(token.clone(), bridge_balance.clone()).formatted();

    if bridge_balance >= required_bridge_balance {
        println!(
            "Bridge funding check: {} already satisfied (required={}, available={})",
            purpose, required_fmt, available_fmt
        );
        return Ok(());
    }

    let deficit = required_bridge_balance - bridge_balance;
    let deficit_fmt = ChainTokenAmount::from_raw(token.clone(), deficit.clone()).formatted();
    let liquidator_balance = ctx
        .main_service
        .get_balance(asset_id)
        .await
        .map_err(|e| format!("failed reading liquidator {} balance: {}", asset_id.symbol, e))?
        .value;
    let liquidator_fmt = ChainTokenAmount::from_raw(token.clone(), liquidator_balance.clone()).formatted();

    if liquidator_balance < deficit {
        return Err(format!(
            "insufficient liquidator {} for bridge {} top-up (need={}, available={})",
            asset_id.symbol, purpose, deficit_fmt, liquidator_fmt
        ));
    }

    println!("Auto-funding bridge source for {}", purpose);
    println!("  asset      : {}", asset_id.symbol);
    println!("  required   : {}", required_fmt);
    println!("  available  : {}", available_fmt);
    println!("  top_up     : {}", deficit_fmt);

    let txid = ctx
        .main_transfers
        .transfer_by_asset_id(asset_id, ChainAccount::Icp(bridge_source_account.clone()), deficit)
        .await
        .map_err(|e| {
            format!(
                "auto-funding transfer failed for {} ({}): {}",
                asset_id.symbol, purpose, e
            )
        })?;

    println!("Bridge source top-up submitted");
    println!("  asset      : {}", asset_id.symbol);
    println!("  txid       : {}", txid);
    Ok(())
}

async fn auto_fund_bridge_source_from_liquidator(
    amount_ckusdc: f64,
    required_cketh_fee: Nat,
    cketh_ledger_id: Principal,
    bridge_source_account: &Account,
) -> Result<(), String> {
    let ctx = init_context()
        .await
        .map_err(|e| format!("bridge auto-funding failed to initialize context: {e}"))?;

    let (ckusdc_id, ckusdc_token) = resolve_icp_asset_by_symbol(&ctx, REVERSE_BRIDGE_SOURCE_ASSET)?;
    let required_ckusdc = ChainTokenAmount::from_formatted(ckusdc_token.clone(), amount_ckusdc).value;
    ensure_bridge_asset_funded_from_liquidator(
        &ctx,
        &ckusdc_id,
        &ckusdc_token,
        required_ckusdc,
        bridge_source_account,
        "reverse bridge amount",
    )
    .await?;

    let fee_with_buffer = required_cketh_fee.clone() + (required_cketh_fee / Nat::from(5u8));
    match resolve_icp_asset_by_symbol(&ctx, "ckETH") {
        Ok((cketh_id, cketh_token)) => {
            ensure_bridge_asset_funded_from_liquidator(
                &ctx,
                &cketh_id,
                &cketh_token,
                fee_with_buffer,
                bridge_source_account,
                "reverse bridge fee budget",
            )
            .await?;
        }
        Err(_) => {
            let main_account = Account {
                owner: ctx.config.liquidator_principal,
                subaccount: None,
            };
            let bridge_balance = icrc1_balance_of(ctx.agent.as_ref(), cketh_ledger_id, bridge_source_account).await?;
            let decimals = icrc1_decimals(ctx.agent.as_ref(), cketh_ledger_id).await.unwrap_or(18);
            let fmt_token = ChainToken::Icp {
                ledger: cketh_ledger_id,
                symbol: "ckETH".to_string(),
                decimals,
                fee: Nat::from(0u8),
            };
            let required_fmt = ChainTokenAmount::from_raw(fmt_token.clone(), fee_with_buffer.clone()).formatted();
            let available_fmt = ChainTokenAmount::from_raw(fmt_token.clone(), bridge_balance.clone()).formatted();

            if bridge_balance < fee_with_buffer {
                let deficit = fee_with_buffer - bridge_balance;
                let deficit_fmt = ChainTokenAmount::from_raw(fmt_token.clone(), deficit.clone()).formatted();
                let liquidator_balance = icrc1_balance_of(ctx.agent.as_ref(), cketh_ledger_id, &main_account).await?;
                let liquidator_fmt =
                    ChainTokenAmount::from_raw(fmt_token.clone(), liquidator_balance.clone()).formatted();

                if liquidator_balance < deficit {
                    return Err(format!(
                        "insufficient liquidator ckETH for bridge reverse bridge fee budget top-up (need={}, available={})",
                        deficit_fmt, liquidator_fmt
                    ));
                }

                println!("Auto-funding bridge source for reverse bridge fee budget");
                println!("  asset      : ckETH");
                println!("  required   : {}", required_fmt);
                println!("  available  : {}", available_fmt);
                println!("  top_up     : {}", deficit_fmt);

                let txid = icrc1_transfer(
                    ctx.agent.as_ref(),
                    cketh_ledger_id,
                    &main_account,
                    bridge_source_account,
                    deficit,
                )
                .await?;

                println!("Bridge source top-up submitted");
                println!("  asset      : ckETH");
                println!("  txid       : {}", txid);
            } else {
                println!(
                    "Bridge funding check: reverse bridge fee budget already satisfied (required={}, available={})",
                    required_fmt, available_fmt
                );
            }
        }
    }

    Ok(())
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
        SMOKE_CKBTC_TO_USDC_FROM_ASSET,
        SMOKE_CKBTC_TO_USDC_TO_ASSET,
        amount_ckbtc,
        execute,
        &destination,
        withdraw_network,
        "CKBTC_BTC,BTC_USDC",
    )
    .await?;

    if bridge_after_withdraw {
        let withdrawn_amount = maybe_withdrawn_amount.ok_or_else(|| "bridge step requires --execute".to_string())?;
        let destination_account = resolve_bridge_destination_account(bridge_destination, config.liquidator_principal)?;
        run_bridge_step_after_mexc_withdraw(&config, withdrawn_amount, destination_account).await?;
    }

    Ok(())
}

fn resolve_ckbtc_withdraw_destination(
    destination: Option<&str>,
    default_liquidator_principal: Principal,
) -> Result<String, String> {
    let Some(destination) = destination else {
        return Ok(default_liquidator_principal.to_text());
    };

    let normalized = destination.trim();
    if normalized.is_empty() {
        return Err("withdraw destination cannot be empty".to_string());
    }

    Ok(normalized.to_string())
}

pub async fn mexc_smoke_bridge_swap_withdraw(
    amount_ckusdc: f64,
    execute: bool,
    withdraw_address: Option<&str>,
    withdraw_network: &str,
) -> Result<(), String> {
    if !amount_ckusdc.is_finite() || amount_ckusdc <= 0.0 {
        return Err("preflight failed: amount_ckusdc must be positive".to_string());
    }
    if withdraw_network.trim().is_empty() {
        return Err("preflight failed: withdraw_network cannot be empty".to_string());
    }

    let config = Config::load().await.map_err(|e| format!("config load failed: {e}"))?;
    let backend = Arc::new(MexcClient::from_env()?);
    let finalizer = build_mexc_smoke_finalizer(backend.clone(), &config);
    let withdraw_destination = resolve_ckbtc_withdraw_destination(withdraw_address, config.liquidator_principal)?;
    let deposit_address = backend
        .get_deposit_address(REVERSE_BRIDGE_TARGET_ASSET, MEXC_ETH_NETWORK)
        .await
        .map_err(|e| {
            format!(
                "preflight failed: could not fetch MEXC deposit address for {} on {}: {}",
                REVERSE_BRIDGE_TARGET_ASSET, MEXC_ETH_NETWORK, e
            )
        })?;

    println!("MEXC bridge smoke preflight");
    println!(
        "  bridge_route     : {}@{} -> {}@{}",
        REVERSE_BRIDGE_SOURCE_ASSET, REVERSE_BRIDGE_SOURCE_CHAIN, REVERSE_BRIDGE_TARGET_ASSET, MEXC_ETH_NETWORK
    );
    println!("  amount_ckusdc    : {}", amount_ckusdc);
    println!("  mexc_deposit     : {}", deposit_address.address);
    if let Some(tag) = deposit_address.tag.as_ref()
        && !tag.is_empty()
    {
        println!("  mexc_deposit_tag : {}", tag);
    }
    println!("  mode             : {}", if execute { "live" } else { "dry-run" });
    println!(
        "  swap             : {} -> {}",
        SMOKE_USDC_TO_CKBTC_FROM_ASSET, SMOKE_USDC_TO_CKBTC_TO_ASSET
    );
    println!("  withdraw_asset   : {}", SMOKE_USDC_TO_CKBTC_TO_ASSET);
    println!("  withdraw_network : {}", withdraw_network);
    println!("  withdraw_dest    : {}", withdraw_destination);

    if !execute {
        println!("Dry-run complete: no side effects executed.");
        return Ok(());
    }

    let usdc_balance_before = backend.get_balance(SMOKE_USDC_TO_CKBTC_FROM_ASSET).await.map_err(|e| {
        format!(
            "failed to fetch baseline {} balance on MEXC: {}",
            SMOKE_USDC_TO_CKBTC_FROM_ASSET, e
        )
    })?;
    println!("MEXC baseline");
    println!(
        "  {} balance before bridge: {}",
        SMOKE_USDC_TO_CKBTC_FROM_ASSET, usdc_balance_before
    );

    run_bridge_step_before_mexc_swap(&config, amount_ckusdc, &deposit_address.address).await?;

    println!(
        "Waiting for MEXC deposit credit (timeout={}s, poll={}s)...",
        MEXC_BALANCE_TIMEOUT.as_secs(),
        MEXC_BALANCE_POLL_INTERVAL.as_secs()
    );
    let credited_usdc = wait_for_cex_balance_delta(
        backend.as_ref(),
        SMOKE_USDC_TO_CKBTC_FROM_ASSET,
        usdc_balance_before,
        amount_ckusdc,
    )
    .await?;
    println!("MEXC deposit credited");
    println!("  credited_usdc    : {}", credited_usdc);

    let withdrawn_ckbtc = run_mexc_smoke_with_backend(
        backend,
        &finalizer,
        SMOKE_USDC_TO_CKBTC_FROM_ASSET,
        SMOKE_USDC_TO_CKBTC_TO_ASSET,
        credited_usdc,
        true,
        &withdraw_destination,
        withdraw_network,
        "USDC_BTC,BTC_CKBTC",
    )
    .await?
    .ok_or_else(|| "live swap+withdraw did not return final withdrawn amount".to_string())?;

    println!("MEXC bridge smoke complete");
    println!("  withdrawn_ckbtc  : {}", withdrawn_ckbtc);
    println!("  destination      : {}", withdraw_destination);

    Ok(())
}

pub(crate) async fn run_mexc_smoke_with_backend<B: CexBackend>(
    backend: Arc<B>,
    finalizer: &MexcFinalizer<B>,
    from_asset: &str,
    to_asset: &str,
    amount_from: f64,
    execute: bool,
    withdraw_destination: &str,
    withdraw_network: &str,
    route_pair_hint: &str,
) -> Result<Option<f64>, String> {
    if !amount_from.is_finite() || amount_from <= 0.0 {
        return Err("preflight failed: amount must be positive".to_string());
    }
    if withdraw_network.trim().is_empty() {
        return Err("preflight failed: withdraw_network cannot be empty".to_string());
    }
    if withdraw_destination.trim().is_empty() {
        return Err("preflight failed: withdraw destination cannot be empty".to_string());
    }

    let available_from = backend
        .get_balance(from_asset)
        .await
        .map_err(|e| format!("preflight failed: could not fetch {from_asset} balance: {e}"))?;
    if available_from + 1e-12 < amount_from {
        return Err(format!(
            "preflight failed: insufficient {} balance on MEXC (available={} required={})",
            from_asset, available_from, amount_from
        ));
    }

    let route_legs = finalizer
        .resolve_trade_legs_for_symbols(from_asset, to_asset)
        .await
        .map_err(|e| format!("preflight failed: {}", with_route_resolution_hint(e, route_pair_hint)))?;

    println!("MEXC smoke preflight");
    println!("  pair            : {} -> {}", from_asset, to_asset);
    println!("  amount_in       : {}", amount_from);
    println!("  available_in    : {}", available_from);
    println!("  mode            : {}", if execute { "live" } else { "dry-run" });
    println!("  withdraw_asset  : {}", to_asset);
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

    let mut current_in = amount_from;
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
        .withdraw(to_asset, withdraw_network, withdraw_destination, current_in)
        .await
        .map_err(|e| {
            format!(
                "live execution failed during withdraw (asset={} network={} destination={} amount={}): {}",
                to_asset, withdraw_network, withdraw_destination, current_in, e
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
    let icp_backend = Arc::new(IcpBackendImpl::new(bridge_agent.clone()));
    let evm_backend = Arc::new(EvmBackendImpl::new(bridge_provider));

    let bridge_backend = CkErc20BridgeBackend::new(
        bridge_agent,
        icp_backend,
        evm_backend,
        config.bridge_cketh_minter_canister,
        config.bridge_ic_owner_principal,
    );

    println!(
        "Waiting for bridge source funds (timeout={}s, poll={}s)...",
        BRIDGE_SOURCE_FUNDING_TIMEOUT.as_secs(),
        BRIDGE_SOURCE_FUNDING_POLL_INTERVAL.as_secs()
    );
    let bridge_balance = wait_for_bridge_source_funding(
        &bridge_backend,
        SMOKE_CKBTC_TO_USDC_TO_ASSET,
        FORWARD_BRIDGE_SOURCE_CHAIN,
        &config.bridge_evm_address,
        amount,
    )
    .await?;
    let bridge_submit_amount = bridge_balance;
    println!("Bridge source funded");
    println!("  available  : {}", bridge_balance);
    println!("  min_needed : {}", amount);
    println!("  submit_all : {}", bridge_submit_amount);
    println!("  source     : {}", config.bridge_evm_address);

    let request = BridgeRequest {
        asset: SMOKE_CKBTC_TO_USDC_TO_ASSET.to_string(),
        source_chain: FORWARD_BRIDGE_SOURCE_CHAIN.to_string(),
        source_address: config.bridge_evm_address.clone(),
        target_asset: FORWARD_BRIDGE_TARGET_ASSET.to_string(),
        destination: BridgeDestination::IcpAccount(destination_account.clone()),
        amount: bridge_submit_amount,
    };

    println!("Starting bridge");
    println!(
        "  route      : {}@{} -> {}",
        SMOKE_CKBTC_TO_USDC_TO_ASSET, FORWARD_BRIDGE_SOURCE_CHAIN, FORWARD_BRIDGE_TARGET_ASSET
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
        SMOKE_CKBTC_TO_USDC_TO_ASSET, FORWARD_BRIDGE_SOURCE_CHAIN, FORWARD_BRIDGE_TARGET_ASSET
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

async fn run_bridge_step_before_mexc_swap(
    config: &Config,
    amount: f64,
    destination_evm_address: &str,
) -> Result<(), String> {
    if !amount.is_finite() || amount <= 0.0 {
        return Err(format!("bridge step amount must be positive; got {amount}"));
    }

    let destination = destination_evm_address.parse::<EvmAddress>().map_err(|e| {
        format!(
            "invalid MEXC EVM deposit destination '{}': {}",
            destination_evm_address, e
        )
    })?;

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
    let icp_backend = Arc::new(IcpBackendImpl::new(bridge_agent.clone()));
    let evm_backend = Arc::new(EvmBackendImpl::new(bridge_provider));

    let required_cketh_fee =
        fetch_reverse_bridge_required_cketh_fee(bridge_agent.as_ref(), config.bridge_cketh_minter_canister).await?;
    let cketh_ledger_id =
        fetch_reverse_bridge_cketh_ledger_id(bridge_agent.as_ref(), config.bridge_cketh_minter_canister).await?;

    let bridge_source_account = config.bridge_ic_account();
    auto_fund_bridge_source_from_liquidator(amount, required_cketh_fee, cketh_ledger_id, &bridge_source_account)
        .await?;

    let bridge_backend = CkErc20BridgeBackend::new(
        bridge_agent,
        icp_backend,
        evm_backend,
        config.bridge_cketh_minter_canister,
        config.bridge_ic_owner_principal,
    );

    let bridge_source = bridge_source_account.to_string();
    println!(
        "Waiting for bridge source funds (timeout={}s, poll={}s)...",
        BRIDGE_SOURCE_FUNDING_TIMEOUT.as_secs(),
        BRIDGE_SOURCE_FUNDING_POLL_INTERVAL.as_secs()
    );
    let bridge_balance = wait_for_bridge_source_funding(
        &bridge_backend,
        REVERSE_BRIDGE_SOURCE_ASSET,
        REVERSE_BRIDGE_SOURCE_CHAIN,
        &bridge_source,
        amount,
    )
    .await?;
    println!("Bridge source funded");
    println!("  available  : {}", bridge_balance);
    println!("  min_needed : {}", amount);
    println!("  source     : {}", bridge_source);

    let request = BridgeRequest {
        asset: REVERSE_BRIDGE_SOURCE_ASSET.to_string(),
        source_chain: REVERSE_BRIDGE_SOURCE_CHAIN.to_string(),
        source_address: bridge_source.clone(),
        target_asset: REVERSE_BRIDGE_TARGET_ASSET.to_string(),
        destination: BridgeDestination::EvmAddress(destination),
        amount,
    };

    println!("Starting bridge");
    println!(
        "  route      : {}@{} -> {}",
        REVERSE_BRIDGE_SOURCE_ASSET, REVERSE_BRIDGE_SOURCE_CHAIN, REVERSE_BRIDGE_TARGET_ASSET
    );
    println!("  source     : {}", bridge_source);
    println!("  amount     : {}", amount);
    println!("  destination: {}", destination_evm_address);

    let submission = bridge_backend
        .submit_bridge(request)
        .await
        .map_err(|err| format!("bridge step failed: {err}"))?;

    println!("Bridge submitted");
    println!(
        "  route      : {}@{} -> {}",
        REVERSE_BRIDGE_SOURCE_ASSET, REVERSE_BRIDGE_SOURCE_CHAIN, REVERSE_BRIDGE_TARGET_ASSET
    );
    println!("  source     : {}", bridge_source);
    println!("  amount     : {}", amount);
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
    source_asset: &str,
    source_chain: &str,
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
            .get_source_balance(source_asset, source_chain, source_address)
            .await
            .map_err(|err| format!("bridge source balance check failed: {err}"))?;

        if balance.is_finite() && balance + 1e-12 >= minimum_required_amount {
            return Ok(balance);
        }

        poll_count += 1;
        println!(
            "  bridge funding: asset={}@{} available={} min_needed={} (poll={} elapsed={}s)",
            source_asset,
            source_chain,
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

async fn wait_for_cex_balance_delta<B>(
    backend: &B,
    asset: &str,
    baseline_balance: f64,
    minimum_delta: f64,
) -> Result<f64, String>
where
    B: CexBackend + Send + Sync + ?Sized,
{
    let started = Instant::now();
    let mut poll_count = 0u64;

    loop {
        let current_balance = backend
            .get_balance(asset)
            .await
            .map_err(|err| format!("MEXC balance check failed for {asset}: {err}"))?;
        let credited = (current_balance - baseline_balance).max(0.0);

        if credited + 1e-12 >= minimum_delta {
            return Ok(credited);
        }

        poll_count += 1;
        println!(
            "  mexc funding: asset={} baseline={} current={} credited={} min_needed={} (poll={} elapsed={}s)",
            asset,
            baseline_balance,
            current_balance,
            credited,
            minimum_delta,
            poll_count,
            started.elapsed().as_secs()
        );

        if started.elapsed() >= MEXC_BALANCE_TIMEOUT {
            return Err(format!(
                "timed out waiting for MEXC balance credit after {}s (asset={} baseline={} credited={} min_needed={})",
                MEXC_BALANCE_TIMEOUT.as_secs(),
                asset,
                baseline_balance,
                credited,
                minimum_delta
            ));
        }

        sleep(MEXC_BALANCE_POLL_INTERVAL).await;
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
            "CKBTC",
            "USDC",
            0.01,
            false,
            "0x1111111111111111111111111111111111111111",
            "ETH",
            "CKBTC_BTC,BTC_USDC",
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
            "CKBTC",
            "USDC",
            0.5,
            true,
            "0x2222222222222222222222222222222222222222",
            "ETH",
            "CKBTC_BTC,BTC_USDC",
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

    #[test]
    fn resolve_ckbtc_withdraw_destination_defaults_to_liquidator_principal() {
        let principal = Principal::management_canister();
        let destination = resolve_ckbtc_withdraw_destination(None, principal).expect("default destination");
        assert_eq!(destination, principal.to_text());
    }

    #[test]
    fn resolve_ckbtc_withdraw_destination_rejects_empty_override() {
        let err = resolve_ckbtc_withdraw_destination(Some("  "), Principal::management_canister())
            .expect_err("empty destination must fail");
        assert!(err.contains("cannot be empty"));
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
            "CKBTC",
            "USDC",
            0.01,
            false,
            "0x3333333333333333333333333333333333333333",
            "ETH",
            "CKBTC_BTC,BTC_USDC",
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
            "CKBTC",
            "USDC",
            0.01,
            false,
            "0x4444444444444444444444444444444444444444",
            "ETH",
            "CKBTC_BTC,BTC_USDC",
        )
        .await
        .expect_err("route resolution should fail");
        assert!(err.contains("CEX_MEXC_AVAILABLE_PAIRS=CKBTC_BTC,BTC_USDC"));
        assert!(err.contains("CEX_MEXC_MAX_HOPS>=1"));
    }
}
