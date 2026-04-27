use std::str::FromStr;
use std::sync::Arc;

use alloy::network::AnyNetwork;
use alloy::primitives::Address as EvmAddress;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use num_traits::ToPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{message::Message, pubkey::Pubkey, signature::Keypair, signer::Signer};
use solana_system_interface::instruction as system_instruction;
use tokio::sync::mpsc;

use crate::config::ConfigTrait;
use crate::swappers::mexc::mexc_adapter::MexcClient;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;

use super::app::{App, BalancesPanel, WithdrawAccountKind, WithdrawDestinationKind, WithdrawField};
use super::events::UiEvent;
use super::format;

const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 200_000_000_000_000;
const EVM_NATIVE_TRANSFER_GAS_LIMIT: u128 = 21_000;
const EVM_ERC20_TRANSFER_GAS_LIMIT: u128 = 100_000;
const EVM_GAS_RESERVE_MULTIPLIER: u128 = 2;
// Reserve enough lamports to keep the source system account rent-exempt
// after transfer plus normal tx fee headroom.
const SOLANA_NATIVE_FEE_RESERVE_LAMPORTS: u128 = 2_000_000;
const SOLANA_SPL_FEE_RESERVE_LAMPORTS: u128 = 3_000_000;

fn evm_private_key_for_source<'a>(config: &'a crate::config::Config, source: WithdrawAccountKind) -> &'a str {
    match source {
        WithdrawAccountKind::Bridge => &config.bridge_evm_private_key,
        _ => &config.evm_private_key,
    }
}

async fn estimate_evm_gas_reserve_and_native_balance_wei(
    config: &crate::config::Config,
    source: WithdrawAccountKind,
    gas_limit: u128,
) -> Result<(u128, u128), String> {
    let source_label = match source {
        WithdrawAccountKind::Main => "main",
        WithdrawAccountKind::Trader => "trader",
        WithdrawAccountKind::Recovery => "recovery",
        WithdrawAccountKind::Bridge => "bridge",
    };
    let signer: PrivateKeySigner = evm_private_key_for_source(config, source)
        .parse()
        .map_err(|e| format!("failed to parse EVM private key for source '{source_label}': {e}"))?;
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

pub(super) fn build_withdrawable_assets(ctx: &crate::context::PipelineContext) -> Vec<AssetId> {
    let mut ids: Vec<AssetId> = ctx.registry.tokens.keys().cloned().collect();
    ids.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then(a.symbol.cmp(&b.symbol))
            .then(a.address.cmp(&b.address))
    });
    ids
}

pub(super) fn deposit_network_for_asset(asset: &AssetId) -> String {
    if asset.chain.eq_ignore_ascii_case("icp") {
        "ICP".to_string()
    } else {
        asset.chain.to_ascii_uppercase()
    }
}

fn parse_evm_destination_address(value: &str) -> Result<String, String> {
    let addr = EvmAddress::from_str(value).map_err(|_| "invalid destination EVM address".to_string())?;
    Ok(addr.to_string())
}

fn parse_solana_destination_address(value: &str) -> Result<String, String> {
    let addr = value
        .parse::<Pubkey>()
        .map_err(|_| "invalid destination Solana base58 pubkey".to_string())?;
    Ok(addr.to_string())
}

fn derive_solana_address(secret_key: [u8; 32]) -> String {
    Keypair::new_from_array(secret_key).pubkey().to_string()
}

fn solana_source_address_for_source(config: &crate::config::Config, source: WithdrawAccountKind) -> String {
    match source {
        WithdrawAccountKind::Bridge => derive_solana_address(config.bridge_solana_private_key_bytes),
        _ => derive_solana_address(config.solana_private_key_bytes),
    }
}

async fn estimate_solana_native_transfer_fee_lamports(
    rpc_url: &str,
    from: &str,
    to: &str,
) -> Result<u64, String> {
    let from_pubkey = from
        .parse::<Pubkey>()
        .map_err(|e| format!("invalid Solana source pubkey `{from}`: {e}"))?;
    let to_pubkey = to
        .parse::<Pubkey>()
        .map_err(|e| format!("invalid Solana destination pubkey `{to}`: {e}"))?;

    let rpc = RpcClient::new(rpc_url.to_string());
    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .map_err(|e| format!("solana get_latest_blockhash failed: {e}"))?;

    let ix = system_instruction::transfer(&from_pubkey, &to_pubkey, 1);
    let msg = Message::new_with_blockhash(&[ix], Some(&from_pubkey), &blockhash);
    let fee = rpc
        .get_fee_for_message(&msg)
        .await
        .map_err(|e| format!("solana get_fee_for_message failed: {e}"))?;

    if fee == 0 {
        return Err("solana estimated transfer fee is zero; refusing sweep".to_string());
    }

    Ok(fee)
}

fn find_solana_native_asset_id(ctx: &crate::context::PipelineContext) -> Option<AssetId> {
    ctx.registry.tokens.iter().find_map(|(id, token)| match token {
        ChainToken::SolanaNative { .. } => Some(id.clone()),
        _ => None,
    })
}

async fn solana_native_balance_lamports(
    ctx: &crate::context::PipelineContext,
    balances: &Arc<liquidium_pipeline_core::balance_service::BalanceService>,
) -> Result<u128, String> {
    let native_asset_id = find_solana_native_asset_id(ctx).ok_or_else(|| {
        "missing Solana native asset in registry; expected an entry like `sol:native:SOL`".to_string()
    })?;
    let balance = balances.get_balance(&native_asset_id).await?;
    balance
        .value
        .0
        .to_u128()
        .ok_or_else(|| "solana native balance too large for u128".to_string())
}

fn validate_manual_destination(token: &ChainToken, destination: &str) -> Result<(), String> {
    match token {
        ChainToken::Icp { .. } => {
            Account::from_str(destination).map_err(|_| "invalid destination ICP account".to_string())?;
            Ok(())
        }
        ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
            parse_evm_destination_address(destination).map(|_| ())
        }
        ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => {
            parse_solana_destination_address(destination).map(|_| ())
        }
    }
}

fn request_mexc_deposit_address(
    app: &mut App,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    asset: AssetId,
    network: String,
    force: bool,
    log: bool,
) {
    let same_asset = app.deposit.asset.as_ref() == Some(&asset) && app.deposit.network.as_deref() == Some(&network);
    if !force && same_asset && app.deposit.last.is_some() {
        return;
    }
    if app.deposit.in_flight && same_asset {
        return;
    }

    let replace = app.deposit.asset.as_ref() != Some(&asset) || app.deposit.network.as_deref() != Some(&network);
    if replace {
        app.deposit.last = None;
        app.deposit.at = None;
    }

    app.deposit.in_flight = true;
    app.deposit.asset = Some(asset.clone());
    app.deposit.network = Some(network.clone());

    if log {
        app.push_log(format!("deposit: fetching {} on {}", asset.symbol, network));
    }

    let ui_tx = ui_tx.clone();
    tokio::spawn(async move {
        let client = match MexcClient::from_env() {
            Ok(v) => v,
            Err(e) => {
                let _ = ui_tx.send(UiEvent::Deposit(Err(e)));
                return;
            }
        };

        let res = client.get_deposit_address(&asset.symbol, &network).await;
        let _ = ui_tx.send(UiEvent::Deposit(res));
    });
}

pub(super) fn ensure_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        return;
    };
    let network = deposit_network_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, network, false, false);
}

pub(super) fn refresh_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("deposit: no withdraw asset selected");
        return;
    };
    let network = deposit_network_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, network, true, true);
}

pub(super) fn open_withdraw_panel(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    if let Some(balances) = &app.balances
        && let Some(selected) = balances.rows.get(app.balances_selected)
        && let Some(idx) = app.withdraw_assets.iter().position(|a| a == &selected.asset)
    {
        app.withdraw.asset_idx = idx;
    }

    app.withdraw.field = WithdrawField::Source;
    app.withdraw.editing = None;
    app.withdraw.edit_backup = None;
    app.withdraw.source = WithdrawAccountKind::Recovery;
    app.withdraw.destination = WithdrawDestinationKind::Main;
    app.withdraw.amount = "all".to_string();

    app.balances_panel = BalancesPanel::Withdraw;
    app.push_log("withdraw: opened");
    ensure_withdraw_deposit_address(app, ui_tx);
}

pub(super) fn open_deposit_panel(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(balances) = &app.balances else {
        app.push_log("deposit: no balances yet (press 'b' first)");
        return;
    };
    let Some(selected) = balances.rows.get(app.balances_selected) else {
        app.push_log("deposit: invalid selection");
        return;
    };

    let asset = selected.asset.clone();
    let network = deposit_network_for_asset(&asset);

    app.balances_panel = BalancesPanel::Deposit;
    request_mexc_deposit_address(app, ui_tx, asset, network, true, true);
}

pub(super) fn submit_withdraw(
    app: &mut App,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    ctx: &Arc<crate::context::PipelineContext>,
) {
    if app.withdraw.in_flight {
        app.push_log("withdraw: already in flight");
        return;
    }
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("withdraw: no asset selected");
        return;
    };

    if matches!(app.withdraw.destination, WithdrawDestinationKind::Manual) {
        let manual_destination = app.withdraw.manual_destination.trim();
        if manual_destination.is_empty() {
            app.push_log("withdraw: manual destination is empty");
            return;
        }

        let Some(token) = ctx.registry.get(&asset) else {
            app.push_log(format!("withdraw: unknown asset {}", asset));
            return;
        };
        if let Err(err) = validate_manual_destination(&token, manual_destination) {
            app.push_log(format!("withdraw: {}", err));
            return;
        }
    }

    app.withdraw.in_flight = true;
    app.push_log("withdraw: sending…");

    let source = app.withdraw.source;
    let destination = app.withdraw.destination;
    let manual_destination = app.withdraw.manual_destination.clone();
    let amount = app.withdraw.amount.clone();

    let ui_tx = ui_tx.clone();
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let res = execute_withdraw(ctx, source, destination, manual_destination, asset, amount).await;
        let _ = ui_tx.send(UiEvent::Withdraw(res));
    });
}

async fn execute_withdraw(
    ctx: Arc<crate::context::PipelineContext>,
    source: WithdrawAccountKind,
    destination: WithdrawDestinationKind,
    manual_destination: String,
    asset: AssetId,
    amount: String,
) -> Result<String, String> {
    let token = ctx
        .registry
        .get(&asset)
        .ok_or_else(|| format!("unknown asset: {}", asset))?
        .clone();

    let (transfers, balances) = match source {
        WithdrawAccountKind::Main => (ctx.main_transfers.clone(), ctx.main_service.clone()),
        WithdrawAccountKind::Trader => (ctx.trader_transfers.clone(), ctx.trader_service.clone()),
        WithdrawAccountKind::Recovery => (ctx.recovery_transfers.clone(), ctx.recovery_service.clone()),
        WithdrawAccountKind::Bridge => (
            ctx.bridge_transfer_service_for_symbol(&asset.symbol),
            ctx.bridge_balance_service_for_symbol(&asset.symbol),
        ),
    };

    let destination_account = resolve_destination_account(&ctx, &token, destination, manual_destination.trim())?;
    let source_solana_address = solana_source_address_for_source(&ctx.config, source);

    let amount_native: Nat = match &token {
        ChainToken::Icp { decimals, fee, .. } => {
            if amount.trim().eq_ignore_ascii_case("all") {
                let bal = balances.get_balance(&asset).await?;
                compute_withdraw_amount_native(bal.value, fee.clone(), amount.trim(), *decimals)?
            } else {
                compute_withdraw_amount_native(Nat::from(0u8), fee.clone(), amount.trim(), *decimals)?
            }
        }
        ChainToken::EvmNative { decimals, .. } => {
            if amount.trim().eq_ignore_ascii_case("all") {
                let (gas_reserve_wei, native_balance_wei) =
                    estimate_evm_gas_reserve_and_native_balance_wei(&ctx.config, source, EVM_NATIVE_TRANSFER_GAS_LIMIT)
                        .await?;
                let send_native = native_balance_wei.saturating_sub(gas_reserve_wei);
                if send_native == 0 {
                    return Err(format!(
                        "native balance too low to cover EVM gas reserve (native_balance_wei={} gas_reserve_wei={})",
                        native_balance_wei, gas_reserve_wei
                    ));
                }
                Nat::from(send_native)
            } else {
                compute_evm_withdraw_amount_native(Nat::from(0u8), amount.trim(), *decimals, true)?
            }
        }
        ChainToken::EvmErc20 { decimals, .. } => {
            if amount.trim().eq_ignore_ascii_case("all") {
                let (gas_reserve_wei, native_balance_wei) =
                    estimate_evm_gas_reserve_and_native_balance_wei(&ctx.config, source, EVM_ERC20_TRANSFER_GAS_LIMIT)
                        .await?;

                if native_balance_wei <= gas_reserve_wei {
                    return Err(format!(
                        "native balance too low to cover EVM token transfer gas (native_balance_wei={} gas_reserve_wei={})",
                        native_balance_wei, gas_reserve_wei
                    ));
                }
                let bal = balances.get_balance(&asset).await?;
                compute_evm_withdraw_amount_native(bal.value, amount.trim(), *decimals, false)?
            } else {
                compute_evm_withdraw_amount_native(Nat::from(0u8), amount.trim(), *decimals, false)?
            }
        }
        ChainToken::SolanaNative { decimals, .. } => {
            if amount.trim().eq_ignore_ascii_case("all") {
                let bal = balances.get_balance(&asset).await?;
                let native_balance_lamports = bal
                    .value
                    .0
                    .to_u128()
                    .ok_or_else(|| "solana native balance too large for u128".to_string())?;
                let destination = match &destination_account {
                    ChainAccount::Solana(addr) => addr.as_str(),
                    _ => return Err("invalid Solana destination selected".to_string()),
                };
                let fee_lamports = estimate_solana_native_transfer_fee_lamports(
                    &ctx.config.solana_rpc_url,
                    &source_solana_address,
                    destination,
                )
                .await? as u128;
                let send_native = native_balance_lamports.saturating_sub(fee_lamports);
                if send_native == 0 {
                    return Err(format!(
                        "native SOL balance too low to sweep (native_balance_lamports={} estimated_fee_lamports={})",
                        native_balance_lamports, fee_lamports
                    ));
                }
                Nat::from(send_native)
            } else {
                let send_units = format::decimal_to_units(amount.trim(), *decimals).ok_or_else(|| {
                    format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')")
                })?;
                let send_nat = Nat::from(send_units);
                let bal = balances.get_balance(&asset).await?;
                let native_balance_lamports = bal
                    .value
                    .0
                    .to_u128()
                    .ok_or_else(|| "solana native balance too large for u128".to_string())?;
                if native_balance_lamports < send_units.saturating_add(SOLANA_NATIVE_FEE_RESERVE_LAMPORTS) {
                    return Err(format!(
                        "native SOL balance too low to keep fee reserve (native_balance_lamports={} required_remaining_lamports={})",
                        native_balance_lamports, SOLANA_NATIVE_FEE_RESERVE_LAMPORTS
                    ));
                }
                send_nat
            }
        }
        ChainToken::SolanaSpl { decimals, .. } => {
            let native_balance_lamports = solana_native_balance_lamports(&ctx, &balances).await?;
            if native_balance_lamports <= SOLANA_SPL_FEE_RESERVE_LAMPORTS {
                return Err(format!(
                    "native SOL balance too low to cover SPL transfer fees (native_balance_lamports={} required_reserve_lamports={})",
                    native_balance_lamports, SOLANA_SPL_FEE_RESERVE_LAMPORTS
                ));
            }
            if amount.trim().eq_ignore_ascii_case("all") {
                let bal = balances.get_balance(&asset).await?;
                bal.value
            } else {
                let units = format::decimal_to_units(amount.trim(), *decimals).ok_or_else(|| {
                    format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')")
                })?;
                Nat::from(units)
            }
        }
    };

    transfers
        .transfer_by_asset_id(&asset, destination_account, amount_native)
        .await
}

fn resolve_destination_account(
    ctx: &crate::context::PipelineContext,
    token: &ChainToken,
    destination: WithdrawDestinationKind,
    manual_destination: &str,
) -> Result<ChainAccount, String> {
    match token {
        ChainToken::Icp { .. } => {
            let dst_account: Account = match destination {
                WithdrawDestinationKind::Main => ctx.config.liquidator_principal.into(),
                WithdrawDestinationKind::Trader => ctx.config.trader_principal.into(),
                WithdrawDestinationKind::Recovery => ctx.config.get_recovery_account(),
                WithdrawDestinationKind::Bridge => ctx.config.bridge_ic_account(),
                WithdrawDestinationKind::Manual => {
                    Account::from_str(manual_destination).map_err(|_| "invalid destination ICP account".to_string())?
                }
            };
            Ok(ChainAccount::Icp(dst_account))
        }
        ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
            let dst_address = match destination {
                WithdrawDestinationKind::Main => ctx.evm_address.clone(),
                WithdrawDestinationKind::Bridge => ctx.config.bridge_evm_address.clone(),
                WithdrawDestinationKind::Manual => parse_evm_destination_address(manual_destination)?,
                WithdrawDestinationKind::Trader | WithdrawDestinationKind::Recovery => {
                    return Err(
                        "destination trader/recovery is ICP-only; use main/bridge/manual for EVM assets".to_string(),
                    );
                }
            };
            Ok(ChainAccount::Evm(dst_address))
        }
        ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => {
            let main_solana_address = derive_solana_address(ctx.config.solana_private_key_bytes);
            let bridge_solana_address = derive_solana_address(ctx.config.bridge_solana_private_key_bytes);
            let dst_address = match destination {
                WithdrawDestinationKind::Main => main_solana_address,
                WithdrawDestinationKind::Bridge => bridge_solana_address,
                WithdrawDestinationKind::Manual => parse_solana_destination_address(manual_destination)?,
                WithdrawDestinationKind::Trader | WithdrawDestinationKind::Recovery => {
                    return Err(
                        "destination trader/recovery is ICP-only; use main/bridge/manual for Solana assets".to_string(),
                    );
                }
            };
            Ok(ChainAccount::Solana(dst_address))
        }
    }
}

fn compute_withdraw_amount_native(balance: Nat, fee: Nat, amount: &str, decimals: u8) -> Result<Nat, String> {
    if amount.trim().eq_ignore_ascii_case("all") {
        let bal_u128 = balance
            .0
            .to_u128()
            .ok_or_else(|| "balance too large for u128".to_string())?;
        let fee_u128 = fee
            .0
            .to_u128()
            .ok_or_else(|| "fee too large to represent".to_string())?;
        if bal_u128 <= fee_u128 {
            return Err("balance too low to cover fee".to_string());
        }
        Ok(Nat::from(bal_u128 - fee_u128))
    } else {
        let units = format::decimal_to_units(amount.trim(), decimals)
            .ok_or_else(|| format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')"))?;
        Ok(Nat::from(units))
    }
}

fn compute_evm_withdraw_amount_native(
    balance: Nat,
    amount: &str,
    decimals: u8,
    reserve_native_gas: bool,
) -> Result<Nat, String> {
    if amount.trim().eq_ignore_ascii_case("all") {
        if reserve_native_gas {
            let bal_u128 = balance
                .0
                .to_u128()
                .ok_or_else(|| "balance too large for u128".to_string())?;
            let send_u128 = bal_u128.saturating_sub(EVM_NATIVE_GAS_BUFFER_WEI);
            if send_u128 == 0 {
                return Err("balance too low to cover EVM gas reserve".to_string());
            }
            Ok(Nat::from(send_u128))
        } else {
            Ok(balance)
        }
    } else {
        let units = format::decimal_to_units(amount.trim(), decimals)
            .ok_or_else(|| format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')"))?;
        Ok(Nat::from(units))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use candid::Nat;

    use super::{EVM_NATIVE_GAS_BUFFER_WEI, compute_evm_withdraw_amount_native, compute_withdraw_amount_native};

    #[test]
    fn all_subtracts_fee_correctly() {
        let got = compute_withdraw_amount_native(Nat::from(100u32), Nat::from(3u32), "all", 8).expect("ok");
        assert_eq!(got, Nat::from(97u32));
    }

    #[test]
    fn all_rejects_balance_lte_fee() {
        let err = compute_withdraw_amount_native(Nat::from(3u32), Nat::from(3u32), "all", 8).expect_err("should fail");
        assert_eq!(err, "balance too low to cover fee");
    }

    #[test]
    fn all_rejects_fee_overflow() {
        let fee = Nat::from_str("340282366920938463463374607431768211456").expect("nat");
        let err = compute_withdraw_amount_native(Nat::from(1u32), fee, "all", 8).expect_err("should fail");
        assert_eq!(err, "fee too large to represent");
    }

    #[test]
    fn all_rejects_balance_overflow() {
        let balance = Nat::from_str("340282366920938463463374607431768211456").expect("nat");
        let err = compute_withdraw_amount_native(balance, Nat::from(1u32), "all", 8).expect_err("should fail");
        assert_eq!(err, "balance too large for u128");
    }

    #[test]
    fn manual_amount_reuses_decimal_validation() {
        let err = compute_withdraw_amount_native(Nat::from(0u8), Nat::from(0u8), ".", 8).expect_err("should fail");
        assert_eq!(err, "invalid amount (expected decimal with <= 8 decimals, or 'all')");

        let got = compute_withdraw_amount_native(Nat::from(0u8), Nat::from(0u8), "1.23", 2).expect("ok");
        assert_eq!(got, Nat::from(123u32));
    }

    #[test]
    fn evm_native_all_reserves_gas_buffer() {
        let balance = Nat::from(EVM_NATIVE_GAS_BUFFER_WEI + 123u128);
        let got = compute_evm_withdraw_amount_native(balance, "all", 18, true).expect("ok");
        assert_eq!(got, Nat::from(123u128));
    }

    #[test]
    fn evm_native_all_rejects_when_only_gas_buffer_available() {
        let err = compute_evm_withdraw_amount_native(Nat::from(EVM_NATIVE_GAS_BUFFER_WEI), "all", 18, true)
            .expect_err("should fail");
        assert_eq!(err, "balance too low to cover EVM gas reserve");
    }
}
