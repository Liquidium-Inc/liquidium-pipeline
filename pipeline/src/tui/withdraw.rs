use std::str::FromStr;
use std::sync::Arc;

use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use num_traits::ToPrimitive;
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

pub(super) fn build_withdrawable_assets(ctx: &crate::context::PipelineContext) -> Vec<AssetId> {
    let mut ids: Vec<AssetId> = ctx
        .registry
        .tokens
        .iter()
        .filter_map(|(id, tok)| match tok {
            ChainToken::Icp { .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    ids.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.address.cmp(&b.address)));
    ids
}

pub(super) fn deposit_network_for_asset(asset: &AssetId) -> String {
    if asset.chain.eq_ignore_ascii_case("icp") {
        "ICP".to_string()
    } else {
        asset.chain.to_ascii_uppercase()
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

pub(super) fn submit_withdraw(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>, ctx: &Arc<crate::context::PipelineContext>) {
    if app.withdraw.in_flight {
        app.push_log("withdraw: already in flight");
        return;
    }
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("withdraw: no asset selected");
        return;
    };

    if matches!(app.withdraw.destination, WithdrawDestinationKind::Manual)
        && app.withdraw.manual_destination.trim().is_empty()
    {
        app.push_log("withdraw: manual destination is empty");
        return;
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
        .ok_or_else(|| format!("unknown asset: {}", asset))?;

    let ChainToken::Icp { decimals, fee, .. } = token else {
        return Err("TUI withdraw currently supports ICP tokens only".to_string());
    };

    let (transfers, balances) = match source {
        WithdrawAccountKind::Main => (ctx.main_transfers.clone(), ctx.main_service.clone()),
        WithdrawAccountKind::Trader => (ctx.trader_transfers.clone(), ctx.trader_service.clone()),
        WithdrawAccountKind::Recovery => (ctx.recovery_transfers.clone(), ctx.recovery_service.clone()),
    };

    let dst_account: Account = match destination {
        WithdrawDestinationKind::Main => ctx.config.liquidator_principal.into(),
        WithdrawDestinationKind::Trader => ctx.config.trader_principal.into(),
        WithdrawDestinationKind::Recovery => ctx.config.get_recovery_account(),
        WithdrawDestinationKind::Manual => {
            Account::from_str(manual_destination.trim()).map_err(|_| "invalid destination ICP account".to_string())?
        }
    };

    let amount_native: Nat = if amount.trim().eq_ignore_ascii_case("all") {
        let bal = balances.get_balance(&asset).await?;
        let bal_u128 = bal
            .value
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
        Nat::from(bal_u128 - fee_u128)
    } else {
        let units = format::decimal_to_units(amount.trim(), decimals)
            .ok_or_else(|| format!("invalid amount (expected decimal with <= {decimals} decimals, or 'all')"))?;
        Nat::from(units)
    };

    transfers
        .transfer_by_asset_id(&asset, ChainAccount::Icp(dst_account), amount_native)
        .await
}
