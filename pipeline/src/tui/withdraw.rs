use std::sync::Arc;

use chrono::Local;
use tokio::sync::mpsc;

use crate::commands::withdrawal::{destination_account, parse_typed_destination, withdraw_amount};
use crate::finalizers::bridge_planner::cex_network_from_destination_kind;
use crate::finalizers::cex::profile::CexVenueProfile;
use crate::swappers::mexc::mexc_adapter::MexcClient;
use liquidium_pipeline_connectors::backend::bridge_backend::resolve_reverse_route_by_source;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, DepositAddress};
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;

use super::app::{App, BalancesPanel, WithdrawDestination, WithdrawField, WithdrawSource};
use super::events::UiEvent;

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

pub(super) fn mexc_deposit_lookup_for_asset(asset: &AssetId) -> (String, String) {
    let source_network = deposit_network_for_asset(asset);
    if !CexVenueProfile::mexc().takes_directly(&asset.symbol)
        && let Some(route) = resolve_reverse_route_by_source(&asset.symbol, &source_network)
    {
        return (
            route.target_asset.to_string(),
            cex_network_from_destination_kind(route.destination_kind),
        );
    }

    (asset.symbol.clone(), source_network)
}

fn request_mexc_deposit_address(
    app: &mut App,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    asset: AssetId,
    mexc_asset: String,
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

    app.deposit.request += 1;
    let request = app.deposit.request;
    app.deposit.in_flight = true;
    app.deposit.asset = Some(asset.clone());
    app.deposit.network = Some(network.clone());

    if log {
        if asset.symbol.eq_ignore_ascii_case(&mexc_asset) {
            app.push_log(format!("deposit: fetching {} on {}", asset.symbol, network));
        } else {
            app.push_log(format!(
                "deposit: fetching {} via {} on {}",
                asset.symbol, mexc_asset, network
            ));
        }
    }

    let ui_tx = ui_tx.clone();
    tokio::spawn(async move {
        let client = match MexcClient::from_env() {
            Ok(v) => v,
            Err(e) => {
                let _ = ui_tx.send(UiEvent::Deposit {
                    request,
                    result: Err(e),
                });
                return;
            }
        };

        let result = client.get_deposit_address(&mexc_asset, &network).await;
        let _ = ui_tx.send(UiEvent::Deposit { request, result });
    });
}

/// Shows a deposit-address reply only if it answers the newest lookup. An
/// older reply belongs to an asset that is no longer selected.
pub(super) fn apply_deposit_reply(app: &mut App, request: u64, result: Result<DepositAddress, String>) {
    if request != app.deposit.request {
        return;
    }
    app.deposit.in_flight = false;
    app.deposit.at = Some(Local::now());
    app.push_log(match &result {
        Ok(v) => format!("deposit: {} {} -> {}", v.asset, v.network, v.address),
        Err(e) => format!("deposit: failed ({})", e),
    });
    app.deposit.last = Some(result);
}

pub(super) fn ensure_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        return;
    };
    let (mexc_asset, network) = mexc_deposit_lookup_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, mexc_asset, network, false, false);
}

pub(super) fn refresh_withdraw_deposit_address(app: &mut App, ui_tx: &mpsc::UnboundedSender<UiEvent>) {
    let Some(asset) = app.withdraw_assets.get(app.withdraw.asset_idx).cloned() else {
        app.push_log("deposit: no withdraw asset selected");
        return;
    };
    let (mexc_asset, network) = mexc_deposit_lookup_for_asset(&asset);
    request_mexc_deposit_address(app, ui_tx, asset, mexc_asset, network, true, true);
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
    app.withdraw.source = WithdrawSource::Recovery;
    app.withdraw.destination = WithdrawDestination::Main;
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
    let (mexc_asset, network) = mexc_deposit_lookup_for_asset(&asset);

    app.balances_panel = BalancesPanel::Deposit;
    request_mexc_deposit_address(app, ui_tx, asset, mexc_asset, network, true, true);
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

    if matches!(app.withdraw.destination, WithdrawDestination::Manual) {
        let manual_destination = app.withdraw.manual_destination.trim();
        if manual_destination.is_empty() {
            app.push_log("withdraw: manual destination is empty");
            return;
        }

        let Some(token) = ctx.registry.get(&asset) else {
            app.push_log(format!("withdraw: unknown asset {}", asset));
            return;
        };
        if let Err(err) = parse_typed_destination(&token, manual_destination) {
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
    source: WithdrawSource,
    destination: WithdrawDestination,
    manual_destination: String,
    asset: AssetId,
    amount: String,
) -> Result<String, String> {
    let token = ctx
        .registry
        .get(&asset)
        .ok_or_else(|| format!("unknown asset: {}", asset))?;
    let to = destination_account(&ctx, &token, destination, &manual_destination)?;
    let amount = withdraw_amount(&ctx, source, &asset, &token, &amount, &to).await?;
    source
        .transfers(&ctx)
        .transfer_by_asset_id(&asset, to, amount.value)
        .await
}

#[cfg(test)]
mod tests {
    use liquidium_pipeline_core::tokens::asset_id::AssetId;

    use super::mexc_deposit_lookup_for_asset;

    #[test]
    fn mexc_deposit_lookup_maps_cketh_to_eth_network() {
        let asset = AssetId {
            chain: "icp".to_string(),
            address: "ss2fx-dyaaa-aaaar-qacoq-cai".to_string(),
            symbol: "ckETH".to_string(),
        };

        assert_eq!(mexc_deposit_lookup_for_asset(&asset), ("ETH".to_string(), "ETH".to_string()));
    }

    #[test]
    fn mexc_deposit_lookup_maps_ckusdc_to_usdc_eth_network() {
        let asset = AssetId {
            chain: "icp".to_string(),
            address: "xevnm-gaaaa-aaaar-qafnq-cai".to_string(),
            symbol: "ckUSDC".to_string(),
        };

        assert_eq!(mexc_deposit_lookup_for_asset(&asset), ("USDC".to_string(), "ETH".to_string()));
    }

    #[test]
    fn mexc_deposit_lookup_maps_cksol_to_sol_network() {
        let asset = AssetId {
            chain: "icp".to_string(),
            address: "cksol-ledger".to_string(),
            symbol: "ckSOL".to_string(),
        };

        assert_eq!(mexc_deposit_lookup_for_asset(&asset), ("SOL".to_string(), "SOL".to_string()));
    }

    #[test]
    fn mexc_deposit_lookup_keeps_direct_assets_unchanged() {
        let asset = AssetId {
            chain: "icp".to_string(),
            address: "ryjl3-tyaaa-aaaaa-aaaba-cai".to_string(),
            symbol: "ICP".to_string(),
        };

        assert_eq!(mexc_deposit_lookup_for_asset(&asset), ("ICP".to_string(), "ICP".to_string()));
    }

    /// MEXC lists ckUSDT on ICP, so a ckUSDT bridge route must not move this deposit to Ethereum.
    #[test]
    fn mexc_deposit_lookup_keeps_ckusdt_on_icp() {
        let asset = AssetId {
            chain: "icp".to_string(),
            address: "cngnf-vqaaa-aaaar-qag4q-cai".to_string(),
            symbol: "ckUSDT".to_string(),
        };

        assert_eq!(
            mexc_deposit_lookup_for_asset(&asset),
            ("ckUSDT".to_string(), "ICP".to_string())
        );
    }
}
