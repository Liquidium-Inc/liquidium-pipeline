use std::sync::Arc;
use std::time::{Duration, Instant};

use candid::{Nat, Principal};
use liquidium_pipeline_core::{
    tokens::{asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait},
    types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    },
};

use crate::{
    commands::mexc_runtime::build_mexc_finalizer,
    context::init_context,
    executors::executor::ExecutorRequest,
    finalizers::cex_finalizer::{CexFinalizerLogic, CexStep},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::SwapRequest,
    utils::now_ts,
};

const SMOKE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const SMOKE_EXECUTION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

fn find_asset_token(registry: &dyn TokenRegistryTrait, symbol: &str, chain: &str) -> Result<ChainToken, String> {
    registry
        .all()
        .into_iter()
        .find(|(asset_id, _)| {
            asset_id.symbol.eq_ignore_ascii_case(symbol) && asset_id.chain.eq_ignore_ascii_case(chain)
        })
        .map(|(_, token)| token)
        .ok_or_else(|| format!("asset not found in registry: {symbol}@{chain}"))
}

fn make_smoke_receipt(
    liq_id: u128,
    liquidator_principal: Principal,
    collateral_asset: ChainToken,
    debt_asset: ChainToken,
    collateral_amount: Nat,
) -> ExecutionReceipt {
    let liquidation = LiquidationRequest {
        borrower: Principal::anonymous(),
        debt_pool_id: Principal::anonymous(),
        collateral_pool_id: Principal::anonymous(),
        debt_amount: Nat::from(0u8),
        receiver_address: liquidator_principal,
        buy_bad_debt: false,
    };

    let swap_args = Some(SwapRequest {
        pay_asset: collateral_asset.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(collateral_asset.clone(), collateral_amount.clone()),
        receive_asset: debt_asset.asset_id(),
        receive_address: None,
        max_slippage_bps: None,
        venue_hint: Some("mexc".to_string()),
    });

    let request = ExecutorRequest {
        liquidation,
        swap_args,
        debt_asset: debt_asset.clone(),
        collateral_asset: collateral_asset.clone(),
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    };

    let liquidation_result = LiquidationResult {
        amounts: LiquidationAmounts {
            collateral_received: collateral_amount,
            debt_repaid: Nat::from(0u8),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        timestamp: now_ts().max(0) as u64,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Success,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Success,
        },
        id: liq_id,
    };

    ExecutionReceipt {
        request,
        liquidation_result: Some(liquidation_result),
        status: ExecutionStatus::Success,
        change_received: true,
    }
}

fn asset_id_for(token: &ChainToken) -> AssetId {
    token.asset_id()
}

pub async fn mexc_smoke_bridge_swap_withdraw_cketh(amount_cketh: f64, execute: bool) -> Result<(), String> {
    if !(amount_cketh.is_finite() && amount_cketh > 0.0) {
        return Err("amount must be a positive finite number".to_string());
    }

    let ctx = Arc::new(init_context().await?);
    let cketh = find_asset_token(ctx.registry.as_ref(), "ckETH", "ICP")?;
    let ckusdc = find_asset_token(ctx.registry.as_ref(), "ckUSDC", "ICP")?;
    let amount = ChainTokenAmount::from_formatted(cketh.clone(), amount_cketh);
    if amount.value == Nat::from(0u8) {
        return Err(format!(
            "amount {} rounds to zero for {} decimals={}",
            amount_cketh,
            cketh.symbol(),
            cketh.decimals()
        ));
    }

    let liq_id = now_ts().max(0) as u128;
    let receipt = make_smoke_receipt(
        liq_id,
        ctx.config.liquidator_principal,
        cketh.clone(),
        ckusdc.clone(),
        amount.value.clone(),
    );
    let finalizer = build_mexc_finalizer(ctx.as_ref()).await?;

    let preview = finalizer.preview_route(&receipt).await?;
    let mut state = finalizer.prepare(&format!("smoke-{liq_id}"), &receipt).await?;

    println!(
        "Smoke plan: deposit {}@{} -> {}@{} (bridge={}), trade market {}, withdraw {}@{} -> {}@{} (bridge={})",
        state.deposit.deposit_asset.symbol(),
        state.deposit.deposit_asset.chain(),
        state
            .deposit
            .bridge
            .deposit_planned_asset
            .clone()
            .unwrap_or_else(|| state.deposit.deposit_asset.symbol()),
        state
            .deposit
            .bridge
            .deposit_planned_network
            .clone()
            .unwrap_or_else(|| state.deposit.deposit_asset.chain()),
        state.deposit.bridge.deposit_bridge_required,
        state.market,
        state
            .withdraw
            .bridge
            .withdraw_planned_asset
            .clone()
            .unwrap_or_else(|| state.withdraw.withdraw_asset.symbol()),
        state
            .withdraw
            .bridge
            .withdraw_planned_network
            .clone()
            .unwrap_or_else(|| state.withdraw.withdraw_asset.chain()),
        state.withdraw.withdraw_asset.symbol(),
        state.withdraw.withdraw_asset.chain(),
        state.withdraw.bridge.withdraw_bridge_required
    );
    println!(
        "Preview: executable={} estimated_receive={} estimated_slippage_bps={}",
        preview.is_executable, preview.estimated_receive_amount, preview.estimated_slippage_bps
    );

    if !preview.is_executable {
        return Err(preview
            .reason
            .unwrap_or_else(|| "route is not executable at current depth".to_string()));
    }

    if !execute {
        println!("Preflight only. Pass --execute to run live bridge+trade+withdraw.");
        return Ok(());
    }

    println!(
        "Executing smoke flow for {} {} (liq_id=smoke-{}).",
        amount_cketh,
        asset_id_for(&cketh).symbol,
        liq_id
    );

    let started_at = Instant::now();
    loop {
        if started_at.elapsed() > SMOKE_EXECUTION_TIMEOUT {
            return Err(format!(
                "smoke flow timed out after {:?} at step {:?}",
                SMOKE_EXECUTION_TIMEOUT, state.step
            ));
        }

        match state.step {
            CexStep::Deposit | CexStep::DepositPending => finalizer.deposit(&mut state).await?,
            CexStep::Trade | CexStep::TradePending => finalizer.trade(&mut state).await?,
            CexStep::Withdraw | CexStep::WithdrawPending => finalizer.withdraw(&mut state).await?,
            CexStep::Completed => break,
            CexStep::Failed => {
                return Err(
                    state
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "mexc finalizer entered failed state".to_string()),
                );
            }
        }

        if matches!(state.step, CexStep::DepositPending | CexStep::WithdrawPending) {
            tokio::time::sleep(SMOKE_POLL_INTERVAL).await;
        }
    }

    let result = finalizer.finish(&receipt, &state).await?;
    let received = ChainTokenAmount::from_raw(ckusdc, result.receive_amount.clone());
    println!(
        "Smoke flow completed in {:?}. Received {} (status={}).",
        started_at.elapsed(),
        received.formatted(),
        result.status
    );

    Ok(())
}
