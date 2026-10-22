use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::{bridge_runtime::BridgeRuntime, mexc::mexc_finalizer::MexcFinalizer},
    swappers::mexc::mexc_adapter::MexcClient,
};

pub async fn build_mexc_finalizer(
    ctx: &PipelineContext,
    bridge: Arc<BridgeRuntime>,
) -> Result<Arc<MexcFinalizer<MexcClient>>, String> {
    let config = ctx.config.clone();
    let (api_key, secret) = config
        .get_cex_credentials("mexc")
        .map_err(|err| format!("Cex credentials not found: {err}"))?;
    let mexc_client = Arc::new(MexcClient::new(&api_key, &secret));

    Ok(Arc::new(
        MexcFinalizer::new_with_tunables(
            mexc_client,
            ctx.trader_transfers.actions(),
            config.liquidator_principal,
            config.max_allowed_cex_slippage_bps as f64,
            config.cex_min_exec_usd,
            config.cex_slice_target_ratio,
            config.cex_buy_truncation_trigger_ratio,
            config.cex_buy_inverse_overspend_bps,
            config.cex_buy_inverse_max_retries,
            config.cex_buy_inverse_enabled,
        )
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_bridge_runtime(bridge)
        .with_route_config(
            config.cex_mexc_available_pairs.clone(),
            config.cex_mexc_max_hops as usize,
        ),
    ))
}
