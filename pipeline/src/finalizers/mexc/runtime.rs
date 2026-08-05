use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::cex_finalizer::{CexFinalizer, CexVenueProfile, runtime::build_cex_bridge_dependencies},
    swappers::mexc::mexc_adapter::MexcClient,
};

pub async fn build_mexc_finalizer(ctx: &PipelineContext) -> Result<Arc<CexFinalizer<MexcClient>>, String> {
    let config = ctx.config.clone();
    let (api_key, secret) = config
        .get_cex_credentials("mexc")
        .map_err(|error| format!("MEXC credentials not found: {error}"))?;
    let mexc_client = Arc::new(MexcClient::new(&api_key, &secret));
    let bridge_dependencies = build_cex_bridge_dependencies(ctx).await?;

    Ok(Arc::new(
        CexFinalizer::new_with_tunables(
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
        .with_profile(CexVenueProfile::mexc())
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_bridge_dependencies(bridge_dependencies)
        .with_route_config(
            config.cex_mexc_available_pairs.clone(),
            config.cex_mexc_max_hops as usize,
        ),
    ))
}
