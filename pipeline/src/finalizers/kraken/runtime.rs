use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::{
        kraken::KrakenFinalizer,
        mexc::{mexc_finalizer::MexcFinalizer, runtime::build_cex_bridge_dependencies},
    },
    swappers::kraken::KrakenClient,
};

pub async fn build_kraken_finalizer(ctx: &PipelineContext) -> Result<Arc<KrakenFinalizer>, String> {
    let config = ctx.config.clone();
    let (api_key, api_secret) = config
        .get_cex_credentials("kraken")
        .map_err(|error| format!("Kraken is enabled but CEX_KRAKEN_API_KEY/CEX_KRAKEN_API_SECRET are missing: {error}"))?;
    let available_pairs = config.cex_kraken_available_pairs.clone();
    let client = Arc::new(KrakenClient::new(api_key, api_secret, available_pairs.clone()));
    let bridge_dependencies = build_cex_bridge_dependencies(ctx).await?;

    Ok(Arc::new(
        MexcFinalizer::new_with_tunables(
            client,
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
        .with_venue_profile("kraken", 40.0, false)
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_bridge_dependencies(bridge_dependencies)
        .with_route_config(available_pairs, config.cex_kraken_max_hops as usize),
    ))
}
