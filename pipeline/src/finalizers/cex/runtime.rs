//! Builds an exchange venue: one CEX machine over the exchange's backend,
//! with the shared bridge, sender and tunables.
use std::sync::Arc;

use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use crate::{
    bridge::runtime::BridgeRuntime,
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::cex::{finalizer::CexVenueFinalizer, profile::CexVenueProfile},
    swappers::{
        kraken::{KrakenClient, normalize_market},
        mexc::mexc_adapter::MexcClient,
    },
};

/// The CEX machine every exchange shares; only the backend, the profile and
/// the market allowlist differ.
fn build_cex_finalizer<C: CexBackend>(
    ctx: &PipelineContext,
    bridge: Arc<BridgeRuntime>,
    profile: CexVenueProfile,
    backend: Arc<C>,
    available_pairs: Vec<String>,
    max_hops: u8,
) -> Arc<CexVenueFinalizer<C>> {
    let config = &ctx.config;
    Arc::new(
        CexVenueFinalizer::new_with_tunables(
            profile,
            backend,
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
        .with_widened_slippage_cap(config.max_widened_cex_slippage_bps as f64)
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_bridge_runtime(bridge)
        .with_outbound(ctx.trader_outbound.clone())
        .with_route_config(available_pairs, max_hops as usize),
    )
}

pub async fn build_mexc_finalizer(
    ctx: &PipelineContext,
    bridge: Arc<BridgeRuntime>,
) -> Result<Arc<CexVenueFinalizer<MexcClient>>, String> {
    let (api_key, secret) = ctx
        .config
        .get_cex_credentials("mexc")
        .map_err(|err| format!("Cex credentials not found: {err}"))?;
    Ok(build_cex_finalizer(
        ctx,
        bridge,
        CexVenueProfile::mexc(),
        Arc::new(MexcClient::new(&api_key, &secret)),
        ctx.config.cex_mexc_available_pairs.clone(),
        ctx.config.cex_mexc_max_hops,
    ))
}

/// Kraken is opt-in: it needs its keys and an explicit market allowlist.
pub async fn build_kraken_finalizer(
    ctx: &PipelineContext,
    bridge: Arc<BridgeRuntime>,
) -> Result<Arc<CexVenueFinalizer<KrakenClient>>, String> {
    let (api_key, secret) = ctx.config.get_cex_credentials("kraken").map_err(|error| {
        format!("Kraken is enabled but CEX_KRAKEN_API_KEY/CEX_KRAKEN_API_SECRET are missing: {error}")
    })?;
    // Kraken spells BTC as XBT, so both the client and the route search get canonical names.
    let available_pairs = ctx
        .config
        .cex_kraken_available_pairs
        .iter()
        .map(|pair| normalize_market(pair))
        .collect::<Vec<_>>();
    let client = KrakenClient::new(api_key, secret, available_pairs.clone())
        .map_err(|error| format!("Kraken is enabled but CEX_KRAKEN_AVAILABLE_PAIRS is unusable: {error}"))?;
    Ok(build_cex_finalizer(
        ctx,
        bridge,
        CexVenueProfile::kraken(),
        Arc::new(client),
        available_pairs,
        ctx.config.cex_kraken_max_hops,
    ))
}
