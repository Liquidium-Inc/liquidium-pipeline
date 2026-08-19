use std::sync::Arc;

use crate::{
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::{
        cex_finalizer::{CexFinalizer, CexVenueProfile, runtime::build_cex_bridge_dependencies},
        kraken::KrakenFinalizer,
    },
    swappers::kraken::{KrakenClient, normalize_market},
};

pub async fn build_kraken_finalizer(ctx: &PipelineContext) -> Result<Arc<KrakenFinalizer>, String> {
    let config = ctx.config.clone();
    let (api_key, api_secret) = config.get_cex_credentials("kraken").map_err(|error| {
        format!("Kraken is enabled but CEX_KRAKEN_API_KEY/CEX_KRAKEN_API_SECRET are missing: {error}")
    })?;
    let available_pairs = config
        .cex_kraken_available_pairs
        .iter()
        .map(|pair| normalize_market(pair))
        .collect::<Vec<_>>();
    // An empty allowlist is not a narrower Kraken, it is an unrestricted one:
    // `KrakenClient` skips the market filter entirely when it has no pairs, so
    // a missing or unparsable setting would quietly let routing reach every
    // market Kraken lists. Refused here for the same reason missing credentials
    // are -- Kraken is opt-in, so only an operator who asked for it is affected.
    if available_pairs.is_empty() {
        return Err(
            "Kraken is enabled but CEX_KRAKEN_AVAILABLE_PAIRS is unset or has no parsable market: an empty allowlist would let Kraken route through every market it lists"
                .to_string(),
        );
    }
    let client = Arc::new(KrakenClient::new(api_key, api_secret, available_pairs.clone()));
    let bridge_dependencies = build_cex_bridge_dependencies(ctx).await?;

    Ok(Arc::new(
        CexFinalizer::new_with_tunables(
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
        // Measured on the live account rather than read off Kraken's published
        // schedule: every crypto pair it has traded -- ETH/USD, ICP/USD, both
        // directions -- charged exactly 80 bps, twice the 40 assumed here
        // before. Stablecoin pairs charge 20, so a route that crosses one of
        // each is quoted 60 bps pessimistic and every other route is quoted
        // right; erring high is the safe direction for a gate that decides
        // whether a liquidation is worth taking. Per-pair rates belong in the
        // profile eventually, from Kraken's own TradeVolume endpoint.
        .with_profile(CexVenueProfile::kraken(80.0))
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_bridge_dependencies(bridge_dependencies)
        .with_route_config(available_pairs, config.cex_kraken_max_hops as usize),
    ))
}
