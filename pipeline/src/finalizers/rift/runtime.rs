//! Builds the Rift venue over the REST client, with the shared bridge.
use std::sync::Arc;

use liquidium_pipeline_connectors::backend::rift_backend::RiftVenueLists;

use crate::{
    bridge::runtime::BridgeRuntime,
    config::ConfigTrait,
    context::PipelineContext,
    finalizers::rift::finalizer::{RiftRefundAddresses, RiftVenueFinalizer},
    swappers::rift::RiftClient,
};

/// Rift delivers to the bridge EVM wallet. A refund goes to the main EVM
/// wallet, or mints ckBTC to the recovery account for a ckBTC leg.
pub fn build_rift_finalizer(
    ctx: &PipelineContext,
    bridge: Arc<BridgeRuntime>,
) -> Result<Arc<RiftVenueFinalizer<RiftClient>>, String> {
    let config = &ctx.config;
    let client = RiftClient::new(&config.rift_api_url, config.rift_api_key.clone())?;
    Ok(Arc::new(
        RiftVenueFinalizer::new(
            Arc::new(client),
            bridge,
            ctx.trader_outbound.clone(),
            config.liquidator_principal,
            RiftRefundAddresses {
                ethereum: ctx.evm_address.clone(),
                bitcoin_mint_account: config.get_recovery_account().to_string(),
            },
        )
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps)
        .with_venues(RiftVenueLists {
            allow: config.rift_venue_allowlist.clone(),
            block: config.rift_venue_blocklist.clone(),
        }),
    ))
}
