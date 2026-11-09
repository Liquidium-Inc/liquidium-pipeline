//! Builds the Rift venue over the REST client, with the shared bridge.
use std::sync::Arc;

use crate::{
    bridge::runtime::BridgeRuntime, context::PipelineContext, finalizers::rift::finalizer::RiftVenueFinalizer,
    swappers::rift::RiftClient,
};

/// Rift delivers to the bridge EVM wallet and refunds to the main EVM wallet.
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
            ctx.evm_address.clone(),
        )
        .with_token_registry(ctx.registry.clone())
        .with_quote_costs(config.cex_route_fee_bps, config.cex_delay_buffer_bps),
    ))
}
