use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    finalizers::dex_finalizer::DexFinalizerLogic,
    swappers::{
        model::{SwapExecution, SwapRequest},
        swap_interface::SwapInterface,
    },
};

/// Production DEX finalizer boundary for ICPSwap.
///
/// The registered venue currently rejects execution. Durable plan persistence,
/// settlement, and recovery will be implemented behind this boundary before DEX
/// modes are enabled.
pub struct IcpswapFinalizer<S>
where
    S: SwapInterface,
{
    swapper: Arc<S>,
}

impl<S> IcpswapFinalizer<S>
where
    S: SwapInterface,
{
    pub fn new(swapper: Arc<S>) -> Self {
        Self { swapper }
    }
}

#[async_trait]
impl<S> DexFinalizerLogic for IcpswapFinalizer<S>
where
    S: SwapInterface + Send + Sync,
{
    async fn swap(&self, request: &SwapRequest) -> Result<SwapExecution, String> {
        self.swapper.execute(request).await
    }
}
