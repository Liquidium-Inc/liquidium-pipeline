use std::sync::Arc;

use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::transfer::transfer_service::TransferService;

use crate::{config::ConfigTrait, persistance::WalStore, swappers::swap_interface::SwapInterface};

pub struct KongSwapFinalizer<D: WalStore, S: SwapInterface, C: ConfigTrait, P: PipelineAgent> {
    pub config: Arc<C>,
    pub db: Arc<D>,
    pub swapper: Arc<S>,
    pub agent: Arc<P>,
    pub transfer_service: Arc<TransferService>,
}

impl<D: WalStore, S: SwapInterface, C: ConfigTrait, P: PipelineAgent> KongSwapFinalizer<D, S, C, P> {
    pub fn new(db: Arc<D>, s: Arc<S>, transfer_service: Arc<TransferService>, config: Arc<C>, agent: Arc<P>) -> Self {
        Self {
            db,
            swapper: s,
            transfer_service,
            config,
            agent,
        }
    }
}
