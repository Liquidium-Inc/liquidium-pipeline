use std::sync::Arc;

use liquidium_pipeline_core::account::actions::AccountActions;

use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use crate::{config::ConfigTrait, persistance::WalStore, swappers::swap_interface::SwapInterface};

pub struct KongSwapFinalizer<D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent> {
    pub config: Arc<C>,
    pub db: Arc<D>,
    pub swapper: Arc<S>,
    pub account: Arc<A>,
    pub agent: Arc<P>,
}

impl<D: WalStore, S: SwapInterface, A: AccountActions, C: ConfigTrait, P: PipelineAgent>
    KongSwapFinalizer<D, S, A, C, P>
{
    pub fn new(db: Arc<D>, s: Arc<S>, account: Arc<A>, config: Arc<C>, agent: Arc<P>) -> Self {
        Self {
            db,
            swapper: s,
            account,
            config,
            agent,
        }
    }
}
