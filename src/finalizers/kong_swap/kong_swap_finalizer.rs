use std::sync::Arc;

use crate::{persistance::WalStore, swappers::swap_interface::IcrcSwapInterface};

pub struct KongSwapFinalizer<D: WalStore, S: IcrcSwapInterface> {
    pub db: Arc<D>,
    pub swapper: Arc<S>,
}

impl<D: WalStore, S: IcrcSwapInterface> KongSwapFinalizer<D, S> {
    pub fn new(db: Arc<D>, s: Arc<S>) -> Self {
        Self { db, swapper: s }
    }
}
