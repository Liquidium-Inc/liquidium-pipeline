use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    config::{ConfigTrait, SwapperMode},
    finalizers::{
        cex_finalizer::CexFinalizerLogic,
        dex_finalizer::DexFinalizerLogic,
        finalizer::{Finalizer, FinalizerResult},
    },
    persistance::{ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
    swappers::swap_interface::SwapInterface,
    wal::liq_id_from_receipt,
};

use num_traits::ToPrimitive;

pub struct HybridFinalizer<C>
where
    C: ConfigTrait,
{
    pub config: Arc<C>,
    // Used only for getting DEX quotes; actual DEX execution is delegated to dex_finalizer.
    pub dex_swapper: Arc<dyn SwapInterface>,
    pub dex_finalizer: Arc<dyn DexFinalizerLogic>, // e.g. KongSwapFinalizer
    pub cex_finalizer: Option<Arc<dyn CexFinalizerLogic>>, // existing CEX finalizer
}

impl<C> HybridFinalizer<C>
where
    C: ConfigTrait,
{
    fn slippage_bps(ref_price: f64, dex_price: f64) -> u32 {
        let diff = (dex_price - ref_price).abs() / ref_price;
        (diff * 10_000.0).round() as u32
    }

    async fn finalize_via_dex(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Delegate full DEX settlement logic to the underlying DEX finalizer.
        self.dex_finalizer.finalize(wal, receipt).await
    }

    async fn finalize_via_cex(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
        _reason: Option<String>,
    ) -> Result<FinalizerResult, String> {
        // Delegate full CEX settlement logic to the underlying CEX finalizer.
        self.cex_finalizer
            .as_ref()
            .expect("missing cex finalizer")
            .finalize(wal, receipt)
            .await
    }
}

#[async_trait]
impl<C> Finalizer for HybridFinalizer<C>
where
    C: ConfigTrait + Send + Sync,
{
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // First, respect the configured swapper mode. In Dex/Cex modes we bypass hybrid logic.
        match self.config.get_swapper_mode() {
            SwapperMode::Dex => {
                // Pure DEX mode: delegate directly to the DEX finalizer.
                return self.finalize_via_dex(wal, receipt).await;
            }
            SwapperMode::Cex => {
                // Pure CEX mode: delegate directly to the CEX finalizer.
                return self.finalize_via_cex(wal, receipt, None).await;
            }
            SwapperMode::Hybrid => {
                // Fall through to hybrid routing logic below.
            }
        }

        // If there is no swap planned, we treat this as a noop in hybrid mode.
        let swap_req = match receipt.request.swap_args.clone() {
            Some(req) => req,
            None => {
                // No swap to perform; mark WAL succeeded and return a noop result.
                let id = liq_id_from_receipt(&receipt)?;
                wal.update_status(&id, ResultStatus::Succeeded, true)
                    .await
                    .map_err(|e| format!("wal update failed: {e}"))?;
                return Ok(FinalizerResult {
                    finalized: true,
                    swap_result: None,
                });
            }
        };

        // ref_price is stored as a Nat in RAY (1e27) on the request.
        let ref_price_ray = receipt.request.ref_price.clone();
        let ref_price_f64 = match ref_price_ray.0.to_f64() {
            Some(x) if x > 0.0 => x / 1e27f64,
            _ => {
                // No usable reference price: fall back to CEX finalizer.
                return self
                    .finalize_via_cex(wal, receipt, Some("missing ref_price; cex-only".to_string()))
                    .await;
            }
        };

        // Use the DEX swapper only to obtain a quote; actual settlement is delegated.
        let quote = match self.dex_swapper.quote(&swap_req).await {
            Ok(q) => q,
            Err(e) => {
                // DEX infra down or quote failed; route to CEX finalizer.
                return self
                    .finalize_via_cex(wal, receipt, Some(format!("dex quote error: {e}")))
                    .await;
            }
        };

        let dex_price = quote.mid_price;
        let slip = Self::slippage_bps(ref_price_f64, dex_price);
        let max_slip = self.config.get_max_allowed_dex_slippage();

        if slip <= max_slip {
            // Slippage acceptable vs oracle: use DEX route.
            return self.finalize_via_dex(wal, receipt).await;
        }

        // Slippage too high vs oracle: fall back to CEX route.
        self.finalize_via_cex(
            wal,
            receipt,
            Some(format!("dex slippage {slip} bps > {max_slip} bps; using cex")),
        )
        .await
    }
}
