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

use tracing::info;
use num_traits::ToPrimitive;
use tracing::instrument;

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
        let finalizer = self
            .cex_finalizer
            .as_ref()
            .ok_or_else(|| "missing cex finalizer".to_string())?;
        finalizer.finalize(wal, receipt).await
    }
}

#[async_trait]
impl<C> Finalizer for HybridFinalizer<C>
where
    C: ConfigTrait + Send + Sync,
{
    #[instrument(name = "hybrid.finalize", skip_all, err)]
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // First, respect the configured swapper mode. In Dex/Cex modes we bypass hybrid logic.
        match self.config.get_swapper_mode() {
            SwapperMode::Dex => {
                // Pure DEX mode: delegate directly to the DEX finalizer.
                let mut res = self.finalize_via_dex(wal, receipt).await?;
                res.swapper = Some("kong".to_string());
                info!("[hybrid] 🟩 forced dex route -> kong");
                return Ok(res);
            }
            SwapperMode::Cex => {
                // Pure CEX mode: delegate directly to the CEX finalizer.
                let mut res = self.finalize_via_cex(wal, receipt, None).await?;
                res.swapper = Some("mexc".to_string());
                info!("[hybrid] 🟧 forced cex route -> mexc");
                return Ok(res);
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
                    swapper: Some("none".to_string()),
                });
            }
        };

        // ref_price is stored as a Nat in RAY (1e27) on the request.
        let ref_price_ray = receipt.request.ref_price.clone();
        let ref_price_f64 = ref_price_ray.0.to_f64().unwrap_or(0.0) / 1e27f64;

        let pay_amount = swap_req.pay_amount.value.0.to_f64().unwrap_or(0.0);
        let pay_scale = 10f64.powi(swap_req.pay_amount.token.decimals() as i32);
        let pay_amount_units = pay_amount / pay_scale;
        let est_value_usd = pay_amount_units * ref_price_f64;

        if est_value_usd < 2.0 {
            info!("[hybrid] 🟩 routing to kong (value_usd={:.4} < 2.00)", est_value_usd);
            let mut res = self.finalize_via_dex(wal, receipt).await?;
            res.swapper = Some("kong".to_string());
            return Ok(res);
        }

        info!("[hybrid] 🟧 routing to mexc (value_usd={:.4} >= 2.00)", est_value_usd);
        let mut res = self
            .finalize_via_cex(wal, receipt, Some("value_usd >= 2".to_string()))
            .await?;
        res.swapper = Some("mexc".to_string());
        Ok(res)
    }
}
