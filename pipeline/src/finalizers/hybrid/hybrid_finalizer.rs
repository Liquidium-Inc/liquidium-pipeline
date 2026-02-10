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
    swappers::{model::SwapRequest, swap_interface::SwapInterface},
    wal::liq_id_from_receipt,
};

use num_traits::ToPrimitive;
use tracing::info;
use tracing::instrument;

/// Route tiny notional swaps to DEX to avoid CEX overhead/noise for dust-sized trades.
const DEX_DUST_MAX_USD: f64 = 2.5;

/// Route venues available in hybrid finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteVenue {
    Dex,
    Cex,
}

/// Comparable route candidate represented by projected net edge.
#[derive(Debug, Clone)]
struct RouteCandidate {
    venue: RouteVenue,
    net_edge_bps: f64,
    reason: String,
}

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
    /// Convert route enum to user-facing swapper label.
    fn swapper_label(venue: RouteVenue) -> &'static str {
        match venue {
            RouteVenue::Dex => "kong",
            RouteVenue::Cex => "mexc",
        }
    }

    /// Human-readable venue name for logs.
    fn venue_name(venue: RouteVenue) -> &'static str {
        match venue {
            RouteVenue::Dex => "dex",
            RouteVenue::Cex => "cex",
        }
    }

    /// Return forced venue in non-hybrid modes.
    fn forced_mode_venue(&self) -> Option<RouteVenue> {
        match self.config.get_swapper_mode() {
            SwapperMode::Dex => Some(RouteVenue::Dex),
            SwapperMode::Cex => Some(RouteVenue::Cex),
            SwapperMode::Hybrid => None,
        }
    }

    /// Net edge in bps after execution frictions and delay-risk haircut.
    fn net_edge_bps(gross_edge_bps: f64, slippage_bps: f64, fee_bps: f64, delay_bps: f64) -> f64 {
        gross_edge_bps - slippage_bps - fee_bps - delay_bps
    }

    /// Estimate gross edge in bps from strategy-side expected profit over repaid debt.
    fn gross_edge_bps(receipt: &ExecutionReceipt) -> f64 {
        let Some(liq) = receipt.liquidation_result.as_ref() else {
            return 0.0;
        };
        let debt_repaid = liq.amounts.debt_repaid.0.to_f64().unwrap_or(0.0);
        if debt_repaid <= 0.0 {
            return 0.0;
        }
        (receipt.request.expected_profit as f64 / debt_repaid) * 10_000.0
    }

    /// Normalize quote slippage into basis points.
    fn dex_slippage_bps(slippage: f64) -> f64 {
        // Kong reports slippage in percentage points, e.g. 0.72 => 0.72%.
        // 1% = 100 bps, hence *100.
        (slippage.max(0.0)) * 100.0
    }

    /// Estimate swap notional in USD using strategy-provided reference price (ray format 1e27).
    fn estimate_swap_value_usd(receipt: &ExecutionReceipt, swap_req: &SwapRequest) -> f64 {
        let ref_price_f64 = receipt.request.ref_price.0.to_f64().unwrap_or(0.0) / 1e27f64;
        if ref_price_f64 <= 0.0 {
            return 0.0;
        }
        let pay_units = swap_req.pay_amount.to_f64();
        (pay_units * ref_price_f64).max(0.0)
    }

    /// Dust swaps are forced to DEX to avoid CEX overhead on tiny notional.
    fn is_dust_swap(receipt: &ExecutionReceipt, swap_req: &SwapRequest) -> bool {
        let est_value_usd = Self::estimate_swap_value_usd(receipt, swap_req);
        est_value_usd > 0.0 && est_value_usd < DEX_DUST_MAX_USD
    }

    /// In hybrid mode, force CEX when estimated notional is above configured threshold.
    /// A non-positive threshold disables this fast-path override.
    fn should_force_cex_over_threshold(&self, receipt: &ExecutionReceipt, swap_req: &SwapRequest) -> bool {
        let est_value_usd = Self::estimate_swap_value_usd(receipt, swap_req);
        let threshold_usd = self.config.get_cex_force_over_usd_threshold();
        if threshold_usd <= 0.0 {
            return false;
        }
        est_value_usd > threshold_usd
    }

    /// Build a successful no-swap result and mark WAL succeeded.
    async fn finalize_without_swap(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
    ) -> Result<FinalizerResult, String> {
        let id = liq_id_from_receipt(receipt)?;
        wal.update_status(&id, ResultStatus::Succeeded, true)
            .await
            .map_err(|e| format!("wal update failed: {e}"))?;
        Ok(FinalizerResult {
            finalized: true,
            swap_result: None,
            swapper: Some("none".to_string()),
        })
    }

    async fn finalize_via_dex(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Delegate full DEX settlement logic to the underlying DEX finalizer.
        self.dex_finalizer.finalize(wal, receipt).await
    }

    async fn finalize_via_cex(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Delegate full CEX settlement logic to the underlying CEX finalizer.
        let finalizer = self
            .cex_finalizer
            .as_ref()
            .ok_or_else(|| "missing cex finalizer".to_string())?;

        finalizer.finalize(wal, receipt).await
    }

    /// Execute chosen venue finalizer and annotate resulting swapper label.
    async fn execute_route(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
        venue: RouteVenue,
        reason: Option<&str>,
    ) -> Result<FinalizerResult, String> {
        let mut res = match venue {
            RouteVenue::Dex => self.finalize_via_dex(wal, receipt).await?,
            RouteVenue::Cex => self.finalize_via_cex(wal, receipt).await?,
        };
        res.swapper = Some(Self::swapper_label(venue).to_string());
        if let Some(reason) = reason {
            info!("[hybrid] routing -> {} reason={}", Self::swapper_label(venue), reason);
        } else {
            info!("[hybrid] routing -> {}", Self::swapper_label(venue));
        }
        Ok(res)
    }

    /// Build DEX candidate using quote slippage and net-edge threshold gate.
    /// Returns `Err` only when previewing fails (transport/API error).
    async fn build_dex_candidate(
        &self,
        swap_req: &SwapRequest,
        gross_edge_bps: f64,
        min_net_edge_bps: f64,
    ) -> Result<Option<RouteCandidate>, String> {
        match self.dex_swapper.quote(swap_req).await {
            Ok(quote) => {
                let slippage_bps = Self::dex_slippage_bps(quote.slippage);
                let net = Self::net_edge_bps(gross_edge_bps, slippage_bps, 0.0, 0.0);
                info!(
                    "[hybrid] dex preview gross_bps={:.2} slippage_bps={:.2} net_bps={:.2}",
                    gross_edge_bps, slippage_bps, net
                );
                if net >= min_net_edge_bps {
                    Ok(Some(RouteCandidate {
                        venue: RouteVenue::Dex,
                        net_edge_bps: net,
                        reason: format!("dex net edge {:.2} bps", net),
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(err) => Err(format!("dex preview failed: {}", err)),
        }
    }

    /// Build CEX candidate from route preview and configured fee/delay haircuts.
    /// Returns `Err` only when previewing fails (transport/API error).
    async fn build_cex_candidate(
        &self,
        receipt: &ExecutionReceipt,
        gross_edge_bps: f64,
        min_net_edge_bps: f64,
    ) -> Result<Option<RouteCandidate>, String> {
        let Some(cex_finalizer) = self.cex_finalizer.as_ref() else {
            info!("[hybrid] cex preview unavailable: missing cex finalizer");
            return Ok(None);
        };

        match cex_finalizer.preview_route(receipt).await {
            Ok(preview) if preview.is_executable => {
                let fee_bps = self.config.get_cex_route_fee_bps() as f64;
                let delay_bps = self.config.get_cex_delay_buffer_bps() as f64;
                let net = Self::net_edge_bps(gross_edge_bps, preview.estimated_slippage_bps, fee_bps, delay_bps);
                info!(
                    "[hybrid] cex preview gross_bps={:.2} slippage_bps={:.2} fee_bps={:.2} delay_bps={:.2} net_bps={:.2}",
                    gross_edge_bps, preview.estimated_slippage_bps, fee_bps, delay_bps, net
                );
                if net >= min_net_edge_bps {
                    Ok(Some(RouteCandidate {
                        venue: RouteVenue::Cex,
                        net_edge_bps: net,
                        reason: format!("cex net edge {:.2} bps", net),
                    }))
                } else {
                    Ok(None)
                }
            }
            Ok(preview) => {
                info!(
                    "[hybrid] cex preview not executable: {}",
                    preview.reason.unwrap_or_else(|| "unknown".to_string())
                );
                Ok(None)
            }
            Err(err) => Err(format!("cex preview failed: {}", err)),
        }
    }

    /// Pick route with the highest net edge among viable candidates.
    fn choose_best_route(dex: Option<RouteCandidate>, cex: Option<RouteCandidate>) -> Option<RouteCandidate> {
        match (dex, cex) {
            (Some(dex), Some(cex)) => {
                if cex.net_edge_bps > dex.net_edge_bps {
                    Some(cex)
                } else {
                    Some(dex)
                }
            }
            (Some(dex), None) => Some(dex),
            (None, Some(cex)) => Some(cex),
            (None, None) => None,
        }
    }
}

#[async_trait]
impl<C> Finalizer for HybridFinalizer<C>
where
    C: ConfigTrait + Send + Sync,
{
    #[instrument(name = "hybrid.finalize", skip_all, err)]
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // 1) Forced mode override: pure DEX or pure CEX bypasses route comparison.
        if let Some(forced) = self.forced_mode_venue() {
            let reason = format!("forced mode {}", Self::venue_name(forced));
            return self.execute_route(wal, receipt, forced, Some(&reason)).await;
        }

        // 2) No swap request: mark succeeded and return a no-op finalizer result.
        let swap_req = match receipt.request.swap_args.clone() {
            Some(req) => req,
            None => return self.finalize_without_swap(wal, &receipt).await,
        };

        // 3) Dust routing: tiny notional is always sent to DEX.
        if Self::is_dust_swap(&receipt, &swap_req) {
            let est_value_usd = Self::estimate_swap_value_usd(&receipt, &swap_req);
            let reason = format!(
                "dust route est_value_usd={:.4} threshold_usd={:.2}",
                est_value_usd, DEX_DUST_MAX_USD
            );
            return self.execute_route(wal, receipt, RouteVenue::Dex, Some(&reason)).await;
        }

        // 4) Force CEX over configured threshold in hybrid mode.
        if self.should_force_cex_over_threshold(&receipt, &swap_req) {
            let est_value_usd = Self::estimate_swap_value_usd(&receipt, &swap_req);
            let threshold_usd = self.config.get_cex_force_over_usd_threshold();
            let reason = format!(
                "force cex route est_value_usd={:.4} threshold_usd={:.2}",
                est_value_usd, threshold_usd
            );
            return self.execute_route(wal, receipt, RouteVenue::Cex, Some(&reason)).await;
        }

        // 5) Candidate build and compare by projected net edge.
        let gross_edge_bps = Self::gross_edge_bps(&receipt);
        let min_net_edge_bps = self.config.get_cex_min_net_edge_bps() as f64;

        // Build route candidates concurrently to reduce decision latency.
        let (dex_result, cex_result) = tokio::join!(
            self.build_dex_candidate(&swap_req, gross_edge_bps, min_net_edge_bps),
            self.build_cex_candidate(&receipt, gross_edge_bps, min_net_edge_bps)
        );

        // Error policy:
        // - If one preview fails, continue with the other route.
        // - If both previews fail, fail the routing decision.
        let dex_error = dex_result.as_ref().err().cloned();
        let cex_error = cex_result.as_ref().err().cloned();

        if let Some(err) = &dex_error {
            info!("[hybrid] {}", err);
        }

        if let Some(err) = &cex_error {
            info!("[hybrid] {}", err);
        }

        if let (Some(dex_err), Some(cex_err)) = (dex_error, cex_error) {
            return Err(format!("route preview failed on both venues: {}; {}", dex_err, cex_err));
        }

        let dex_candidate = dex_result.ok().flatten();
        let cex_candidate = cex_result.ok().flatten();

        if let Some(chosen) = Self::choose_best_route(dex_candidate, cex_candidate) {
            return self
                .execute_route(wal, receipt, chosen.venue, Some(chosen.reason.as_str()))
                .await;
        }

        // 6) Neither route met threshold constraints.
        Err(format!(
            "no viable route: gross_edge_bps={:.2}, min_required_bps={:.2}",
            gross_edge_bps, min_net_edge_bps
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };

    use crate::config::MockConfigTrait;
    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::dex_finalizer::DexFinalizerLogic;
    use crate::persistance::MockWalStore;
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
    use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};
    use crate::swappers::swap_interface::MockSwapInterface;

    struct NoopDexFinalizer;

    #[async_trait]
    impl DexFinalizerLogic for NoopDexFinalizer {
        async fn swap(&self, _req: &SwapRequest) -> Result<SwapExecution, String> {
            Err("dex finalizer should not run".to_string())
        }
    }

    fn make_receipt(est_value_usd: f64) -> ExecutionReceipt {
        let collateral = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        let debt = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };

        let swap_req = SwapRequest {
            pay_asset: collateral.asset_id(),
            pay_amount: ChainTokenAmount::from_formatted(collateral.clone(), 1.0),
            receive_asset: debt.asset_id(),
            receive_address: Some("dest".to_string()),
            max_slippage_bps: Some(100),
            venue_hint: None,
        };

        let liquidation = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(1_000u64),
            receiver_address: Principal::anonymous(),
            buy_bad_debt: false,
        };

        let liq_result = LiquidationResult {
            id: 42,
            timestamp: 0,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(1_000u64),
                debt_repaid: Nat::from(1_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
        };

        let ref_price_ray = (est_value_usd * 1e27f64).round() as u128;
        let req = ExecutorRequest {
            liquidation,
            swap_args: Some(swap_req),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 0,
            ref_price: Nat::from(ref_price_ray),
            debt_approval_needed: false,
        };

        ExecutionReceipt {
            request: req,
            liquidation_result: Some(liq_result),
            status: ExecutionStatus::Success,
            change_received: true,
        }
    }

    #[tokio::test]
    async fn hybrid_force_cex_over_threshold_skips_candidate_previews() {
        let mut config = MockConfigTrait::new();
        config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
        config.expect_get_cex_force_over_usd_threshold().return_const(2.5);

        let mut dex_swapper = MockSwapInterface::new();
        dex_swapper.expect_quote().times(0);
        dex_swapper.expect_execute().times(0);

        let wal = MockWalStore::new();
        let finalizer = HybridFinalizer {
            config: Arc::new(config),
            dex_swapper: Arc::new(dex_swapper),
            dex_finalizer: Arc::new(NoopDexFinalizer),
            cex_finalizer: None,
        };

        let err = finalizer
            .finalize(&wal, make_receipt(3.0))
            .await
            .expect_err("route should fail because cex finalizer is missing");

        assert!(err.contains("missing cex finalizer"));
    }

    #[tokio::test]
    async fn hybrid_does_not_force_cex_at_exact_threshold() {
        let mut config = MockConfigTrait::new();
        config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
        config.expect_get_cex_force_over_usd_threshold().return_const(2.5);
        config.expect_get_cex_min_net_edge_bps().return_const(1_000u32);

        let mut dex_swapper = MockSwapInterface::new();
        dex_swapper.expect_quote().times(1).returning(|req| {
            Ok(SwapQuote {
                pay_asset: req.pay_asset.clone(),
                pay_amount: req.pay_amount.value.clone(),
                receive_asset: req.receive_asset.clone(),
                receive_amount: Nat::from(1u8),
                mid_price: 1.0,
                exec_price: 1.0,
                slippage: 0.0,
                legs: vec![],
            })
        });
        dex_swapper.expect_execute().times(0);

        let wal = MockWalStore::new();
        let finalizer = HybridFinalizer {
            config: Arc::new(config),
            dex_swapper: Arc::new(dex_swapper),
            dex_finalizer: Arc::new(NoopDexFinalizer),
            cex_finalizer: None,
        };

        let err = finalizer
            .finalize(&wal, make_receipt(2.5))
            .await
            .expect_err("no route should satisfy min net edge");

        assert!(err.contains("no viable route"));
    }

    #[tokio::test]
    async fn hybrid_zero_force_threshold_disables_force_path() {
        let mut config = MockConfigTrait::new();
        config.expect_get_swapper_mode().return_const(SwapperMode::Hybrid);
        config.expect_get_cex_force_over_usd_threshold().return_const(0.0);
        config.expect_get_cex_min_net_edge_bps().return_const(1_000u32);

        let mut dex_swapper = MockSwapInterface::new();
        // If force-path is disabled, candidate preview logic runs and calls DEX quote.
        dex_swapper.expect_quote().times(1).returning(|req| {
            Ok(SwapQuote {
                pay_asset: req.pay_asset.clone(),
                pay_amount: req.pay_amount.value.clone(),
                receive_asset: req.receive_asset.clone(),
                receive_amount: Nat::from(1u8),
                mid_price: 1.0,
                exec_price: 1.0,
                slippage: 0.0,
                legs: vec![],
            })
        });
        dex_swapper.expect_execute().times(0);

        let wal = MockWalStore::new();
        let finalizer = HybridFinalizer {
            config: Arc::new(config),
            dex_swapper: Arc::new(dex_swapper),
            dex_finalizer: Arc::new(NoopDexFinalizer),
            cex_finalizer: None,
        };

        let err = finalizer
            .finalize(&wal, make_receipt(100.0))
            .await
            .expect_err("no route should satisfy min net edge");

        assert!(err.contains("no viable route"));
    }
}
