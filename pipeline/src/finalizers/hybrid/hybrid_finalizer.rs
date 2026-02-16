use async_trait::async_trait;
use candid::Nat;
use std::sync::Arc;

use super::utils::{
    DEX_DUST_MAX_USD, RouteCandidate, RouteVenue, choose_best_route, debt_repaid_f64, dex_slippage_bps,
    estimate_swap_value_usd, is_dust_swap, make_snapshot, net_edge_bps, preview_gross_edge_bps,
    should_force_cex_over_threshold, swapper_id,
};
use crate::{
    config::{ConfigTrait, SwapperMode},
    error::AppError,
    finalizers::{
        cex_finalizer::CexFinalizerLogic,
        dex_finalizer::DexFinalizerLogic,
        finalizer::{Finalizer, FinalizerResult},
    },
    persistance::{FinalizerDecisionSnapshot, ResultStatus, WalStore},
    stages::executor::ExecutionReceipt,
    swappers::{model::SwapRequest, swap_interface::SwapInterface},
    wal::{decode_receipt_wrapper, encode_meta, liq_id_from_receipt, wal_load},
};

use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::TransferActions,
};
use tracing::info;
use tracing::instrument;

pub struct HybridFinalizer<C>
where
    C: ConfigTrait,
{
    pub config: Arc<C>,
    pub trader_transfers: Arc<dyn TransferActions + Send + Sync>,
    // Used only for getting DEX quotes; actual DEX execution is delegated to dex_finalizer.
    pub dex_swapper: Arc<dyn SwapInterface>,
    pub dex_finalizer: Arc<dyn DexFinalizerLogic>, // e.g. KongSwapFinalizer
    pub cex_finalizer: Option<Arc<dyn CexFinalizerLogic>>, // existing CEX finalizer
}

struct FinalizationContext<'a> {
    wal: &'a dyn WalStore,
    mode: &'static str,
    min_required_bps: f64,
    dex_preview: Option<&'a RouteCandidate>,
    cex_preview: Option<&'a RouteCandidate>,
}

impl<'a> FinalizationContext<'a> {
    fn new(wal: &'a dyn WalStore, mode: &'static str, min_required_bps: f64) -> Self {
        Self {
            wal,
            mode,
            min_required_bps,
            dex_preview: None,
            cex_preview: None,
        }
    }

    fn with_previews(
        mut self,
        dex_preview: Option<&'a RouteCandidate>,
        cex_preview: Option<&'a RouteCandidate>,
    ) -> Self {
        self.dex_preview = dex_preview;
        self.cex_preview = cex_preview;
        self
    }

    fn snapshot(&self, chosen: &str, reason: impl Into<String>) -> FinalizerDecisionSnapshot {
        make_snapshot(
            self.mode,
            chosen,
            reason,
            self.min_required_bps,
            self.dex_preview,
            self.cex_preview,
        )
    }
}

impl<C> HybridFinalizer<C>
where
    C: ConfigTrait,
{
    /// Return forced venue in non-hybrid modes.
    fn forced_mode_venue(&self) -> Option<RouteVenue> {
        match self.config.get_swapper_mode() {
            SwapperMode::Dex => Some(RouteVenue::Dex),
            SwapperMode::Cex => Some(RouteVenue::Cex),
            SwapperMode::Hybrid => None,
        }
    }

    /// Build a successful no-swap result and mark WAL succeeded.
    async fn finalize_without_swap(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
    ) -> Result<FinalizerResult, AppError> {
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

    async fn finalize_via_dex(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
    ) -> Result<FinalizerResult, AppError> {
        // Delegate full DEX settlement logic to the underlying DEX finalizer.
        self.dex_finalizer.finalize(wal, receipt).await
    }

    async fn finalize_via_cex(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
    ) -> Result<FinalizerResult, AppError> {
        // Delegate full CEX settlement logic to the underlying CEX finalizer.
        let finalizer = self
            .cex_finalizer
            .as_ref()
            .ok_or_else(|| "missing cex finalizer".to_string())?;

        finalizer.finalize(wal, receipt).await
    }

    async fn finalize_to_recovery(&self, receipt: &ExecutionReceipt) -> Result<FinalizerResult, AppError> {
        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;

        let collateral_received = liq.amounts.collateral_received.clone();
        let fee = receipt.request.collateral_asset.fee();
        let transfer_amount = if collateral_received <= fee {
            Nat::from(0u8)
        } else {
            collateral_received.clone() - fee
        };

        if transfer_amount == 0u8 {
            info!(
                "[hybrid] recovery route: skip transfer due to zero transferable amount (received={} fee={})",
                collateral_received,
                receipt.request.collateral_asset.fee()
            );
            return Ok(FinalizerResult {
                finalized: true,
                swap_result: None,
                swapper: Some("recovery".to_string()),
            });
        }

        match &receipt.request.collateral_asset {
            ChainToken::Icp { .. } => {
                let recovery_account = self.config.get_recovery_account();
                let destination = ChainAccount::Icp(recovery_account);
                self.trader_transfers
                    .transfer(&receipt.request.collateral_asset, &destination, transfer_amount.clone())
                    .await
                    .map_err(|e| format!("recovery transfer failed: {}", e))?;
                info!(
                    "[hybrid] recovery route: transferred collateral={} asset={} to recovery",
                    transfer_amount,
                    receipt.request.collateral_asset.symbol()
                );
            }
            _ => {
                info!(
                    "[hybrid] recovery route: non-ICP collateral {}; skipping transfer",
                    receipt.request.collateral_asset.symbol()
                );
            }
        }

        Ok(FinalizerResult {
            finalized: true,
            swap_result: None,
            swapper: Some("recovery".to_string()),
        })
    }

    /// Execute chosen venue finalizer and annotate resulting swapper label.
    async fn execute_route(
        &self,
        wal: &dyn WalStore,
        receipt: ExecutionReceipt,
        venue: RouteVenue,
        reason: Option<&str>,
    ) -> Result<FinalizerResult, AppError> {
        let mut res = match venue {
            RouteVenue::Dex => self.finalize_via_dex(wal, receipt).await?,
            RouteVenue::Cex => self.finalize_via_cex(wal, receipt).await?,
        };

        if res.swapper.is_none() {
            res.swapper = Some(swapper_id(venue).to_string());
        }

        if let Some(reason) = reason {
            info!("[hybrid] routing -> {} reason={}", venue, reason);
        } else {
            info!("[hybrid] routing -> {}", venue);
        }
        Ok(res)
    }

    /// Build DEX candidate from quote output.
    /// Returns `Err` only when previewing fails (transport/API error).
    async fn build_dex_candidate(
        &self,
        receipt: &ExecutionReceipt,
        swap_req: &SwapRequest,
        debt_repaid_amount: f64,
    ) -> Result<Option<RouteCandidate>, AppError> {
        match self.dex_swapper.quote(swap_req).await {
            Ok(quote) => {
                let estimated_receive_amount =
                    ChainTokenAmount::from_raw(receipt.request.debt_asset.clone(), quote.receive_amount.clone())
                        .to_f64();
                let gross_edge_bps = preview_gross_edge_bps(estimated_receive_amount, debt_repaid_amount);
                let slippage_bps = dex_slippage_bps(quote.slippage);
                // DEX quote receive amount already includes execution impact; avoid double-counting slippage.
                let net_edge_bps = gross_edge_bps;
                info!(
                    "[hybrid] dex preview preview_gross_bps={:.2} slippage_bps={:.2} preview_net_bps={:.2}",
                    gross_edge_bps, slippage_bps, net_edge_bps
                );
                Ok(Some(RouteCandidate {
                    venue: RouteVenue::Dex,
                    gross_edge_bps,
                    net_edge_bps,
                    reason: format!("dex preview net edge {:.2} bps", net_edge_bps),
                }))
            }
            Err(err) => Err(format!("dex preview failed: {}", err).into()),
        }
    }

    /// Build CEX candidate from route preview and configured fee/delay haircuts.
    /// Returns `Err` only when previewing fails (transport/API error).
    async fn build_cex_candidate(
        &self,
        receipt: &ExecutionReceipt,
        debt_repaid_amount: f64,
    ) -> Result<Option<RouteCandidate>, AppError> {
        let Some(cex_finalizer) = self.cex_finalizer.as_ref() else {
            info!("[hybrid] cex preview unavailable: missing cex finalizer");
            return Ok(None);
        };

        match cex_finalizer.preview_route(receipt).await {
            Ok(preview) if preview.is_executable => {
                let gross_edge_bps = preview_gross_edge_bps(preview.estimated_receive_amount, debt_repaid_amount);
                let route_fee_bps = self.config.get_cex_route_fee_bps() as f64;
                let execution_delay_buffer_bps = self.config.get_cex_delay_buffer_bps() as f64;
                // `gross_edge_bps` already reflects previewed execution output (slippage included),
                // so only fixed route fee and delay-risk haircut are subtracted here.
                let net_edge_bps = net_edge_bps(gross_edge_bps, route_fee_bps, execution_delay_buffer_bps);
                info!(
                    "[hybrid] cex preview preview_gross_bps={:.2} slippage_bps={:.2} fee_bps={:.2} delay_bps={:.2} preview_net_bps={:.2}",
                    gross_edge_bps,
                    preview.estimated_slippage_bps,
                    route_fee_bps,
                    execution_delay_buffer_bps,
                    net_edge_bps
                );
                Ok(Some(RouteCandidate {
                    venue: RouteVenue::Cex,
                    gross_edge_bps,
                    net_edge_bps,
                    reason: format!("cex preview net edge {:.2} bps", net_edge_bps),
                }))
            }
            Ok(preview) => {
                info!(
                    "[hybrid] cex preview not executable: {}",
                    preview.reason.unwrap_or_else(|| "unknown".to_string())
                );
                Ok(None)
            }
            Err(err) => Err(format!("cex preview failed: {}", err).into()),
        }
    }

    async fn persist_decision_snapshot(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
        snapshot: FinalizerDecisionSnapshot,
    ) -> Result<(), AppError> {
        let liq_id = liq_id_from_receipt(receipt)?;
        let Some(mut row) = wal_load(wal, &liq_id).await? else {
            info!(
                "[hybrid] decision snapshot skipped; missing wal row liq_id={} chosen={} mode={}",
                liq_id, snapshot.chosen, snapshot.mode
            );
            return Ok(());
        };

        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt wrapper in WAL meta_json for {}", row.id))?;
        wrapper.finalizer_decision = Some(snapshot);
        encode_meta(&mut row, &wrapper)?;
        wal.upsert_result(row)
            .await
            .map_err(|e| format!("wal decision snapshot upsert failed for {}: {}", liq_id, e))?;
        Ok(())
    }

    async fn finalize_with_route_decision(
        &self,
        ctx: FinalizationContext<'_>,
        receipt: ExecutionReceipt,
        venue: RouteVenue,
        reason: String,
    ) -> Result<FinalizerResult, AppError> {
        let chosen = match venue {
            RouteVenue::Dex => "dex",
            RouteVenue::Cex => "cex",
        };
        let snapshot = ctx.snapshot(chosen, reason.clone());
        self.persist_decision_snapshot(ctx.wal, &receipt, snapshot).await?;
        self.execute_route(ctx.wal, receipt, venue, Some(&reason)).await
    }

    async fn finalize_with_recovery_decision(
        &self,
        ctx: FinalizationContext<'_>,
        receipt: ExecutionReceipt,
        reason: String,
    ) -> Result<FinalizerResult, AppError> {
        info!(
            "[hybrid] routing -> recovery reason={} dex_preview_net_bps={:?} cex_preview_net_bps={:?} min_required_bps={:.2}",
            reason,
            ctx.dex_preview.map(|candidate| candidate.net_edge_bps),
            ctx.cex_preview.map(|candidate| candidate.net_edge_bps),
            ctx.min_required_bps
        );
        let snapshot = ctx.snapshot("recovery", reason);
        self.persist_decision_snapshot(ctx.wal, &receipt, snapshot).await?;
        self.finalize_to_recovery(&receipt).await
    }

    async fn finalize_with_error_decision(
        &self,
        ctx: FinalizationContext<'_>,
        receipt: &ExecutionReceipt,
        reason: String,
        error: String,
    ) -> Result<FinalizerResult, AppError> {
        let snapshot = ctx.snapshot("error", reason);
        self.persist_decision_snapshot(ctx.wal, receipt, snapshot).await?;
        Err(error.into())
    }
}

#[async_trait]
impl<C> Finalizer for HybridFinalizer<C>
where
    C: ConfigTrait + Send + Sync,
{
    #[instrument(name = "hybrid.finalize", skip_all, err)]
    async fn finalize(&self, wal: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, AppError> {
        // 1) No swap request:
        // The liquidation is complete and there is nothing to route/swap.
        // We still mark WAL as succeeded to close the lifecycle deterministically.
        let swap_req = match receipt.request.swap_args.clone() {
            Some(req) => req,
            None => return self.finalize_without_swap(wal, &receipt).await,
        };

        // Common decision inputs used by all branches below.
        let debt_repaid_amount = debt_repaid_f64(&receipt)?;
        let min_net_edge_bps = self.config.get_cex_min_net_edge_bps() as f64;

        // 2) Forced mode:
        // Respect operator-selected venue (DEX/CEX), but never execute blindly.
        // We always require a successful preview and positive preview edge first.
        if let Some(forced) = self.forced_mode_venue() {
            let forced_candidate_result = match forced {
                RouteVenue::Dex => self.build_dex_candidate(&receipt, &swap_req, debt_repaid_amount).await,
                RouteVenue::Cex => self.build_cex_candidate(&receipt, debt_repaid_amount).await,
            };

            let forced_candidate = match forced_candidate_result {
                Ok(Some(candidate)) => candidate,
                Ok(None) => {
                    // Forced venue answered but had no executable route; treat as retryable error.
                    let reason = format!("forced_preview_unavailable venue={}", forced);
                    let error = format!("forced {} preview unavailable", forced);
                    return self
                        .finalize_with_error_decision(
                            FinalizationContext::new(wal, "forced", min_net_edge_bps),
                            &receipt,
                            reason,
                            error,
                        )
                        .await;
                }
                Err(err) => {
                    // Preview transport/API failure is retryable, not a recovery trigger.
                    let reason = format!("forced_preview_unavailable venue={} err={}", forced, err);
                    let error = format!("forced {} preview failed: {}", forced, err);
                    return self
                        .finalize_with_error_decision(
                            FinalizationContext::new(wal, "forced", min_net_edge_bps),
                            &receipt,
                            reason,
                            error,
                        )
                        .await;
                }
            };

            let (dex_preview, cex_preview) = match forced {
                RouteVenue::Dex => (Some(&forced_candidate), None),
                RouteVenue::Cex => (None, Some(&forced_candidate)),
            };

            if forced_candidate.net_edge_bps > 0.0 {
                // Forced route is profitable enough (>0), execute on forced venue.
                let reason = format!(
                    "forced_execute venue={} preview_net_bps={:.2}",
                    forced, forced_candidate.net_edge_bps
                );
                return self
                    .finalize_with_route_decision(
                        FinalizationContext::new(wal, "forced", min_net_edge_bps)
                            .with_previews(dex_preview, cex_preview),
                        receipt,
                        forced,
                        reason,
                    )
                    .await;
            }

            // Forced route preview is explicitly non-profitable; move collateral to recovery.
            let reason = format!(
                "forced_non_positive venue={} preview_net_bps={:.2}",
                forced, forced_candidate.net_edge_bps
            );
            return self
                .finalize_with_recovery_decision(
                    FinalizationContext::new(wal, "forced", min_net_edge_bps).with_previews(dex_preview, cex_preview),
                    receipt,
                    reason,
                )
                .await;
        }

        // 3) Dust routing:
        // Small notionals default to DEX to avoid CEX overhead/noise, but we still
        // enforce positive preview edge before execution.
        if is_dust_swap(&receipt, &swap_req) {
            let estimated_swap_value_usd = estimate_swap_value_usd(&receipt, &swap_req);
            let dex_candidate = match self.build_dex_candidate(&receipt, &swap_req, debt_repaid_amount).await {
                Ok(Some(candidate)) => candidate,
                Ok(None) => {
                    // Dust path had no executable DEX preview; return retryable error.
                    let reason = "dust_preview_unavailable".to_string();
                    let error = "dust route preview unavailable on dex".to_string();
                    return self
                        .finalize_with_error_decision(
                            FinalizationContext::new(wal, "dust", min_net_edge_bps),
                            &receipt,
                            reason,
                            error,
                        )
                        .await;
                }
                Err(err) => {
                    // Preview failure should retry instead of silently recovering.
                    let reason = format!("dust_preview_failed err={}", err);
                    let error = format!("dust route preview failed on dex: {}", err);
                    return self
                        .finalize_with_error_decision(
                            FinalizationContext::new(wal, "dust", min_net_edge_bps),
                            &receipt,
                            reason,
                            error,
                        )
                        .await;
                }
            };

            if dex_candidate.net_edge_bps > 0.0 {
                // Dust route is net-positive; execute on DEX.
                let reason = format!(
                    "dust_execute est_value_usd={:.4} threshold_usd={:.2} preview_net_bps={:.2}",
                    estimated_swap_value_usd, DEX_DUST_MAX_USD, dex_candidate.net_edge_bps
                );
                return self
                    .finalize_with_route_decision(
                        FinalizationContext::new(wal, "dust", min_net_edge_bps)
                            .with_previews(Some(&dex_candidate), None),
                        receipt,
                        RouteVenue::Dex,
                        reason,
                    )
                    .await;
            }

            // Dust route is non-profitable; recover collateral instead of swapping.
            let reason = format!("dust_non_positive preview_net_bps={:.2}", dex_candidate.net_edge_bps);
            return self
                .finalize_with_recovery_decision(
                    FinalizationContext::new(wal, "dust", min_net_edge_bps).with_previews(Some(&dex_candidate), None),
                    receipt,
                    reason,
                )
                .await;
        }

        // 4) Hybrid force-threshold fast path:
        // For large notionals we prefer CEX first, but only if preview is executable
        // and positive. Otherwise we either recover (non-positive) or fall back.
        if should_force_cex_over_threshold(&receipt, &swap_req, self.config.get_cex_force_over_usd_threshold()) {
            if self.cex_finalizer.is_none() {
                // Missing CEX finalizer: record fallback reason and continue to normal compare.
                info!(
                    "[hybrid] force-cex threshold reached but cex finalizer is unavailable; continuing with route candidates"
                );
                let snapshot = FinalizationContext::new(wal, "force_threshold", min_net_edge_bps)
                    .snapshot("error", "force_threshold_fallback_missing_cex");
                self.persist_decision_snapshot(wal, &receipt, snapshot).await?;
            } else {
                let estimated_swap_value_usd = estimate_swap_value_usd(&receipt, &swap_req);
                let force_cex_threshold_usd = self.config.get_cex_force_over_usd_threshold();
                match self.build_cex_candidate(&receipt, debt_repaid_amount).await {
                    Ok(Some(cex_candidate)) => {
                        if cex_candidate.net_edge_bps > 0.0 {
                            // Threshold route is positive: execute CEX immediately.
                            let reason = format!(
                                "force_threshold_execute est_value_usd={:.4} threshold_usd={:.2} preview_net_bps={:.2}",
                                estimated_swap_value_usd, force_cex_threshold_usd, cex_candidate.net_edge_bps
                            );
                            return self
                                .finalize_with_route_decision(
                                    FinalizationContext::new(wal, "force_threshold", min_net_edge_bps)
                                        .with_previews(None, Some(&cex_candidate)),
                                    receipt,
                                    RouteVenue::Cex,
                                    reason,
                                )
                                .await;
                        }

                        // Threshold route exists but is non-positive: send to recovery.
                        let reason = format!(
                            "force_threshold_non_positive preview_net_bps={:.2}",
                            cex_candidate.net_edge_bps
                        );
                        return self
                            .finalize_with_recovery_decision(
                                FinalizationContext::new(wal, "force_threshold", min_net_edge_bps)
                                    .with_previews(None, Some(&cex_candidate)),
                                receipt,
                                reason,
                            )
                            .await;
                    }
                    Ok(None) => {
                        // No executable CEX preview at threshold time: continue to normal compare.
                        info!(
                            "[hybrid] force-cex threshold reached but cex preview not executable; continuing with route candidates"
                        );
                        let snapshot = FinalizationContext::new(wal, "force_threshold", min_net_edge_bps)
                            .snapshot("error", "force_threshold_fallback_preview_unavailable");
                        self.persist_decision_snapshot(wal, &receipt, snapshot).await?;
                    }
                    Err(err) => {
                        // Threshold preview failure is retryable and should not auto-recover.
                        let reason = format!("force_threshold_preview_failed err={}", err);
                        let error = format!("force-threshold cex preview failed: {}", err);
                        return self
                            .finalize_with_error_decision(
                                FinalizationContext::new(wal, "force_threshold", min_net_edge_bps),
                                &receipt,
                                reason,
                                error,
                            )
                            .await;
                    }
                }
            }
        }

        // 5) Standard hybrid compare:
        // Preview both venues concurrently, then choose best by net edge.
        let (dex_result, cex_result) = tokio::join!(
            self.build_dex_candidate(&receipt, &swap_req, debt_repaid_amount),
            self.build_cex_candidate(&receipt, debt_repaid_amount)
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
            // If both preview channels fail, we cannot make a safe decision -> retryable error.
            let reason = format!("hybrid_both_preview_errors dex={} cex={}", dex_err, cex_err);
            let error = format!("route preview failed on both venues: {}; {}", dex_err, cex_err);
            return self
                .finalize_with_error_decision(
                    FinalizationContext::new(wal, "hybrid", min_net_edge_bps),
                    &receipt,
                    reason,
                    error,
                )
                .await;
        }

        let dex_candidate = dex_result.ok().flatten();
        let cex_candidate = cex_result.ok().flatten();

        if let Some(chosen) = choose_best_route(dex_candidate.clone(), cex_candidate.clone()) {
            if chosen.net_edge_bps >= min_net_edge_bps {
                // Profitable and above minimum policy threshold: execute chosen venue.
                let reason = format!(
                    "hybrid_execute venue={} preview_net_bps={:.2} detail={}",
                    chosen.venue, chosen.net_edge_bps, chosen.reason
                );
                return self
                    .finalize_with_route_decision(
                        FinalizationContext::new(wal, "hybrid", min_net_edge_bps)
                            .with_previews(dex_candidate.as_ref(), cex_candidate.as_ref()),
                        receipt,
                        chosen.venue,
                        reason,
                    )
                    .await;
            }

            if chosen.net_edge_bps <= 0.0 {
                // Explicitly non-positive route: skip swap, recover collateral.
                let reason = format!(
                    "hybrid_non_positive best_venue={} preview_net_bps={:.2}",
                    chosen.venue, chosen.net_edge_bps
                );
                return self
                    .finalize_with_recovery_decision(
                        FinalizationContext::new(wal, "hybrid", min_net_edge_bps)
                            .with_previews(dex_candidate.as_ref(), cex_candidate.as_ref()),
                        receipt,
                        reason,
                    )
                    .await;
            }

            // Positive but below required policy threshold: do not execute, return no-viable.
            let error = format!(
                "no viable route: preview_best_net_edge_bps={:.2}, min_required_bps={:.2}",
                chosen.net_edge_bps, min_net_edge_bps
            );
            let reason = format!(
                "hybrid_positive_below_min best_venue={} preview_net_bps={:.2} detail={}",
                chosen.venue, chosen.net_edge_bps, chosen.reason
            );
            return self
                .finalize_with_error_decision(
                    FinalizationContext::new(wal, "hybrid", min_net_edge_bps)
                        .with_previews(dex_candidate.as_ref(), cex_candidate.as_ref()),
                    &receipt,
                    reason,
                    error,
                )
                .await;
        }

        // 6) No executable candidates from either venue preview.
        let error = format!(
            "no viable route: no executable preview candidates, min_required_bps={:.2}",
            min_net_edge_bps
        );
        self.finalize_with_error_decision(
            FinalizationContext::new(wal, "hybrid", min_net_edge_bps)
                .with_previews(dex_candidate.as_ref(), cex_candidate.as_ref()),
            &receipt,
            "hybrid_no_executable_preview_candidates".to_string(),
            error,
        )
        .await
    }
}

#[cfg(test)]
#[path = "hybrid_finalizer_tests.rs"]
mod tests;
