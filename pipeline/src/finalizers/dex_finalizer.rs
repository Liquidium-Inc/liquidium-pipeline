use std::{fmt, sync::Arc};

use async_trait::async_trait;
use log::debug;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    finalizers::finalizer::{Finalizer, FinalizerResult},
    persistance::{FinalizerDecisionSnapshot, WalStore},
    stages::executor::{ExecutionReceipt, ExecutionStatus},
    swappers::model::{SwapExecution, SwapQuote, SwapRequest},
};

#[derive(Clone)]
pub struct DexRoutePreview {
    pub quote: SwapQuote,
    venue: Arc<str>,
    route: Arc<[u8]>,
}

impl DexRoutePreview {
    pub fn new<T: Serialize>(quote: SwapQuote, venue: impl Into<Arc<str>>, route: &T) -> Result<Self, String> {
        Ok(Self {
            quote,
            venue: venue.into(),
            route: serde_json::to_vec(route)
                .map_err(|error| format!("failed encoding DEX route preview: {error}"))?
                .into(),
        })
    }

    pub(crate) fn route<T: DeserializeOwned>(&self, expected_venue: &str) -> Result<T, String> {
        if self.venue.as_ref() != expected_venue {
            return Err(format!(
                "DEX route belongs to venue {}, not {expected_venue}",
                self.venue
            ));
        }
        serde_json::from_slice(&self.route)
            .map_err(|error| format!("failed decoding {} DEX route preview: {error}", self.venue))
    }
}

impl fmt::Debug for DexRoutePreview {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DexRoutePreview")
            .field("quote", &self.quote)
            .field("venue", &self.venue)
            .finish_non_exhaustive()
    }
}

/// One coherent DEX dependency for previewing and executing a persisted route.
/// A future implementation may aggregate several DEX venues behind this trait.
#[async_trait]
pub trait DexRouteFinalizer: Finalizer + Send + Sync {
    fn venue_id(&self) -> &'static str;

    async fn preview_route(&self, request: &SwapRequest) -> Result<DexRoutePreview, String>;

    async fn has_committed_route(&self, wal: &dyn WalStore, receipt: &ExecutionReceipt) -> Result<bool, String>;

    async fn commit_route(
        &self,
        wal: &dyn WalStore,
        receipt: &ExecutionReceipt,
        decision: FinalizerDecisionSnapshot,
        preview: DexRoutePreview,
    ) -> Result<(), String>;
}

// Tunables
const BASE_SLIPPAGE_BPS: u32 = 125; // 1.25%
const STEP_SLIPPAGE_BPS: u32 = 50; // +0.5% per retry
const MAX_SLIPPAGE_BPS: u32 = 500; // 5.0% cap
const MAX_SLIPPAGE_RETRIES: u32 = 3; // total attempts = MAX_SLIPPAGE_RETRIES + 1

fn slippage_for_retry(retry: u32, explicit_cap: Option<u32>) -> u32 {
    // Start at BASE_SLIPPAGE_BPS and bump per retry, respecting the explicit cap if provided.
    let cap = explicit_cap.unwrap_or(MAX_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS);
    let base = BASE_SLIPPAGE_BPS.min(cap);
    let bump = STEP_SLIPPAGE_BPS.saturating_mul(retry);
    base.saturating_add(bump).min(cap)
}

#[async_trait]
pub trait DexFinalizerLogic: Send + Sync {
    async fn swap(&self, req: &SwapRequest) -> Result<SwapExecution, String>;

    async fn swap_with_slippage_retry(&self, swap_req: SwapRequest) -> Result<SwapExecution, String> {
        let mut last_err: Option<String> = None;

        for retry in 0..=MAX_SLIPPAGE_RETRIES {
            let mut req = swap_req.clone();

            let eff_slippage = slippage_for_retry(retry, req.max_slippage_bps);
            req.max_slippage_bps = Some(eff_slippage);

            debug!("[Slippage] {}", eff_slippage);

            match self.swap(&req).await {
                Ok(exec) => return Ok(exec),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "swap failed with no error".to_string()))
    }
}

#[async_trait]
impl Finalizer for dyn DexFinalizerLogic {
    async fn finalize(&self, _: &dyn WalStore, receipt: ExecutionReceipt) -> Result<FinalizerResult, String> {
        // Only finalize successful executions
        if !matches!(receipt.status, ExecutionStatus::Success) {
            return Ok(FinalizerResult::noop());
        }

        // If no swap is needed, noop
        let Some(swap_req) = &receipt.request.swap_args else {
            return Ok(FinalizerResult::noop());
        };

        let swap_exec = self.swap_with_slippage_retry(swap_req.clone()).await?;

        let finlizer_result = FinalizerResult {
            swap_result: Some(swap_exec),
            finalized: true,
            swapper: None,
            reason: None,
        };

        Ok(finlizer_result)
    }
}
