use std::env;
use std::sync::Arc;
use std::time::Instant;


use crate::swappers::model::SwapRequest;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use log::info;


use crate::context::init_context;
use crate::finalizers::mexc::mexc_finalizer::{MexcBridge, MexcFinalizer};
use crate::swappers::mexc::mexc_ccxt_adapter::MexcCcxtAdapter;

use crate::persistance::sqlite::SqliteWalStore;
use crate::swappers::swap_interface::SwapInterface;

// Note: This is a dumb smoke test using the real MexcFinalizer.
// It does not use the liquidation loop. It only:
// - builds deps by hand
// - constructs a hardcoded SwapRequest
// - calls execute_mexc_swap
// - logs timings

pub async fn mexc_finalizer_smoke_test() -> Result<(), String> {
    // Load config so we have TokenRegistry + accounts + agent available.
    let ctx = init_context().await?;

    // Load API keys from env
    let api_key = env::var("MEXC_API_KEY").map_err(|_| "MEXC_API_KEY not set".to_string())?;
    let api_secret = env::var("MEXC_API_SECRET").map_err(|_| "MEXC_API_SECRET not set".to_string())?;

    // Build ccxt adapter
    let ccxt = Arc::new(MexcCcxtAdapter::new(&api_key, &api_secret));

    // Minimal DB
    let db = Arc::new(SqliteWalStore::new(&ctx.config.db_path).map_err(|e| e.to_string())?);

    // Build a dumb swapper for MEXC. This should wrap the ccxt backend.
    let swapper: Arc<dyn SwapInterface + Send + Sync> =
        Arc::new(crate::swappers::mexc::mexc_swapper::MexcSwapper::new(ccxt.clone()));

    // Build finalizer
    let bridge = Arc::new(MexcBridge::new(
        ctx.config,
        db.clone(),
        ccxt.clone(),
        ctx.registry.clone(),
        ctx.main_transfers.actions(),
        swapper.clone(),
    ));

    let finalizer = Arc::new(MexcFinalizer::new(bridge));

    // Hardcode test swap using real TokenRegistry assets
    // OGY on ICP as input
    let pay = AssetId {
        chain: "icp".to_string(),
        address: "lkwrt-vyaaa-aaaaq-aadhq-cai".to_string(),
        symbol: "OGY".to_string(),
    };

    // USDT (ERC20) on Ethereum as output
    let receive = AssetId {
        chain: "evm-eth".to_string(),
        address: "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(),
        symbol: "USDT".to_string(),
    };

    let request = SwapRequest {
        pay_asset: pay,
        pay_amount: 1000u64.into(),
        receive_asset: receive,
        receive_address: Some(ctx.evm_address),
        max_slippage_bps: Some(100), // 1% slippage cap for the smoke test
        referred_by: None,
        venue_hint: Some("mexc".to_string()),
    };

    info!("Running MEXC finalizer smoke test...");

    let t0 = Instant::now();
    let res = finalizer.execute_mexc_swap("mexc-smoke-test", 0, request).await?;
    let t1 = Instant::now();

    info!("Smoke test completed in {:?}. Result: {:?}", t1.duration_since(t0), res);

    Ok(())
}
