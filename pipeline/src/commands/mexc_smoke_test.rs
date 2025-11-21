use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::swappers::mexc::mexc_adapter::MexcAdapter;
use crate::swappers::model::SwapRequest;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;
use log::info;

use crate::context::init_context;
use crate::finalizers::mexc::mexc_finalizer::{MexcBridge, MexcFinalizer};

use crate::persistance::sqlite::SqliteWalStore;
use crate::swappers::swap_interface::SwapInterface;

pub async fn mexc_finalizer_smoke_test() -> Result<(), String> {
    // Load config so we have TokenRegistry + accounts + agent available.
    let ctx = init_context().await?;

    // Load API keys from env
    let api_key = env::var("MEXC_API_KEY").map_err(|_| "MEXC_API_KEY not set".to_string())?;
    let api_secret = env::var("MEXC_API_SECRET").map_err(|_| "MEXC_API_SECRET not set".to_string())?;

    // Build ccxt adapter
    let cex_client = Arc::new(MexcAdapter::new(&api_key, &api_secret));

    // Minimal DB
    let db = Arc::new(SqliteWalStore::new(&ctx.config.db_path).map_err(|e| e.to_string())?);

    // Build a dumb swapper for MEXC. This should wrap the ccxt backend.
    let swapper: Arc<dyn SwapInterface + Send + Sync> = Arc::new(
        crate::swappers::mexc::mexc_swapper::MexcSwapper::new(cex_client.clone()),
    );

    // Build finalizer
    let bridge = Arc::new(MexcBridge::new(
        ctx.config.clone(),
        db.clone(),
        cex_client.clone(),
        ctx.registry.clone(),
        ctx.main_transfers.actions(),
        swapper.clone(),
    ));

    let finalizer = Arc::new(MexcFinalizer::new(bridge));

    // Hardcode test swap using real TokenRegistry assets
    // OGY on ICP as input
    // let pay = AssetId {
    //     chain: "icp".to_string(),
    //     address: "lkwrt-vyaaa-aaaaq-aadhq-cai".to_string(),
    //     symbol: "OGY".to_string(),
    // };

    // USDT (ERC20) on Ethereum as output
    let usdt_entries = ctx.registry.by_symbol("USDT");
    let (receive_asset_id, token) = usdt_entries
        .get(0)
        .cloned()
        .ok_or_else(|| "USDT not found in token registry".to_string())?;

    // let token = ctx.registry.get(&pay).unwrap();

    let pay_amount = ChainTokenAmount::from_formatted(token.clone(), 1.0f64);
    let request = SwapRequest {
        pay_asset: receive_asset_id.clone(),
        pay_amount,
        receive_asset: receive_asset_id.clone(),
        receive_address: Some(ctx.evm_address),
        max_slippage_bps: Some(100), // 1% slippage cap for the smoke test
        venue_hint: Some("mexc".to_string()),
    };

    let t0 = Instant::now();
    let balance_before = ctx.main_service.get_balance(&receive_asset_id).await?;

    finalizer
        .execute_mexc_swap("mexc-smoke-test", 0, request)
        .await
        .map_err(|e| e.to_string())?;

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let current_balance = ctx.main_service.get_balance(&receive_asset_id).await?;

        info!("MEXC smoke test:wating for balance update");

        if current_balance != balance_before {
            let elapsed = t0.elapsed();
            info!(
                "MEXC smoke test: balance updated after {:?} (before: {:?}, after: {:?})",
                elapsed, balance_before, current_balance
            );
            break;
        }

        if t0.elapsed().as_secs() > 120 {
            info!("MEXC smoke test: balance did not update within 120 seconds");
            break;
        }
    }

    info!("Smoke test completed.");

    Ok(())
}
