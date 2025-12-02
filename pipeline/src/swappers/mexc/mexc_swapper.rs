use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use log::{debug, info};

use crate::swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest};
use crate::swappers::router::SwapVenue;

fn f64_to_nat(v: f64) -> Nat {
    Nat::from(v as u128)
}

fn market_symbol(token_in: &AssetId, token_out: &AssetId) -> String {
    // You will probably want a proper mapping later;
    // for now assume "<IN>/<OUT>" is the MEXC market.
    format!("{}/{}", token_in.symbol, token_out.symbol)
}

pub struct MexcSwapVenue<C: CexBackend> {
    pub client: Arc<C>,
}

impl<C: CexBackend> MexcSwapVenue<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<C: CexBackend> SwapVenue for MexcSwapVenue<C> {
    fn venue_name(&self) -> &'static str {
        "mexc"
    }

    async fn init(&self) -> Result<(), String> {
        // Epmty impl;

        Ok(())
    }

    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        let market = market_symbol(&req.pay_asset, &req.receive_asset);
        let amount_in_f = req.pay_amount.to_f64();

        info!(
            "MEXC quote {} {} -> {} on {}",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            market
        );

        let out_f = self.client.get_quote(&market, amount_in_f).await?;
        debug!("MEXC quote result: in={} out={}", amount_in_f, out_f);

        let leg = SwapQuoteLeg {
            venue: "mexc".to_string(),
            route_id: market.clone(),

            pay_chain: req.pay_asset.chain.clone(),
            pay_symbol: req.pay_asset.symbol.clone(),
            pay_amount: req.pay_amount.value.clone(),

            receive_chain: req.receive_asset.chain.clone(),
            receive_symbol: req.receive_asset.symbol.clone(),
            receive_amount: f64_to_nat(out_f),

            price: if amount_in_f > 0.0 { out_f / amount_in_f } else { 0.0 },
            lp_fee: Nat::from(0u8),
            gas_fee: Nat::from(0u8),
        };

        Ok(SwapQuote {
            pay_asset: req.pay_asset.clone(),
            pay_amount: req.pay_amount.value.clone(),
            receive_asset: req.receive_asset.clone(),
            receive_amount: leg.receive_amount.clone(),
            mid_price: leg.price,
            exec_price: leg.price,
            slippage: 0.0, // you can compute from orderbook later
            legs: vec![leg],
        })
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        let market = market_symbol(&req.pay_asset, &req.receive_asset);
        let amount_in_f = req.pay_amount.to_f64();
        let side = "sell"; // assuming pay_asset is the base

        info!(
            "MEXC swap {} {} -> {} on {}",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            market
        );

        let out_f = self.client.execute_swap(&market, side, amount_in_f).await?;

        let price = if amount_in_f > 0.0 { out_f / amount_in_f } else { 0.0 };

        let leg = SwapQuoteLeg {
            venue: "mexc".to_string(),
            route_id: market.clone(),

            pay_chain: req.pay_asset.chain.clone(),
            pay_symbol: req.pay_asset.symbol.clone(),
            pay_amount: req.pay_amount.value.clone(),

            receive_chain: req.receive_asset.chain.clone(),
            receive_symbol: req.receive_asset.symbol.clone(),
            receive_amount: f64_to_nat(out_f),

            price,
            lp_fee: Nat::from(0u8),
            gas_fee: Nat::from(0u8),
        };

        Ok(SwapExecution {
            swap_id: 0,    // you can fill from WAL or an internal sequence later
            request_id: 0, // same
            status: "filled".to_string(),

            pay_asset: req.pay_asset.clone(),
            pay_amount: req.pay_amount.value.clone(),
            receive_asset: req.receive_asset.clone(),
            receive_amount: leg.receive_amount.clone(),

            mid_price: price,
            exec_price: price,
            slippage: 0.0,

            legs: vec![leg],
            ts: 0, // populate from clock if needed
        })
    }
}
