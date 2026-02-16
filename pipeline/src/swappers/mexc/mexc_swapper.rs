use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, OrderBookLevel};
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use log::{debug, info};

use crate::error::AppError;
use crate::swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest};
use crate::swappers::router::SwapVenue;

fn f64_to_nat(v: f64) -> Nat {
    Nat::from(v as u128)
}

const DEFAULT_ORDERBOOK_LIMIT: u32 = 50;
const LIQUIDITY_EPS: f64 = 1e-9;

fn quote_sell_from_bids(bids: &[OrderBookLevel], amount_in: f64) -> Result<f64, AppError> {
    if amount_in <= 0.0 {
        return Err("amount_in must be positive".into());
    }

    let mut remaining = amount_in;
    let mut proceeds = 0.0;

    for level in bids {
        if remaining <= 0.0 {
            break;
        }
        if level.quantity <= 0.0 || level.price <= 0.0 {
            continue;
        }

        let take = remaining.min(level.quantity);
        proceeds += take * level.price;
        remaining -= take;
    }

    if remaining > LIQUIDITY_EPS {
        return Err("not enough bid liquidity".into());
    }

    Ok(proceeds)
}

fn quote_buy_from_asks(asks: &[OrderBookLevel], quote_in: f64) -> Result<f64, AppError> {
    if quote_in <= 0.0 {
        return Err("amount_in must be positive".into());
    }

    let mut remaining_quote = quote_in;
    let mut base_out = 0.0;

    for level in asks {
        if remaining_quote <= 0.0 {
            break;
        }
        if level.quantity <= 0.0 || level.price <= 0.0 {
            continue;
        }

        let max_base = remaining_quote / level.price;
        let take = level.quantity.min(max_base);
        let cost = take * level.price;
        base_out += take;
        remaining_quote -= cost;
    }

    if remaining_quote > LIQUIDITY_EPS {
        return Err("not enough ask liquidity".into());
    }

    Ok(base_out)
}

pub struct MexcSwapVenue<C: CexBackend> {
    pub client: Arc<C>,
}

impl<C: CexBackend> MexcSwapVenue<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }

    async fn resolve_trade(
        &self,
        pay_asset: &AssetId,
        receive_asset: &AssetId,
        amount_in: f64,
    ) -> Result<(String, String, f64), AppError> {
        let pay = pay_asset.symbol.to_ascii_uppercase();
        let recv = receive_asset.symbol.to_ascii_uppercase();

        let sell_market = format!("{}/{}", pay, recv);
        let buy_market = format!("{}/{}", recv, pay);
        let mut errors: Vec<String> = Vec::new();

        match self
            .client
            .get_orderbook(&sell_market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await
        {
            Ok(orderbook) => {
                if !orderbook.bids.is_empty() {
                    let out = quote_sell_from_bids(&orderbook.bids, amount_in)?;
                    return Ok((sell_market, "sell".to_string(), out));
                }
                errors.push(format!("{} has no bids", sell_market));
            }
            Err(err) => errors.push(format!("{}: {}", sell_market, err)),
        }

        match self
            .client
            .get_orderbook(&buy_market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await
        {
            Ok(orderbook) => {
                if !orderbook.asks.is_empty() {
                    let out = quote_buy_from_asks(&orderbook.asks, amount_in)?;
                    return Ok((buy_market, "buy".to_string(), out));
                }
                errors.push(format!("{} has no asks", buy_market));
            }
            Err(err) => errors.push(format!("{}: {}", buy_market, err)),
        }

        Err(format!(
            "could not resolve direct market for {} -> {} ({})",
            pay,
            recv,
            errors.join(" | ")
        )
        .into())
    }
}

#[async_trait]
impl<C: CexBackend> SwapVenue for MexcSwapVenue<C> {
    fn venue_name(&self) -> &'static str {
        "mexc"
    }

    async fn init(&self) -> Result<(), AppError> {
        // Empty impl

        Ok(())
    }

    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, AppError> {
        let amount_in_f = req.pay_amount.to_f64();
        let (market, side, out_f) = self
            .resolve_trade(&req.pay_asset, &req.receive_asset, amount_in_f)
            .await?;

        info!(
            "MEXC quote {} {} -> {} on {} ({})",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            market,
            side
        );

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

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, AppError> {
        let amount_in_f = req.pay_amount.to_f64();
        let (market, side, _out_f) = self
            .resolve_trade(&req.pay_asset, &req.receive_asset, amount_in_f)
            .await?;

        info!(
            "MEXC swap {} {} -> {} on {} ({})",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            market,
            side
        );

        let out_f = self.client.execute_swap(&market, &side, amount_in_f).await?;

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
            approval_count: None,
            ts: 0, // populate from clock if needed
        })
    }
}
