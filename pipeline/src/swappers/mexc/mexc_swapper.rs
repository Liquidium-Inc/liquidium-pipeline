use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use log::{debug, info};

use crate::swappers::mexc::orderbook_quote::{simulate_buy_from_asks, simulate_sell_from_bids};
use crate::swappers::model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, adverse_price_impact_bps};
use crate::swappers::venue::{ExecutableSwapVenue, SwapVenue};

fn f64_to_nat(v: f64) -> Nat {
    Nat::from(v as u128)
}

const DEFAULT_ORDERBOOK_LIMIT: u32 = 50;
const LIQUIDITY_EPS: f64 = 1e-9;

struct ResolvedTradeQuote {
    market: String,
    side: String,
    estimated_output: f64,
    reference_price: f64,
    execution_price: f64,
    estimated_price_impact_bps: f64,
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
    ) -> Result<ResolvedTradeQuote, String> {
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
                    let (output, vwap, _side_native_impact_bps, unfilled) =
                        simulate_sell_from_bids(&orderbook.bids, amount_in)?;
                    if unfilled > LIQUIDITY_EPS {
                        return Err("not enough bid liquidity".to_string());
                    }
                    return Ok(ResolvedTradeQuote {
                        market: sell_market,
                        side: "sell".to_string(),
                        estimated_output: output,
                        reference_price: orderbook.bids[0].price,
                        execution_price: vwap,
                        estimated_price_impact_bps: adverse_price_impact_bps(orderbook.bids[0].price, vwap),
                    });
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
                    let (output, vwap, _side_native_impact_bps, unspent) =
                        simulate_buy_from_asks(&orderbook.asks, amount_in)?;
                    if unspent > LIQUIDITY_EPS {
                        return Err("not enough ask liquidity".to_string());
                    }
                    let reference_price = 1.0 / orderbook.asks[0].price;
                    let execution_price = 1.0 / vwap;
                    return Ok(ResolvedTradeQuote {
                        market: buy_market,
                        side: "buy".to_string(),
                        estimated_output: output,
                        // Common quote prices are receive units per pay unit;
                        // MEXC asks are quote units per base unit, so invert.
                        reference_price,
                        execution_price,
                        estimated_price_impact_bps: adverse_price_impact_bps(reference_price, execution_price),
                    });
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
        ))
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
        let amount_in_f = req.pay_amount.to_f64();
        let resolved = self
            .resolve_trade(&req.pay_asset, &req.receive_asset, amount_in_f)
            .await?;

        info!(
            "MEXC quote {} {} -> {} on {} ({})",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            resolved.market,
            resolved.side
        );

        debug!(
            "MEXC quote result: in={} out={} impact_bps={}",
            amount_in_f, resolved.estimated_output, resolved.estimated_price_impact_bps
        );

        let leg = SwapQuoteLeg {
            venue: "mexc".to_string(),
            route_id: resolved.market.clone(),

            pay_chain: req.pay_asset.chain.clone(),
            pay_symbol: req.pay_asset.symbol.clone(),
            pay_amount: req.pay_amount.value.clone(),

            receive_chain: req.receive_asset.chain.clone(),
            receive_symbol: req.receive_asset.symbol.clone(),
            receive_amount: f64_to_nat(resolved.estimated_output),

            price: resolved.execution_price,
            lp_fee: Nat::from(0u8),
            gas_fee: Nat::from(0u8),
        };

        Ok(SwapQuote {
            pay_asset: req.pay_asset.clone(),
            pay_amount: req.pay_amount.value.clone(),
            receive_asset: req.receive_asset.clone(),
            receive_amount: leg.receive_amount.clone(),
            mid_price: resolved.reference_price,
            exec_price: resolved.execution_price,
            estimated_price_impact_bps: resolved.estimated_price_impact_bps,
            legs: vec![leg],
        })
    }
}

#[async_trait]
impl<C: CexBackend> ExecutableSwapVenue for MexcSwapVenue<C> {
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        let amount_in_f = req.pay_amount.to_f64();
        let resolved = self
            .resolve_trade(&req.pay_asset, &req.receive_asset, amount_in_f)
            .await?;

        info!(
            "MEXC swap {} {} -> {} on {} ({})",
            req.pay_amount.formatted(),
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            resolved.market,
            resolved.side
        );

        let out_f = self
            .client
            .execute_swap(&resolved.market, &resolved.side, amount_in_f)
            .await?;

        let price = if amount_in_f > 0.0 { out_f / amount_in_f } else { 0.0 };

        let leg = SwapQuoteLeg {
            venue: "mexc".to_string(),
            route_id: resolved.market,

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
            realized_slippage_bps: 0.0,

            legs: vec![leg],
            approval_count: None,
            ts: 0, // populate from clock if needed
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liquidium_pipeline_connectors::backend::cex_backend::{MockCexBackend, OrderBook, OrderBookLevel};
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

    fn token_with_decimals(symbol: &str, decimals: u8) -> ChainToken {
        ChainToken::EvmNative {
            chain: "test".to_string(),
            symbol: symbol.to_string(),
            decimals,
            fee: Nat::from(0u8),
        }
    }

    fn token(symbol: &str) -> ChainToken {
        token_with_decimals(symbol, 0)
    }

    #[tokio::test]
    async fn quote_normalizes_best_bid_vwap_and_price_impact() {
        let pay = token("ICP");
        let receive = token("USDC");
        let request = SwapRequest {
            pay_asset: pay.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(pay, Nat::from(3u8)),
            receive_asset: receive.asset_id(),
            receive_address: None,
            max_slippage_bps: Some(100),
            venue_hint: Some("mexc".to_string()),
        };
        let mut backend = MockCexBackend::new();
        backend
            .expect_get_orderbook()
            .times(1)
            .withf(|market, limit| market == "ICP/USDC" && *limit == Some(DEFAULT_ORDERBOOK_LIMIT))
            .return_once(|_, _| {
                Ok(OrderBook {
                    bids: vec![
                        OrderBookLevel {
                            price: 10.0,
                            quantity: 1.0,
                        },
                        OrderBookLevel {
                            price: 9.9,
                            quantity: 2.0,
                        },
                    ],
                    asks: vec![],
                })
            });

        let quote = MexcSwapVenue::new(Arc::new(backend))
            .quote(&request)
            .await
            .expect("MEXC quote");

        assert_eq!(quote.mid_price, 10.0);
        assert!((quote.exec_price - (29.8 / 3.0)).abs() < 1e-9);
        assert!((quote.estimated_price_impact_bps - 66.666_666_666_67).abs() < 1e-9);
    }

    #[tokio::test]
    async fn buy_quote_normalizes_ask_prices_to_receive_per_pay_units() {
        let pay = token_with_decimals("USDC", 1);
        let receive = token("ICP");
        let request = SwapRequest {
            pay_asset: pay.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(pay, Nat::from(298u16)),
            receive_asset: receive.asset_id(),
            receive_address: None,
            max_slippage_bps: Some(100),
            venue_hint: Some("mexc".to_string()),
        };
        let mut backend = MockCexBackend::new();
        backend
            .expect_get_orderbook()
            .times(2)
            .returning(|market, _| match market {
                "USDC/ICP" => Ok(OrderBook {
                    bids: vec![],
                    asks: vec![],
                }),
                "ICP/USDC" => Ok(OrderBook {
                    bids: vec![],
                    asks: vec![
                        OrderBookLevel {
                            price: 9.9,
                            quantity: 1.0,
                        },
                        OrderBookLevel {
                            price: 9.95,
                            quantity: 2.0,
                        },
                    ],
                }),
                other => Err(format!("unexpected market {other}")),
            });

        let quote = MexcSwapVenue::new(Arc::new(backend))
            .quote(&request)
            .await
            .expect("MEXC buy quote");
        let vwap = 29.8 / 3.0;

        assert!((quote.mid_price - (1.0 / 9.9)).abs() < 1e-9);
        assert!((quote.exec_price - (1.0 / vwap)).abs() < 1e-9);
        assert!((quote.estimated_price_impact_bps - ((vwap - 9.9) / vwap * 10_000.0)).abs() < 1e-9);
    }
}
