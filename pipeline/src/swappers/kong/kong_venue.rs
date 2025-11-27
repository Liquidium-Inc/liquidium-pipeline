use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use log::info;

use crate::swappers::kong::kong_swapper::KongSwapSwapper;
use crate::swappers::kong::kong_types::{
    SwapAmountsReply as KongSwapAmountsReply, SwapArgs as KongSwapArgs, SwapReply as KongSwapReply,
};
use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};
use crate::swappers::router::SwapVenue;

/// KongVenue is a generic venue wrapper over the KongSwapSwapper.
/// It takes a generic SwapRequest / SwapQuote / SwapExecution and
/// bridges them to the Kong-specific types and canister calls.
pub struct KongVenue<A: PipelineAgent> {
    pub swapper: Arc<KongSwapSwapper<A>>,
    pub tokens: Vec<ChainToken>, // registry of known ICRC tokens (by symbol)
}

impl<A: PipelineAgent> KongVenue<A> {
    pub fn new(swapper: Arc<KongSwapSwapper<A>>, tokens: Vec<ChainToken>) -> Self {
        Self { swapper, tokens }
    }

    fn find_token(&self, symbol: &str) -> Result<ChainToken, String> {
        self.tokens
            .iter()
            .find(|t| t.symbol() == symbol)
            .cloned()
            .ok_or_else(|| format!("Unknown ICRC token symbol in KongVenue: {}", symbol))
    }

    fn build_amount(&self, req: &SwapRequest) -> Result<ChainTokenAmount, String> {
        let pay_token = self.find_token(&req.pay_asset.symbol)?;
        let value = req.pay_amount.value.clone();
        Ok(ChainTokenAmount {
            token: pay_token,
            value,
        })
    }
}

#[async_trait]
impl<A> SwapVenue for KongVenue<A>
where
    A: PipelineAgent + Send + Sync + 'static,
{
    fn venue_name(&self) -> &'static str {
        "kong"
    }

    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        let token_out = self.find_token(&req.receive_asset.symbol)?;
        let token_in = self.find_token(&req.pay_asset.symbol)?;

        info!(
            "KongVenue quote {} {} -> {} | {}",
            req.pay_amount.formatted(),
            req.pay_amount.token.symbol(),
            token_out.symbol(),
            req.receive_asset.symbol
        );

        // Use the existing KongSwapSwapper IcrcSwapInterface implementation
        let kong_reply: KongSwapAmountsReply = self
            .swapper
            .get_swap_info(&token_in, &token_out, &req.pay_amount)
            .await?;

        // Convert KongSwapAmountsReply -> generic SwapQuote via adapter
        Ok(SwapQuote::from(kong_reply))
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        // Generic SwapRequest -> KongSwapArgs via adapter
        let mut kong_req: KongSwapArgs = KongSwapArgs::from(req.clone());
        kong_req.pay_amount = req.pay_amount.value.clone();

        info!(
            "KongVenue execute {} {} -> {}",
            kong_req.pay_amount, kong_req.pay_token, kong_req.receive_token
        );

        // Execute swap on Kong
        let reply: KongSwapReply = self.swapper.swap(kong_req).await?;

        // KongSwapReply -> generic SwapExecution via adapter
        Ok(SwapExecution::from(reply))
    }
}
