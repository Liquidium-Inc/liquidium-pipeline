use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use log::info;

use crate::icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount};
use crate::pipeline_agent::PipelineAgent;
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
    pub tokens: Vec<IcrcToken>, // registry of known ICRC tokens (by symbol)
}

impl<A: PipelineAgent> KongVenue<A> {
    pub fn new(swapper: Arc<KongSwapSwapper<A>>, tokens: Vec<IcrcToken>) -> Self {
        Self { swapper, tokens }
    }

    fn ensure_pay_amount(req: &SwapRequest) -> Result<Nat, String> {
        // For now just use req.pay_amount; if you ever support
        // "receive_exact" flows, you can adapt here.
        Ok(req.pay_amount.clone())
    }

    fn find_token(&self, symbol: &str) -> Result<IcrcToken, String> {
        self.tokens
            .iter()
            .find(|t| t.symbol == symbol)
            .cloned()
            .ok_or_else(|| format!("Unknown ICRC token symbol in KongVenue: {}", symbol))
    }

    fn build_amount(&self, req: &SwapRequest) -> Result<IcrcTokenAmount, String> {
        let pay_token = self.find_token(&req.pay_asset.symbol)?;
        let value = Self::ensure_pay_amount(req)?;
        Ok(IcrcTokenAmount {
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
        // Resolve ICRC tokens from generic AssetId
        let token_in = self.find_token(&req.pay_asset.symbol)?;
        let token_out = self.find_token(&req.receive_asset.symbol)?;
        let amount = self.build_amount(req)?;

        info!(
            "KongVenue quote {} {} -> {}",
            amount.value, token_in.symbol, token_out.symbol
        );

        // Use the existing KongSwapSwapper IcrcSwapInterface implementation
        let kong_reply: KongSwapAmountsReply = self.swapper.get_swap_info(&token_in, &token_out, &amount).await?;

        // Convert KongSwapAmountsReply -> generic SwapQuote via adapter
        Ok(SwapQuote::from(kong_reply))
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        // Generic SwapRequest -> KongSwapArgs via adapter
        let mut kong_req: KongSwapArgs = KongSwapArgs::from(req.clone());
        kong_req.pay_amount = Self::ensure_pay_amount(req)?;

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
