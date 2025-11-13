use async_trait::async_trait;

use crate::{
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    swappers::{kong::kong_types::{SwapAmountsReply, SwapArgs, SwapReply}, model::{AssetId, SwapExecution, SwapQuote, SwapRequest}},
};

use std::sync::Arc;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcrcSwapInterface: Send + Sync {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String>;

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String>;
}

#[async_trait]
pub trait SwapInterface: Send + Sync {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

pub struct IcrcSwapAdapter<S: SwapInterface> {
    inner: Arc<S>,
    ic_chain: String, // usually "IC"
}

impl<S: SwapInterface> IcrcSwapAdapter<S> {
    pub fn new(inner: Arc<S>, ic_chain: impl Into<String>) -> Self {
        Self {
            inner,
            ic_chain: ic_chain.into(),
        }
    }

    fn ic_asset(&self, symbol: &str) -> AssetId {
        AssetId {
            chain: self.ic_chain.clone(),
            symbol: symbol.to_string(),
        }
    }
}

#[async_trait]
impl<S> IcrcSwapInterface for IcrcSwapAdapter<S>
where
    S: SwapInterface + Send + Sync + 'static,
{
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String> {
        let req = SwapRequest {
            pay_asset: self.ic_asset(&token_in.symbol),
            pay_amount: amount.value.clone(),
            pay_tx_ref: None,
            receive_asset: self.ic_asset(&token_out.symbol),
            receive_address: None,
            max_slippage_bps: None,
            referred_by: None,
            venue_hint: None,
        };

        let quote = self.inner.quote(&req).await?;

        // either:
        // - map SwapQuote -> Kong-like SwapAmountsReply, or
        // - just use Kong directly for this adapter
        // For now, if you mainly use Kong for ICRC paths, keep calling KongVenue directly.
        todo!("Map SwapQuote into SwapAmountsReply");
    }

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String> {
        // same idea: build SwapRequest from args, call self.inner.execute
        // and map SwapExecution into SwapReply
        todo!()
    }
}
