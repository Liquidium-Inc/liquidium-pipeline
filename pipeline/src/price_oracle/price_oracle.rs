use std::sync::Arc;

use async_trait::async_trait;
use candid::{Encode, Nat, Principal};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait PriceOracle: Sync + Send {
    async fn get_price(&self, token_in: &str, token_out: &str) -> Result<(Nat, u32), String>;
}

pub struct LiquidationPriceOracle<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub lending_canister: Principal,
}

impl<A> LiquidationPriceOracle<A>
where
    A: PipelineAgent,
{
    pub fn new(agent: Arc<A>, lending_canister: Principal) -> Self {
        Self {
            agent,
            lending_canister,
        }
    }
}

#[async_trait]
impl<A: PipelineAgent> PriceOracle for LiquidationPriceOracle<A> {
    // Fetches price data from the lending canister
    async fn get_price(&self, token_in: &str, token_out: &str) -> Result<(Nat, u32), String> {
        let args =
            Encode!(&token_in, &token_out).map_err(|e| format!("price query encode error: {}", e))?;
        self.agent
            .call_query::<Result<(Nat, u32), String>>(
                &self.lending_canister,
                "get_price",
                args,
            )
            .await?
    }
}
