use std::sync::Arc;

use async_trait::async_trait;
use candid::{Encode, Principal};

use crate::pipeline_agent::PipelineAgent;

#[async_trait]
pub trait PriceOracle: Sync + Send {
    async fn get_price(&self, token_in: &str, token_out: &str) -> Result<(u64, u32), String>;
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
    async fn get_price(&self, token_in: &str, token_out: &str) -> Result<(u64, u32), String> {
        let price = self
            .agent
            .call_query(
                &self.lending_canister,
                "get_price",
                Encode!(&token_in, &token_out).expect("could not encode params"),
            )
            .await?;

        Ok(price)
    }
}
