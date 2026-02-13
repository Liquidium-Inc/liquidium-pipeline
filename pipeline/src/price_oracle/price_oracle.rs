use std::sync::Arc;

use crate::error::{AppError, AppResult, error_codes};
use async_trait::async_trait;
use candid::{Encode, Nat, Principal};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait PriceOracle: Sync + Send {
    async fn get_price(&self, token_in: &str, token_out: &str) -> AppResult<(Nat, u32)>;
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
    async fn get_price(&self, token_in: &str, token_out: &str) -> AppResult<(Nat, u32)> {
        let args = Encode!(&token_in, &token_out).map_err(|e| {
            AppError::from_def(error_codes::ENCODE_ERROR).with_context(format!("price query encode error: {}", e))
        })?;
        let res = self
            .agent
            .call_query::<Result<(Nat, u32), String>>(&self.lending_canister, "get_price", args)
            .await?;
        res.map_err(|e| {
            AppError::from_def(error_codes::EXTERNAL_CALL_FAILED).with_context(format!("get_price error: {e}"))
        })
    }
}
