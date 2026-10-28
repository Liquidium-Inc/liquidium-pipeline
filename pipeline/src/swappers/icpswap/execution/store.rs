use async_trait::async_trait;

use super::types::IcpswapExecutionState;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapExecutionStateStore: Send + Sync {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String>;
    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String>;
}
