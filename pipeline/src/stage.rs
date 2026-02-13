use async_trait::async_trait;

use crate::error::AppResult;

#[async_trait]
pub trait PipelineStage<'a, I, O> {
    async fn process(&self, input: &'a I) -> AppResult<O>;
}
