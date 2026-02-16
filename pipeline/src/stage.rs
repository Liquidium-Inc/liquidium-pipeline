use async_trait::async_trait;

use crate::error::AppError;

#[async_trait]
pub trait PipelineStage<'a, I, O> {
    async fn process(&self, input: &'a I) -> Result<O, AppError>;
}
