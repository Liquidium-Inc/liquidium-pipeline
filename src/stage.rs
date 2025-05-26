use async_trait::async_trait;

#[async_trait]
pub trait PipelineStage<I, O> {
    async fn process(&self, input: I) -> Result<O, String>;
}
