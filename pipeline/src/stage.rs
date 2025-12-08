use async_trait::async_trait;

#[async_trait]
pub trait PipelineStage<'a, I, O> {
    async fn process(&self, input: &'a I) -> Result<O, String>;
}
