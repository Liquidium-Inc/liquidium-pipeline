use candid::Principal;

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PipelineAgent: Send + Sync {
    async fn call_query(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<Vec<u8>, String>;
    async fn call_update(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl PipelineAgent for ic_agent::Agent {
    async fn call_query(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        self.query(&canister, method)
            .with_arg(arg)
            .call()
            .await
            .map_err(|e| e.to_string())
    }
    async fn call_update(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        self.update(&canister, method)
            .with_arg(arg)
            .call_and_wait()
            .await
            .map_err(|e| e.to_string())
    }
}
