use candid::{decode_args, decode_one, utils::ArgumentDecoder, CandidType, Decode, Principal};
use serde::de::DeserializeOwned;

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PipelineAgent: Send + Sync {
    async fn call_query<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String>;

    async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned + for<'a> candid::utils::ArgumentDecoder<'a>>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String>;

    async fn call_update<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String>;
}

#[async_trait::async_trait]
impl PipelineAgent for ic_agent::Agent {
    async fn call_query<R: CandidType + Sized + DeserializeOwned>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self
            .query(&canister, method)
            .with_arg(arg)
            .call()
            .await
            .map_err(|e| e.to_string());

        // Decode the candid response
        let res =
            Decode!(&res.unwrap(), R).map_err(|e| format!("Candid decode error: {e}"))?;

        Ok(res)
    }

    async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned + for<'a> candid::utils::ArgumentDecoder<'a>>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self
            .query(&canister, method)
            .with_arg(arg)
            .call()
            .await
            .map_err(|e| e.to_string());

        let res = candid::utils::decode_args::<R>(&res.unwrap())
            .map_err(|e| format!("Candid decode error: {}", e))?;

        Ok(res)
    }

    async fn call_update<R: CandidType + Sized + DeserializeOwned>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self
            .update(&canister, method)
            .with_arg(arg)
            .call_and_wait()
            .await
            .map_err(|e| e.to_string());

        // Decode the candid response
        let res =
            Decode!(&res.unwrap(), R).map_err(|e| format!("Candid decode error: {e}"))?;

        Ok(res)
    }
}
