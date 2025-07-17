use candid::{CandidType, Decode, Principal};
use ic_agent::Agent;
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

    #[allow(dead_code)]
    async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String>;

    async fn call_update_raw(&self, canister: &Principal, method: &str, arg: Vec<u8>) -> Result<Vec<u8>, String>;

    async fn call_update<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self.call_update_raw(canister, method, arg).await;
        // Decode the candid response
        let res = Decode!(&res.unwrap(), R).map_err(|e| format!("Candid decode error: {e}"))?;
        Ok(res)
    }

    fn agent(&self) -> Agent;
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
        let res = Decode!(&res.unwrap(), R).map_err(|e| format!("Candid decode error: {e}"))?;

        Ok(res)
    }

    async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned>(
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

        let res = candid::utils::decode_one::<R>(&res.unwrap()).map_err(|e| format!("Candid decode error: {}", e))?;

        Ok(res)
    }

    async fn call_update_raw(&self, canister: &Principal, method: &str, arg: Vec<u8>) -> Result<Vec<u8>, String> {
        let res = self.update(&canister, method).with_arg(arg).call_and_wait().await;
        res.map_err(|e| e.to_string())
    }

    fn agent(&self) -> Agent {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Encode;
    use mockall::predicate::*;

    #[tokio::test]
    async fn test_get_price_decodes_tuple() {
        let canister = Principal::anonymous();
        let method = "get_price";
        let arg = Encode!(&"BTC", &"USDT").expect("encoding failed");

        // Create mock agent
        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_query_tuple::<(u64, u32)>()
            .with(eq(canister), eq(method), eq(arg.clone()))
            .returning(move |_, _, _| Ok((80_000_000_000u64, 9u32)));

        // Call
        let result = mock_agent
            .call_query_tuple::<(u64, u32)>(&canister, method, arg)
            .await
            .expect("call should succeed");

        assert_eq!(result, (80_000_000_000, 9));
    }

    #[test]
    fn test_decode_u64_u32_tuple() {
        let price: u64 = 85_000_000_000_000;
        let decimals: u32 = 9;

        let encoded = Encode!(&(price, decimals)).expect("encoding failed");
        println!("Encoded {:?}", encoded);

        let (decoded_price, decoded_decimals): (u64, u32) = Decode!(&encoded, (u64, u32)).expect("decoding failed");

        assert_eq!(decoded_price, price);
        assert_eq!(decoded_decimals, decimals);
    }
}
