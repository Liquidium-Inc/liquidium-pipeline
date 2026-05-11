use candid::{CandidType, Decode, Encode, Nat, Principal};
use ic_agent::Agent;
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[mockall::automock]
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

    async fn call_update_via_proxy_raw(
        &self,
        proxy_canister: &Principal,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
        cycles: Nat,
    ) -> Result<Vec<u8>, String>;

    async fn call_update<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self
            .call_update_raw(canister, method, arg)
            .await
            .map_err(|e| format!("Call error: {e}"))?;
        // Decode the candid response
        let res = Decode!(&res, R).map_err(|e| format!("Candid decode error: {e}"))?;
        Ok(res)
    }

    async fn call_update_via_proxy<R: Sized + CandidType + DeserializeOwned + 'static>(
        &self,
        proxy_canister: &Principal,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
        cycles: Nat,
    ) -> Result<R, String> {
        let res = self
            .call_update_via_proxy_raw(proxy_canister, canister, method, arg, cycles)
            .await
            .map_err(|e| format!("Proxy forwarded call error: {e}"))?;
        let decoded = Decode!(&res, R).map_err(|e| format!("Candid decode error: {e}"))?;
        Ok(decoded)
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
            .query(canister, method)
            .with_arg(arg)
            .call()
            .await
            .map_err(|e| format!("Query call failed: {e}"))?;

        // Decode the candid response
        let res = Decode!(&res, R).map_err(|e| format!("Candid decode error: {e}"))?;

        Ok(res)
    }

    async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned>(
        &self,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
    ) -> Result<R, String> {
        let res = self
            .query(canister, method)
            .with_arg(arg)
            .call()
            .await
            .map_err(|e| format!("Query call failed: {e}"))?;

        let res = candid::utils::decode_one::<R>(&res).map_err(|e| format!("Candid decode error: {}", e))?;

        Ok(res)
    }

    async fn call_update_raw(&self, canister: &Principal, method: &str, arg: Vec<u8>) -> Result<Vec<u8>, String> {
        let res = self.update(canister, method).with_arg(arg).call_and_wait().await;
        res.map_err(|e| e.to_string())
    }

    async fn call_update_via_proxy_raw(
        &self,
        proxy_canister: &Principal,
        canister: &Principal,
        method: &str,
        arg: Vec<u8>,
        cycles: Nat,
    ) -> Result<Vec<u8>, String> {
        #[derive(CandidType)]
        struct ProxyArgs {
            canister_id: Principal,
            method: String,
            args: Vec<u8>,
            cycles: Nat,
        }

        #[derive(CandidType, Deserialize)]
        struct ProxyOk {
            result: Vec<u8>,
        }

        #[derive(CandidType, Deserialize)]
        struct ProxyInsufficientCyclesError {
            available: Nat,
            required: Nat,
        }

        #[derive(CandidType, Deserialize)]
        struct ProxyCallFailedError {
            reason: String,
        }

        #[derive(CandidType, Deserialize)]
        enum ProxyError {
            InsufficientCycles(ProxyInsufficientCyclesError),
            CallFailed(ProxyCallFailedError),
            UnauthorizedUser,
        }

        #[derive(CandidType, Deserialize)]
        enum ProxyResult {
            Ok(ProxyOk),
            Err(ProxyError),
        }

        let proxy_call_args = Encode!(&ProxyArgs {
            canister_id: *canister,
            method: method.to_string(),
            args: arg.clone(),
            cycles: cycles.clone(),
        })
        .map_err(|e| format!("failed to encode proxy args: {e}"))?;

        let raw_proxy_response = self
            .update(proxy_canister, "proxy")
            .with_arg(proxy_call_args)
            .call_and_wait()
            .await
            .map_err(|e| format!("proxy call failed: {e}"))?;

        let proxy_result =
            Decode!(&raw_proxy_response, ProxyResult).map_err(|e| format!("failed to decode proxy response: {e}"))?;

        match proxy_result {
            ProxyResult::Ok(ok) => Ok(ok.result),
            ProxyResult::Err(ProxyError::InsufficientCycles(err)) => Err(format!(
                "proxy returned error: InsufficientCycles (available={}, required={})",
                err.available, err.required
            )),
            ProxyResult::Err(ProxyError::CallFailed(err)) => {
                Err(format!("proxy returned error: CallFailed ({})", err.reason))
            }
            ProxyResult::Err(ProxyError::UnauthorizedUser) => Err("proxy returned error: UnauthorizedUser".to_string()),
        }
    }

    fn agent(&self) -> Agent {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Encode, Principal};
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

    struct ProxyRawOnlyAgent {
        response: std::sync::Mutex<Option<Result<Vec<u8>, String>>>,
    }

    impl ProxyRawOnlyAgent {
        fn new(response: Result<Vec<u8>, String>) -> Self {
            Self {
                response: std::sync::Mutex::new(Some(response)),
            }
        }
    }

    #[async_trait::async_trait]
    impl PipelineAgent for ProxyRawOnlyAgent {
        async fn call_query<R: Sized + CandidType + DeserializeOwned + 'static>(
            &self,
            _canister: &Principal,
            _method: &str,
            _arg: Vec<u8>,
        ) -> Result<R, String> {
            panic!("unused in test")
        }

        async fn call_query_tuple<R: Sized + CandidType + DeserializeOwned + 'static>(
            &self,
            _canister: &Principal,
            _method: &str,
            _arg: Vec<u8>,
        ) -> Result<R, String> {
            panic!("unused in test")
        }

        async fn call_update_raw(
            &self,
            _canister: &Principal,
            _method: &str,
            _arg: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            panic!("unused in test")
        }

        async fn call_update_via_proxy_raw(
            &self,
            _proxy_canister: &Principal,
            _canister: &Principal,
            _method: &str,
            _arg: Vec<u8>,
            _cycles: Nat,
        ) -> Result<Vec<u8>, String> {
            self.response
                .lock()
                .expect("mutex")
                .take()
                .expect("response should be set")
        }

        fn agent(&self) -> Agent {
            panic!("unused in test")
        }
    }

    #[tokio::test]
    async fn proxy_forwarded_typed_helper_decodes_success() {
        let proxy_canister = Principal::anonymous();
        let canister = Principal::management_canister();
        let arg = Encode!(&()).expect("encode");
        let encoded_res = Encode!(&"ok".to_string()).expect("encode");
        let agent = ProxyRawOnlyAgent::new(Ok(encoded_res));

        let out = agent
            .call_update_via_proxy::<String>(&proxy_canister, &canister, "method", arg, Nat::from(10u64))
            .await
            .expect("proxy forwarded decode");
        assert_eq!(out, "ok".to_string());
    }

    #[tokio::test]
    async fn proxy_forwarded_typed_helper_surfaces_proxy_error() {
        let proxy_canister = Principal::anonymous();
        let canister = Principal::management_canister();
        let arg = Encode!(&()).expect("encode");
        let agent = ProxyRawOnlyAgent::new(Err("proxy returned error: nope".to_string()));

        let err = agent
            .call_update_via_proxy::<String>(&proxy_canister, &canister, "method", arg, Nat::from(10u64))
            .await
            .expect_err("must surface proxy error");
        assert!(err.contains("Proxy forwarded call error"));
        assert!(err.contains("proxy returned error: nope"));
    }

    #[tokio::test]
    async fn proxy_forwarded_typed_helper_surfaces_decode_failure() {
        let proxy_canister = Principal::anonymous();
        let canister = Principal::management_canister();
        let arg = Encode!(&()).expect("encode");
        let agent = ProxyRawOnlyAgent::new(Ok(vec![0xde, 0xad, 0xbe, 0xef]));

        let err = agent
            .call_update_via_proxy::<String>(&proxy_canister, &canister, "method", arg, Nat::from(10u64))
            .await
            .expect_err("must fail candid decode");
        assert!(err.contains("Candid decode error"));
    }
}
