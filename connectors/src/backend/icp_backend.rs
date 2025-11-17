use std::sync::Arc;

use async_trait::async_trait;
use candid::{CandidType, Decode, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::account::{Account, Subaccount};
use serde::de::DeserializeOwned;

use crate::pipeline_agent::PipelineAgent;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpBackend: Send + Sync {
    async fn icrc1_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String>;

    async fn icrc1_transfer(&self, ledger: Principal, from: &Account, to: &Account, amount: Nat)
    -> Result<Nat, String>;

    async fn icrc1_decimals(&self, ledger: Principal) -> Result<u8, String>;
}

pub struct IcpBackendImpl<A: PipelineAgent> {
    pub agent: Arc<A>,
}

impl<A: PipelineAgent> IcpBackendImpl<A> {
    pub fn new(agent: Arc<A>) -> Self {
        Self { agent }
    }

    async fn query<R>(&self, ledger: Principal, method: &str, arg: impl CandidType) -> Result<R, String>
    where
        R: CandidType + DeserializeOwned + 'static,
    {
        let arg_blob = Encode!(&arg).map_err(|e| format!("encode args: {e}"))?;
        self.agent.call_query::<R>(&ledger, method, arg_blob).await
    }

    async fn update<R>(&self, ledger: Principal, method: &str, arg: impl CandidType) -> Result<R, String>
    where
        R: CandidType + DeserializeOwned + 'static,
    {
        let arg_blob = Encode!(&arg).map_err(|e| format!("encode args: {e}"))?;
        self.agent.call_update::<R>(&ledger, method, arg_blob).await
    }
}

#[async_trait]
impl<A: PipelineAgent> IcpBackend for IcpBackendImpl<A> {
    async fn icrc1_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String> {
        self.query::<Nat>(ledger, "icrc1_balance", *account).await
    }

    async fn icrc1_transfer(
        &self,
        ledger: Principal,
        from: &Account,
        to: &Account,
        amount: Nat,
    ) -> Result<Nat, String> {
        #[derive(CandidType)]
        struct TransferArg {
            from_subaccount: Option<Subaccount>,
            to: Account,
            amount: Nat,
            fee: Option<Nat>,
            memo: Option<Vec<u8>>,
            created_at_time: Option<u64>,
        }

        #[derive(CandidType, serde::Deserialize)]
        enum TransferResult {
            Ok(Nat),
            Err(String), // simplify
        }

        let arg = TransferArg {
            from_subaccount: from.subaccount,
            to: *to,
            amount,
            fee: None,
            memo: None,
            created_at_time: None,
        };

        match self.update::<TransferResult>(ledger, "icrc1_transfer", arg).await? {
            TransferResult::Ok(idx) => Ok(idx),
            TransferResult::Err(e) => Err(format!("icrc1_transfer error: {e}")),
        }
    }

    async fn icrc1_decimals(&self, ledger: Principal) -> Result<u8, String> {
        self.query::<u8>(ledger, "icrc1_decimals", ()).await
    }
}
