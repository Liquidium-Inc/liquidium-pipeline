use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_connectors::{backend::icp_backend::IcpBackend, pipeline_agent::PipelineAgent};

use super::types::{
    IcpswapClientError, IcpswapDepositArgs, IcpswapGetPoolArgs, IcpswapPoolData, IcpswapResult, IcpswapSwapArgs,
    IcpswapToken, IcpswapUnusedBalance, IcpswapWithdrawArgs,
};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapReadClient: Send + Sync {
    async fn get_pool(
        &self,
        token0: &IcpswapToken,
        token1: &IcpswapToken,
        fee: &Nat,
    ) -> Result<IcpswapPoolData, IcpswapClientError>;

    async fn quote(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError>;

    async fn ledger_fee(&self, ledger: Principal) -> Result<Nat, IcpswapClientError>;
}

/// Calls used by the official transfer -> deposit -> swap -> withdraw workflow.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapManualClient: Send + Sync {
    async fn requote(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError>;
    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapClientError>;
    async fn ledger_transfer(&self, ledger: Principal, args: TransferArg) -> Result<Nat, IcpswapClientError>;
    async fn unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapClientError>;
    async fn deposit(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapClientError>;
    async fn swap(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError>;
    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapClientError>;
}

pub struct IcpswapClient<A: PipelineAgent, B: IcpBackend> {
    agent: Arc<A>,
    icp_backend: Arc<B>,
    factory: Principal,
}

impl<A: PipelineAgent, B: IcpBackend> IcpswapClient<A, B> {
    pub fn new(agent: Arc<A>, icp_backend: Arc<B>, factory: Principal) -> Self {
        Self {
            agent,
            icp_backend,
            factory,
        }
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpBackend> IcpswapReadClient for IcpswapClient<A, B> {
    async fn get_pool(
        &self,
        token0: &IcpswapToken,
        token1: &IcpswapToken,
        fee: &Nat,
    ) -> Result<IcpswapPoolData, IcpswapClientError> {
        const METHOD: &str = "getPool";
        let args = IcpswapGetPoolArgs {
            token0: token0.clone(),
            token1: token1.clone(),
            fee: fee.clone(),
        };
        let encoded = Encode!(&args).map_err(|error| IcpswapClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<IcpswapPoolData>>(&self.factory, METHOD, encoded)
            .await
            .map_err(|message| IcpswapClientError::Transport {
                canister: self.factory,
                method: METHOD,
                message,
            })?;

        match result {
            IcpswapResult::Ok(pool) => Ok(pool),
            IcpswapResult::Err(error) => Err(IcpswapClientError::Protocol { method: METHOD, error }),
        }
    }

    async fn quote(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError> {
        const METHOD: &str = "quote";
        let encoded = Encode!(args).map_err(|error| IcpswapClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<Nat>>(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapClientError::Transport {
                canister: pool,
                method: METHOD,
                message,
            })?;

        match result {
            IcpswapResult::Ok(amount) => Ok(amount),
            IcpswapResult::Err(error) => Err(IcpswapClientError::Protocol { method: METHOD, error }),
        }
    }

    async fn ledger_fee(&self, ledger: Principal) -> Result<Nat, IcpswapClientError> {
        self.icp_backend
            .icrc1_fee(ledger)
            .await
            .map_err(|message| IcpswapClientError::LedgerFee { ledger, message })
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpBackend> IcpswapManualClient for IcpswapClient<A, B> {
    async fn requote(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError> {
        IcpswapReadClient::quote(self, pool, args).await
    }

    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapClientError> {
        self.icp_backend
            .icrc1_balance(ledger, account)
            .await
            .map_err(|message| IcpswapClientError::LedgerBalance { ledger, message })
    }

    async fn ledger_transfer(&self, ledger: Principal, args: TransferArg) -> Result<Nat, IcpswapClientError> {
        self.icp_backend
            .icrc1_transfer_with_args(ledger, args)
            .await
            .map_err(|message| IcpswapClientError::LedgerTransfer { ledger, message })
    }

    async fn unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapClientError> {
        const METHOD: &str = "getUserUnusedBalance";
        let encoded = Encode!(&owner).map_err(|error| IcpswapClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<IcpswapUnusedBalance>>(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapClientError::Transport {
                canister: pool,
                method: METHOD,
                message,
            })?;
        match result {
            IcpswapResult::Ok(balance) => Ok(balance),
            IcpswapResult::Err(error) => Err(IcpswapClientError::Protocol { method: METHOD, error }),
        }
    }

    async fn deposit(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapClientError> {
        call_manual_update(self.agent.as_ref(), pool, "deposit", args).await
    }

    async fn swap(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapClientError> {
        call_manual_update(self.agent.as_ref(), pool, "swap", args).await
    }

    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapClientError> {
        call_manual_update(self.agent.as_ref(), pool, "withdraw", args).await
    }
}

async fn call_manual_update<A: PipelineAgent, T: candid::CandidType>(
    agent: &A,
    pool: Principal,
    method: &'static str,
    args: &T,
) -> Result<Nat, IcpswapClientError> {
    let encoded = Encode!(args).map_err(|error| IcpswapClientError::Encode {
        method,
        message: error.to_string(),
    })?;
    let response = agent
        .call_update_raw(&pool, method, encoded)
        .await
        .map_err(|message| IcpswapClientError::SubmissionUnknown { pool, method, message })?;
    let result = Decode!(&response, IcpswapResult<Nat>).map_err(|error| IcpswapClientError::SubmissionUnknown {
        pool,
        method,
        message: format!("Candid decode error: {error}"),
    })?;
    match result {
        IcpswapResult::Ok(amount) => Ok(amount),
        IcpswapResult::Err(error) => Err(IcpswapClientError::Protocol { method, error }),
    }
}
