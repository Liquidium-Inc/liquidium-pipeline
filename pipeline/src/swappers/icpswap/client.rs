use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use icrc_ledger_types::{icrc1::account::Account, icrc2::approve::ApproveArgs};
use liquidium_pipeline_connectors::{backend::icp_backend::IcpBackend, pipeline_agent::PipelineAgent};

use super::types::{
    IcpswapApprovalRequest, IcpswapClientError, IcpswapDepositAndSwapArgs, IcpswapExecutionClientError,
    IcpswapGetPoolArgs, IcpswapPoolData, IcpswapResult, IcpswapSwapArgs, IcpswapToken,
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

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapLedgerClient: Send + Sync {
    async fn ledger_fee(&self, ledger: Principal) -> Result<Nat, String>;
    async fn allowance(&self, ledger: Principal, owner: &Account, spender: &Account) -> Result<Nat, String>;
    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, String>;
}

#[async_trait]
impl<T: IcpBackend> IcpswapLedgerClient for T {
    async fn ledger_fee(&self, ledger: Principal) -> Result<Nat, String> {
        self.icrc1_fee(ledger).await
    }

    async fn allowance(&self, ledger: Principal, owner: &Account, spender: &Account) -> Result<Nat, String> {
        self.icrc2_allowance(ledger, owner, spender).await
    }

    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, String> {
        self.icrc2_approve(
            request.ledger,
            ApproveArgs {
                from_subaccount: request.owner.subaccount,
                spender: request.spender,
                amount: request.required_allowance,
                expected_allowance: Some(request.current_allowance),
                expires_at: None,
                fee: None,
                memo: None,
                created_at_time: Some(request.created_at_time),
            },
        )
        .await
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapExecutionClient: Send + Sync {
    async fn allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapExecutionClientError>;

    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapExecutionClientError>;

    async fn deposit_from_and_swap(
        &self,
        pool: Principal,
        args: &IcpswapDepositAndSwapArgs,
    ) -> Result<Nat, IcpswapExecutionClientError>;
}

pub struct IcpswapClient<A: PipelineAgent, B: IcpswapLedgerClient> {
    agent: Arc<A>,
    icp_backend: Arc<B>,
    factory: Principal,
}

impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapClient<A, B> {
    pub fn new(agent: Arc<A>, icp_backend: Arc<B>, factory: Principal) -> Self {
        Self {
            agent,
            icp_backend,
            factory,
        }
    }

    pub fn factory(&self) -> Principal {
        self.factory
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapReadClient for IcpswapClient<A, B> {
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
            .ledger_fee(ledger)
            .await
            .map_err(|message| IcpswapClientError::LedgerFee { ledger, message })
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapExecutionClient for IcpswapClient<A, B> {
    async fn allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapExecutionClientError> {
        self.icp_backend
            .allowance(ledger, owner, spender)
            .await
            .map_err(|message| IcpswapExecutionClientError::Allowance { ledger, message })
    }

    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapExecutionClientError> {
        let ledger = request.ledger;
        self.icp_backend
            .approve(request)
            .await
            .map_err(|message| IcpswapExecutionClientError::Approval { ledger, message })
    }

    async fn deposit_from_and_swap(
        &self,
        pool: Principal,
        args: &IcpswapDepositAndSwapArgs,
    ) -> Result<Nat, IcpswapExecutionClientError> {
        const METHOD: &str = "depositFromAndSwap";
        let encoded = Encode!(args).map_err(|error| IcpswapExecutionClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let response = self
            .agent
            .call_update_raw(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapExecutionClientError::SubmissionUnknown {
                pool,
                method: METHOD,
                message,
            })?;
        let result =
            Decode!(&response, IcpswapResult<Nat>).map_err(|error| IcpswapExecutionClientError::SubmissionUnknown {
                pool,
                method: METHOD,
                message: format!("Candid decode error: {error}"),
            })?;

        match result {
            IcpswapResult::Ok(amount) => Ok(amount),
            IcpswapResult::Err(error) => Err(IcpswapExecutionClientError::Protocol { method: METHOD, error }),
        }
    }
}
