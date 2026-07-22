use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use icrc_ledger_types::{icrc1::account::Account, icrc2::approve::ApproveArgs};
use liquidium_pipeline_connectors::{backend::icp_backend::IcpBackend, pipeline_agent::PipelineAgent};

use super::types::{
    IcpswapApprovalRequest, IcpswapClientError, IcpswapDepositArgs, IcpswapGetPoolArgs, IcpswapManualClientError,
    IcpswapPoolData, IcpswapResult, IcpswapSwapArgs, IcpswapToken, IcpswapUnusedBalance, IcpswapWithdrawArgs,
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
    async fn balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String>;
    async fn allowance(&self, ledger: Principal, owner: &Account, spender: &Account) -> Result<Nat, String>;
    async fn approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, String>;
}

#[async_trait]
impl<T: IcpBackend> IcpswapLedgerClient for T {
    async fn ledger_fee(&self, ledger: Principal) -> Result<Nat, String> {
        self.icrc1_fee(ledger).await
    }

    async fn balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String> {
        self.icrc1_balance(ledger, account).await
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

/// Calls used by the official depositFrom -> swap -> withdraw workflow.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapManualClient: Send + Sync {
    async fn quote_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError>;
    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapManualClientError>;
    async fn manual_allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapManualClientError>;
    async fn manual_approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapManualClientError>;
    async fn manual_unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapManualClientError>;
    async fn deposit_from(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapManualClientError>;
    async fn swap_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError>;
    async fn withdraw_manual(
        &self,
        pool: Principal,
        args: &IcpswapWithdrawArgs,
    ) -> Result<Nat, IcpswapManualClientError>;
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
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapManualClient for IcpswapClient<A, B> {
    async fn quote_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError> {
        const METHOD: &str = "quote";
        let encoded = Encode!(args).map_err(|error| IcpswapManualClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<Nat>>(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapManualClientError::Query {
                pool,
                method: METHOD,
                message,
            })?;
        match result {
            IcpswapResult::Ok(amount) => Ok(amount),
            IcpswapResult::Err(error) => Err(IcpswapManualClientError::Protocol { method: METHOD, error }),
        }
    }

    async fn ledger_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, IcpswapManualClientError> {
        self.icp_backend
            .balance(ledger, account)
            .await
            .map_err(|message| IcpswapManualClientError::LedgerBalance { ledger, message })
    }

    async fn manual_allowance(
        &self,
        ledger: Principal,
        owner: &Account,
        spender: &Account,
    ) -> Result<Nat, IcpswapManualClientError> {
        self.icp_backend
            .allowance(ledger, owner, spender)
            .await
            .map_err(|message| IcpswapManualClientError::Allowance { ledger, message })
    }

    async fn manual_approve(&self, request: IcpswapApprovalRequest) -> Result<Nat, IcpswapManualClientError> {
        let ledger = request.ledger;
        self.icp_backend
            .approve(request)
            .await
            .map_err(|message| IcpswapManualClientError::Approval { ledger, message })
    }

    async fn manual_unused_balance(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<IcpswapUnusedBalance, IcpswapManualClientError> {
        const METHOD: &str = "getUserUnusedBalance";
        let encoded = Encode!(&owner).map_err(|error| IcpswapManualClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<IcpswapUnusedBalance>>(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapManualClientError::Query {
                pool,
                method: METHOD,
                message,
            })?;
        match result {
            IcpswapResult::Ok(balance) => Ok(balance),
            IcpswapResult::Err(error) => Err(IcpswapManualClientError::Protocol { method: METHOD, error }),
        }
    }

    async fn deposit_from(&self, pool: Principal, args: &IcpswapDepositArgs) -> Result<Nat, IcpswapManualClientError> {
        call_manual_update(self.agent.as_ref(), pool, "depositFrom", args).await
    }

    async fn swap_manual(&self, pool: Principal, args: &IcpswapSwapArgs) -> Result<Nat, IcpswapManualClientError> {
        call_manual_update(self.agent.as_ref(), pool, "swap", args).await
    }

    async fn withdraw_manual(
        &self,
        pool: Principal,
        args: &IcpswapWithdrawArgs,
    ) -> Result<Nat, IcpswapManualClientError> {
        call_manual_update(self.agent.as_ref(), pool, "withdraw", args).await
    }
}

async fn call_manual_update<A: PipelineAgent, T: candid::CandidType>(
    agent: &A,
    pool: Principal,
    method: &'static str,
    args: &T,
) -> Result<Nat, IcpswapManualClientError> {
    let encoded = Encode!(args).map_err(|error| IcpswapManualClientError::Encode {
        method,
        message: error.to_string(),
    })?;
    let response = agent
        .call_update_raw(&pool, method, encoded)
        .await
        .map_err(|message| IcpswapManualClientError::SubmissionUnknown { pool, method, message })?;
    let result =
        Decode!(&response, IcpswapResult<Nat>).map_err(|error| IcpswapManualClientError::SubmissionUnknown {
            pool,
            method,
            message: format!("Candid decode error: {error}"),
        })?;
    match result {
        IcpswapResult::Ok(amount) => Ok(amount),
        IcpswapResult::Err(error) => Err(IcpswapManualClientError::Protocol { method, error }),
    }
}
