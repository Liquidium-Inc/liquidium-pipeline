use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use icrc_ledger_types::{
    icrc1::{
        account::Account,
        transfer::{TransferArg, TransferError},
    },
    icrc2::approve::ApproveArgs,
};
use liquidium_pipeline_connectors::{backend::icp_backend::IcpBackend, pipeline_agent::PipelineAgent};

use super::types::{
    IcpswapApprovalRequest, IcpswapClientError, IcpswapDepositAndSwapArgs, IcpswapExecutionClientError,
    IcpswapGetPoolArgs, IcpswapPoolData, IcpswapRecoveryClientError, IcpswapRecoveryTransferClientError,
    IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferRequest, IcpswapResult, IcpswapSwapArgs, IcpswapToken,
    IcpswapTransaction, IcpswapUnusedBalance, IcpswapWithdrawArgs,
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
    async fn latest_transaction_id(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Option<Nat>, IcpswapExecutionClientError>;

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

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapReconciliationClient: Send + Sync {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String>;

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapRecoveryClient: Send + Sync {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String>;

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String>;

    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapRecoveryClientError>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapRecoveryTransferClient: Send + Sync {
    async fn transfer_recovered_funds(
        &self,
        request: &IcpswapRecoveryTransferRequest,
    ) -> Result<IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferClientError>;
}

/// Complete client capability required by the durable ICPSwap workflow.
/// Individual algorithms depend on the narrower traits above; composition
/// roots and routers can hold this single opaque client view.
pub trait IcpswapWorkflowClient:
    IcpswapExecutionClient
    + IcpswapReconciliationClient
    + IcpswapRecoveryClient
    + IcpswapRecoveryTransferClient
    + Send
    + Sync
{
}

impl<T> IcpswapWorkflowClient for T where
    T: IcpswapExecutionClient
        + IcpswapReconciliationClient
        + IcpswapRecoveryClient
        + IcpswapRecoveryTransferClient
        + Send
        + Sync
{
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
    async fn latest_transaction_id(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Option<Nat>, IcpswapExecutionClientError> {
        IcpswapReconciliationClient::transactions_by_owner(self, pool, owner)
            .await
            .map(|transactions| transactions.into_iter().map(|(id, _)| id).max())
            .map_err(|message| IcpswapExecutionClientError::TransactionQuery { pool, message })
    }

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

#[async_trait]
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapReconciliationClient for IcpswapClient<A, B> {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String> {
        const METHOD: &str = "getTransactionsByOwner";
        let encoded = Encode!(&owner).map_err(|error| format!("failed to encode {METHOD}: {error}"))?;
        match self
            .agent
            .call_query::<IcpswapResult<Vec<(Nat, IcpswapTransaction)>>>(&pool, METHOD, encoded)
            .await?
        {
            IcpswapResult::Ok(transactions) => Ok(transactions),
            IcpswapResult::Err(error) => Err(format!("{METHOD} returned {error:?}")),
        }
    }

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String> {
        const METHOD: &str = "getUserUnusedBalance";
        let encoded = Encode!(&owner).map_err(|error| format!("failed to encode {METHOD}: {error}"))?;
        match self
            .agent
            .call_query::<IcpswapResult<IcpswapUnusedBalance>>(&pool, METHOD, encoded)
            .await?
        {
            IcpswapResult::Ok(balance) => Ok(balance),
            IcpswapResult::Err(error) => Err(format!("{METHOD} returned {error:?}")),
        }
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapRecoveryClient for IcpswapClient<A, B> {
    async fn transactions_by_owner(
        &self,
        pool: Principal,
        owner: Principal,
    ) -> Result<Vec<(Nat, IcpswapTransaction)>, String> {
        IcpswapReconciliationClient::transactions_by_owner(self, pool, owner).await
    }

    async fn unused_balance(&self, pool: Principal, owner: Principal) -> Result<IcpswapUnusedBalance, String> {
        IcpswapReconciliationClient::unused_balance(self, pool, owner).await
    }

    async fn withdraw(&self, pool: Principal, args: &IcpswapWithdrawArgs) -> Result<Nat, IcpswapRecoveryClientError> {
        const METHOD: &str = "withdraw";
        let encoded = Encode!(args).map_err(|error| IcpswapRecoveryClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let response = self
            .agent
            .call_update_raw(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapRecoveryClientError::SubmissionUnknown {
                pool,
                method: METHOD,
                message,
            })?;
        let result =
            Decode!(&response, IcpswapResult<Nat>).map_err(|error| IcpswapRecoveryClientError::SubmissionUnknown {
                pool,
                method: METHOD,
                message: format!("Candid decode error: {error}"),
            })?;

        match result {
            IcpswapResult::Ok(amount) => Ok(amount),
            IcpswapResult::Err(error) => Err(IcpswapRecoveryClientError::Protocol { method: METHOD, error }),
        }
    }
}

#[async_trait]
impl<A: PipelineAgent, B: IcpswapLedgerClient> IcpswapRecoveryTransferClient for IcpswapClient<A, B> {
    async fn transfer_recovered_funds(
        &self,
        request: &IcpswapRecoveryTransferRequest,
    ) -> Result<IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferClientError> {
        let args = TransferArg {
            from_subaccount: request.from.subaccount,
            to: request.to,
            amount: request.amount.clone(),
            fee: Some(request.fee.clone()),
            memo: None,
            created_at_time: Some(request.created_at_time),
        };
        let encoded = Encode!(&args).map_err(|error| IcpswapRecoveryTransferClientError::Encode(error.to_string()))?;
        let response = self
            .agent
            .call_update_raw(&request.ledger, "icrc1_transfer", encoded)
            .await
            .map_err(|message| IcpswapRecoveryTransferClientError::SubmissionUnknown {
                ledger: request.ledger,
                message,
            })?;
        let result = Decode!(&response, Result<Nat, TransferError>).map_err(|error| {
            IcpswapRecoveryTransferClientError::SubmissionUnknown {
                ledger: request.ledger,
                message: format!("Candid decode error: {error}"),
            }
        })?;

        match result {
            Ok(block_index) => Ok(IcpswapRecoveryTransferOutcome::Completed(block_index)),
            Err(TransferError::Duplicate { duplicate_of }) => {
                Ok(IcpswapRecoveryTransferOutcome::Duplicate(duplicate_of))
            }
            Err(error) => Err(IcpswapRecoveryTransferClientError::Rejected {
                ledger: request.ledger,
                message: error.to_string(),
            }),
        }
    }
}
