use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use ic_ledger_types::{AccountIdentifier, Subaccount};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_connectors::{
    backend::icp_backend::{IcpBackend, IcrcTransferError},
    pipeline_agent::PipelineAgent,
};

use super::types::{
    IcpswapClientError, IcpswapDepositArgs, IcpswapGetPoolArgs, IcpswapPoolData, IcpswapPoolMetadata, IcpswapResult,
    IcpswapSwapArgs, IcpswapToken, IcpswapUnusedBalance, IcpswapWithdrawArgs,
};
use crate::utils::ICP_LEDGER_PRINCIPAL;

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

    async fn pool_metadata(&self, pool: Principal) -> Result<IcpswapPoolMetadata, IcpswapClientError>;

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

    async fn pool_metadata(&self, pool: Principal) -> Result<IcpswapPoolMetadata, IcpswapClientError> {
        const METHOD: &str = "metadata";
        let encoded = Encode!().map_err(|error| IcpswapClientError::Encode {
            method: METHOD,
            message: error.to_string(),
        })?;
        let result = self
            .agent
            .call_query::<IcpswapResult<IcpswapPoolMetadata>>(&pool, METHOD, encoded)
            .await
            .map_err(|message| IcpswapClientError::Transport {
                canister: pool,
                method: METHOD,
                message,
            })?;

        match result {
            IcpswapResult::Ok(metadata) => Ok(metadata),
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
        let native_icp = Principal::from_text(ICP_LEDGER_PRINCIPAL).map_err(|error| {
            IcpswapClientError::LedgerBalance {
                ledger,
                message: format!("invalid native ICP ledger principal: {error}"),
            }
        })?;
        let balance = if ledger == native_icp {
            let account_id = AccountIdentifier::new(
                &account.owner,
                &Subaccount(account.subaccount.unwrap_or([0; 32])),
            )
            .to_hex();
            self.icp_backend.icp_account_balance(ledger, &account_id).await
        } else {
            self.icp_backend.icrc1_balance(ledger, account).await
        };
        balance.map_err(|message| IcpswapClientError::LedgerBalance { ledger, message })
    }

    async fn ledger_transfer(&self, ledger: Principal, args: TransferArg) -> Result<Nat, IcpswapClientError> {
        self.icp_backend
            .icrc1_transfer_with_args(ledger, args)
            .await
            .map_err(|error| {
                let message = error.to_string();
                match error {
                    IcrcTransferError::TooOld => IcpswapClientError::LedgerTransferTooOld { ledger, message },
                    IcrcTransferError::CreatedInFuture { .. } => {
                        IcpswapClientError::LedgerTransferCreatedInFuture { ledger, message }
                    }
                    IcrcTransferError::Other(_) => IcpswapClientError::LedgerTransfer { ledger, message },
                }
            })
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
        .map_err(|message| classify_update_failure(pool, method, message))?;
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

/// A replica-confirmed trap from `swap` is a definite rejection: the pool
/// message rolled back rather than committing a trade. Transport failures stay
/// ambiguous. This is deliberately swap-specific because deposit and withdraw
/// can perform awaited ledger calls before they fail.
fn classify_update_failure(pool: Principal, method: &'static str, message: String) -> IcpswapClientError {
    let replica_confirmed_swap_trap = method == "swap"
        && message.contains("reject code CanisterError")
        && (message.contains("ic0.trap") || message.contains("IC0503"));
    if replica_confirmed_swap_trap {
        IcpswapClientError::Protocol {
            method,
            error: crate::swappers::icpswap::types::IcpswapError::InternalError(message),
        }
    } else {
        IcpswapClientError::SubmissionUnknown { pool, method, message }
    }
}
