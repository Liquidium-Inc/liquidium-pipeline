use std::sync::Arc;

use crate::pipeline_agent::PipelineAgent;
use alloy::hex;
use async_trait::async_trait;
use candid::{CandidType, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use icrc_ledger_types::icrc1::transfer::{TransferArg, TransferError};
use icrc_ledger_types::icrc2::allowance::{Allowance, AllowanceArgs};
use icrc_ledger_types::icrc2::approve::{ApproveArgs, ApproveError};
use liquidium_pipeline_core::transfer::actions::TransferFailure;
use num_traits::ToPrimitive;
use serde::{Deserialize, de::DeserializeOwned};
use thiserror::Error;

/// Why an `icrc1_transfer` submission failed, keeping the two `created_at_time`
/// verdicts separable from every other failure.
///
/// The split is a safety distinction, not a cosmetic one. A generic failure is
/// *ambiguous* -- the transfer may already have been applied, and resubmitting
/// the identical arguments is what lets ledger deduplication resolve it -- while
/// these two are decided rejections of the timestamp itself, which deduplication
/// can no longer speak to.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcrcTransferError {
    /// `created_at_time` is older than the ledger's transaction window. That
    /// window is also the deduplication window, so these arguments are refused
    /// permanently and a replay can no longer reveal whether an earlier
    /// identical submission was applied.
    #[error("icrc1_transfer rejected a created_at_time older than the ledger transaction window")]
    TooOld,
    /// `created_at_time` is ahead of the ledger's clock. Ledger time only moves
    /// forward, so no submission carrying this timestamp can have been applied
    /// earlier either.
    #[error("icrc1_transfer rejected a created_at_time in the future; ledger time is {ledger_time}")]
    CreatedInFuture { ledger_time: u64 },
    /// The ledger evaluated the transfer and refused it. Nothing was applied,
    /// so there is no ambiguity for a replay to resolve.
    #[error("{0}")]
    Rejected(String),
    /// Every remaining failure, ambiguous by default: the transfer may already
    /// have been applied and only a replay can reveal it.
    #[error("{0}")]
    Other(String),
}

impl From<IcrcTransferError> for TransferFailure {
    fn from(error: IcrcTransferError) -> Self {
        let message = error.to_string();
        match error {
            // Ledger time only moves forward, so no submission carrying a
            // future timestamp can have been applied earlier either.
            IcrcTransferError::CreatedInFuture { .. } | IcrcTransferError::Rejected(_) => {
                TransferFailure::Rejected(message)
            }
            // `TooOld` refuses this submission, but it also means the
            // deduplication window can no longer say whether an identical
            // earlier one landed. Stay on the safe side of that.
            IcrcTransferError::TooOld | IcrcTransferError::Other(_) => TransferFailure::Ambiguous(message),
        }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpBackend: Send + Sync {
    async fn icrc1_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String>;

    /// Reads a native ICP balance by its legacy 32-byte account identifier.
    async fn icp_account_balance(&self, ledger: Principal, account_id_hex: &str) -> Result<Nat, String> {
        let _ = (ledger, account_id_hex);
        Err("legacy ICP account balance is not implemented by this backend".to_string())
    }

    async fn icrc1_transfer(
        &self,
        ledger: Principal,
        from: &Account,
        to: &Account,
        amount: Nat,
    ) -> Result<Nat, TransferFailure>;

    async fn icrc1_transfer_with_args(&self, ledger: Principal, args: TransferArg)
    -> Result<Nat, IcrcTransferError>;

    async fn icp_transfer(
        &self,
        ledger: Principal,
        to_account_id_hex: &str,
        amount_e8s: Nat,
    ) -> Result<u64, TransferFailure>;

    async fn icrc1_decimals(&self, ledger: Principal) -> Result<u8, String>;
    async fn icrc1_fee(&self, ledger: Principal) -> Result<Nat, String>;
    async fn icrc2_allowance(&self, ledger: Principal, account: &Account, spender: &Account) -> Result<Nat, String>;

    async fn icrc2_approve(&self, ledger: Principal, args: ApproveArgs) -> Result<Nat, String>;
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
        self.query::<Nat>(ledger, "icrc1_balance_of", *account).await
    }

    async fn icp_account_balance(&self, ledger: Principal, account_id_hex: &str) -> Result<Nat, String> {
        #[derive(CandidType)]
        struct AccountBalanceArgs {
            account: Vec<u8>,
        }

        #[derive(CandidType, Deserialize)]
        struct Tokens {
            e8s: u64,
        }

        let account = hex::decode(account_id_hex).map_err(|error| error.to_string())?;
        let balance: Tokens = self.query(ledger, "account_balance", AccountBalanceArgs { account }).await?;
        Ok(Nat::from(balance.e8s))
    }

    async fn icrc1_transfer(
        &self,
        ledger: Principal,
        from: &Account,
        to: &Account,
        amount: Nat,
    ) -> Result<Nat, TransferFailure> {
        let arg = TransferArg {
            from_subaccount: from.subaccount,
            to: *to,
            amount,
            fee: None,
            memo: None,
            created_at_time: None,
        };

        self.icrc1_transfer_with_args(ledger, arg)
            .await
            .map_err(TransferFailure::from)
    }

    async fn icrc1_transfer_with_args(
        &self,
        ledger: Principal,
        args: TransferArg,
    ) -> Result<Nat, IcrcTransferError> {
        let result: Result<Nat, TransferError> = self
            .update(ledger, "icrc1_transfer", args)
            .await
            .map_err(IcrcTransferError::Other)?;
        match result {
            Ok(idx) => Ok(idx),
            // A deduplicated transfer is the original transfer: report the block
            // it landed in rather than an error.
            Err(TransferError::Duplicate { duplicate_of }) => Ok(duplicate_of),
            Err(TransferError::TooOld) => Err(IcrcTransferError::TooOld),
            Err(TransferError::CreatedInFuture { ledger_time }) => {
                Err(IcrcTransferError::CreatedInFuture { ledger_time })
            }
            // The ledger answered. Whatever it refused, it refused without
            // applying anything.
            Err(e) => Err(IcrcTransferError::Rejected(format!("icrc1_transfer error: {e}"))),
        }
    }

    async fn icp_transfer(
        &self,
        ledger: Principal,
        to_account_id_hex: &str,
        amount_e8s: Nat,
    ) -> Result<u64, TransferFailure> {
        #[derive(CandidType, Deserialize, Debug)]
        struct Tokens {
            e8s: u64,
        }

        #[derive(CandidType, Deserialize, Debug)]
        struct TimeStamp {
            timestamp_nanos: u64,
        }

        #[derive(CandidType, Deserialize, Debug)]
        struct TransferArgs {
            to: Vec<u8>,
            fee: Tokens,
            memo: u64,
            from_subaccount: Option<Vec<u8>>,
            created_at_time: Option<TimeStamp>,
            amount: Tokens,
        }

        #[derive(CandidType, Deserialize, Debug)]
        enum TransferError1 {
            TxTooOld { allowed_window_nanos: u64 },
            BadFee { expected_fee: Tokens },
            TxDuplicate { duplicate_of: u64 },
            TxCreatedInFuture,
            InsufficientFunds { balance: Tokens },
        }

        #[derive(CandidType, Deserialize)]
        enum Result6 {
            Ok(u64),
            Err(TransferError1),
        }

        // Nothing below this point has been submitted yet, so every early
        // return is a refusal that moved no funds.
        let to = hex::decode(to_account_id_hex).map_err(|e| TransferFailure::Rejected(e.to_string()))?;

        let fee = Tokens { e8s: 10_000 }; // default ICP fee

        let e8s = amount_e8s
            .0
            .to_u64()
            .ok_or_else(|| TransferFailure::Rejected("amount too large for ICP transfer".to_string()))?;
        let amount = Tokens { e8s };

        let arg = TransferArgs {
            to,
            fee,
            memo: 0,
            from_subaccount: None,
            created_at_time: None,
            amount,
        };

        // A failed call may still have been executed, so it stays ambiguous.
        let res: Result6 = self
            .update(ledger, "transfer", arg)
            .await
            .map_err(TransferFailure::Ambiguous)?;

        match res {
            Result6::Ok(block_index) => Ok(block_index),
            // The ledger answered. Whatever it refused, it refused without
            // applying anything.
            Result6::Err(e) => Err(TransferFailure::Rejected(format!("icp_transfer error: {e:?}"))),
        }
    }

    async fn icrc2_approve(&self, ledger: Principal, args: ApproveArgs) -> Result<Nat, String> {
        let result: Result<Nat, ApproveError> = self.update(ledger, "icrc2_approve", args).await?;
        match result {
            Ok(idx) => Ok(idx),
            Err(e) => Err(format!("icrc2_approve error: {e}")),
        }
    }

    async fn icrc2_allowance(&self, ledger: Principal, account: &Account, spender: &Account) -> Result<Nat, String> {
        let args = AllowanceArgs {
            account: *account,
            spender: *spender,
        };
        let result: Allowance = self.query(ledger, "icrc2_allowance", args).await?;
        Ok(result.allowance)
    }

    async fn icrc1_decimals(&self, ledger: Principal) -> Result<u8, String> {
        self.query::<u8>(ledger, "icrc1_decimals", ()).await
    }

    async fn icrc1_fee(&self, ledger: Principal) -> Result<Nat, String> {
        self.query::<Nat>(ledger, "icrc1_fee", ()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_agent::MockPipelineAgent;

    #[tokio::test]
    async fn deduplicated_transfer_returns_the_original_block_index() {
        let ledger = Principal::from_slice(&[1]);
        let duplicate_of = Nat::from(77u64);
        let duplicate_response = duplicate_of.clone();
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<Nat, TransferError>>()
            .times(1)
            .withf(move |canister, method, _| *canister == ledger && method == "icrc1_transfer")
            .return_once(move |_, _, _| {
                Ok(Err(TransferError::Duplicate {
                    duplicate_of: duplicate_response,
                }))
            });
        let backend = IcpBackendImpl::new(Arc::new(agent));
        let args = TransferArg {
            from_subaccount: None,
            to: Account {
                owner: Principal::from_slice(&[2]),
                subaccount: Some([3; 32]),
            },
            amount: Nat::from(100u64),
            fee: Some(Nat::from(10u64)),
            memo: None,
            created_at_time: Some(123),
        };

        assert_eq!(backend.icrc1_transfer_with_args(ledger, args).await, Ok(duplicate_of));
    }

    #[tokio::test]
    async fn created_at_time_rejections_stay_distinguishable_from_generic_failures() {
        let ledger = Principal::from_slice(&[1]);
        let cases = [
            (TransferError::TooOld, IcrcTransferError::TooOld),
            (
                TransferError::CreatedInFuture { ledger_time: 42 },
                IcrcTransferError::CreatedInFuture { ledger_time: 42 },
            ),
            // The ledger answered, so nothing was applied. It stays separate
            // from the two timestamp verdicts without becoming ambiguous.
            (
                TransferError::TemporarilyUnavailable,
                IcrcTransferError::Rejected("icrc1_transfer error: the ledger is temporarily unavailable".to_string()),
            ),
        ];

        for (response, expected) in cases {
            let mut agent = MockPipelineAgent::new();
            agent
                .expect_call_update::<Result<Nat, TransferError>>()
                .times(1)
                .return_once(move |_, _, _| Ok(Err(response)));
            let backend = IcpBackendImpl::new(Arc::new(agent));
            let args = TransferArg {
                from_subaccount: None,
                to: Account {
                    owner: Principal::from_slice(&[2]),
                    subaccount: None,
                },
                amount: Nat::from(100u64),
                fee: Some(Nat::from(10u64)),
                memo: None,
                created_at_time: Some(123),
            };

            assert_eq!(backend.icrc1_transfer_with_args(ledger, args).await, Err(expected));
        }
    }

    /// A call that never returns a verdict is the one case where the transfer
    /// may already have been applied. It must not be reported as a refusal.
    #[tokio::test]
    async fn a_failed_call_stays_ambiguous_while_a_ledger_verdict_does_not() {
        let ledger = Principal::from_slice(&[1]);
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<Nat, TransferError>>()
            .times(1)
            .return_once(|_, _, _| Err("replica timed out".to_string()));
        let backend = IcpBackendImpl::new(Arc::new(agent));
        let args = TransferArg {
            from_subaccount: None,
            to: Account {
                owner: Principal::from_slice(&[2]),
                subaccount: None,
            },
            amount: Nat::from(100u64),
            fee: Some(Nat::from(10u64)),
            memo: None,
            created_at_time: Some(123),
        };

        let error = backend
            .icrc1_transfer_with_args(ledger, args)
            .await
            .expect_err("a failed call must not report success");

        assert_eq!(error, IcrcTransferError::Other("replica timed out".to_string()));
        assert_eq!(
            TransferFailure::from(error),
            TransferFailure::Ambiguous("replica timed out".to_string())
        );
        // Whereas anything the ledger decided is known to have moved nothing.
        assert_eq!(
            TransferFailure::from(IcrcTransferError::Rejected("insufficient funds".to_string())),
            TransferFailure::Rejected("insufficient funds".to_string())
        );
    }
}
