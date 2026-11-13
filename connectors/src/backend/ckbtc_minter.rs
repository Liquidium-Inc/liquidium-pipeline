//! The ckBTC minter as the bridge calls it. Out: `retrieve_btc_with_approval`
//! and its status, the fee estimate and the minimum. In: the deposit address
//! of an account and `update_balance`, which mints what arrived there.
use std::sync::Arc;

use candid::{CandidType, Encode, Principal};
use icrc_ledger_types::icrc1::account::Account;
use serde::{Deserialize, de::DeserializeOwned};

use crate::pipeline_agent::PipelineAgent;

pub const BTC_DECIMALS: u8 = 8;

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CkBtcMinterInfo {
    pub retrieve_btc_min_amount: u64,
    /// A deposit under it never mints.
    pub deposit_btc_min_amount: Option<u64>,
    /// Bitcoin confirmations a deposit needs before the mint.
    pub min_confirmations: u32,
    /// Taken out of every deposit before the mint.
    pub kyt_fee: u64,
}

/// The account whose deposit address or balance the call is about.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MinterAccountArgs {
    pub owner: Option<Principal>,
    pub subaccount: Option<Vec<u8>>,
}

impl From<&Account> for MinterAccountArgs {
    fn from(account: &Account) -> Self {
        Self {
            owner: Some(account.owner),
            subaccount: account.subaccount.map(|bytes| bytes.to_vec()),
        }
    }
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OutPoint {
    /// In the minter's byte order; see [`display_txid`].
    pub txid: Vec<u8>,
    pub vout: u32,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Utxo {
    pub height: u32,
    pub value: u64,
    pub outpoint: OutPoint,
}

/// What `update_balance` did with one deposit.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UtxoStatus {
    /// Under the deposit minimum; it never mints.
    ValueTooSmall(Utxo),
    /// Quarantined by the minter's compliance check. Only its support moves it.
    Tainted(Utxo),
    Minted {
        minted_amount: u64,
        block_index: u64,
        utxo: Utxo,
    },
    Checked(Utxo),
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PendingUtxo {
    pub confirmations: u32,
    pub value: u64,
    pub outpoint: OutPoint,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SuspendedReason {
    ValueTooSmall,
    Quarantined,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SuspendedUtxo {
    pub utxo: Utxo,
    pub earliest_retry: u64,
    pub reason: SuspendedReason,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UpdateBalanceError {
    GenericError {
        error_message: String,
        error_code: u64,
    },
    TemporarilyUnavailable(String),
    AlreadyProcessing,
    /// Nothing new to mint. The pending list says what still confirms.
    NoNewUtxos {
        suspended_utxos: Option<Vec<SuspendedUtxo>>,
        required_confirmations: u32,
        pending_utxos: Option<Vec<PendingUtxo>>,
        current_confirmations: Option<u32>,
    },
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UpdateBalanceRet {
    Ok(Vec<UtxoStatus>),
    Err(UpdateBalanceError),
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EstimateWithdrawalFeeArg {
    pub amount: Option<u64>,
}

/// Both parts come out of the retrieved amount, in satoshi.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WithdrawalFee {
    pub minter_fee: u64,
    pub bitcoin_fee: u64,
}

impl WithdrawalFee {
    pub fn total(&self) -> u64 {
        self.minter_fee.saturating_add(self.bitcoin_fee)
    }
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetrieveBtcWithApprovalArgs {
    pub from_subaccount: Option<Vec<u8>>,
    pub address: String,
    pub amount: u64,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetrieveBtcOk {
    pub block_index: u64,
}

/// Why the minter did not queue the retrieval. Only some variants prove
/// that the burn did not happen; the bridge's mapping says which.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RetrieveBtcWithApprovalError {
    MalformedAddress(String),
    GenericError { error_message: String, error_code: u64 },
    TemporarilyUnavailable(String),
    InsufficientAllowance { allowance: u64 },
    AlreadyProcessing,
    AmountTooLow(u64),
    InsufficientFunds { balance: u64 },
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RetrieveBtcWithApprovalRet {
    Ok(RetrieveBtcOk),
    Err(RetrieveBtcWithApprovalError),
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetrieveBtcStatusRequest {
    pub block_index: u64,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ReimbursementReason {
    CallFailed,
    TaintedDestination { kyt_fee: u64, kyt_provider: Principal },
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReimbursementRequest {
    pub account: Account,
    pub amount: u64,
    pub reason: ReimbursementReason,
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReimbursedDeposit {
    pub account: Account,
    pub mint_block_index: u64,
    pub amount: u64,
    pub reason: ReimbursementReason,
}

/// Where a retrieval stands. `txid` is in the minter's byte order; see
/// [`display_txid`] for the usual hex.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RetrieveBtcStatusV2 {
    Unknown,
    Pending,
    Signing,
    Sending { txid: Vec<u8> },
    Submitted { txid: Vec<u8> },
    AmountTooLow,
    Confirmed { txid: Vec<u8> },
    WillReimburse(ReimbursementRequest),
    Reimbursed(ReimbursedDeposit),
}

#[derive(CandidType, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetrieveBtcStatusEntry {
    pub block_index: u64,
    pub status_v2: Option<RetrieveBtcStatusV2>,
}

/// A Bitcoin transaction id as explorers show it: the bytes reversed, in hex.
pub fn display_txid(txid: &[u8]) -> String {
    txid.iter().rev().map(|byte| format!("{byte:02x}")).collect()
}

/// A thin client over the minter canister. Each method is one canister call.
pub struct CkBtcMinter<A: PipelineAgent> {
    agent: Arc<A>,
    canister: Principal,
}

impl<A: PipelineAgent> CkBtcMinter<A> {
    pub fn new(agent: Arc<A>, canister: Principal) -> Self {
        Self { agent, canister }
    }

    pub fn canister(&self) -> Principal {
        self.canister
    }

    pub fn agent(&self) -> &A {
        self.agent.as_ref()
    }

    async fn query<R>(&self, method: &str, arg: &impl CandidType) -> Result<R, String>
    where
        R: CandidType + DeserializeOwned + 'static,
    {
        let arg = Encode!(arg).map_err(|e| e.to_string())?;
        self.agent
            .call_query::<R>(&self.canister, method, arg)
            .await
            .map_err(|e| format!("{method} failed for ckBTC minter {}: {e}", self.canister))
    }

    async fn update<R>(&self, method: &str, arg: &impl CandidType) -> Result<R, String>
    where
        R: CandidType + DeserializeOwned + 'static,
    {
        let arg = Encode!(arg).map_err(|e| e.to_string())?;
        self.agent
            .call_update::<R>(&self.canister, method, arg)
            .await
            .map_err(|e| format!("{method} failed for ckBTC minter {}: {e}", self.canister))
    }

    pub async fn minter_info(&self) -> Result<CkBtcMinterInfo, String> {
        self.query("get_minter_info", &()).await
    }

    /// The Bitcoin address whose deposits mint to `account`.
    pub async fn get_btc_address(&self, account: &Account) -> Result<String, String> {
        self.update("get_btc_address", &MinterAccountArgs::from(account)).await
    }

    /// Mints the confirmed deposits of `account`. Safe to repeat: a deposit
    /// mints once, and a repeat reports what is pending.
    pub async fn update_balance(&self, account: &Account) -> Result<UpdateBalanceRet, String> {
        self.update("update_balance", &MinterAccountArgs::from(account)).await
    }

    /// The fee a retrieval of `amount` pays, or the fee of a typical one.
    pub async fn estimate_withdrawal_fee(&self, amount: Option<u64>) -> Result<WithdrawalFee, String> {
        self.query("estimate_withdrawal_fee", &EstimateWithdrawalFeeArg { amount })
            .await
    }

    /// Burns the approved ckBTC and queues the Bitcoin send. A failed call
    /// may still have burned, so the caller looks the burn up before a retry.
    pub async fn retrieve_btc_with_approval(
        &self,
        args: &RetrieveBtcWithApprovalArgs,
    ) -> Result<RetrieveBtcWithApprovalRet, String> {
        self.update("retrieve_btc_with_approval", args).await
    }

    pub async fn retrieve_btc_status_v2(&self, block_index: u64) -> Result<RetrieveBtcStatusV2, String> {
        self.query("retrieve_btc_status_v2", &RetrieveBtcStatusRequest { block_index })
            .await
    }

    /// Every retrieval of `account`, by burn index. A certified read goes
    /// through consensus, so a negative answer is safe to act on.
    pub async fn retrieve_btc_status_v2_by_account(
        &self,
        account: &Account,
        certified: bool,
    ) -> Result<Vec<RetrieveBtcStatusEntry>, String> {
        let method = "retrieve_btc_status_v2_by_account";
        if certified {
            self.update(method, &Some(*account)).await
        } else {
            self.query(method, &Some(*account)).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_txid_is_shown_reversed_in_hex() {
        assert_eq!(display_txid(&[0x01, 0xab, 0xff]), "ffab01");
    }

    #[test]
    fn the_fee_is_the_sum_of_both_parts() {
        let fee = WithdrawalFee {
            minter_fee: 10,
            bitcoin_fee: 1_500,
        };
        assert_eq!(fee.total(), 1_510);
    }
}
