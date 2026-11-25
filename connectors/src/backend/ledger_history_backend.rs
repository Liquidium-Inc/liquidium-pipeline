//! Reads the newest transactions of an account from the index canister of a ledger.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{CandidType, Encode, Nat, Principal, Reserved};
use icrc_ledger_types::icrc1::account::Account;
use serde::Deserialize;

use super::bridge_backend::{account_transactions, ledger_index};
use crate::pipeline_agent::PipelineAgent;

/// The ICP ledger and its index canister. The ICP ledger does not give its index (no ICRC-106).
pub const ICP_LEDGER: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";
const ICP_INDEX: &str = "qhbym-qaaaa-aaaaa-aaafq-cai";

/// One side of a transfer. The ICP index gives an account identifier, the other indexes give an account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerParty {
    Account(Account),
    /// The account identifier of the ICP ledger, as hex text.
    AccountId(String),
}

/// One transfer from an index canister, in the same form for every ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerTransfer {
    pub index: u64,
    pub from: LedgerParty,
    pub to: LedgerParty,
    pub amount: Nat,
    pub memo: Option<Vec<u8>>,
    /// The time of the block in nanoseconds, when the index gives it.
    pub timestamp_nanos: Option<u64>,
}

#[mockall::automock]
#[async_trait]
pub trait LedgerHistoryBackend: Send + Sync {
    /// Returns the transfers among the newest `max_results` transactions of `account`, newest first.
    async fn recent_transfers(
        &self,
        ledger: Principal,
        account: Account,
        max_results: u64,
    ) -> Result<Vec<LedgerTransfer>, String>;
    /// Returns the index of the newest transaction of `account`, of any kind, or 0 when it has none.
    async fn newest_index(&self, ledger: Principal, account: Account) -> Result<u64, String>;
}

/// Reads the history from the index canisters, and keeps the index canister of each ledger.
pub struct IndexLedgerHistory<A: PipelineAgent> {
    agent: Arc<A>,
    index_by_ledger: Mutex<HashMap<Principal, Principal>>,
}

impl<A: PipelineAgent> IndexLedgerHistory<A> {
    pub fn new(agent: Arc<A>) -> Self {
        Self {
            agent,
            index_by_ledger: Mutex::new(HashMap::new()),
        }
    }

    async fn index_of(&self, ledger: Principal) -> Result<Principal, String> {
        if let Some(index) = self.index_by_ledger.lock().expect("index cache").get(&ledger) {
            return Ok(*index);
        }
        let index = ledger_index(self.agent.as_ref(), ledger).await?;
        self.index_by_ledger.lock().expect("index cache").insert(ledger, index);
        Ok(index)
    }

    async fn icp_transactions(&self, account: Account, max_results: u64) -> Result<Vec<IcpIndexedTransaction>, String> {
        let request = IcpAccountTransactionsArgs {
            account,
            start: None,
            max_results: Nat::from(max_results),
        };
        let arg = Encode!(&request).map_err(|error| error.to_string())?;
        let index = Principal::from_text(ICP_INDEX).expect("ICP index principal");
        let answer: Result<IcpAccountTransactions, IcpIndexError> =
            self.agent.call_query(&index, "get_account_transactions", arg).await?;
        Ok(answer
            .map_err(|error| format!("ICP index did not list {account}: {}", error.message))?
            .transactions)
    }
}

fn is_icp_ledger(ledger: Principal) -> bool {
    ledger.to_text() == ICP_LEDGER
}

#[derive(CandidType)]
struct IcpAccountTransactionsArgs {
    account: Account,
    start: Option<Nat>,
    max_results: Nat,
}

#[derive(CandidType, Deserialize)]
struct IcpIndexError {
    message: String,
}

#[derive(CandidType, Deserialize)]
struct IcpAccountTransactions {
    transactions: Vec<IcpIndexedTransaction>,
}

#[derive(CandidType, Deserialize)]
struct IcpIndexedTransaction {
    id: u64,
    transaction: IcpTransaction,
}

#[derive(CandidType, Deserialize)]
struct IcpTransaction {
    icrc1_memo: Option<Vec<u8>>,
    operation: IcpOperation,
    timestamp: Option<IcpTimeStamp>,
}

#[derive(CandidType, Deserialize)]
struct IcpTimeStamp {
    timestamp_nanos: u64,
}

#[derive(CandidType, Deserialize)]
struct IcpTokens {
    e8s: u64,
}

/// The operations of the ICP index. The bot uses only a transfer.
#[derive(CandidType, Deserialize)]
enum IcpOperation {
    Approve(Reserved),
    Burn(Reserved),
    Mint(Reserved),
    Transfer {
        to: String,
        from: String,
        amount: IcpTokens,
    },
}

#[async_trait]
impl<A: PipelineAgent> LedgerHistoryBackend for IndexLedgerHistory<A> {
    async fn recent_transfers(
        &self,
        ledger: Principal,
        account: Account,
        max_results: u64,
    ) -> Result<Vec<LedgerTransfer>, String> {
        if is_icp_ledger(ledger) {
            let transactions = self.icp_transactions(account, max_results).await?;
            return Ok(transactions
                .into_iter()
                .filter_map(|entry| match entry.transaction.operation {
                    IcpOperation::Transfer { to, from, amount } => Some(LedgerTransfer {
                        index: entry.id,
                        from: LedgerParty::AccountId(from),
                        to: LedgerParty::AccountId(to),
                        amount: Nat::from(amount.e8s),
                        memo: entry.transaction.icrc1_memo,
                        timestamp_nanos: entry.transaction.timestamp.map(|time| time.timestamp_nanos),
                    }),
                    _ => None,
                })
                .collect());
        }
        let index = self.index_of(ledger).await?;
        let transactions = account_transactions(self.agent.as_ref(), index, &account, max_results).await?;
        Ok(transactions
            .into_iter()
            .filter_map(|(id, transaction)| {
                let timestamp_nanos = Some(transaction.timestamp);
                transaction.transfer.map(|transfer| LedgerTransfer {
                    index: id,
                    from: LedgerParty::Account(transfer.from),
                    to: LedgerParty::Account(transfer.to),
                    amount: transfer.amount,
                    memo: transfer.memo.map(|memo| memo.0.into_vec()),
                    timestamp_nanos,
                })
            })
            .collect())
    }

    async fn newest_index(&self, ledger: Principal, account: Account) -> Result<u64, String> {
        if is_icp_ledger(ledger) {
            let transactions = self.icp_transactions(account, 1).await?;
            return Ok(transactions.iter().map(|entry| entry.id).max().unwrap_or(0));
        }
        let index = self.index_of(ledger).await?;
        let transactions = account_transactions(self.agent.as_ref(), index, &account, 1).await?;
        Ok(transactions.iter().map(|(id, _)| *id).max().unwrap_or(0))
    }
}
