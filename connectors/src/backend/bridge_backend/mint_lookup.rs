//! Finds the mint a specific bridge deposit produced, by name rather than by
//! balance.
//!
//! The minter stamps every mint with a memo naming the Ethereum transaction that
//! caused it, so a leg can identify its own credit instead of inferring it from a
//! balance that siblings also settle into. A shared account makes a balance delta
//! ambiguous — a larger sibling credit satisfies a smaller leg's expectation too —
//! and no amount of arithmetic recovers the attribution a delta never carried.
//!
//! The memo is decoded by the minter itself rather than here. Its encoding is the
//! minter's private detail; a copy of it in this repository would be a schema we
//! do not own and would have to keep in step.

use candid::{CandidType, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use serde::Deserialize;

use crate::pipeline_agent::PipelineAgent;

/// Mints scanned per lookup, newest first.
///
/// A credit lands 15-20 minutes after its deposit settles, so it is among the
/// most recent entries on any account this pipeline bridges into. The window
/// bounds the query rather than the search: a leg whose mint has aged out simply
/// reports no match and keeps waiting, which is the safe direction.
const MINT_SCAN_WINDOW: u64 = 50;

#[derive(CandidType)]
struct GetAccountTransactionsArgs {
    account: Account,
    start: Option<Nat>,
    max_results: Nat,
}

#[derive(CandidType, Deserialize)]
struct IndexMint {
    amount: Nat,
    memo: Option<Vec<u8>>,
}

#[derive(CandidType, Deserialize)]
struct IndexTransaction {
    kind: String,
    mint: Option<IndexMint>,
}

#[derive(CandidType, Deserialize)]
struct IndexTransactionWithId {
    transaction: IndexTransaction,
}

#[derive(CandidType, Deserialize)]
struct IndexGetTransactions {
    transactions: Vec<IndexTransactionWithId>,
}

#[derive(CandidType, Deserialize)]
struct IndexGetTransactionsErr {
    message: String,
}

#[derive(CandidType, Deserialize)]
enum IndexGetTransactionsResult {
    Ok(IndexGetTransactions),
    Err(IndexGetTransactionsErr),
}

#[derive(CandidType)]
enum MemoType {
    #[allow(dead_code)]
    Burn,
    Mint,
}

#[derive(CandidType)]
struct DecodeLedgerMemoArgs {
    memo_type: MemoType,
    encoded_memo: Vec<u8>,
}

#[derive(CandidType, Deserialize)]
struct ConvertMintMemo {
    tx_hash: String,
}

/// Only the variant this pipeline produces is declared. The minter wraps this in
/// `opt` precisely so it can add variants, and candid decodes an `opt` it cannot
/// match as `null` rather than failing, so a reimbursement memo -- or any memo
/// added later -- arrives as `Mint(None)` and reads as "not this leg's credit".
/// That is the one direction that cannot book funds nobody holds, and it comes
/// from the match below rather than from swallowing a failed call.
#[derive(CandidType, Deserialize)]
enum MintMemo {
    Convert(ConvertMintMemo),
}

#[derive(CandidType, Deserialize)]
enum DecodedMemo {
    Mint(Option<MintMemo>),
}

#[derive(CandidType, Deserialize)]
enum DecodeLedgerMemoResult {
    Ok(Option<DecodedMemo>),
    Err(Option<DecodeLedgerMemoError>),
}

#[derive(CandidType, Deserialize)]
enum DecodeLedgerMemoError {
    InvalidMemo(String),
}

/// Returns the amount minted for `deposit_tx_hash`, or `None` while no mint on
/// this account names it.
pub(super) async fn find_convert_mint<A: PipelineAgent>(
    agent: &A,
    minter: Principal,
    index: Principal,
    account: Account,
    deposit_tx_hash: &str,
) -> Result<Option<Nat>, String> {
    let args = Encode!(&GetAccountTransactionsArgs {
        account,
        start: None,
        max_results: Nat::from(MINT_SCAN_WINDOW),
    })
    .map_err(|e| format!("encode get_account_transactions args failed: {e}"))?;

    let page = agent
        .call_query::<IndexGetTransactionsResult>(&index, "get_account_transactions", args)
        .await
        .map_err(|e| format!("get_account_transactions failed on index {index}: {e}"))?;
    let page = match page {
        IndexGetTransactionsResult::Ok(page) => page,
        IndexGetTransactionsResult::Err(error) => {
            return Err(format!(
                "index {index} rejected get_account_transactions: {}",
                error.message
            ));
        }
    };

    for entry in page.transactions {
        if entry.transaction.kind != "mint" {
            continue;
        }
        let Some(mint) = entry.transaction.mint else { continue };
        let Some(memo) = mint.memo.clone() else { continue };
        if memo_names_deposit(agent, minter, memo, deposit_tx_hash).await? {
            return Ok(Some(mint.amount));
        }
    }

    Ok(None)
}

/// Whether the minter reads this memo as a conversion of `deposit_tx_hash`.
///
/// Anything the minter cannot decode as such -- a burn, a variant this build does
/// not know, an outright decode error -- is not a match. A transport failure is
/// still an error, because silence about a mint must not look like its absence.
async fn memo_names_deposit<A: PipelineAgent>(
    agent: &A,
    minter: Principal,
    memo: Vec<u8>,
    deposit_tx_hash: &str,
) -> Result<bool, String> {
    let args = Encode!(&DecodeLedgerMemoArgs {
        memo_type: MemoType::Mint,
        encoded_memo: memo,
    })
    .map_err(|e| format!("encode decode_ledger_memo args failed: {e}"))?;

    let decoded = agent
        .call_query::<DecodeLedgerMemoResult>(&minter, "decode_ledger_memo", args)
        .await
        .map_err(|e| format!("decode_ledger_memo failed on minter {minter}: {e}"))?;

    Ok(match decoded {
        DecodeLedgerMemoResult::Ok(Some(DecodedMemo::Mint(Some(MintMemo::Convert(convert))))) => {
            convert.tx_hash.eq_ignore_ascii_case(deposit_tx_hash)
        }
        _ => false,
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Decode, Deserialize};

    /// `memo_names_deposit` propagates a failed `decode_ledger_memo` call, which
    /// is only safe because a memo shape this build does not know is not a
    /// failure. The minter wraps `MintMemo` in `opt` so it can add variants, and
    /// candid decodes an unmatched `opt` as `null`: `ReimburseWithdrawal` exists
    /// on the live minter today and lands on the same account this scan walks.
    /// If that ever became a hard decode error, propagating would turn one
    /// reimbursement into a permanently failing credit scan.
    #[test]
    fn a_mint_memo_this_build_does_not_know_decodes_as_absent_rather_than_failing() {
        // The minter's own declaration, as its candid interface gives it.
        #[derive(CandidType, Deserialize)]
        enum WireMintMemo {
            #[allow(dead_code)]
            Convert(ConvertMintMemo),
            ReimburseWithdrawal {
                withdrawal_id: u64,
            },
        }
        #[derive(CandidType, Deserialize)]
        enum WireDecodedMemo {
            Mint(Option<WireMintMemo>),
        }
        #[derive(CandidType, Deserialize)]
        enum WireResult {
            Ok(Option<WireDecodedMemo>),
            #[allow(dead_code)]
            Err(Option<DecodeLedgerMemoError>),
        }

        let wire = WireResult::Ok(Some(WireDecodedMemo::Mint(Some(WireMintMemo::ReimburseWithdrawal {
            withdrawal_id: 7,
        }))));
        let bytes = candid::Encode!(&wire).expect("the minter's shape encodes");

        let decoded = Decode!(&bytes, DecodeLedgerMemoResult).expect("an unknown variant must not fail to decode");

        assert!(
            matches!(decoded, DecodeLedgerMemoResult::Ok(Some(DecodedMemo::Mint(None)))),
            "an unknown mint memo must arrive as absent, so the match below reads it as not our credit"
        );
    }
}
