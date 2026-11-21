//! The pure decisions of the recovery: which ledger transfer is the offer of a journal entry, and whether a canister result fits it.
use candid::{Nat, Principal};
use liquidium_pipeline_connectors::backend::ledger_history_backend::LedgerTransfer;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::protocol_types::{AssetType, LiquidationResult};

use crate::executors::executor::ExecutorRequest;
use crate::liquidation::ledger_party_utils::{is_default_account, is_owned_by};

/// The canister writes the liquidation id into the memo of the offer transfer, as 16 bytes.
const LIQUIDATION_MEMO_BYTES: usize = 16;
/// A transfer can be this much older than the journal entry, for a difference between the two clocks.
const CLOCK_TOLERANCE_SECS: i64 = 120;
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// The facts of one journal entry that identify its offer transfer.
#[derive(Debug, Clone)]
pub(super) struct OfferQuery {
    /// The account that pays the offer.
    pub(super) liquidator: Principal,
    pub(super) debt_pool: Principal,
    /// The exact amount of the offer, in native units.
    pub(super) offer: Nat,
    /// The time when the bot wrote the journal entry, in seconds.
    pub(super) created_at: i64,
}

/// The result of the search for the offer transfer of one journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OfferTransfer {
    /// Exactly one transfer fits. It has this liquidation id.
    Found(u128),
    /// No transfer fits. This does not prove that the call did not run, because the index can be behind.
    NotFound,
    /// More than one transfer fits, so the bot cannot tell which one belongs to the entry.
    Ambiguous,
}

/// The reason that a canister result does not fit a journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Mismatch {
    LiquidationId,
    DebtAsset,
    CollateralAsset,
    /// The result is a share payout, but the request did not ask for shares.
    SharesNotRequested,
}

/// Returns the liquidation id of a memo with exactly 16 bytes, for example the bytes of `1833`.
pub(super) fn liquidation_id_from_memo(memo: &[u8]) -> Option<u128> {
    let bytes: [u8; LIQUIDATION_MEMO_BYTES] = memo.try_into().ok()?;
    Some(u128::from_be_bytes(bytes))
}

/// Returns the liquidation ids of the transfers that can be the offer of `query`.
/// Such a transfer goes from the liquidator to the debt pool, has the exact offer amount, is not older than the entry, and has an id as memo.
pub(super) fn offer_candidates(query: &OfferQuery, transfers: &[LedgerTransfer]) -> Vec<u128> {
    let earliest_nanos = (query.created_at - CLOCK_TOLERANCE_SECS).max(0) as u64 * NANOS_PER_SEC;
    let mut ids: Vec<u128> = transfers
        .iter()
        .filter(|transfer| is_default_account(&transfer.from, query.liquidator))
        .filter(|transfer| is_owned_by(&transfer.to, query.debt_pool))
        .filter(|transfer| transfer.amount == query.offer)
        // A transfer without a time cannot be placed after the entry, so it does not count.
        .filter(|transfer| transfer.timestamp_nanos.is_some_and(|time| time >= earliest_nanos))
        .filter_map(|transfer| transfer.memo.as_deref().and_then(liquidation_id_from_memo))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Returns the one id that belongs to the entry, from the candidates that no row and no other entry has yet.
pub(super) fn choose_offer(open_candidates: &[u128]) -> OfferTransfer {
    match open_candidates {
        [] => OfferTransfer::NotFound,
        [id] => OfferTransfer::Found(*id),
        _ => OfferTransfer::Ambiguous,
    }
}

fn is_asset_of(asset: &AssetType, token: &ChainToken) -> bool {
    matches!((asset, token), (AssetType::CkAsset(ledger), ChainToken::Icp { ledger: expected, .. }) if ledger == expected)
}

/// Returns `Ok` when the canister result is the answer to the request of the journal entry.
pub(super) fn confirms(request: &ExecutorRequest, id: u128, result: &LiquidationResult) -> Result<(), Mismatch> {
    if result.id != id {
        return Err(Mismatch::LiquidationId);
    }
    if !is_asset_of(&result.debt_asset, &request.debt_asset) {
        return Err(Mismatch::DebtAsset);
    }
    if !is_asset_of(&result.collateral_asset, &request.collateral_asset) {
        return Err(Mismatch::CollateralAsset);
    }
    let asked_for_shares = request.liquidation.collateral_as_shares == Some(true);
    if result.collateral_share_receiver.is_some() && !asked_for_shares {
        return Err(Mismatch::SharesNotRequested);
    }
    Ok(())
}
