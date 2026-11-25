//! Shared builders for the tests of the recovery.
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::ledger_history_backend::{LedgerParty, LedgerTransfer};
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus, TxStatus,
};

use crate::executors::executor::ExecutorRequest;
use crate::swappers::model::SwapRequest;

mod offer_matching;
mod recovery_flows;

pub(super) const OFFER: u64 = 7_179_194;
pub(super) const CREATED_AT: i64 = 1_795_101_146;

pub(super) fn p(text: &str) -> Principal {
    Principal::from_text(text).unwrap()
}

pub(super) fn liquidator() -> Principal {
    p("3ugdn-ltmy3-mzxow-4g5rz-azgmz-3xla6-plvcn-hozc4-br2nb-bn6cv-iae")
}

pub(super) fn debt_pool() -> Principal {
    p("7dcux-qqaaa-aaaae-qfc3a-cai")
}

pub(super) fn debt_ledger() -> Principal {
    p("cngnf-vqaaa-aaaar-qag4q-cai")
}

pub(super) fn collateral_ledger() -> Principal {
    p("ss2fx-dyaaa-aaaar-qacoq-cai")
}

fn token(ledger: Principal, symbol: &str, decimals: u8) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals,
        fee: Nat::from(10_000u64),
    }
}

/// A request for a ckETH collateral and a ckUSDT debt, with a swap.
pub(super) fn request(collateral_share_receiver: bool) -> ExecutorRequest {
    let debt = token(debt_ledger(), "ckUSDT", 6);
    let collateral = token(collateral_ledger(), "ckETH", 18);
    ExecutorRequest {
        liquidation: LiquidationRequest {
            borrower: p("mfddq-cyaaa-aaaaa-aaaaa-aaaaa-aaaaa-pbai"),
            debt_pool_id: debt_pool(),
            collateral_pool_id: p("jrgdg-siaaa-aaaae-qkfmq-cai"),
            debt_amount: Nat::from(OFFER),
            receiver_address: liquidator(),
            buy_bad_debt: false,
            collateral_share_receiver: collateral_share_receiver
                .then_some(Principal::from_text("g4oj7-diaaa-aaaaa-aaaaa-aaaaa-aaaaa-phai").unwrap()),
        },
        swap_args: Some(SwapRequest {
            pay_asset: collateral.asset_id(),
            pay_amount: liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
                collateral.clone(),
                Nat::from(1u8),
            ),
            receive_asset: debt.asset_id(),
            receive_address: None,
            max_slippage_bps: None,
            venue_hint: None,
        }),
        debt_asset: debt,
        collateral_asset: collateral,
        expected_profit: 0,
        ref_price: Nat::from(0u8),
        debt_ref_price: Nat::from(0u8),
        ref_price_at: 0,
        debt_approval_needed: false,
        min_collateral_amount: Nat::from(0u8),
    }
}

/// The canister result of liquidation `id`: a share payout when `share_receiver` is set, else a token payout.
pub(super) fn result(id: u128, share_receiver: Option<Principal>) -> LiquidationResult {
    let done = || TxStatus {
        tx_id: None,
        status: TransferStatus::Success,
    };
    LiquidationResult {
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(6_554_695_335_714_284u64),
            debt_repaid: Nat::from(6_570_340u64),
        },
        collateral_asset: AssetType::CkAsset(collateral_ledger()),
        debt_asset: AssetType::CkAsset(debt_ledger()),
        status: LiquidationStatus::Success,
        timestamp: CREATED_AT as u64,
        change_tx: done(),
        collateral_tx: done(),
        id,
        collateral_share_receiver: share_receiver,
    }
}

/// The offer transfer of liquidation `id`, made `secs_after` the journal entry.
pub(super) fn offer_transfer(id: u128, amount: u64, secs_after: i64) -> LedgerTransfer {
    LedgerTransfer {
        index: 1_000 + id as u64,
        from: LedgerParty::Account(Account {
            owner: liquidator(),
            subaccount: None,
        }),
        to: LedgerParty::Account(Account {
            owner: debt_pool(),
            subaccount: None,
        }),
        amount: Nat::from(amount),
        memo: Some(id.to_be_bytes().to_vec()),
        timestamp_nanos: Some((CREATED_AT + secs_after) as u64 * 1_000_000_000),
    }
}
