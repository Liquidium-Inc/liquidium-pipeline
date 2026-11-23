use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use candid::{Decode, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationInflowRecord, LiquidationRequest, LiquidationStatus, TxStatus,
};

use crate::{
    approval_state::ApprovalState, persistance::MockLiquidationIntentStore, stage::PipelineStage,
    swappers::model::SwapRequest,
};

fn p(text: &str) -> Principal {
    Principal::from_text(text).expect("invalid principal")
}

fn make_request(borrower: Principal, debt_asset: ChainToken) -> ExecutorRequest {
    ExecutorRequest {
        liquidation: LiquidationRequest {
            borrower,
            debt_pool_id: p("hkmli-faaaa-aaaar-qb4ba-cai"),
            collateral_pool_id: p("hnnn4-iyaaa-aaaar-qb4bq-cai"),
            debt_amount: Nat::from(11_244u64),
            receiver_address: p("2vxsx-fae"),
            buy_bad_debt: false,
            collateral_share_receiver: None,
        },
        swap_args: Some(SwapRequest {
            pay_asset: debt_asset.asset_id(),
            pay_amount: liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
                debt_asset.clone(),
                Nat::from(1_000u64),
            ),
            receive_asset: debt_asset.asset_id(),
            receive_address: None,
            max_slippage_bps: None,
            venue_hint: None,
        }),
        debt_asset: debt_asset.clone(),
        collateral_asset: debt_asset,
        expected_profit: 3,
        ref_price: Nat::from(0u8),
        debt_ref_price: Nat::from(0u8),
        ref_price_at: 0,
        debt_approval_needed: true,
        min_collateral_amount: Nat::from(42u64),
    }
}

fn make_liquidation_result(id: u128) -> LiquidationResult {
    LiquidationResult {
        amounts: LiquidationAmounts {
            collateral_received: Nat::from(0u8),
            debt_repaid: Nat::from(0u8),
        },
        collateral_asset: AssetType::Unknown,
        debt_asset: AssetType::Unknown,
        status: LiquidationStatus::Success,
        timestamp: 0,
        change_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Success,
        },
        collateral_tx: TxStatus {
            tx_id: None,
            status: TransferStatus::Success,
        },
        id,
        collateral_share_receiver: None,
    }
}

fn inflow_record() -> LiquidationInflowRecord {
    LiquidationInflowRecord {
        debt_amount: Nat::from(11_244u64),
        collateral_pool_id: p("hnnn4-iyaaa-aaaar-qb4bq-cai"),
        timestamp: 0,
        liquidator: p("2vxsx-fae"),
        debt_pool_id: p("hkmli-faaaa-aaaar-qb4ba-cai"),
    }
}

fn lending_canister() -> Principal {
    p("nja4y-2yaaa-aaaae-qddxa-cai")
}

fn test_executor(
    agent: MockPipelineAgent,
    intents: MockLiquidationIntentStore,
) -> BasicExecutor<MockPipelineAgent, MockLiquidationIntentStore> {
    BasicExecutor::new(
        Arc::new(agent),
        Account {
            owner: p("2vxsx-fae"),
            subaccount: None,
        },
        lending_canister(),
        Arc::new(intents),
        Arc::new(ApprovalState::new()),
    )
}

/// A native ICP liquidation for the default borrower.
fn icp_request() -> ExecutorRequest {
    make_request(
        p("2vxsx-fae"),
        ChainToken::Icp {
            ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        },
    )
}

fn accepting_intent_store(expected: usize) -> MockLiquidationIntentStore {
    let mut intents = MockLiquidationIntentStore::new();
    intents
        .expect_create_submitting()
        .times(expected)
        .returning(|_, _| Ok(()));
    intents
        .expect_mark_accepted()
        .times(expected)
        .returning(|_, _, _| Ok(()));
    intents
}

#[derive(Default)]
struct RecordingWatchdog(std::sync::Mutex<Vec<(String, String)>>);

#[async_trait]
impl crate::watchdog::Watchdog for RecordingWatchdog {
    async fn notify(&self, event: WatchdogEvent<'_>) {
        if let WatchdogEvent::OperatorRequired {
            execution_id,
            pending_step,
            ..
        } = event
        {
            self.0.lock().unwrap().push((execution_id, pending_step));
        }
    }
}

mod execution_flows;
mod profile_errors;
mod receipts;
