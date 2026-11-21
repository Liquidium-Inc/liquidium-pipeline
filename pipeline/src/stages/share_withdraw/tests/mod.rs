//! Shared builders for the tests of the withdraw step.
use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus, TxStatus,
};

use crate::executors::executor::ExecutorRequest;
use crate::liquidation::share_position_adapter::{MockSharePositions, SharePosition, WithdrawAccepted};
use crate::persistance::sqlite::SqliteWalStore;
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::swappers::model::SwapRequest;

const SEIZED: u64 = 100_000;

fn p(text: &str) -> Principal {
    Principal::from_text(text).unwrap()
}

fn pool() -> Principal {
    p("42svn-2yaaa-aaaae-qfcsq-cai")
}

fn trader() -> Principal {
    p("q4iil-7u3vj-sta62-rv37o-mquq5-bdnbh-lyw22-laizm-tirnp-2p7yo-dae")
}

fn liquidator() -> Principal {
    p("3ugdn-ltmy3-mzxow-4g5rz-azgmz-3xla6-plvcn-hozc4-br2nb-bn6cv-iae")
}

fn token(symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger: p("mxzaz-hqaaa-aaaar-qaada-cai"),
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn held_record(id: &str, seized: u64, created_at: i64, with_swap: bool) -> LiqResultRecord {
    let collateral = token("ckBTC", 10);
    let debt = token("ckUSDC", 10_000);
    let receipt = ExecutionReceipt {
        request: ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: pool(),
                debt_amount: Nat::from(50_000_000u64),
                receiver_address: liquidator(),
                buy_bad_debt: false,
                collateral_as_shares: Some(true),
            },
            swap_args: with_swap.then(|| SwapRequest {
                pay_asset: collateral.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(collateral.clone(), Nat::from(seized)),
                receive_asset: debt.asset_id(),
                receive_address: None,
                max_slippage_bps: None,
                venue_hint: None,
            }),
            debt_asset: debt,
            collateral_asset: collateral,
            expected_profit: 0,
            ref_price: Nat::from(60_000u64) * Nat::from(10u128.pow(27)),
            debt_ref_price: Nat::from(10u128.pow(27)),
            ref_price_at: 1,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(0u8),
        },
        liquidation_result: Some(LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(seized),
                debt_repaid: Nat::from(50_000_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 1,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            id: id.parse().unwrap(),
            collateral_share_receiver: Some(liquidator()),
        }),
        status: ExecutionStatus::Success,
        change_received: true,
    };
    let wrapper = LiqMetaWrapper {
        share_holding: Some(ShareHolding::new(liquidator(), pool(), Nat::from(seized), created_at)),
        ..LiqMetaWrapper::new(receipt)
    };
    let mut row = LiqResultRecord {
        id: id.to_string(),
        status: ResultStatus::HeldAsShares,
        attempt: 0,
        error_count: 0,
        last_error: None,
        created_at,
        updated_at: created_at,
        meta_json: String::new(),
    };
    encode_meta(&mut row, &wrapper).unwrap();
    row
}

struct Fixture {
    _file: tempfile::NamedTempFile,
    wal: Arc<SqliteWalStore>,
}

impl Fixture {
    async fn with_rows(rows: Vec<LiqResultRecord>) -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        let wal = Arc::new(SqliteWalStore::new(file.path().to_str().unwrap()).unwrap());
        for row in rows {
            wal.upsert_result(row).await.unwrap();
        }
        Self { _file: file, wal }
    }

    /// Each call makes a new watcher, as after a restart. Thus the state must come from the store.
    async fn tick(&self, positions: MockSharePositions) {
        ShareWithdrawWatcher::new(self.wal.clone(), Arc::new(positions), trader(), liquidator())
            .tick()
            .await
            .unwrap();
    }

    async fn row(&self, id: &str) -> (LiqResultRecord, LiqMetaWrapper) {
        let row = self.wal.get_result(id).await.unwrap().unwrap();
        let wrapper = decode_receipt_wrapper(&row).unwrap().unwrap();
        (row, wrapper)
    }

    async fn holding(&self, id: &str) -> ShareHolding {
        self.row(id).await.1.share_holding.unwrap()
    }

    /// Makes the next try due now, as at the end of the wait.
    async fn make_due(&self, id: &str) {
        let (mut row, mut wrapper) = self.row(id).await;
        wrapper.share_holding.as_mut().unwrap().next_try_at = 0;
        encode_meta(&mut row, &wrapper).unwrap();
        self.wal.upsert_result(row).await.unwrap();
    }
}

/// The share count of the test position. Interest changes the native value only.
const SHARES: u64 = 777;

fn position(native: u64, shares: u64) -> SharePosition {
    SharePosition {
        native: Nat::from(native),
        shares: Nat::from(shares),
    }
}

/// A pool that can pay, with a position of `deposited` and a destination whose newest transaction is 500.
fn liquid_pool(liquidity: u64, deposited: u64) -> MockSharePositions {
    let mut positions = MockSharePositions::new();
    positions
        .expect_available_liquidity()
        .returning(move |_| Ok(Nat::from(liquidity)));
    positions
        .expect_position()
        .returning(move |_, _| Ok(position(deposited, SHARES)));
    positions.expect_newest_index().returning(|_, _| Ok(500));
    positions
}

fn accept(positions: &mut MockSharePositions, expected_ask: u64, amount: u64, outflow_id: &'static str) {
    positions
        .expect_withdraw()
        .times(1)
        .returning(move |profile, withdraw_pool, ask, destination| {
            assert_eq!(profile, liquidator());
            assert_eq!(withdraw_pool, pool());
            assert_eq!(ask, Nat::from(expected_ask));
            assert_eq!(destination, trader());
            Ok(WithdrawAccepted {
                outflow_id: outflow_id.to_string(),
                amount: Nat::from(amount),
            })
        });
}

fn payout(arrived: Option<u64>) -> MockSharePositions {
    let mut positions = MockSharePositions::new();
    positions
        .expect_arrived()
        .returning(move |_| Ok(arrived.map(Nat::from)));
    positions
}

mod withdraw_flows;
mod withdraw_rules;
