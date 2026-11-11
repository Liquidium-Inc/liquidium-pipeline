use std::sync::{Arc, Mutex};

use candid::Principal;
use tracing::instrument;
use tracing::{info, warn};

use crate::liquidation::lending_queries::get_liquidation;
use crate::liquidation::supply_funding::{claim_collateral, pool_free_liquidity};
use crate::persistance::{LiqResultRecord, ResultStatus, WalStore};
use crate::stages::executor::ExecutionReceipt;
use crate::wal::{decode_receipt_wrapper, wal_save_receipt};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::TransferStatus;

/// Rows checked per status in one tick. The watcher walks all rows in pages,
/// so rows that never settle cannot hide newer ones (audit L09).
const SETTLEMENT_PAGE_SIZE: usize = 100;

pub struct SettlementWatcher<A, D>
where
    A: PipelineAgent,
    D: WalStore,
{
    pub wal: Arc<D>,
    pub agent: Arc<A>,
    pub lending_canister: Principal,
    /// The last row id each walk reached. `None` starts from the first row.
    collateral_cursor: Mutex<Option<String>>,
    profit_cursor: Mutex<Option<String>>,
}

#[allow(dead_code)]
impl<A, D> SettlementWatcher<A, D>
where
    A: PipelineAgent + Send + Sync,
    D: WalStore + Send + Sync,
{
    pub fn new(wal: Arc<D>, agent: Arc<A>, lending_canister: Principal) -> Self {
        Self {
            wal,
            agent,
            lending_canister,
            collateral_cursor: Mutex::new(None),
            profit_cursor: Mutex::new(None),
        }
    }

    pub(crate) async fn tick(&self) -> Result<(), String> {
        let mut rows = self
            .next_page(ResultStatus::WaitingCollateral, &self.collateral_cursor)
            .await?;
        let mut profit_rows = self.next_page(ResultStatus::WaitingProfit, &self.profit_cursor).await?;
        rows.append(&mut profit_rows);

        for row in rows {
            if let Err(err) = self.process_row(row).await {
                warn!("[settlement] row processing failed: {}", err);
            }
        }
        Ok(())
    }

    /// The next page of one status, continuing where the last tick stopped.
    async fn next_page(
        &self,
        status: ResultStatus,
        cursor: &Mutex<Option<String>>,
    ) -> Result<Vec<LiqResultRecord>, String> {
        let after_id = cursor.lock().expect("settlement cursor").clone();
        let rows = self
            .wal
            .list_by_status_after(status, after_id, SETTLEMENT_PAGE_SIZE)
            .await
            .map_err(|e| e.to_string())?;
        // A short page ends the walk, so the next tick starts from the first row again.
        *cursor.lock().expect("settlement cursor") = if rows.len() < SETTLEMENT_PAGE_SIZE {
            None
        } else {
            rows.last().map(|row| row.id.clone())
        };
        Ok(rows)
    }

    #[instrument(name = "settlement.process_row", skip_all, err, fields(row_id = %row.id))]
    async fn process_row(&self, row: LiqResultRecord) -> Result<(), String> {
        let meta = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id))?;
        let mut receipt: ExecutionReceipt = meta.receipt;

        let liq_id = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?
            .id;

        let fresh = match get_liquidation(self.agent.as_ref(), &self.lending_canister, liq_id).await {
            Ok(liq) => liq,
            Err(err) => {
                warn!("[settlement] get_liquidation failed liq_id={} err={}", liq_id, err);
                return Ok(());
            }
        };

        if receipt.apply_pool_view(fresh) {
            let touch_meta = row.status != ResultStatus::WaitingProfit;
            self.update_receipt_meta(&row, &receipt, touch_meta).await?;
        }

        if receipt.collateral_held_as_supply {
            self.claim_held_collateral(&row, &mut receipt).await?;
        }

        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

        if !matches!(liq.collateral_tx.status, TransferStatus::Success) {
            if row.status != ResultStatus::WaitingCollateral {
                self.wal
                    .update_status(&row.id, ResultStatus::WaitingCollateral, false)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            return Ok(());
        }

        // A row without a swap is enqueued too: the finalize stage records its
        // profit, export and notification (audit L03).
        self.wal
            .update_status(&row.id, ResultStatus::Enqueued, true)
            .await
            .map_err(|e| e.to_string())?;
        info!("[settlement] ✅ liq_id={} -> enqueued for finalization", liq.id);
        Ok(())
    }

    /// Claims collateral that the liquidation left in the liquidator's supply, once the
    /// collateral pool can pay it. The canister pays a liquidation out once, so a claim whose
    /// answer was lost is simply asked again on the next pass.
    async fn claim_held_collateral(&self, row: &LiqResultRecord, receipt: &mut ExecutionReceipt) -> Result<(), String> {
        let Some(liq) = receipt.liquidation_result.clone() else {
            return Ok(());
        };
        if !matches!(liq.collateral_tx.status, TransferStatus::Pending) {
            return Ok(());
        }

        // Read-only check first, so an illiquid pool costs a query per pass and not an update call.
        let needed = liq.amounts.collateral_received.clone() + receipt.request.collateral_asset.fee();
        let pool = receipt.request.liquidation.collateral_pool_id;
        match pool_free_liquidity(self.agent.as_ref(), &self.lending_canister, pool).await {
            Ok(free) if free >= needed => {}
            Ok(free) => {
                info!(
                    "[settlement] liq_id={} collateral stays supplied: pool has {free} free, {needed} needed",
                    liq.id
                );
                return Ok(());
            }
            Err(err) => {
                warn!("[settlement] liq_id={} pool liquidity read failed: {err}", liq.id);
                return Ok(());
            }
        }

        match claim_collateral(self.agent.as_ref(), &self.lending_canister, liq.id).await {
            Ok(Ok(fresh)) => {
                info!("[settlement] liq_id={} held collateral claimed", liq.id);
                if receipt.apply_pool_view(fresh) {
                    self.update_receipt_meta(row, receipt, true).await?;
                }
            }
            // Refused: nothing changed, so the next pass asks again.
            Ok(Err(refusal)) => info!("[settlement] liq_id={} collateral claim refused: {refusal:?}", liq.id),
            Err(reason) => warn!(
                "[settlement] liq_id={} collateral claim got no answer: {reason}",
                liq.id
            ),
        }
        Ok(())
    }

    async fn update_receipt_meta(
        &self,
        row: &LiqResultRecord,
        receipt: &ExecutionReceipt,
        touch: bool,
    ) -> Result<(), String> {
        wal_save_receipt(&*self.wal, row.clone(), receipt, touch).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::executors::executor::ExecutorRequest;
    use crate::persistance::{
        FinalizerDecisionSnapshot, LiqMetaWrapper, MockWalStore, ResultStatus, WalProfitSnapshot,
    };
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::SwapRequest;
    use crate::utils::now_ts;
    use crate::wal::encode_meta;
    use candid::{Encode, Nat};
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::asset_id::AssetId;
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;
    use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, ProtocolError,
        TransferStatus, TxStatus,
    };
    use mockall::predicate::eq;

    fn make_request(buy_bad_debt: bool, swap_args: Option<SwapRequest>) -> ExecutorRequest {
        let debt_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        let collateral_asset = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };
        ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(0u32),
                receiver_address: Principal::anonymous(),
                buy_bad_debt,
            },
            swap_args,
            debt_asset,
            collateral_asset,
            expected_profit: 0,
            ref_price: Nat::from(0u8),
            debt_ref_price: Nat::from(0u8),
            ref_price_at: 0,
            debt_approval_needed: false,
            supply_funded: false,
            min_collateral_amount: Nat::from(0u8),
        }
    }

    fn make_swap_args() -> SwapRequest {
        let pay_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };
        let pay_amount = ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(1_000_000u64));
        SwapRequest {
            pay_asset: pay_token.asset_id(),
            pay_amount,
            receive_asset: AssetId {
                chain: "icp".to_string(),
                address: "ledger-usdt".to_string(),
                symbol: "ckUSDT".to_string(),
            },
            receive_address: Some("test-address".to_string()),
            max_slippage_bps: Some(100),
            venue_hint: Some("kong".to_string()),
        }
    }

    fn make_liq_result(liq_id: u128, collateral_status: TransferStatus) -> LiquidationResult {
        LiquidationResult {
            id: liq_id,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(0u32),
                debt_repaid: Nat::from(1_000_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 0,
            // Settled, to match the receipts' `change_received: true`.
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: collateral_status,
            },
        }
    }

    /// One tick's first pages: these collateral rows and these profit rows.
    fn expect_pages(wal: &mut MockWalStore, collateral: Vec<LiqResultRecord>, profit: Vec<LiqResultRecord>) {
        wal.expect_list_by_status_after()
            .with(eq(ResultStatus::WaitingCollateral), eq(None), eq(SETTLEMENT_PAGE_SIZE))
            .times(1)
            .return_once(move |_, _, _| Ok(collateral));
        wal.expect_list_by_status_after()
            .with(eq(ResultStatus::WaitingProfit), eq(None), eq(SETTLEMENT_PAGE_SIZE))
            .times(1)
            .return_once(move |_, _, _| Ok(profit));
    }

    fn make_row(status: ResultStatus, receipt: ExecutionReceipt) -> LiqResultRecord {
        let mut row = LiqResultRecord {
            id: receipt.liquidation_result.as_ref().unwrap().id.to_string(),
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now_ts(),
            updated_at: now_ts(),
            meta_json: "{}".to_string(),
        };
        let wrapper = LiqMetaWrapper::new(receipt);
        encode_meta(&mut row, &wrapper).expect("encode_meta should succeed");
        row
    }

    #[tokio::test]
    async fn watcher_enqueues_for_multi_venue_planning() {
        let liq_id = 9u128;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_held_as_supply: false,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        expect_pages(&mut wal, vec![row], vec![]);
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());

        watcher.tick().await.expect("tick should succeed");
    }

    #[tokio::test]
    async fn watcher_enqueues_settled_swap_rows_without_a_quote_dependency() {
        let liq_id = 10u128;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_held_as_supply: false,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        expect_pages(&mut wal, vec![row], vec![]);
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());

        watcher.tick().await.expect("tick should succeed");
    }

    /// Audit L03: a row without a swap also goes to the finalize stage, which
    /// records its profit, export and notification.
    #[tokio::test]
    async fn watcher_enqueues_settled_rows_without_a_swap() {
        let liq_id = 12u128;
        let receipt = ExecutionReceipt {
            request: make_request(false, None),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_held_as_supply: false,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt);
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        expect_pages(&mut wal, vec![row], vec![]);
        wal.expect_update_status()
            .with(eq(row_id), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());
        watcher.tick().await.expect("tick should succeed");
    }

    /// Audit L09: rows that never settle must not hide newer ones. Each tick
    /// continues after the last row it saw, and a short page starts over.
    #[tokio::test]
    async fn watcher_walks_every_waiting_row_in_pages() {
        let page = |ids: std::ops::Range<usize>| {
            ids.map(|i| LiqResultRecord {
                id: format!("row-{i:03}"),
                status: ResultStatus::WaitingCollateral,
                attempt: 0,
                error_count: 0,
                last_error: None,
                created_at: 0,
                updated_at: 0,
                meta_json: "{}".to_string(),
            })
            .collect::<Vec<_>>()
        };
        let full_page = page(0..SETTLEMENT_PAGE_SIZE);
        let last_id = full_page.last().unwrap().id.clone();
        let mut sequence = mockall::Sequence::new();
        let mut wal = MockWalStore::new();
        wal.expect_list_by_status_after()
            .with(eq(ResultStatus::WaitingCollateral), eq(None), eq(SETTLEMENT_PAGE_SIZE))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(move |_, _, _| Ok(full_page));
        wal.expect_list_by_status_after()
            .with(
                eq(ResultStatus::WaitingCollateral),
                eq(Some(last_id)),
                eq(SETTLEMENT_PAGE_SIZE),
            )
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(move |_, _, _| Ok(page(SETTLEMENT_PAGE_SIZE..SETTLEMENT_PAGE_SIZE + 1)));
        wal.expect_list_by_status_after()
            .with(eq(ResultStatus::WaitingCollateral), eq(None), eq(SETTLEMENT_PAGE_SIZE))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(|_, _, _| Ok(vec![]));
        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
        );

        for expected in [SETTLEMENT_PAGE_SIZE, 1, 0] {
            let rows = watcher
                .next_page(ResultStatus::WaitingCollateral, &watcher.collateral_cursor)
                .await
                .expect("page");
            assert_eq!(rows.len(), expected);
        }
    }

    /// A ready settled row is handed to the finalizer without venue-specific routing.
    #[tokio::test]
    async fn watcher_enqueue_behavior_is_independent_of_venue_selection() {
        // given
        const LIQUIDATION_ID: u128 = 11;
        let liq_id = LIQUIDATION_ID;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_held_as_supply: false,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        expect_pages(&mut wal, vec![row], vec![]);
        wal.expect_upsert_result().times(0);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());

        // when
        watcher.tick().await.expect("tick should succeed");

        // Expectations above assert the Enqueued transition.
    }

    #[tokio::test]
    async fn watcher_reenqueues_legacy_waiting_profit_rows() {
        let liq_id = 12u128;
        let swap_args = make_swap_args();
        let row = make_row(
            ResultStatus::WaitingProfit,
            ExecutionReceipt {
                request: make_request(false, Some(swap_args.clone())),
                liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
                status: ExecutionStatus::Success,
                change_received: true,
                collateral_held_as_supply: false,
            },
        );
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        expect_pages(&mut wal, vec![], vec![row]);
        wal.expect_update_status()
            .with(eq(row_id.clone()), eq(ResultStatus::Enqueued), eq(true))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());

        watcher.tick().await.expect("tick should succeed");
    }

    #[tokio::test]
    async fn update_receipt_meta_preserves_wrapper_extensions() {
        let liq_id = 13u128;
        let old_receipt = ExecutionReceipt {
            request: make_request(false, Some(make_swap_args())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Pending)),
            status: ExecutionStatus::CollateralTransferFailed("pending".to_string()),
            change_received: true,
            collateral_held_as_supply: false,
        };
        let new_receipt = ExecutionReceipt {
            request: make_request(false, Some(make_swap_args())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_held_as_supply: false,
        };

        let mut row = make_row(ResultStatus::WaitingCollateral, old_receipt.clone());
        let wrapper = LiqMetaWrapper {
            receipt: old_receipt,
            meta: vec![7, 8, 9],
            finalizer_decision: Some(FinalizerDecisionSnapshot {
                mode: "hybrid".to_string(),
                chosen: "cex".to_string(),
                reason: "test".to_string(),
                min_required_bps: 25.0,
                dex_preview_gross_bps: Some(40.0),
                dex_preview_net_bps: Some(28.0),
                cex_preview_gross_bps: Some(33.0),
                cex_preview_net_bps: Some(26.0),
                ts: 123,
                multi_venue_allocation: None,
            }),
            profit_snapshot: Some(WalProfitSnapshot {
                expected_profit_raw: "1000".to_string(),
                realized_profit_raw: Some("900".to_string()),
                debt_symbol: "ckBTC".to_string(),
                debt_decimals: 8,
                updated_at: 123,
            }),
            venue_execution: None,
            meta_v2: None,
        };
        encode_meta(&mut row, &wrapper).expect("encode wrapper");

        let mut wal = MockWalStore::new();
        wal.expect_upsert_result().times(1).returning(move |updated_row| {
            let updated_wrapper = decode_receipt_wrapper(&updated_row)
                .expect("decode wrapper")
                .expect("wrapper exists");
            assert_eq!(updated_wrapper.meta, vec![7, 8, 9]);
            assert!(updated_wrapper.finalizer_decision.is_some());
            assert!(updated_wrapper.profit_snapshot.is_some());
            assert!(
                matches!(updated_wrapper.receipt.status, ExecutionStatus::Success),
                "receipt status should be updated"
            );
            Ok(())
        });

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(MockPipelineAgent::new()),
            Principal::anonymous(),
        );

        watcher
            .update_receipt_meta(&row, &new_receipt, true)
            .await
            .expect("receipt meta update should succeed");
    }

    mod held_collateral {
        use super::*;
        use crate::liquidation::supply_funding::LendingPoolView;
        use liquidium_pipeline_core::types::protocol_types::AssetType;

        const LIQ_ID: u128 = 77;
        const NET_COLLATERAL: u64 = 500_000;
        const FEE: u64 = 1_000;

        fn held_result(collateral: TransferStatus) -> LiquidationResult {
            let mut liq = make_liq_result(LIQ_ID, collateral);
            liq.amounts.collateral_received = Nat::from(NET_COLLATERAL);
            liq
        }

        fn held_row() -> LiqResultRecord {
            let receipt = ExecutionReceipt {
                request: make_request(false, Some(make_swap_args())),
                liquidation_result: Some(held_result(TransferStatus::Pending)),
                status: ExecutionStatus::CollateralTransferFailed("collateral pending".to_string()),
                change_received: true,
                collateral_held_as_supply: true,
            };
            make_row(ResultStatus::WaitingCollateral, receipt)
        }

        /// An agent whose lending canister still shows the collateral leg pending, and whose
        /// collateral pool has `free` cash.
        fn agent_with_pool_liquidity(free: u64) -> MockPipelineAgent {
            let ray = Nat::from(1_000_000_000_000_000_000_000_000_000u128);
            let mut agent = MockPipelineAgent::new();
            agent
                .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
                .returning(|_, _, _| Ok(Ok(held_result(TransferStatus::Pending))));
            agent
                .expect_call_query::<Vec<LendingPoolView>>()
                .returning(move |_, _, _| {
                    Ok(vec![LendingPoolView {
                        principal: Principal::anonymous(),
                        asset_type: AssetType::Unknown,
                        // One unit above `free`: the debt side is rounded up by one.
                        total_supply_at_last_sync: Nat::from(1_000_000u64 + free + 1),
                        total_debt_at_last_sync: Nat::from(1_000_000u64),
                        lending_index: ray.clone(),
                        borrow_index: ray.clone(),
                    }])
                });
            agent
        }

        #[tokio::test]
        async fn held_collateral_is_claimed_once_the_pool_is_liquid() {
            // given a pool that can pay the collateral plus its transfer fee
            let row = held_row();
            let row_id = row.id.clone();
            let mut wal = MockWalStore::new();
            expect_pages(&mut wal, vec![row], vec![]);
            wal.expect_upsert_result().times(1).returning(|row| {
                let receipt = decode_receipt_wrapper(&row).unwrap().unwrap().receipt;
                let liq = receipt.liquidation_result.unwrap();
                assert!(matches!(liq.collateral_tx.status, TransferStatus::Success));
                assert_eq!(liq.amounts.collateral_received, Nat::from(NET_COLLATERAL - 7));
                Ok(())
            });
            wal.expect_update_status()
                .with(eq(row_id), eq(ResultStatus::Enqueued), eq(true))
                .times(1)
                .returning(|_, _, _| Ok(()));

            let mut agent = agent_with_pool_liquidity(NET_COLLATERAL + FEE);
            let args = Encode!(&LIQ_ID).unwrap();
            agent
                .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
                .with(eq(Principal::anonymous()), eq("claim_liquidation_collateral"), eq(args))
                .times(1)
                .returning(|_, _, _| {
                    // The canister reports what it actually paid out.
                    let mut paid = held_result(TransferStatus::Success);
                    paid.amounts.collateral_received = Nat::from(NET_COLLATERAL - 7);
                    Ok(Ok(paid))
                });

            // when
            let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());
            watcher.tick().await.expect("tick should succeed");
        }

        #[tokio::test]
        async fn held_collateral_is_not_claimed_while_the_pool_is_illiquid() {
            // given a pool one unit short
            let mut wal = MockWalStore::new();
            expect_pages(&mut wal, vec![held_row()], vec![]);
            wal.expect_upsert_result().times(0);
            wal.expect_update_status().times(0);
            let mut agent = agent_with_pool_liquidity(NET_COLLATERAL + FEE - 1);
            agent
                .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
                .times(0);

            // when / then the row keeps waiting and no update call is made
            let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());
            watcher.tick().await.expect("tick should succeed");
        }

        #[tokio::test]
        async fn a_claim_without_an_answer_leaves_the_row_waiting_for_the_next_pass() {
            // given
            let mut wal = MockWalStore::new();
            expect_pages(&mut wal, vec![held_row()], vec![]);
            wal.expect_upsert_result().times(0);
            wal.expect_update_status().times(0);
            let mut agent = agent_with_pool_liquidity(NET_COLLATERAL + FEE);
            agent
                .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
                .times(1)
                .returning(|_, _, _| Err("request timed out".to_string()));

            // when / then nothing is recorded; the canister pays once however often it is asked
            let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());
            watcher.tick().await.expect("tick should succeed");
        }

        #[tokio::test]
        async fn an_ordinary_pending_collateral_transfer_is_never_claimed() {
            // given a cash payout that is still in flight
            let mut row_receipt = decode_receipt_wrapper(&held_row()).unwrap().unwrap().receipt;
            row_receipt.collateral_held_as_supply = false;
            let mut wal = MockWalStore::new();
            expect_pages(
                &mut wal,
                vec![make_row(ResultStatus::WaitingCollateral, row_receipt)],
                vec![],
            );
            wal.expect_upsert_result().times(0);
            wal.expect_update_status().times(0);
            let mut agent = MockPipelineAgent::new();
            agent
                .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
                .returning(|_, _, _| Ok(Ok(held_result(TransferStatus::Pending))));

            // when / then no liquidity read and no claim happen
            let watcher = SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous());
            watcher.tick().await.expect("tick should succeed");
        }
    }
}
