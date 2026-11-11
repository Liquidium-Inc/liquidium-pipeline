use std::sync::{Arc, Mutex};

use candid::Principal;
use tracing::instrument;
use tracing::{info, warn};

use crate::liquidation::lending_queries::get_liquidation;
use crate::liquidation::supply_funding::{CollateralRelease, ReleaseError, release_collateral};
use crate::persistance::{LiqResultRecord, ResultStatus, WalStore};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::wal::{decode_receipt_wrapper, wal_save_receipt};
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::TransferStatus;

/// How long after an accepted withdrawal the collateral counts as arrived. The canister pays
/// it out right after accepting; the finalizer retries if the payout is slower than this.
const COLLATERAL_RELEASE_SETTLE_SECS: i64 = 30;

/// Who withdraws collateral that a supply-funded liquidation left in the liquidator's supply.
#[derive(Debug, Clone, Copy)]
pub struct SupplyRelease {
    pub liquidator: Principal,
    pub profile: Principal,
}

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
    supply_release: Option<SupplyRelease>,
    watchdog: Arc<dyn Watchdog>,
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
            supply_release: None,
            watchdog: noop_watchdog(),
            collateral_cursor: Mutex::new(None),
            profit_cursor: Mutex::new(None),
        }
    }

    /// Enables the release of collateral held as supply (supply funding mode).
    pub fn with_supply_release(mut self, release: SupplyRelease, watchdog: Arc<dyn Watchdog>) -> Self {
        self.supply_release = Some(release);
        self.watchdog = watchdog;
        self
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

        if let Some(release) = receipt.collateral_release.clone() {
            return self.release_held_collateral(row, receipt, release).await;
        }

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

    /// Moves collateral that sits in the liquidator's supply to the account that swaps it, then
    /// enqueues the row. A withdrawal is marked in the WAL before it is called, so a crash in
    /// between never leads to a second withdrawal.
    async fn release_held_collateral(
        &self,
        row: LiqResultRecord,
        mut receipt: ExecutionReceipt,
        release: CollateralRelease,
    ) -> Result<(), String> {
        let liq_id = receipt.liquidation_result.as_ref().map(|liq| liq.id);
        match release {
            CollateralRelease::Requested { at } => {
                if now_ts().saturating_sub(at) < COLLATERAL_RELEASE_SETTLE_SECS {
                    return Ok(());
                }
                receipt.collateral_release = None;
                receipt.status = ExecutionStatus::Success;
                self.update_receipt_meta(&row, &receipt, true).await?;
                self.wal
                    .update_status(&row.id, ResultStatus::Enqueued, true)
                    .await
                    .map_err(|e| e.to_string())?;
                info!("[settlement] ✅ liq_id={liq_id:?} collateral released -> enqueued for finalization");
                Ok(())
            }
            // Read back after a restart: the call may have run.
            CollateralRelease::Withdrawing => {
                self.park_release(&row, "the bot stopped during the withdrawal call")
                    .await
            }
            CollateralRelease::Held => {
                let Some(supply) = self.supply_release else {
                    return Err(format!(
                        "row {} holds collateral as supply but supply funding is not enabled",
                        row.id
                    ));
                };
                let liq = receipt
                    .liquidation_result
                    .clone()
                    .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;
                let request = receipt.request.liquidation.clone();

                receipt.collateral_release = Some(CollateralRelease::Withdrawing);
                self.update_receipt_meta(&row, &receipt, false).await?;

                let outcome = release_collateral(
                    self.agent.as_ref(),
                    &self.lending_canister,
                    supply.liquidator,
                    supply.profile,
                    request.collateral_pool_id,
                    request.receiver_address,
                    liq.amounts.collateral_received.clone(),
                )
                .await;

                match outcome {
                    Ok(withdrawn) => {
                        // The receiver gets the withdrawn amount less the payout's ledger fee.
                        let fee = receipt.request.collateral_asset.fee();
                        let arriving = if withdrawn > fee { withdrawn - fee } else { 0u8.into() };
                        if let Some(result) = receipt.liquidation_result.as_mut() {
                            result.amounts.collateral_received = arriving;
                        }
                        receipt.collateral_release = Some(CollateralRelease::Requested { at: now_ts() });
                        self.update_receipt_meta(&row, &receipt, true).await?;
                        info!("[settlement] liq_id={liq_id:?} collateral withdrawal accepted");
                        Ok(())
                    }
                    Err(ReleaseError::Ambiguous(reason)) => self.park_release(&row, &reason).await,
                    // Nothing was called or the canister refused it: back to waiting.
                    Err(not_released) => {
                        receipt.collateral_release = Some(CollateralRelease::Held);
                        self.update_receipt_meta(&row, &receipt, false).await?;
                        info!("[settlement] liq_id={liq_id:?} collateral stays supplied: {not_released}");
                        Ok(())
                    }
                }
            }
        }
    }

    /// Hands a withdrawal with no decided answer to an operator. The row is never retried.
    async fn park_release(&self, row: &LiqResultRecord, reason: &str) -> Result<(), String> {
        self.wal
            .update_status(&row.id, ResultStatus::OperatorRequired, true)
            .await
            .map_err(|e| e.to_string())?;
        let details = format!(
            "collateral of liquidation row {} was credited as supply, and its withdrawal may or may not have run: {reason}. Check the liquidator's supply in the collateral pool and the receiver's balance before retrying",
            row.id
        );
        warn!("[settlement] {details}");
        self.watchdog
            .notify(WatchdogEvent::OperatorRequired {
                execution_id: row.id.clone(),
                venue: "protocol".to_string(),
                pending_step: "collateral-release".to_string(),
                owner: self
                    .supply_release
                    .map(|supply| supply.profile.to_text())
                    .unwrap_or_default(),
                details,
            })
            .await;
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
            collateral_release: None,
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
            collateral_release: None,
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
            collateral_release: None,
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
            collateral_release: None,
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
                collateral_release: None,
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
            collateral_release: None,
        };
        let new_receipt = ExecutionReceipt {
            request: make_request(false, Some(make_swap_args())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
            collateral_release: None,
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

    mod collateral_release {
        use super::*;
        use crate::liquidation::supply_funding::{LendingPoolView, WithdrawOutflow};
        use liquidium_pipeline_core::types::protocol_types::AssetType;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const LIQ_ID: u128 = 77;
        const SEIZED: u64 = 500_000;
        const FEE: u64 = 1_000;

        #[derive(Default)]
        struct CountingWatchdog(AtomicUsize);

        #[async_trait::async_trait]
        impl Watchdog for CountingWatchdog {
            async fn notify(&self, _ev: WatchdogEvent<'_>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn held_receipt(release: CollateralRelease) -> ExecutionReceipt {
            let mut liq = make_liq_result(LIQ_ID, TransferStatus::Success);
            liq.amounts.collateral_received = Nat::from(SEIZED);
            ExecutionReceipt {
                request: make_request(false, Some(make_swap_args())),
                liquidation_result: Some(liq),
                status: ExecutionStatus::CollateralTransferFailed("held as supply".to_string()),
                change_received: true,
                collateral_release: Some(release),
            }
        }

        /// A WAL that serves `row` once and records every receipt written back.
        fn wal_with(row: LiqResultRecord) -> (MockWalStore, Arc<Mutex<Vec<ExecutionReceipt>>>) {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let mut wal = MockWalStore::new();
            expect_pages(&mut wal, vec![row], vec![]);
            let sink = saved.clone();
            wal.expect_upsert_result().returning(move |row| {
                let receipt = decode_receipt_wrapper(&row).unwrap().unwrap().receipt;
                sink.lock().unwrap().push(receipt);
                Ok(())
            });
            (wal, saved)
        }

        /// An agent whose collateral pool has `free` cash.
        fn agent_with_pool_liquidity(free: u64) -> MockPipelineAgent {
            let ray = Nat::from(1_000_000_000_000_000_000_000_000_000u128);
            let mut agent = MockPipelineAgent::new();
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

        fn watcher(
            wal: MockWalStore,
            agent: MockPipelineAgent,
            watchdog: Arc<CountingWatchdog>,
        ) -> SettlementWatcher<MockPipelineAgent, MockWalStore> {
            SettlementWatcher::new(Arc::new(wal), Arc::new(agent), Principal::anonymous()).with_supply_release(
                SupplyRelease {
                    liquidator: Principal::anonymous(),
                    profile: Principal::anonymous(),
                },
                watchdog,
            )
        }

        #[tokio::test]
        async fn held_collateral_is_withdrawn_once_the_pool_is_liquid() {
            // given
            let row = make_row(ResultStatus::WaitingCollateral, held_receipt(CollateralRelease::Held));
            let (mut wal, saved) = wal_with(row);
            wal.expect_update_status().times(0);
            let mut agent = agent_with_pool_liquidity(SEIZED);
            agent
                .expect_call_update::<Result<WithdrawOutflow, ProtocolError>>()
                .withf(|_, method, _| method == "withdraw")
                .times(1)
                .returning(|_, _, _| {
                    Ok(Ok(WithdrawOutflow {
                        amount: Nat::from(SEIZED),
                    }))
                });

            // when
            watcher(wal, agent, Arc::default()).tick().await.unwrap();

            // then the call is marked first, and the accepted withdrawal is recorded after it
            let saved = saved.lock().unwrap();
            assert_eq!(saved[0].collateral_release, Some(CollateralRelease::Withdrawing));
            let last = saved.last().unwrap();
            assert!(matches!(
                last.collateral_release,
                Some(CollateralRelease::Requested { .. })
            ));
            assert_eq!(
                last.liquidation_result.as_ref().unwrap().amounts.collateral_received,
                Nat::from(SEIZED - FEE),
                "the swap sees what arrives after the payout's ledger fee"
            );
        }

        #[tokio::test]
        async fn held_collateral_waits_while_the_pool_is_illiquid() {
            // given a pool one unit short
            let row = make_row(ResultStatus::WaitingCollateral, held_receipt(CollateralRelease::Held));
            let (mut wal, saved) = wal_with(row);
            wal.expect_update_status().times(0);
            let mut agent = agent_with_pool_liquidity(SEIZED - 1);
            agent
                .expect_call_update::<Result<WithdrawOutflow, ProtocolError>>()
                .times(0);

            // when
            watcher(wal, agent, Arc::default()).tick().await.unwrap();

            // then no withdrawal is called and the row goes back to waiting
            assert_eq!(
                saved.lock().unwrap().last().unwrap().collateral_release,
                Some(CollateralRelease::Held)
            );
        }

        #[tokio::test]
        async fn a_withdrawal_without_an_answer_goes_to_an_operator_and_is_not_repeated() {
            // given
            let row = make_row(ResultStatus::WaitingCollateral, held_receipt(CollateralRelease::Held));
            let row_id = row.id.clone();
            let (mut wal, saved) = wal_with(row);
            wal.expect_update_status()
                .with(eq(row_id), eq(ResultStatus::OperatorRequired), eq(true))
                .times(1)
                .returning(|_, _, _| Ok(()));
            let mut agent = agent_with_pool_liquidity(SEIZED);
            agent
                .expect_call_update::<Result<WithdrawOutflow, ProtocolError>>()
                .times(1)
                .returning(|_, _, _| Err("request timed out".to_string()));
            let watchdog = Arc::new(CountingWatchdog::default());

            // when
            watcher(wal, agent, watchdog.clone()).tick().await.unwrap();

            // then
            assert_eq!(
                saved.lock().unwrap().last().unwrap().collateral_release,
                Some(CollateralRelease::Withdrawing)
            );
            assert_eq!(watchdog.0.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn a_withdrawal_interrupted_by_a_restart_goes_to_an_operator() {
            // given a row left in the middle of the withdrawal call
            let row = make_row(
                ResultStatus::WaitingCollateral,
                held_receipt(CollateralRelease::Withdrawing),
            );
            let row_id = row.id.clone();
            let (mut wal, _saved) = wal_with(row);
            wal.expect_update_status()
                .with(eq(row_id), eq(ResultStatus::OperatorRequired), eq(true))
                .times(1)
                .returning(|_, _, _| Ok(()));
            let mut agent = MockPipelineAgent::new();
            agent
                .expect_call_update::<Result<WithdrawOutflow, ProtocolError>>()
                .times(0);
            let watchdog = Arc::new(CountingWatchdog::default());

            // when
            watcher(wal, agent, watchdog.clone()).tick().await.unwrap();

            // then
            assert_eq!(watchdog.0.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn an_accepted_withdrawal_is_enqueued_after_the_settle_window() {
            // given a withdrawal accepted long ago
            let row = make_row(
                ResultStatus::WaitingCollateral,
                held_receipt(CollateralRelease::Requested { at: 0 }),
            );
            let row_id = row.id.clone();
            let (mut wal, saved) = wal_with(row);
            wal.expect_update_status()
                .with(eq(row_id), eq(ResultStatus::Enqueued), eq(true))
                .times(1)
                .returning(|_, _, _| Ok(()));

            // when
            watcher(wal, MockPipelineAgent::new(), Arc::default())
                .tick()
                .await
                .unwrap();

            // then
            let saved = saved.lock().unwrap();
            assert_eq!(saved.last().unwrap().collateral_release, None);
            assert!(matches!(saved.last().unwrap().status, ExecutionStatus::Success));
        }
    }
}
