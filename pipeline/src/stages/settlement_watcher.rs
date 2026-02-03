use std::sync::Arc;
use std::time::Duration;

use candid::{Encode, Principal};
use num_traits::ToPrimitive;
use tokio::time::sleep;
use tracing::instrument;
use tracing::{info, warn};

use crate::persistance::{LiqMetaWrapper, LiqResultRecord, ResultStatus, WalStore};
use crate::stages::executor::ExecutionReceipt;
use crate::stages::executor::ExecutionStatus;
use crate::swappers::swap_interface::SwapInterface;
use crate::utils::now_ts;
use crate::wal::{decode_receipt_wrapper, encode_meta};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, ProtocolError, TransferStatus};
const MAX_UNPROFITABLE_SECS: i64 = 180;

pub struct SettlementWatcher<A, S, D>
where
    A: PipelineAgent,
    S: SwapInterface,
    D: WalStore,
{
    pub wal: Arc<D>,
    pub agent: Arc<A>,
    pub swapper: Arc<S>,
    pub lending_canister: Principal,
    pub poll_interval: Duration,
}

impl<A, S, D> SettlementWatcher<A, S, D>
where
    A: PipelineAgent + Send + Sync,
    S: SwapInterface + Send + Sync,
    D: WalStore + Send + Sync,
{
    pub fn new(
        wal: Arc<D>,
        agent: Arc<A>,
        swapper: Arc<S>,
        lending_canister: Principal,
        poll_interval: Duration,
    ) -> Self {
        Self {
            wal,
            agent,
            swapper,
            lending_canister,
            poll_interval,
        }
    }

    pub async fn run(self) {
        loop {
            if let Err(err) = self.tick().await {
                warn!("[settlement] tick error: {}", err);
            }
            sleep(self.poll_interval).await;
        }
    }

    async fn tick(&self) -> Result<(), String> {
        let mut rows = self
            .wal
            .list_by_status(ResultStatus::WaitingCollateral, 100)
            .await
            .map_err(|e| e.to_string())?;
        let mut profit_rows = self
            .wal
            .list_by_status(ResultStatus::WaitingProfit, 100)
            .await
            .map_err(|e| e.to_string())?;
        rows.append(&mut profit_rows);

        for row in rows {
            if let Err(err) = self.process_row(row).await {
                warn!("[settlement] row processing failed: {}", err);
            }
        }
        Ok(())
    }

    #[instrument(name = "settlement.process_row", skip_all, err, fields(row_id = %row.id))]
    async fn process_row(&self, row: LiqResultRecord) -> Result<(), String> {
        let meta = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("receipt not found in WAL meta_json for {}", row.id))?;
        let mut receipt: ExecutionReceipt = meta.receipt;

        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| format!("missing liquidation_result for WAL id {}", row.id))?;

        let fresh = match self.refresh_liquidation(liq.id).await {
            Ok(liq) => liq,
            Err(err) => {
                warn!("[settlement] get_liquidation failed liq_id={} err={}", liq.id, err);
                return Ok(());
            }
        };

        let mut updated = false;
        if fresh != *liq {
            receipt.liquidation_result = Some(fresh.clone());
            updated = true;
        }
        if matches!(fresh.collateral_tx.status, TransferStatus::Success)
            && matches!(receipt.status, ExecutionStatus::CollateralTransferFailed(_))
        {
            receipt.status = ExecutionStatus::Success;
            updated = true;
        }
        if updated {
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

        if receipt.request.swap_args.is_none() {
            self.wal
                .update_status(&row.id, ResultStatus::Succeeded, true)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }

        if receipt.request.liquidation.buy_bad_debt {
            self.wal
                .update_status(&row.id, ResultStatus::Enqueued, true)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }

        let swap_req = receipt
            .request
            .swap_args
            .as_ref()
            .ok_or_else(|| "missing swap args".to_string())?;

        let quote = match self.swapper.quote(swap_req).await {
            Ok(q) => q,
            Err(err) => {
                warn!("[settlement] quote failed liq_id={} err={}", liq.id, err);
                return self.handle_unprofitable(&row, liq.id).await;
            }
        };

        let recv = quote.receive_amount.0.to_i128();
        let debt = liq.amounts.debt_repaid.0.to_i128();
        let profitable = matches!((recv, debt), (Some(r), Some(d)) if r >= d);

        if profitable {
            self.wal
                .update_status(&row.id, ResultStatus::Enqueued, true)
                .await
                .map_err(|e| e.to_string())?;
            info!("[settlement] ✅ liq_id={} profitable -> enqueued", liq.id);
            return Ok(());
        }

        self.handle_unprofitable(&row, liq.id).await
    }

    async fn handle_unprofitable(&self, row: &LiqResultRecord, liq_id: u128) -> Result<(), String> {
        if row.status != ResultStatus::WaitingProfit {
            self.wal
                .update_status(&row.id, ResultStatus::WaitingProfit, false)
                .await
                .map_err(|e| e.to_string())?;
            info!("[settlement] ⏳ liq_id={} not profitable -> waiting", liq_id);
            return Ok(());
        }

        let elapsed = now_ts().saturating_sub(row.updated_at);
        if elapsed >= MAX_UNPROFITABLE_SECS {
            self.wal
                .update_failure(
                    &row.id,
                    ResultStatus::FailedPermanent,
                    "unprofitable after 180s".to_string(),
                    true,
                )
                .await
                .map_err(|e| e.to_string())?;
            warn!("[settlement] ❌ liq_id={} unprofitable > 180s -> failed", liq_id);
        }
        Ok(())
    }

    async fn refresh_liquidation(&self, liq_id: u128) -> Result<LiquidationResult, String> {
        let args = Encode!(&liq_id).map_err(|e| format!("get_liquidation encode error: {e}"))?;
        let res = self
            .agent
            .call_query::<Result<LiquidationResult, ProtocolError>>(&self.lending_canister, "get_liquidation", args)
            .await?;
        match res {
            Ok(liq) => Ok(liq),
            Err(err) => Err(format!("get_liquidation error: {err:?}")),
        }
    }

    async fn update_receipt_meta(
        &self,
        row: &LiqResultRecord,
        receipt: &ExecutionReceipt,
        touch: bool,
    ) -> Result<(), String> {
        let mut row = row.clone();
        let wrapper = LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
        };
        encode_meta(&mut row, &wrapper)?;
        if touch {
            row.updated_at = now_ts();
        }
        self.wal.upsert_result(row).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::executors::executor::ExecutorRequest;
    use crate::persistance::{MockWalStore, ResultStatus};
    use crate::stages::executor::ExecutionStatus;
    use crate::swappers::model::{SwapQuote, SwapRequest};
    use crate::swappers::swap_interface::MockSwapInterface;
    use candid::Nat;
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::asset_id::AssetId;
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;
    use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
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
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: collateral_status,
            },
        }
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
        let wrapper = LiqMetaWrapper {
            receipt,
            meta: Vec::new(),
        };
        encode_meta(&mut row, &wrapper).expect("encode_meta should succeed");
        row
    }

    #[tokio::test]
    async fn watcher_enqueues_when_profitable() {
        let liq_id = 9u128;
        let swap_args = make_swap_args();
        let receipt = ExecutionReceipt {
            request: make_request(false, Some(swap_args.clone())),
            liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let row = make_row(ResultStatus::WaitingCollateral, receipt.clone());
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(100usize))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(100usize))
            .times(1)
            .returning(|_, _| Ok(vec![]));
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

        let mut swapper = MockSwapInterface::new();
        let quote = SwapQuote {
            pay_asset: swap_args.pay_asset.clone(),
            pay_amount: swap_args.pay_amount.value.clone(),
            receive_asset: swap_args.receive_asset.clone(),
            receive_amount: Nat::from(2_000_000u64),
            mid_price: 1.0,
            exec_price: 1.0,
            slippage: 0.0,
            legs: vec![],
        };
        swapper
            .expect_quote()
            .withf(move |req| req.pay_asset.symbol == "ckBTC" && req.receive_asset.symbol == "ckUSDT")
            .times(1)
            .returning(move |_| Ok(quote.clone()));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Arc::new(swapper),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher.tick().await.expect("tick should succeed");
    }

    #[tokio::test]
    async fn watcher_fails_after_unprofitable_window() {
        let liq_id = 12u128;
        let swap_args = make_swap_args();
        let mut row = make_row(
            ResultStatus::WaitingProfit,
            ExecutionReceipt {
                request: make_request(false, Some(swap_args.clone())),
                liquidation_result: Some(make_liq_result(liq_id, TransferStatus::Success)),
                status: ExecutionStatus::Success,
                change_received: true,
            },
        );
        row.updated_at = now_ts() - 181;
        let row_id = row.id.clone();

        let mut wal = MockWalStore::new();
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingCollateral), eq(100usize))
            .times(1)
            .returning(|_, _| Ok(vec![]));
        wal.expect_list_by_status()
            .with(eq(ResultStatus::WaitingProfit), eq(100usize))
            .times(1)
            .returning(move |_, _| Ok(vec![row.clone()]));
        wal.expect_update_failure()
            .with(
                eq(row_id.clone()),
                eq(ResultStatus::FailedPermanent),
                eq("unprofitable after 180s".to_string()),
                eq(true),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let mut agent = MockPipelineAgent::new();
        let args = Encode!(&liq_id).unwrap();
        let fresh = make_liq_result(liq_id, TransferStatus::Success);
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .with(eq(Principal::anonymous()), eq("get_liquidation"), eq(args))
            .times(1)
            .returning(move |_, _, _| Ok(Ok(fresh.clone())));

        let mut swapper = MockSwapInterface::new();
        let quote = SwapQuote {
            pay_asset: swap_args.pay_asset.clone(),
            pay_amount: swap_args.pay_amount.value.clone(),
            receive_asset: swap_args.receive_asset.clone(),
            receive_amount: Nat::from(1u64),
            mid_price: 1.0,
            exec_price: 1.0,
            slippage: 0.0,
            legs: vec![],
        };
        swapper
            .expect_quote()
            .withf(move |req| req.pay_asset.symbol == "ckBTC" && req.receive_asset.symbol == "ckUSDT")
            .times(1)
            .returning(move |_| Ok(quote.clone()));

        let watcher = SettlementWatcher::new(
            Arc::new(wal),
            Arc::new(agent),
            Arc::new(swapper),
            Principal::anonymous(),
            Duration::from_secs(3),
        );

        watcher.tick().await.expect("tick should succeed");
    }
}
