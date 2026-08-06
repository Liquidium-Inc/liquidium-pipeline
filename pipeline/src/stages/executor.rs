use async_trait::async_trait;
use candid::Encode;

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use tracing::{debug, info, warn};

use crate::{
    executors::{basic::basic_executor::BasicExecutor, executor::ExecutorRequest},
    persistance::LiquidationIntentStore,
    stage::PipelineStage,
};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use liquidium_pipeline_core::types::protocol_types::{
    LiquidationResult, LiquidationStatus, ProtocolError, TransferStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Pending,
    Success,
    LiquidationCallFailed(String),
    FailedLiquidation(String),
    CollateralTransferFailed(String),
    ChangeTransferFailed(String),
    SwapFailed(String),
}

impl ExecutionStatus {
    pub fn description(&self) -> String {
        match self {
            ExecutionStatus::Pending => "Pending".to_string(),
            ExecutionStatus::Success => "Success".to_string(),
            ExecutionStatus::LiquidationCallFailed(msg) => format!("LiquidationCallFailed: {}", msg),
            ExecutionStatus::FailedLiquidation(msg) => format!("FailedLiquidation: {}", msg),
            ExecutionStatus::CollateralTransferFailed(msg) => format!("CollateralTransferFailed: {}", msg),
            ExecutionStatus::ChangeTransferFailed(msg) => format!("ChangeTransferFailed: {}", msg),
            ExecutionStatus::SwapFailed(msg) => format!("SwapFailed: {}", msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub request: ExecutorRequest,
    pub liquidation_result: Option<LiquidationResult>,
    pub status: ExecutionStatus,
    /// Whether the change transfer had already settled *at execution time*.
    ///
    /// This is a single observation, not a durable verdict: the protocol
    /// queues the change alongside the liquidation, so `false` here usually
    /// means "not yet" rather than "never", and nothing re-reads it later.
    /// Treat it as a timing hint only -- it is not evidence that funds are
    /// missing, and no retry or recovery keys off it.
    pub change_received: bool,
}

#[async_trait]
impl<'a, A: PipelineAgent, D: LiquidationIntentStore> PipelineStage<'a, Vec<ExecutorRequest>, Vec<ExecutionReceipt>>
    for BasicExecutor<A, D>
{
    #[instrument(name = "executor.process", skip_all, err, fields(request_count = executor_requests.len()))]
    async fn process(&self, executor_requests: &'a Vec<ExecutorRequest>) -> Result<Vec<ExecutionReceipt>, String> {
        debug!("Executing request {:?}", executor_requests);
        // One future per request, all run concurrently
        let futures = executor_requests.iter().map(|executor_request| {
            let executor_request = executor_request.clone();

            async move {
                let mut receipt = ExecutionReceipt {
                    request: executor_request.clone(),
                    liquidation_result: None,
                    status: ExecutionStatus::Success,
                    change_received: true,
                };

                let liq_req = executor_request.liquidation.clone();

                info!(
                    "[executor] ⚡ liquidation req | borrower={} | debt_pool={} | collateral_pool={} | debt={} | bad_debt={} | min_collateral_amount={}",
                    liq_req.borrower.to_text(),
                    liq_req.debt_pool_id.to_text(),
                    liq_req.collateral_pool_id.to_text(),
                    liq_req.debt_amount,
                    liq_req.buy_bad_debt,
                    executor_request.min_collateral_amount
                );

                let args =
                    Encode!(&liq_req, &executor_request.min_collateral_amount).map_err(|e| e.to_string())?;
                let intent_id = uuid::Uuid::new_v4().to_string();
                self.intents
                    .create_submitting(&intent_id, &executor_request)
                    .await
                    .map_err(|error| format!("failed to persist liquidation intent {intent_id}: {error}"))?;

                let liq_call = match self
                    .agent
                    .call_update::<Result<LiquidationResult, ProtocolError>>(
                        &self.lending_canister,
                        "liquidate_with_slippage",
                        args,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        warn!("Liquidation call failed {err}");
                        receipt.status = ExecutionStatus::LiquidationCallFailed(err.clone());
                        self.intents
                            .mark_ambiguous(&intent_id, &err)
                            .await
                            .map_err(|store_error| {
                                format!("failed to mark liquidation intent {intent_id} ambiguous: {store_error}")
                            })?;
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                };

                let liq = match liq_call {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(
                            "[executor] liquidate_with_slippage rejected by canister | borrower={} | debt_pool={} | collateral_pool={} | debt={} | min_collateral_amount={} | err={:?}",
                            liq_req.borrower.to_text(),
                            liq_req.debt_pool_id.to_text(),
                            liq_req.collateral_pool_id.to_text(),
                            liq_req.debt_amount,
                            executor_request.min_collateral_amount,
                            err
                        );
                        let error = format!("{:?}", err);
                        receipt.status = ExecutionStatus::FailedLiquidation(error.clone());
                        self.intents
                            .mark_failed(&intent_id, None, Some(receipt.clone()), &error)
                            .await
                            .map_err(|store_error| {
                                format!("failed to mark liquidation intent {intent_id} failed: {store_error}")
                            })?;
                        return Ok::<ExecutionReceipt, String>(receipt);
                    }
                };

                receipt.liquidation_result = Some(liq.clone());
                if let LiquidationStatus::FailedLiquidation(err) = &liq.status {
                    warn!(
                        "[executor] liquidate_with_slippage returned FailedLiquidation | liq_id={} | borrower={} | debt_pool={} | collateral_pool={} | debt={} | min_collateral_amount={} | err={}",
                        liq.id,
                        liq_req.borrower.to_text(),
                        liq_req.debt_pool_id.to_text(),
                        liq_req.collateral_pool_id.to_text(),
                        liq_req.debt_amount,
                        executor_request.min_collateral_amount,
                        err
                    );

                    receipt.status = ExecutionStatus::FailedLiquidation(err.clone());
                    self.intents
                        .mark_failed(
                            &intent_id,
                            Some(liq.id.to_string()),
                            Some(receipt.clone()),
                            err,
                        )
                        .await
                        .map_err(|store_error| {
                            format!("failed to mark liquidation intent {intent_id} failed: {store_error}")
                        })?;

                    return Ok::<ExecutionReceipt, String>(receipt);
                }

                // The change transfer returns the unspent part of the debt
                // tokens we sent. That is our money, but it is not on the
                // critical path: the collateral leg below is what has to be
                // swapped and finalized. Returning here dropped the receipt
                // before it reached the WAL, and the WAL is the only handoff to
                // the finalizer -- so the liquidation settled on-chain and then
                // was never touched again, leaving the collateral stranded.
                //
                // `change_tx` is read once, in the same response that reports the
                // liquidation, so a change the protocol has queued but not yet
                // settled reads as `Pending` here and is never re-observed. That
                // is the ordinary case and it settles seconds later, so it must
                // not be reported as money that failed to arrive.
                match &liq.change_tx.status {
                    TransferStatus::Success => {}
                    TransferStatus::Pending => {
                        info!(
                            "[executor] 💱 change still settling at execution time; continuing with the collateral leg | liq_id={}",
                            liq.id
                        );
                        receipt.change_received = false;
                    }
                    TransferStatus::Failed(err) => {
                        warn!(
                            "[executor] 💱 change transfer failed; the unspent debt is still with the protocol | liq_id={} err={}",
                            liq.id, err
                        );
                        receipt.change_received = false;
                    }
                }

                match &liq.collateral_tx.status {
                    TransferStatus::Success => {}
                    TransferStatus::Pending => {
                        info!(
                            "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                            liq.collateral_tx.status, liq.id
                        );
                        receipt.status = ExecutionStatus::CollateralTransferFailed("collateral pending".to_string());
                    }
                    TransferStatus::Failed(err) => {
                        info!(
                            "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                            liq.collateral_tx.status, liq.id
                        );
                        receipt.status = ExecutionStatus::CollateralTransferFailed(err.clone());
                    }
                }

                debug!("Executed liquidation {:?}", liq);
                self.intents
                    .mark_accepted(&intent_id, &liq.id.to_string(), &receipt)
                    .await
                    .map_err(|store_error| {
                        format!("failed to mark liquidation intent {intent_id} accepted: {store_error}")
                    })?;

                Ok::<ExecutionReceipt, String>(receipt)
            }
        });

        let results = join_all(futures).await;

        let mut liquidations = Vec::with_capacity(results.len());
        for res in results {
            let receipt = res?;
            debug!("Receipt result {:?}", receipt);
            liquidations.push(receipt);
        }

        Ok(liquidations)
    }
}

#[cfg(test)]
mod tests {
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
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationStatus, TxStatus,
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
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Success,
            },
            id,
        }
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

    #[tokio::test]
    async fn executor_calls_liquidate_with_slippage_without_allowance_preflight() {
        let lending_canister = p("nja4y-2yaaa-aaaae-qddxa-cai");
        let first_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let second_ledger = p("ryjl3-tyaaa-aaaaa-aaaba-cai");

        let mut agent = MockPipelineAgent::new();

        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .withf(move |canister, method, arg| {
                if *canister != lending_canister || method != "liquidate_with_slippage" {
                    return false;
                }

                match candid::Decode!(arg, LiquidationRequest, Nat) {
                    Ok((liq_req, min_collateral_amount)) => {
                        liq_req.debt_amount == Nat::from(11_244u64) && min_collateral_amount == Nat::from(42u64)
                    }
                    Err(_) => false,
                }
            })
            .times(2)
            .returning(move |_, _, _| Ok(Ok(make_liquidation_result(31u128))));

        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            lending_canister,
            Arc::new(accepting_intent_store(2)),
            Arc::new(ApprovalState::new()),
        );

        let request_fail = make_request(
            p("tfeop-4aaaa-aaaaa-aaaaa-aaaaa-aaaaa-bdai"),
            ChainToken::Icp {
                ledger: first_ledger,
                symbol: "ckBTC".to_string(),
                decimals: 8,
                fee: Nat::from(10u64),
            },
        );
        let request_ok = make_request(
            p("2vxsx-fae"),
            ChainToken::Icp {
                ledger: second_ledger,
                symbol: "ICP".to_string(),
                decimals: 8,
                fee: Nat::from(10_000u64),
            },
        );

        let receipts = executor
            .process(&vec![request_fail.clone(), request_ok.clone()])
            .await
            .expect("executor process should succeed");
        assert_eq!(receipts.len(), 2);

        assert!(
            matches!(receipts[0].status, ExecutionStatus::Success),
            "first request should execute via liquidate_with_slippage call"
        );
        assert!(receipts[0].liquidation_result.is_some());

        assert!(
            matches!(receipts[1].status, ExecutionStatus::Success),
            "second request should continue and execute"
        );
        assert!(receipts[1].liquidation_result.is_some());
    }

    /// A pending change transfer must not prevent the accepted liquidation
    /// receipt from entering the durable handoff journal.
    #[tokio::test]
    async fn pending_change_still_accepts_the_liquidation_intent() {
        let lending_canister = p("nja4y-2yaaa-aaaae-qddxa-cai");

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(move |_, _, _| Ok(Ok(make_liquidation_result(1_556u128))));

        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents
            .expect_mark_accepted()
            .withf(|_, liquidation_id, receipt| liquidation_id == "1556" && !receipt.change_received)
            .times(1)
            .returning(|_, _, _| Ok(()));

        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            lending_canister,
            Arc::new(intents),
            Arc::new(ApprovalState::new()),
        );

        let request = make_request(
            p("2vxsx-fae"),
            ChainToken::Icp {
                ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
                symbol: "ICP".to_string(),
                decimals: 8,
                fee: Nat::from(10_000u64),
            },
        );

        let receipts = executor
            .process(&vec![request])
            .await
            .expect("executor process should succeed");

        assert_eq!(receipts.len(), 1);
        assert!(
            !receipts[0].change_received,
            "pending change must stay visible on the receipt"
        );
        assert!(
            matches!(receipts[0].status, ExecutionStatus::Success),
            "a pending change does not fail the liquidation, status was {:?}",
            receipts[0].status
        );
    }

    #[tokio::test]
    async fn intent_is_durable_before_the_liquidation_call_begins() {
        let persisted = Arc::new(AtomicBool::new(false));
        let store_flag = persisted.clone();
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(move |_, _| {
            store_flag.store(true, Ordering::SeqCst);
            Ok(())
        });
        intents.expect_mark_accepted().times(1).returning(|_, _, _| Ok(()));

        let call_flag = persisted.clone();
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(move |_, _, _| {
                assert!(
                    call_flag.load(Ordering::SeqCst),
                    "external call began before intent commit"
                );
                Ok(Ok(make_liquidation_result(77)))
            });

        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Arc::new(intents),
            Arc::new(ApprovalState::new()),
        );
        executor
            .process(&vec![make_request(
                p("2vxsx-fae"),
                ChainToken::Icp {
                    ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
                    symbol: "ICP".to_string(),
                    decimals: 8,
                    fee: Nat::from(10_000u64),
                },
            )])
            .await
            .expect("liquidation succeeds");
    }

    #[tokio::test]
    async fn intent_write_failure_prevents_the_external_call() {
        let mut intents = MockLiquidationIntentStore::new();
        intents
            .expect_create_submitting()
            .times(1)
            .returning(|_, _| Err(anyhow::anyhow!("disk unavailable")));
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(0);
        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Arc::new(intents),
            Arc::new(ApprovalState::new()),
        );

        let error = executor
            .process(&vec![make_request(
                p("2vxsx-fae"),
                ChainToken::Icp {
                    ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
                    symbol: "ICP".to_string(),
                    decimals: 8,
                    fee: Nat::from(10_000u64),
                },
            )])
            .await
            .expect_err("missing intent durability must stop submission");
        assert!(error.contains("failed to persist liquidation intent"));
    }

    #[tokio::test]
    async fn transport_failure_parks_the_intent_as_ambiguous() {
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents
            .expect_mark_ambiguous()
            .withf(|_, error| error == "response lost")
            .times(1)
            .returning(|_, _| Ok(()));
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Err("response lost".to_string()));
        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Arc::new(intents),
            Arc::new(ApprovalState::new()),
        );

        let receipts = executor
            .process(&vec![make_request(
                p("2vxsx-fae"),
                ChainToken::Icp {
                    ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
                    symbol: "ICP".to_string(),
                    decimals: 8,
                    fee: Nat::from(10_000u64),
                },
            )])
            .await
            .expect("ambiguous result remains observable");
        assert!(matches!(receipts[0].status, ExecutionStatus::LiquidationCallFailed(_)));
    }

    #[tokio::test]
    async fn canister_rejection_marks_a_definite_failure_without_handoff() {
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents
            .expect_mark_failed()
            .withf(|_, liquidation_id, receipt, error| {
                liquidation_id.is_none() && receipt.is_some() && error.contains("InsufficientCollateral")
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Ok(Err(ProtocolError::InsufficientCollateral)));
        let executor = BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Arc::new(intents),
            Arc::new(ApprovalState::new()),
        );

        let receipts = executor
            .process(&vec![make_request(
                p("2vxsx-fae"),
                ChainToken::Icp {
                    ledger: p("ryjl3-tyaaa-aaaaa-aaaba-cai"),
                    symbol: "ICP".to_string(),
                    decimals: 8,
                    fee: Nat::from(10_000u64),
                },
            )])
            .await
            .expect("definite rejection remains observable");
        assert!(matches!(receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
    }
}
