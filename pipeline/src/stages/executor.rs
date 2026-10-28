use async_trait::async_trait;
use candid::Encode;

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use tracing::{debug, info, warn};

use crate::{
    executors::{basic::basic_executor::BasicExecutor, executor::ExecutorRequest},
    liquidation::lending_queries::get_liquidation_inflow,
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
    /// Whether the change transfer had settled at the last read of the
    /// liquidation. For display only; no retry or recovery reads it.
    pub change_received: bool,
}

impl ExecutionReceipt {
    /// Takes the pool's current view of the liquidation. A collateral transfer
    /// that has landed clears its earlier failure. Returns whether anything changed.
    pub fn apply_pool_view(&mut self, fresh: LiquidationResult) -> bool {
        let collateral_landed = matches!(fresh.collateral_tx.status, TransferStatus::Success);
        let change_received = matches!(fresh.change_tx.status, TransferStatus::Success);
        let mut changed = false;
        if self.change_received != change_received {
            self.change_received = change_received;
            changed = true;
        }
        if self.liquidation_result.as_ref() != Some(&fresh) {
            self.liquidation_result = Some(fresh);
            changed = true;
        }
        if collateral_landed && matches!(self.status, ExecutionStatus::CollateralTransferFailed(_)) {
            self.status = ExecutionStatus::Success;
            changed = true;
        }
        changed
    }
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
                let summary = request_summary(&executor_request);

                info!("[executor] ⚡ liquidation req | {summary}");

                let args =
                    Encode!(&liq_req, &executor_request.min_collateral_amount).map_err(|e| e.to_string())?;
                // Every call gets a new intent, also for a borrower whose last call lost its answer
                // (audit L02). A repeat is a valid liquidation, or the pool rejects it and refunds the debt.
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
                            .mark_ambiguous(&intent_id, &err, None)
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
                        warn!("[executor] liquidate_with_slippage rejected by canister | {summary} | err={err:?}");
                        let error = format!("{:?}", err);
                        receipt.status = ExecutionStatus::FailedLiquidation(error.clone());
                        return self.save_failed(&intent_id, None, receipt, &error).await;
                    }
                };

                receipt.liquidation_result = Some(liq.clone());
                if let LiquidationStatus::FailedLiquidation(err) = &liq.status {
                    warn!(
                        "[executor] liquidate_with_slippage returned FailedLiquidation | liq_id={} | {summary} | err={err}",
                        liq.id
                    );
                    receipt.status = ExecutionStatus::FailedLiquidation(err.clone());
                    return self.save_failed(&intent_id, Some(liq.id.to_string()), receipt, err).await;
                }

                // A missing change must not stop the receipt: the WAL is the only handoff of the
                // collateral to the finalizer. The batch end reports the unpaid change (audit L04).
                receipt.change_received = matches!(liq.change_tx.status, TransferStatus::Success);

                let collateral_error = match &liq.collateral_tx.status {
                    TransferStatus::Success => None,
                    TransferStatus::Pending => Some("collateral pending".to_string()),
                    TransferStatus::Failed(err) => Some(err.clone()),
                };
                if let Some(error) = collateral_error {
                    info!(
                        "[executor] 🧱 collateral_tx status={:?} liq_id={}",
                        liq.collateral_tx.status, liq.id
                    );
                    receipt.status = ExecutionStatus::CollateralTransferFailed(error);
                }

                debug!("Executed liquidation {:?}", liq);
                persist_accepted_receipt(self.intents.as_ref(), &intent_id, &liq.id.to_string(), &receipt).await?;

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

        for liquidation in liquidations
            .iter()
            .filter_map(|receipt| receipt.liquidation_result.as_ref())
        {
            self.report_unpaid_change_or_refund(liquidation).await;
        }

        Ok(liquidations)
    }
}

impl<A: PipelineAgent, D: LiquidationIntentStore> BasicExecutor<A, D> {
    /// Saves a rejected liquidation on its intent and returns its receipt.
    async fn save_failed(
        &self,
        intent_id: &str,
        liquidation_id: Option<String>,
        receipt: ExecutionReceipt,
        error: &str,
    ) -> Result<ExecutionReceipt, String> {
        self.intents
            .mark_failed(intent_id, liquidation_id, Some(receipt.clone()), error)
            .await
            .map_err(|store_error| format!("failed to mark liquidation intent {intent_id} failed: {store_error}"))?;
        Ok(receipt)
    }

    /// Logs a liquidation whose change or refund has not landed yet (audit L04).
    /// The pool retries a temporary failure for about nine hours, and only a
    /// pool admin can recover a payment that never lands, so the bot only reports it.
    async fn report_unpaid_change_or_refund(&self, liquidation: &LiquidationResult) {
        if matches!(liquidation.change_tx.status, TransferStatus::Success) {
            return;
        }
        // The pool keeps this record until it owes nothing. A failed deposit
        // that moved no money has none, so it needs no report.
        match get_liquidation_inflow(self.agent.as_ref(), &self.lending_canister, liquidation.id).await {
            Ok(None) => {}
            Ok(Some(inflow)) => tracing::error!(
                liquidation_id = %liquidation.id,
                debt_amount = %inflow.debt_amount,
                change_tx = ?liquidation.change_tx.status,
                "[executor] the pool has not returned the change or refund yet; it retries a temporary failure for about nine hours, and if the money never lands a pool admin must recover this liquidation"
            ),
            Err(error) => tracing::error!(
                liquidation_id = %liquidation.id,
                change_tx = ?liquidation.change_tx.status,
                "[executor] could not read what the pool still owes for this liquidation: {error}"
            ),
        }
    }
}

fn request_summary(request: &ExecutorRequest) -> String {
    let liquidation = &request.liquidation;
    format!(
        "borrower={} | debt_pool={} | collateral_pool={} | debt={} | bad_debt={} | min_collateral_amount={}",
        liquidation.borrower.to_text(),
        liquidation.debt_pool_id.to_text(),
        liquidation.collateral_pool_id.to_text(),
        liquidation.debt_amount,
        liquidation.buy_bad_debt,
        request.min_collateral_amount
    )
}

async fn persist_accepted_receipt<D: LiquidationIntentStore>(
    intents: &D,
    intent_id: &str,
    liquidation_id: &str,
    receipt: &ExecutionReceipt,
) -> Result<(), String> {
    // The chain has already moved funds. Retry only the atomic local write,
    // never the liquidation call. Keep retries bounded within the cycle timeout.
    let mut last_error = String::new();
    for attempt in 0..3 {
        match intents.mark_accepted(intent_id, liquidation_id, receipt).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    let mut details = format!(
        "liquidation {liquidation_id} succeeded on-chain but intent {intent_id} could not be accepted: {last_error}; operator reconciliation required; do not resubmit"
    );
    // Preserve the successful receipt separately from the failed execution-row
    // insert. Ambiguous intents are never picked up for automatic resubmission.
    if let Err(error) = intents.mark_ambiguous(intent_id, &details, Some(receipt.clone())).await {
        details.push_str(&format!("; could not persist ambiguous state: {error}"));
    }
    tracing::error!(intent_id, liquidation_id, receipt = ?receipt, "{details}");
    Err(details)
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

    #[tokio::test]
    async fn executor_calls_liquidate_with_slippage_without_allowance_preflight() {
        let lending_canister = lending_canister();
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

        let executor = test_executor(agent, accepting_intent_store(2));

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
        let lending_canister = lending_canister();

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(move |_, _, _| {
                let mut result = make_liquidation_result(1_556u128);
                result.change_tx.status = TransferStatus::Pending;
                Ok(Ok(result))
            });
        agent
            .expect_call_query::<Option<LiquidationInflowRecord>>()
            .withf(move |canister, method, arg| {
                *canister == lending_canister
                    && method == "get_liquidation_inflow"
                    && Decode!(arg, u128).is_ok_and(|id| id == 1_556)
            })
            .times(1)
            .returning(|_, _, _| Ok(Some(inflow_record())));

        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents
            .expect_mark_accepted()
            .withf(|_, liquidation_id, receipt| liquidation_id == "1556" && !receipt.change_received)
            .times(1)
            .returning(|_, _, _| Ok(()));

        let executor = test_executor(agent, intents);

        let request = icp_request();

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

    /// A failed deposit that moved no money leaves no pool record, so the
    /// executor asks once and reports nothing.
    #[tokio::test]
    async fn a_failed_liquidation_asks_the_pool_what_it_still_owes() {
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents.expect_mark_failed().times(1).returning(|_, _, _, _| Ok(()));
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| {
                let mut result = make_liquidation_result(9);
                result.status = LiquidationStatus::FailedLiquidation("deposit underlying failed".to_string());
                result.change_tx.status = TransferStatus::Pending;
                Ok(Ok(result))
            });
        agent
            .expect_call_query::<Option<LiquidationInflowRecord>>()
            .withf(|_, method, arg| method == "get_liquidation_inflow" && Decode!(arg, u128).is_ok_and(|id| id == 9))
            .times(1)
            .returning(|_, _, _| Ok(None));
        let executor = test_executor(agent, intents);

        let receipts = executor
            .process(&vec![icp_request()])
            .await
            .expect("a failed liquidation is a receipt, not an error");
        assert!(matches!(receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
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

        let executor = test_executor(agent, intents);
        executor
            .process(&vec![icp_request()])
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
        let executor = test_executor(agent, intents);

        let error = executor
            .process(&vec![icp_request()])
            .await
            .expect_err("missing intent durability must stop submission");
        assert!(error.contains("failed to persist liquidation intent"));
    }

    #[tokio::test]
    async fn acceptance_failures_retry_only_persistence_and_preserve_the_receipt() {
        for recover in [true, false] {
            let mut intents = MockLiquidationIntentStore::new();
            intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
            let mut sequence = mockall::Sequence::new();
            intents.expect_mark_accepted()
                .times(if recover { 1 } else { 3 })
                .in_sequence(&mut sequence)
                .returning(|_, _, _| Err(anyhow::anyhow!("database locked")));
            if recover {
                intents.expect_mark_accepted().times(1).in_sequence(&mut sequence).returning(|_, _, _| Ok(()));
            } else {
                intents.expect_mark_ambiguous().times(1).in_sequence(&mut sequence)
                    .returning(|_, _, receipt| {
                        let receipt = receipt.expect("retain the successful chain receipt");
                        assert_eq!(receipt.liquidation_result.unwrap().id, Nat::from(77u64));
                        Ok(())
                    });
            }
            let mut agent = MockPipelineAgent::new();
            agent.expect_call_update::<Result<LiquidationResult, ProtocolError>>()
                .times(1)
                .returning(|_, _, _| Ok(Ok(make_liquidation_result(77))));
            let executor = test_executor(agent, intents);
            let result = executor.process(&vec![icp_request()]).await;
            if recover {
                assert!(result.is_ok());
            } else {
                assert!(result.unwrap_err().contains("operator reconciliation required"));
            }
        }
    }

    #[tokio::test]
    async fn transport_failure_parks_the_intent_as_ambiguous() {
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents
            .expect_mark_ambiguous()
            .withf(|_, error, receipt| error == "response lost" && receipt.is_none())
            .times(1)
            .returning(|_, _, _| Ok(()));
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Err("response lost".to_string()));
        let executor = test_executor(agent, intents);

        let receipts = executor
            .process(&vec![icp_request()])
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
        let executor = test_executor(agent, intents);

        let receipts = executor
            .process(&vec![icp_request()])
            .await
            .expect("definite rejection remains observable");
        assert!(matches!(receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
    }
}
