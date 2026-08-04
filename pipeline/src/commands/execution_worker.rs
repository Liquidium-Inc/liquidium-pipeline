use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use futures::{FutureExt, StreamExt, stream};
use ic_agent::Agent;
use liquidium_pipeline_core::types::protocol_types::TransferStatus;
use prettytable::{Cell, Row, Table, format};
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use crate::{
    finalizers::{
        liquidation_outcome::LiquidationOutcome, multi_venue::MultiVenueFinalizer,
        profit_calculator::SimpleProfitCalculator,
    },
    persistance::{
        LiqMetaWrapper, LiqResultRecord, LiquidationHandoff, LiquidationIntentStatus, LiquidationIntentStore,
        ResultStatus, WalProfitSnapshot, liquidation_intake::SqliteLiquidationIntentStore, sqlite::SqliteWalStore,
    },
    stage::PipelineStage,
    stages::{
        executor::{ExecutionReceipt, ExecutionStatus},
        export::ExportStage,
        finalize::FinalizeStage,
        settlement_watcher::SettlementWatcher,
    },
    utils::now_ts,
    wal::encode_meta,
    watchdog::{Watchdog, WatchdogEvent},
};

const EXECUTION_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);
const EXECUTION_WORKER_PANIC_DELAY: Duration = Duration::from_secs(1);
const INTAKE_BATCH_LIMIT: usize = 100;
const SETTLEMENT_STAGE_TIMEOUT: Duration = Duration::from_secs(120);
const FINALIZER_STAGE_TIMEOUT: Duration = Duration::from_secs(300);
const EXPORT_STAGE_TIMEOUT: Duration = Duration::from_secs(20);
const LIQUIDATION_NOTIFY_STAGE_TIMEOUT: Duration = Duration::from_secs(10);
const LIQUIDATION_NOTIFY_CONCURRENCY: usize = 4;

pub(crate) type RuntimeFinalizer = FinalizeStage<MultiVenueFinalizer, SqliteWalStore, SimpleProfitCalculator, Agent>;
pub(crate) type RuntimeSettlementWatcher = SettlementWatcher<Agent, SqliteWalStore>;

/// Starts venue execution on a dedicated OS thread with its own Tokio runtime.
///
/// Discovery and liquidation submission must remain responsive while a venue
/// performs slow network or consensus round trips. The WAL is the durable
/// handoff between those paths, so the worker needs no in-memory work queue.
pub(crate) fn spawn_execution_worker(
    finalizer: Arc<RuntimeFinalizer>,
    wal: Arc<SqliteWalStore>,
    intake: Arc<SqliteLiquidationIntentStore>,
    settlement: Arc<RuntimeSettlementWatcher>,
    exporter: Arc<ExportStage>,
    enabled_venues: Vec<String>,
    icpswap_mnemonic: Arc<str>,
    slack_watchdog: Option<Arc<dyn Watchdog>>,
    ui_enabled: bool,
) -> Result<JoinHandle<()>, String> {
    let runtime = build_execution_runtime()?;
    spawn_runtime_thread(
        runtime,
        run_execution_worker(
            finalizer,
            wal,
            intake,
            settlement,
            exporter,
            enabled_venues,
            icpswap_mnemonic,
            slack_watchdog,
            ui_enabled,
        ),
    )
}

fn build_execution_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("failed to initialize execution worker runtime: {error}"))
}

fn spawn_runtime_thread<F>(runtime: tokio::runtime::Runtime, worker: F) -> Result<JoinHandle<()>, String>
where
    F: Future<Output = ()> + Send + 'static,
{
    thread::Builder::new()
        .name("liquidation-execution".to_string())
        .spawn(move || runtime.block_on(worker))
        .map_err(|error| format!("failed to spawn execution worker thread: {error}"))
}

async fn run_execution_worker(
    finalizer: Arc<RuntimeFinalizer>,
    wal: Arc<SqliteWalStore>,
    intake: Arc<SqliteLiquidationIntentStore>,
    settlement: Arc<RuntimeSettlementWatcher>,
    exporter: Arc<ExportStage>,
    enabled_venues: Vec<String>,
    icpswap_mnemonic: Arc<str>,
    slack_watchdog: Option<Arc<dyn Watchdog>>,
    ui_enabled: bool,
) {
    loop {
        match crate::commands::liquidation_loop::park_unresumable_committed_rows(
            wal.as_ref(),
            &enabled_venues,
            &icpswap_mnemonic,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                error!("Execution WAL startup recovery failed; retrying: {error}");
                sleep(EXECUTION_WORKER_PANIC_DELAY).await;
            }
        }
    }

    loop {
        let cycle = run_execution_cycle(
            finalizer.as_ref(),
            wal.as_ref(),
            intake.as_ref(),
            settlement.as_ref(),
            exporter.as_ref(),
            slack_watchdog.as_ref(),
            ui_enabled,
        );

        if AssertUnwindSafe(cycle).catch_unwind().await.is_err() {
            error!("Execution worker cycle panicked; recovering and continuing");
            sleep(EXECUTION_WORKER_PANIC_DELAY).await;
            continue;
        }

        sleep(EXECUTION_WORKER_POLL_INTERVAL).await;
    }
}

async fn run_execution_cycle(
    finalizer: &RuntimeFinalizer,
    wal: &SqliteWalStore,
    intake: &SqliteLiquidationIntentStore,
    settlement: &RuntimeSettlementWatcher,
    exporter: &ExportStage,
    slack_watchdog: Option<&Arc<dyn Watchdog>>,
    ui_enabled: bool,
) {
    if let Err(error) = import_accepted_handoffs(intake, wal).await {
        error!("Execution worker intake import failed: {error}");
    }

    match timeout(SETTLEMENT_STAGE_TIMEOUT, settlement.tick()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!("Execution worker settlement sweep failed: {error}"),
        Err(_) => error!(
            timeout_ms = SETTLEMENT_STAGE_TIMEOUT.as_millis() as u64,
            "Execution worker settlement sweep timed out"
        ),
    }

    let outcomes = match timeout(FINALIZER_STAGE_TIMEOUT, finalizer.process(&())).await {
        Ok(Ok(results)) => results,
        Ok(Err(error)) => {
            error!("Execution worker finalizer failed: {error}");
            return;
        }
        Err(_) => {
            error!(
                timeout_ms = FINALIZER_STAGE_TIMEOUT.as_millis() as u64,
                "Execution worker finalizer timed out"
            );
            return;
        }
    };

    if outcomes.is_empty() {
        return;
    }

    match timeout(EXPORT_STAGE_TIMEOUT, exporter.process(&outcomes)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!("Failed to export execution results: {error}"),
        Err(_) => error!(
            timeout_ms = EXPORT_STAGE_TIMEOUT.as_millis() as u64,
            "Execution result export timed out"
        ),
    }

    log_execution_results(&outcomes);

    if timeout(
        LIQUIDATION_NOTIFY_STAGE_TIMEOUT,
        notify_liquidation_outcomes(slack_watchdog, &outcomes),
    )
    .await
    .is_err()
    {
        error!(
            timeout_ms = LIQUIDATION_NOTIFY_STAGE_TIMEOUT.as_millis() as u64,
            "Execution outcome notifications timed out"
        );
    }

    if ui_enabled {
        print_execution_results(outcomes);
    }
}

async fn import_accepted_handoffs(
    intake: &SqliteLiquidationIntentStore,
    wal: &SqliteWalStore,
) -> Result<usize, String> {
    let mut cursor = wal
        .intake_import_sequence()
        .map_err(|error| format!("read WAL intake cursor: {error}"))?;
    let mut imported = 0usize;

    loop {
        let handoffs = intake
            .list_handoffs_after(cursor, INTAKE_BATCH_LIMIT)
            .await
            .map_err(|error| format!("read accepted liquidation handoffs after {cursor}: {error}"))?;
        if handoffs.is_empty() {
            break;
        }
        let page_len = handoffs.len();
        for handoff in handoffs {
            let sequence = handoff.sequence;
            let row = wal_row_from_handoff(&handoff);
            if wal
                .import_handoff(sequence, row)
                .await
                .map_err(|error| format!("import liquidation handoff {sequence}: {error}"))?
            {
                imported += 1;
            }
            cursor = sequence;
        }
        if page_len < INTAKE_BATCH_LIMIT {
            break;
        }
    }

    if imported != 0 {
        info!(imported, cursor, "Imported accepted liquidations into execution WAL");
    }
    Ok(imported)
}

fn wal_row_from_handoff(handoff: &LiquidationHandoff) -> LiqResultRecord {
    match decode_handoff(handoff) {
        Ok((receipt, status)) => {
            let expected_profit_raw = receipt.request.expected_profit.to_string();
            let debt_symbol = receipt.request.debt_asset.symbol().to_string();
            let debt_decimals = receipt.request.debt_asset.decimals();
            let mut row = LiqResultRecord {
                id: handoff
                    .intent
                    .liquidation_id
                    .clone()
                    .expect("validated accepted handoff has liquidation id"),
                status,
                attempt: 0,
                error_count: 0,
                last_error: None,
                created_at: handoff.intent.created_at,
                updated_at: now_ts(),
                meta_json: "{}".to_string(),
            };
            let wrapper = LiqMetaWrapper {
                receipt,
                meta: Vec::new(),
                finalizer_decision: None,
                profit_snapshot: Some(WalProfitSnapshot {
                    expected_profit_raw,
                    realized_profit_raw: None,
                    debt_symbol,
                    debt_decimals,
                    updated_at: now_ts(),
                }),
                venue_execution: None,
                meta_v2: None,
            };
            if let Err(error) = encode_meta(&mut row, &wrapper) {
                return unresumable_handoff_row(handoff, format!("encode WAL metadata: {error}"));
            }
            row
        }
        Err(error) => unresumable_handoff_row(handoff, error),
    }
}

fn decode_handoff(handoff: &LiquidationHandoff) -> Result<(ExecutionReceipt, ResultStatus), String> {
    if handoff.intent.status != LiquidationIntentStatus::Accepted {
        return Err(format!(
            "handoff references intent in {:?} state",
            handoff.intent.status
        ));
    }
    let expected_id = handoff
        .intent
        .liquidation_id
        .as_deref()
        .ok_or_else(|| "accepted handoff has no liquidation id".to_string())?;
    let request = serde_json::from_str(&handoff.intent.request_json)
        .map_err(|error| format!("decode liquidation request: {error}"))?;
    let mut receipt: ExecutionReceipt = serde_json::from_str(
        handoff
            .intent
            .receipt_json
            .as_deref()
            .ok_or_else(|| "accepted handoff has no receipt".to_string())?,
    )
    .map_err(|error| format!("decode liquidation receipt: {error}"))?;
    let liquidation = receipt
        .liquidation_result
        .as_ref()
        .ok_or_else(|| "accepted receipt has no liquidation result".to_string())?;
    if liquidation.id.to_string() != expected_id {
        return Err(format!(
            "accepted receipt liquidation id {} does not match intent id {expected_id}",
            liquidation.id
        ));
    }
    receipt.request = request;
    let status = match liquidation.collateral_tx.status {
        TransferStatus::Success if receipt.request.swap_args.is_none() => ResultStatus::Succeeded,
        TransferStatus::Success => ResultStatus::Enqueued,
        TransferStatus::Pending | TransferStatus::Failed(_) => ResultStatus::WaitingCollateral,
    };
    Ok((receipt, status))
}

fn unresumable_handoff_row(handoff: &LiquidationHandoff, error: String) -> LiqResultRecord {
    let id = handoff
        .intent
        .liquidation_id
        .clone()
        .unwrap_or_else(|| format!("intake-{}", handoff.intent.intent_id));
    error!(
        handoff_sequence = handoff.sequence,
        intent_id = %handoff.intent.intent_id,
        liquidation_id = %id,
        reason = %error,
        "Accepted liquidation handoff is unresumable"
    );
    LiqResultRecord {
        id,
        status: ResultStatus::Unresumable,
        attempt: 0,
        error_count: 1,
        last_error: Some(format!("intake handoff {} is invalid: {error}", handoff.sequence)),
        created_at: handoff.intent.created_at,
        updated_at: now_ts(),
        meta_json: "{}".to_string(),
    }
}

/// Structured per-outcome logs consumed by journald/OTEL and auditor tooling.
fn log_execution_results(results: &[LiquidationOutcome]) {
    let success_count = results
        .iter()
        .filter(|result| matches!(result.status, ExecutionStatus::Success))
        .count();

    info!(
        outcome_count = results.len(),
        success_count,
        failed_count = results.len() - success_count,
        "Liquidation outcomes finalized"
    );

    for result in results {
        let liquidation_id = result
            .execution_receipt
            .liquidation_result
            .as_ref()
            .map(|liquidation| liquidation.id.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let swap_status = result
            .finalizer_result
            .swap_result
            .as_ref()
            .map(|swap| swap.status.clone())
            .unwrap_or_else(|| "none".to_string());
        let status = result.status.description();

        info!(
            event = "liquidation_outcome",
            liquidation_id = %liquidation_id,
            borrower = %result.request.liquidation.borrower.to_text(),
            debt_asset = %result.request.debt_asset.symbol(),
            collateral_asset = %result.request.collateral_asset.symbol(),
            debt_repaid = %result.formatted_debt_repaid(),
            collateral_received = %result.formatted_received_collateral(),
            swap_output = %result.formatted_swap_output(),
            swapper = %result.formatted_swapper(),
            swap_status = %swap_status,
            status = %status,
            expected_profit = result.expected_profit,
            realized_profit = result.realized_profit,
            profit_delta = result.realized_profit - result.expected_profit,
            round_trip_secs = result.round_trip_secs.unwrap_or(-1),
            "Liquidation outcome"
        );
    }
}

async fn notify_liquidation_outcomes(slack_watchdog: Option<&Arc<dyn Watchdog>>, outcomes: &[LiquidationOutcome]) {
    let Some(watchdog) = slack_watchdog else {
        return;
    };

    stream::iter(outcomes)
        .for_each_concurrent(LIQUIDATION_NOTIFY_CONCURRENCY, |outcome| async move {
            watchdog.notify(liquidation_finalized_event(outcome)).await;
        })
        .await;
}

fn liquidation_finalized_event(outcome: &LiquidationOutcome) -> WatchdogEvent<'static> {
    let liquidation_id = outcome
        .execution_receipt
        .liquidation_result
        .as_ref()
        .map(|liquidation| liquidation.id.to_string())
        .unwrap_or_else(|| "n/a".to_string());

    WatchdogEvent::LiquidationFinalized {
        liquidation_id,
        borrower: outcome.request.liquidation.borrower.to_text(),
        debt_asset: outcome.request.debt_asset.symbol(),
        collateral_asset: outcome.request.collateral_asset.symbol(),
        status: outcome.status.description(),
        debt_repaid: outcome.formatted_debt_repaid(),
        collateral_received: outcome.formatted_received_collateral(),
        swap_output: outcome.formatted_swap_output(),
        swapper: outcome.formatted_swapper(),
        expected_profit: outcome.formatted_expected_profit(),
        realized_profit: outcome.formatted_realized_profit(),
        profit_delta: outcome.formatted_profit_delta(),
        round_trip_secs: outcome.formatted_round_trip_secs(),
    }
}

fn print_execution_results(results: Vec<LiquidationOutcome>) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![
        Cell::new("Realized (Δ)"),
        Cell::new("Expected"),
        Cell::new("Debt Repaid"),
        Cell::new("Collateral"),
        Cell::new("Swap Output"),
        Cell::new("Swap Status"),
        Cell::new("Swapper"),
        Cell::new("Round Trip (s)"),
        Cell::new("Status"),
    ]));

    for result in results {
        let debt = result.formatted_debt_repaid();
        let collateral = result.formatted_received_collateral();
        let (received, swap_status) = match &result.finalizer_result.swap_result {
            Some(swap) => (result.formatted_swap_output(), swap.status.clone()),
            None => ("-".to_string(), "-".to_string()),
        };

        let delta = result.realized_profit - result.expected_profit;
        let delta_cell = {
            let text = format!(
                "{} ({})",
                result.formatted_realized_profit(),
                result.formatted_profit_delta()
            );
            match delta.cmp(&0) {
                std::cmp::Ordering::Greater => Cell::new(&text).style_spec("Fg"),
                std::cmp::Ordering::Less => Cell::new(&text).style_spec("Fr"),
                std::cmp::Ordering::Equal => Cell::new(&text),
            }
        };

        let status_text = result.status.description();
        let status_cell = match &result.status {
            ExecutionStatus::Success => Cell::new(&status_text).style_spec("Fg"),
            _ => Cell::new(&status_text).style_spec("Fr"),
        };

        table.add_row(Row::new(vec![
            delta_cell,
            Cell::new(&result.formatted_expected_profit()),
            Cell::new(&debt),
            Cell::new(&collateral),
            Cell::new(&received),
            Cell::new(&swap_status),
            Cell::new(&result.formatted_swapper()),
            Cell::new(&result.formatted_round_trip_secs()),
            status_cell,
        ]));
    }

    table.printstd();
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread};

    use candid::{Nat, Principal};
    use liquidium_pipeline_core::{
        tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
        types::protocol_types::{
            AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
            TxStatus,
        },
    };

    use crate::{
        executors::executor::ExecutorRequest,
        persistance::{LiquidationHandoff, LiquidationIntentStore, ResultStatus, WalStore},
        stages::executor::{ExecutionReceipt, ExecutionStatus},
        swappers::model::SwapRequest,
    };

    use super::{
        SqliteLiquidationIntentStore, SqliteWalStore, build_execution_runtime, import_accepted_handoffs,
        spawn_runtime_thread, wal_row_from_handoff,
    };

    #[test]
    fn execution_runtime_runs_on_the_named_os_thread() {
        let runtime = build_execution_runtime().expect("execution runtime");
        let (sender, receiver) = mpsc::channel();
        let worker = spawn_runtime_thread(runtime, async move {
            tokio::task::yield_now().await;
            sender
                .send(thread::current().name().map(str::to_owned))
                .expect("send thread name");
        })
        .expect("spawn execution thread");

        assert_eq!(
            receiver.recv().expect("receive thread name").as_deref(),
            Some("liquidation-execution")
        );
        worker.join().expect("execution thread should exit cleanly");
    }

    fn request(with_swap: bool) -> ExecutorRequest {
        let token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        };
        ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::anonymous(),
                collateral_pool_id: Principal::anonymous(),
                debt_amount: Nat::from(100_000u64),
                receiver_address: Principal::anonymous(),
                buy_bad_debt: false,
            },
            swap_args: with_swap.then(|| SwapRequest {
                pay_asset: token.asset_id(),
                pay_amount: ChainTokenAmount::from_raw(token.clone(), Nat::from(100_000u64)),
                receive_asset: token.asset_id(),
                receive_address: None,
                max_slippage_bps: None,
                venue_hint: None,
            }),
            debt_asset: token.clone(),
            collateral_asset: token,
            expected_profit: 1,
            ref_price: Nat::from(1u8),
            debt_ref_price: Nat::from(1u8),
            ref_price_at: 1,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(90_000u64),
        }
    }

    fn receipt(id: u128, collateral_status: TransferStatus, with_swap: bool) -> ExecutionReceipt {
        let status = if matches!(collateral_status, TransferStatus::Success) {
            ExecutionStatus::Success
        } else {
            ExecutionStatus::CollateralTransferFailed("waiting".to_string())
        };
        ExecutionReceipt {
            request: request(with_swap),
            liquidation_result: Some(LiquidationResult {
                amounts: LiquidationAmounts {
                    collateral_received: Nat::from(100_000u64),
                    debt_repaid: Nat::from(90_000u64),
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
                    status: collateral_status,
                },
                id,
            }),
            status,
            change_received: true,
        }
    }

    #[tokio::test]
    async fn importer_maps_accepted_receipts_and_advances_the_cursor() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let intake_path = temp.path().join("liquidations.db");
        let wal_path = temp.path().join("wal.db");
        let intake_writer = SqliteLiquidationIntentStore::new(intake_path.to_str().unwrap()).expect("intake writer");

        for (intent_id, liquidation_id, receipt) in [
            ("swap-ready", "1", receipt(1, TransferStatus::Success, true)),
            ("waiting", "2", receipt(2, TransferStatus::Pending, true)),
            ("no-swap", "3", receipt(3, TransferStatus::Success, false)),
        ] {
            intake_writer
                .create_submitting(intent_id, &receipt.request)
                .await
                .expect("create intent");
            intake_writer
                .mark_accepted(intent_id, liquidation_id, &receipt)
                .await
                .expect("accept intent");
        }

        let intake_reader =
            SqliteLiquidationIntentStore::new_read_only_with_busy_timeout(intake_path.to_str().unwrap(), 5_000)
                .expect("intake reader");
        let wal = SqliteWalStore::new(wal_path.to_str().unwrap()).expect("WAL");
        assert_eq!(import_accepted_handoffs(&intake_reader, &wal).await.unwrap(), 3);
        assert_eq!(wal.intake_import_sequence().unwrap(), 3);
        assert_eq!(
            wal.get_result("1").await.unwrap().unwrap().status,
            ResultStatus::Enqueued
        );
        assert_eq!(
            wal.get_result("2").await.unwrap().unwrap().status,
            ResultStatus::WaitingCollateral
        );
        assert_eq!(
            wal.get_result("3").await.unwrap().unwrap().status,
            ResultStatus::Succeeded
        );
        assert_eq!(import_accepted_handoffs(&intake_reader, &wal).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn malformed_accepted_handoff_is_parked_as_unresumable() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let store = SqliteLiquidationIntentStore::new(temp.path().to_str().unwrap()).expect("intake store");
        let receipt = receipt(9, TransferStatus::Success, true);
        store.create_submitting("bad", &receipt.request).await.unwrap();
        store.mark_accepted("bad", "9", &receipt).await.unwrap();
        let mut intent = store.get_intent("bad").await.unwrap().unwrap();
        intent.receipt_json = Some("not-json".to_string());

        let row = wal_row_from_handoff(&LiquidationHandoff { sequence: 1, intent });
        assert_eq!(row.id, "9");
        assert_eq!(row.status, ResultStatus::Unresumable);
        assert!(row.last_error.unwrap().contains("decode liquidation receipt"));
    }
}
